import os
import json
import logging
import random
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

import requests
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler, CommandHandler, Updater

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)

# ─── Version ──────────────────────────────────────────────────────────────────
BOT_VERSION = "Kurator 📀 Music Discovery Engine (v3.1.0)"

LASTFM_USER    = "burbq"
LASTFM_API     = os.environ["LASTFM_API_KEY"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
DISCOGS_TOKEN  = os.environ["DISCOGS_TOKEN"]

SCROBBLE_LIMIT     = 600
SEED_ARTISTS       = 25
SIMILAR_EXPANSION  = 40
PLAYLIST_SIZE      = 30
RARE_MAX_LISTENERS = 500_000
TAGS_PAGE_SIZE     = 24

HISTORY_FILE   = "history.json"
TAG_INDEX_FILE = "tag_index.json"
SCENE_FILE     = "scene_memory.json"

CACHE_TTL = 600  # seconds

# ─── Simple in-memory cache ───────────────────────────────────────────────────
_cache = {}

def cache_get(key):
    entry = _cache.get(key)
    if entry and time.time() - entry["ts"] < CACHE_TTL:
        return entry["value"]
    return None

def cache_set(key, value):
    _cache[key] = {"value": value, "ts": time.time()}

# ─── JSON helpers ─────────────────────────────────────────────────────────────

def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return default

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f)

# ─── Persistent history (by track) ───────────────────────────────────────────

def load_history():
    d = load_json(HISTORY_FILE, {})
    return {"tracks": set(d.get("tracks", []))}

def save_history():
    save_json(HISTORY_FILE, {"tracks": list(history["tracks"])})

history = load_history()

# ─── Persistent tag_index + scene_memory ─────────────────────────────────────

tag_index = load_json(TAG_INDEX_FILE, {})

_scene_raw = load_json(SCENE_FILE, {})
scene_memory = {int(k): v for k, v in _scene_raw.items()}

def save_tag_index():
    save_json(TAG_INDEX_FILE, tag_index)

def save_scene_memory():
    save_json(SCENE_FILE, {str(k): v for k, v in scene_memory.items()})

# ─── Helpers ──────────────────────────────────────────────────────────────────

def spotify_url(track):
    return f"https://open.spotify.com/search/{quote(track)}"

def lastfm(method, **params):
    payload = {"method": method, "api_key": LASTFM_API, "format": "json", **params}
    try:
        r = requests.get("http://ws.audioscrobbler.com/2.0/", params=payload, timeout=10)
        return r.json()
    except Exception as e:
        log.error(f"Last.fm error ({method}): {e}")
        return {}

def normalize(name):
    return name.lower().strip()

def get_recent_tracks():
    cached = cache_get("recent_tracks")
    if cached is not None:
        return cached
    data = lastfm("user.getrecenttracks", user=LASTFM_USER, limit=SCROBBLE_LIMIT)
    tracks = data.get("recenttracks", {}).get("track", [])
    cache_set("recent_tracks", tracks)
    return tracks

def extract_seed_artists():
    counter = Counter()
    for t in get_recent_tracks():
        artist = t["artist"]["#text"]
        if artist:
            counter[artist] += 1
    return [a for a, _ in counter.most_common(SEED_ARTISTS)]

# ─── Artist graph expansion ───────────────────────────────────────────────────

def _fetch_similar_names(artist):
    """Returns list of similar artist names. No listener filter — API doesn't provide it here."""
    data = lastfm("artist.getsimilar", artist=artist, limit=SIMILAR_EXPANSION)
    return [s["name"] for s in data.get("similarartists", {}).get("artist", [])]

def _fetch_listeners(artist):
    """Returns (artist, listeners) using artist.getinfo — the only reliable source."""
    data = lastfm("artist.getinfo", artist=artist)
    try:
        listeners = int(data["artist"]["stats"]["listeners"])
    except (KeyError, ValueError):
        listeners = 0
    return artist, listeners

def expand_artist_graph(seed_artists):
    """Level-1 expansion: similar artists of seeds."""
    pool = set()
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(_fetch_similar_names, a) for a in seed_artists]
        for f in as_completed(futures):
            try:
                pool.update(f.result())
            except Exception as e:
                log.error(f"expand_artist_graph L1 error: {e}")
    return list(pool)

def expand_artist_graph_deep(seed_artists):
    """
    Level-2 expansion for /dig: similar of similar.
    Naturally surfaces less mainstream artists without needing listener data.
    """
    level1 = expand_artist_graph(seed_artists)
    sample = random.sample(level1, min(len(level1), 30))
    pool   = set(level1)
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(_fetch_similar_names, a) for a in sample]
        for f in as_completed(futures):
            try:
                pool.update(f.result())
            except Exception as e:
                log.error(f"expand_artist_graph L2 error: {e}")
    for s in seed_artists:
        pool.discard(s)
    return list(pool)

def expand_artist_graph_rare(seed_artists):
    """
    Level-1 expansion then listener filter via artist.getinfo.
    This is the correct way — getsimilar doesn't return listener counts.
    """
    candidates = expand_artist_graph(seed_artists)
    filtered   = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(_fetch_listeners, a) for a in candidates]
        for f in as_completed(futures):
            try:
                artist, listeners = f.result()
                if listeners < RARE_MAX_LISTENERS:
                    filtered.append(artist)
            except Exception as e:
                log.error(f"expand_artist_graph_rare error: {e}")
    return filtered

# ─── Track selection ──────────────────────────────────────────────────────────

def _fetch_top_track(artist):
    data = lastfm("artist.gettoptracks", artist=artist, limit=10)
    top  = data.get("toptracks", {}).get("track", [])
    random.shuffle(top)
    for t in top:
        key = f"{normalize(artist)}-{normalize(t['name'])}"
        if key not in history["tracks"]:
            return (artist, t["name"], key)
    return None

def select_tracks(artists):
    tracks     = []
    keys_added = set()
    random.shuffle(artists)
    candidates = artists[:PLAYLIST_SIZE * 4]

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(_fetch_top_track, a) for a in candidates]
        for f in as_completed(futures):
            if len(tracks) >= PLAYLIST_SIZE:
                break
            try:
                result = f.result()
                if result:
                    artist, track_name, key = result
                    if key not in history["tracks"] and key not in keys_added:
                        tracks.append(f"{artist} - {track_name}")
                        keys_added.add(key)
            except Exception as e:
                log.error(f"select_tracks error: {e}")

    history["tracks"].update(keys_added)
    save_history()
    return tracks

# ─── Spotify store ────────────────────────────────────────────────────────────

_spotify_store = {}

def _store_tracks(tracks):
    key = str(int(time.time()))[-6:]
    _spotify_store[key] = tracks
    return key

# ─── Playlist sender ──────────────────────────────────────────────────────────

def send_playlist(message, tracks, title="✦ Kurator's Pick", branded=True):
    """
    branded=True  → Kurator's editorial tone  (playlist, dig, rare)
    branded=False → neutral/explore tone      (trail, scene/build, tag search)
    """
    if not tracks:
        message.reply_text(
            f"{title}\n\n"
            "⚠️ No new tracks found.\n\n"
            "Your history may be exhausted. Use /reset to start fresh.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔄 Reset history", callback_data="cmd|reset"),
                InlineKeyboardButton("⬅ Menu",          callback_data="cmd|menu"),
            ]])
        )
        return

    track_list = "\n".join(tracks)
    key        = _store_tracks(tracks)
    subtitle   = "✦ Selected from Kurator's collection" if branded else "🔍 Open discovery"

    message.reply_text(
        f"{title} — {len(tracks)} tracks\n"
        f"{subtitle}\n\n"
        f"{track_list}\n\n"
        f"Import plain text at soundiiz.com",
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎧 Open tracks in Spotify", callback_data=f"spotify|{key}")],
            [InlineKeyboardButton("⬅ Main menu",               callback_data="cmd|menu")],
        ])
    )

# ─── Main menu — Kurator's Picks / Explore ────────────────────────────────────

def main_menu_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("── ✦ KURATOR'S PICKS ──────────────", callback_data="noop")],
        [InlineKeyboardButton("📀 Playlist — Kurator's selection",    callback_data="cmd|playlist")],
        [InlineKeyboardButton("🕳️ Dig — Deeper into Kurator's taste", callback_data="cmd|dig")],
        [InlineKeyboardButton("🧪 Rare — Kurator's hidden gems",      callback_data="cmd|rare")],
        [InlineKeyboardButton("── 🔍 EXPLORE ──────────────────────", callback_data="noop")],
        [InlineKeyboardButton("🔗 Trail — Follow an artist",           callback_data="cmd|trail_prompt")],
        [InlineKeyboardButton("🧠 Scene — Map styles via Discogs",     callback_data="cmd|scene_prompt")],
        [InlineKeyboardButton("🏷️ Tags — Browse collected genres",    callback_data="cmd|tags")],
        [
            InlineKeyboardButton("📊 Status", callback_data="cmd|status"),
            InlineKeyboardButton("🔄 Reset",  callback_data="cmd|reset"),
            InlineKeyboardButton("❓ Help",   callback_data="cmd|help"),
        ],
    ])

# ─── Help text ────────────────────────────────────────────────────────────────

def _help_text():
    return """❓ Help

✦ KURATOR'S PICKS
Selections drawn from Kurator's own listening history.

📀 /playlist
Hand-picked by Kurator from his collection.

🕳️ /dig
Deeper cuts — two degrees of separation from Kurator's taste.

🧪 /rare
Kurator's hidden gems — artists under 500K listeners.

🔍 EXPLORE
Open-ended discovery tools — not tied to Kurator's taste.

🔗 /trail <artist>
Follow artists similar to one you name.

🧠 /scene <artist>
Map styles and subgenres via Discogs.

🏷️ /tags
Browse all genres collected across /scene calls.

📀 /playlist <tag>
Top artists for any genre tag (e.g. /playlist jazz).

📊 /status — Your history and tag stats.
🔄 /reset  — Clear history and start fresh.

Kurator is built around taste, not algorithms.
Some responses may take a few seconds.

Be patient.
"""

# ─── Commands ─────────────────────────────────────────────────────────────────

def start(update, context):
    update.message.reply_text(
        f"{BOT_VERSION}\n\nTap a command to begin:",
        reply_markup=main_menu_markup()
    )

def help_command(update, context):
    update.message.reply_text(_help_text())

def playlist(update, context):
    msg = update.message
    if context.args:
        tag = " ".join(context.args)
        msg.reply_text(f"🔍 Searching top artists for \"{tag}\"…")
        data  = lastfm("tag.gettopartists", tag=tag, limit=50)
        names = [a["name"] for a in data.get("topartists", {}).get("artist", [])]
        send_playlist(msg, select_tracks(names), title=f"🔍 {tag}", branded=False)
    else:
        msg.reply_text("📀 Kurator is selecting tracks for you… ⏳")
        seeds = extract_seed_artists()
        msg.reply_text("🔍 Expanding the collection…")
        send_playlist(msg, select_tracks(expand_artist_graph(seeds)),
                      title="✦ Kurator's Pick", branded=True)

def dig(update, context):
    msg = update.message
    msg.reply_text("🕳️ Going deeper into Kurator's taste… ⏳")
    seeds = extract_seed_artists()
    msg.reply_text("🔍 Mapping two degrees of separation…")
    send_playlist(msg, select_tracks(expand_artist_graph_deep(seeds)),
                  title="✦ Kurator's Dig", branded=True)

def rare(update, context):
    msg = update.message
    msg.reply_text("🧪 Searching Kurator's hidden gems… ⏳\nThis may take a moment.")
    seeds = extract_seed_artists()
    send_playlist(msg, select_tracks(expand_artist_graph_rare(seeds)),
                  title="✦ Kurator's Rare", branded=True)

def trail(update, context):
    msg = update.message
    if not context.args:
        msg.reply_text("🔗 Trail\n\nType:\n/trail <artist>")
        return
    artist = " ".join(context.args)
    msg.reply_text(f"🔗 Following trail from {artist}…")
    data  = lastfm("artist.getsimilar", artist=artist, limit=60)
    names = [a["name"] for a in data.get("similarartists", {}).get("artist", [])]
    send_playlist(msg, select_tracks(names), title=f"🔗 {artist}", branded=False)

def scene(update, context):
    msg = update.message
    if not context.args:
        msg.reply_text("🧠 Scene\n\nType:\n/scene <artist>")
        return
    artist_query = " ".join(context.args)
    msg.reply_text("🧠 Mapping scene…")
    _render_scene(msg, artist_query, update.effective_chat.id)

def tags(update, context):
    _render_tags(update.message, page=0)

def status(update, context):
    _render_status(update.message)

def reset(update, context):
    _do_reset(update.message)

# ─── Scene renderer ───────────────────────────────────────────────────────────

def _render_scene(message, artist_query, chat_id):
    try:
        data = requests.get(
            "https://api.discogs.com/database/search",
            params={"artist": artist_query, "type": "release",
                    "per_page": 100, "token": DISCOGS_TOKEN},
            timeout=15
        ).json()
    except Exception as e:
        log.error(f"Discogs error: {e}")
        message.reply_text("⚠️ Discogs request failed. Try again.")
        return

    counter = {}
    for rel in data.get("results", []):
        for s in rel.get("style", []):
            counter[s] = counter.get(s, 0) + 1
            tag_index[s] = tag_index.get(s, 0) + 1
        for g in rel.get("genre", []):
            tag_index[g] = tag_index.get(g, 0) + 1

    save_tag_index()

    sorted_styles = sorted(counter.items(), key=lambda x: x[1], reverse=True)[:15]
    scene_memory[chat_id] = {"artist": artist_query, "styles": sorted_styles}
    save_scene_memory()

    if not sorted_styles:
        message.reply_text(f'🧠 No styles found for "{artist_query}".')
        return

    buttons = [
        [InlineKeyboardButton(f"{style}  ({count})", callback_data=f"scene_style|{style}")]
        for style, count in sorted_styles
    ]
    buttons.append([InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")])

    message.reply_text(
        f"🧠 {artist_query}\n\nChoose a style:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# ─── Tags renderer — paginated ────────────────────────────────────────────────

def _build_tags_buttons(sorted_tags, page):
    start     = page * TAGS_PAGE_SIZE
    end       = start + TAGS_PAGE_SIZE
    page_tags = sorted_tags[start:end]

    buttons = []
    row     = []
    for tag, count in page_tags:
        row.append(InlineKeyboardButton(f"{tag} ({count})", callback_data=f"scene_style|{tag}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀ Prev", callback_data=f"tags_page|{page - 1}"))
    if end < len(sorted_tags):
        nav.append(InlineKeyboardButton("Next ▶", callback_data=f"tags_page|{page + 1}"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")])
    return buttons

def _render_tags(message, page=0):
    if not tag_index:
        message.reply_text(
            "No tags collected yet.\n\nUse /scene <artist> first to start building your tag library."
        )
        return

    sorted_tags = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
    total_pages = max(1, (len(sorted_tags) - 1) // TAGS_PAGE_SIZE + 1)
    start       = page * TAGS_PAGE_SIZE
    page_tags   = sorted_tags[start:start + TAGS_PAGE_SIZE]
    tag_list    = "\n".join(f"• {tag}  ×{count}" for tag, count in page_tags)

    message.reply_text(
        f"🏷️ Tag Library — {len(sorted_tags)} genres (page {page + 1}/{total_pages})\n\n{tag_list}",
        reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page))
    )

# ─── Status + Reset ───────────────────────────────────────────────────────────

def _render_status(message):
    message.reply_text(
        f"📊 Kurator Status\n\n"
        f"🎵 Tracks in history: {len(history['tracks'])}\n"
        f"🏷️ Tags collected: {len(tag_index)}\n"
        f"🧠 Scene sessions: {len(scene_memory)}\n\n"
        f"Use /reset to clear history and start fresh.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Reset", callback_data="cmd|reset"),
            InlineKeyboardButton("⬅ Menu",  callback_data="cmd|menu"),
        ]])
    )

def _do_reset(message):
    history["tracks"].clear()
    save_history()
    message.reply_text(
        "🔄 History cleared!\n\nYou'll start seeing fresh artists and tracks again.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")
        ]])
    )

# ─── Callback router ──────────────────────────────────────────────────────────

def handle_buttons(update, context):
    query   = update.callback_query
    query.answer()

    chat_id = query.message.chat.id
    message = query.message
    parts   = query.data.split("|", 1)
    action  = parts[0]
    value   = parts[1] if len(parts) > 1 else ""

    # ── noop (section header buttons — do nothing) ─────────────────────────────
    if action == "noop":
        return

    # ── cmd actions ────────────────────────────────────────────────────────────
    elif action == "cmd":

        if value == "menu":
            query.edit_message_text(
                f"{BOT_VERSION}\n\nTap a command to begin:",
                reply_markup=main_menu_markup()
            )

        elif value == "playlist":
            query.edit_message_text("📀 Kurator is selecting tracks… ⏳")
            seeds = extract_seed_artists()
            send_playlist(message, select_tracks(expand_artist_graph(seeds)),
                          title="✦ Kurator's Pick", branded=True)

        elif value == "dig":
            query.edit_message_text("🕳️ Going deeper into Kurator's taste… ⏳")
            seeds = extract_seed_artists()
            send_playlist(message, select_tracks(expand_artist_graph_deep(seeds)),
                          title="✦ Kurator's Dig", branded=True)

        elif value == "rare":
            query.edit_message_text("🧪 Searching Kurator's hidden gems… ⏳")
            seeds = extract_seed_artists()
            send_playlist(message, select_tracks(expand_artist_graph_rare(seeds)),
                          title="✦ Kurator's Rare", branded=True)

        elif value == "trail_prompt":
            query.edit_message_text("🔗 Trail\n\nSend:\n/trail <artist>")

        elif value == "scene_prompt":
            query.edit_message_text("🧠 Scene\n\nSend:\n/scene <artist>")

        elif value == "tags":
            if not tag_index:
                query.edit_message_text(
                    "No tags yet.\n\nUse /scene <artist> first.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("⬅ Back", callback_data="cmd|menu")
                    ]])
                )
                return
            sorted_tags = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
            total_pages = max(1, (len(sorted_tags) - 1) // TAGS_PAGE_SIZE + 1)
            page_tags   = sorted_tags[:TAGS_PAGE_SIZE]
            tag_list    = "\n".join(f"• {tag}  ×{count}" for tag, count in page_tags)
            query.edit_message_text(
                f"🏷️ Tag Library — {len(sorted_tags)} genres (page 1/{total_pages})\n\n{tag_list}",
                reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, 0))
            )

        elif value == "status":
            query.edit_message_text(
                f"📊 Kurator Status\n\n"
                f"🎵 Tracks in history: {len(history['tracks'])}\n"
                f"🏷️ Tags collected: {len(tag_index)}\n"
                f"🧠 Scene sessions: {len(scene_memory)}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔄 Reset", callback_data="cmd|reset"),
                    InlineKeyboardButton("⬅ Menu",  callback_data="cmd|menu"),
                ]])
            )

        elif value == "reset":
            history["tracks"].clear()
            save_history()
            query.edit_message_text(
                "🔄 History cleared!\n\nYou'll start seeing fresh artists and tracks again.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")
                ]])
            )

        elif value == "help":
            query.edit_message_text(
                _help_text(),
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")
                ]])
            )

    # ── tags pagination ────────────────────────────────────────────────────────
    elif action == "tags_page":
        page        = int(value)
        sorted_tags = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
        total_pages = max(1, (len(sorted_tags) - 1) // TAGS_PAGE_SIZE + 1)
        start       = page * TAGS_PAGE_SIZE
        page_tags   = sorted_tags[start:start + TAGS_PAGE_SIZE]
        tag_list    = "\n".join(f"• {tag}  ×{count}" for tag, count in page_tags)
        query.edit_message_text(
            f"🏷️ Tag Library — {len(sorted_tags)} genres (page {page + 1}/{total_pages})\n\n{tag_list}",
            reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page))
        )

    # ── scene_style ───────────────────────────────────────────────────────────
    elif action == "scene_style":
        mem    = scene_memory.get(chat_id, {})
        artist = mem.get("artist", "")
        back_label = f"⬅ Back to {artist[:25]}" if artist else "⬅ Back"

        buttons = [
            [InlineKeyboardButton("✅ Generate playlist", callback_data=f"build|{value}")],
            [InlineKeyboardButton(back_label,             callback_data=f"scene_back|{chat_id}")],
            [InlineKeyboardButton("⬅ Main menu",          callback_data="cmd|menu")],
        ]
        query.edit_message_text(
            f"🎧 {value}\n\nGenerate a playlist for this style?",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    # ── scene_back ────────────────────────────────────────────────────────────
    elif action == "scene_back":
        mem    = scene_memory.get(chat_id, {})
        artist = mem.get("artist", "")
        styles = mem.get("styles", [])

        if not artist or not styles:
            query.edit_message_text(
                f"{BOT_VERSION}\n\nTap a command to begin:",
                reply_markup=main_menu_markup()
            )
            return

        buttons = [
            [InlineKeyboardButton(f"{style}  ({count})", callback_data=f"scene_style|{style}")]
            for style, count in styles
        ]
        buttons.append([InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")])
        query.edit_message_text(
            f"🧠 {artist}\n\nChoose a style:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    # ── spotify: expand individual track links ────────────────────────────────
    elif action == "spotify":
        tracks = _spotify_store.get(value, [])
        if not tracks:
            query.answer("Links expired. Generate a new playlist.", show_alert=True)
            return
        buttons = []
        for t in tracks:
            parts = t.split(" - ", 1)
            label = f"{parts[0][:22]} – {parts[1][:22]}" if len(parts) == 2 else t[:48]
            buttons.append([InlineKeyboardButton(label, url=spotify_url(t))])
        buttons.append([InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")])
        query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(buttons))

    # ── build: playlist from scene style (Explore) ────────────────────────────
    elif action == "build":
        query.edit_message_text(f"🔍 Building {value} playlist… ⏳")
        data  = lastfm("tag.gettopartists", tag=value, limit=50)
        names = [a["name"] for a in data.get("topartists", {}).get("artist", [])]
        send_playlist(message, select_tracks(names), title=f"🔍 {value}", branded=False)

# ─── Boot ─────────────────────────────────────────────────────────────────────

updater = Updater(TELEGRAM_TOKEN)
dp = updater.dispatcher

dp.add_handler(CommandHandler("start",    start))
dp.add_handler(CommandHandler("help",     help_command))
dp.add_handler(CommandHandler("playlist", playlist))
dp.add_handler(CommandHandler("scene",    scene))
dp.add_handler(CommandHandler("tags",     tags))
dp.add_handler(CommandHandler("dig",      dig))
dp.add_handler(CommandHandler("trail",    trail))
dp.add_handler(CommandHandler("rare",     rare))
dp.add_handler(CommandHandler("status",   status))
dp.add_handler(CommandHandler("reset",    reset))
dp.add_handler(CallbackQueryHandler(handle_buttons))

log.info(BOT_VERSION)
print(BOT_VERSION)
updater.start_polling()
updater.idle()
