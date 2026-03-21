import os
import json
import requests
import random
from collections import Counter
from urllib.parse import quote
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

BOT_VERSION = "Kurator 📀 Music Discovery Engine (v2.5.0)"

LASTFM_USER = "burbq"
LASTFM_API = os.environ["LASTFM_API_KEY"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]

SCROBBLE_LIMIT = 600
SEED_ARTISTS = 25
SIMILAR_EXPANSION = 40
PLAYLIST_SIZE = 30
RARE_MAX_LISTENERS = 500_000

HISTORY_FILE = "history.json"

# ─── Persistent history ────────────────────────────────────────────────────────

def load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE) as f:
                d = json.load(f)
                return {"artists": set(d.get("artists", [])), "tracks": set(d.get("tracks", []))}
        except Exception:
            pass
    return {"artists": set(), "tracks": set()}

def save_history():
    with open(HISTORY_FILE, "w") as f:
        json.dump({"artists": list(history["artists"]), "tracks": list(history["tracks"])}, f)

history = load_history()

# tag_index: tag -> count (persists across /scene calls)
tag_index = {}

# scene_memory: chat_id -> {"artist": str, "styles": [(style, count)]}
scene_memory = {}

# ─── Helpers ──────────────────────────────────────────────────────────────────

def spotify_url(track):
    return f"https://open.spotify.com/search/{quote(track)}"

def lastfm(method, **params):
    payload = {"method": method, "api_key": LASTFM_API, "format": "json", **params}
    try:
        r = requests.get("http://ws.audioscrobbler.com/2.0/", params=payload, timeout=10)
        return r.json()
    except Exception:
        return {}

def normalize(name):
    return name.lower().strip()

def get_recent_tracks():
    data = lastfm("user.getrecenttracks", user=LASTFM_USER, limit=SCROBBLE_LIMIT)
    return data.get("recenttracks", {}).get("track", [])

def extract_seed_artists():
    counter = Counter()
    for t in get_recent_tracks():
        artist = t["artist"]["#text"]
        if artist:
            counter[artist] += 1
    return [a for a, _ in counter.most_common(SEED_ARTISTS)]

def expand_artist_graph(seed_artists, max_listeners=None):
    pool = set()
    for artist in seed_artists:
        data = lastfm("artist.getsimilar", artist=artist, limit=SIMILAR_EXPANSION)
        for s in data.get("similarartists", {}).get("artist", []):
            listeners = int(s.get("listeners", 0))
            if max_listeners and listeners > max_listeners:
                continue
            pool.add(s["name"])
    return list(pool)

def select_tracks(artists):
    tracks = []
    random.shuffle(artists)
    for a in artists:
        if len(tracks) >= PLAYLIST_SIZE:
            break
        if normalize(a) in history["artists"]:
            continue
        data = lastfm("artist.gettoptracks", artist=a, limit=10)
        top = data.get("toptracks", {}).get("track", [])
        random.shuffle(top)
        for t in top:
            key = f"{normalize(a)}-{normalize(t['name'])}"
            if key in history["tracks"]:
                continue
            tracks.append(f"{a} - {t['name']}")
            history["artists"].add(normalize(a))
            history["tracks"].add(key)
            break
    save_history()
    return tracks

def send_playlist(message, tracks, title="📀 Playlist"):
    message.reply_text(f"{title} ({len(tracks)} tracks)\n\n" + "\n".join(tracks))
    export_text = """
━━━━━━━━━━━━━━━━━━━
🎧 Export options
━━━━━━━━━━━━━━━━━━━

🔹 Soundiiz (recommended)
1. Go to https://soundiiz.com
2. Import → Text
3. Paste this list
4. Export to your preferred platform

🔹 Spotify quick access
Tap any track below
"""
    buttons = [[InlineKeyboardButton(t[:50], url=spotify_url(t))] for t in tracks]
    message.reply_text(export_text, reply_markup=InlineKeyboardMarkup(buttons))

# ─── Main menu ────────────────────────────────────────────────────────────────

def main_menu_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📀 Playlist",              callback_data="cmd|playlist")],
        [InlineKeyboardButton("🕳️ Dig (deep discovery)", callback_data="cmd|dig")],
        [InlineKeyboardButton("🔗 Trail (artist)",        callback_data="cmd|trail_prompt")],
        [InlineKeyboardButton("🧠 Scene (artist)",        callback_data="cmd|scene_prompt")],
        [InlineKeyboardButton("🏷️ Tags (explore)",       callback_data="cmd|tags")],
        [InlineKeyboardButton("🧪 Rare (hidden artists)", callback_data="cmd|rare")],
        [InlineKeyboardButton("❓ Help",                  callback_data="cmd|help")],
    ])

# ─── Commands ─────────────────────────────────────────────────────────────────

def start(update, context):
    update.message.reply_text(
        f"{BOT_VERSION}\n\nTap a command to begin:",
        reply_markup=main_menu_markup()
    )

def help_command(update, context):
    update.message.reply_text("""❓ Help

📀 /playlist  
Curated from your Last.fm history

🕳️ /dig  
Deep discovery — favors less mainstream artists

🔗 /trail <artist>  
Explore artists similar to one you name

🧠 /scene <artist>  
Navigate styles and subgenres via Discogs

🏷️ /tags  
Browse all genres collected across /scene calls

🧪 /rare  
Artists under 500K listeners

Kurator is built around taste, not algorithms.
Some responses may take a few seconds — be patient.
""")

def playlist(update, context):
    msg = update.message
    if context.args:
        tag = " ".join(context.args)
        msg.reply_text(f"📀 Building {tag} playlist…")
        data = lastfm("tag.gettopartists", tag=tag, limit=50)
        names = [a["name"] for a in data.get("topartists", {}).get("artist", [])]
        send_playlist(msg, select_tracks(names), f"📀 {tag}")
    else:
        msg.reply_text("📀 Building playlist…")
        send_playlist(msg, select_tracks(expand_artist_graph(extract_seed_artists())))

def dig(update, context):
    msg = update.message
    msg.reply_text("🕳️ Digging deep…")
    send_playlist(msg, select_tracks(expand_artist_graph(extract_seed_artists())), "🕳️ Dig")

def rare(update, context):
    msg = update.message
    msg.reply_text("🧪 Searching rare artists…")
    send_playlist(
        msg,
        select_tracks(expand_artist_graph(extract_seed_artists(), max_listeners=RARE_MAX_LISTENERS)),
        "🧪 Rare"
    )

def trail(update, context):
    msg = update.message
    if not context.args:
        msg.reply_text("🔗 Trail\n\nType:\n/trail <artist>")
        return
    artist = " ".join(context.args)
    msg.reply_text("🔗 Following trail…")
    data = lastfm("artist.getsimilar", artist=artist, limit=60)
    names = [a["name"] for a in data.get("similarartists", {}).get("artist", [])]
    send_playlist(msg, select_tracks(names), f"🔗 {artist}")

def scene(update, context):
    msg = update.message
    if not context.args:
        msg.reply_text("🧠 Scene\n\nType:\n/scene <artist>")
        return
    artist_query = " ".join(context.args)
    msg.reply_text("🧠 Mapping scene…")
    _render_scene(msg, artist_query, update.effective_chat.id)

def tags(update, context):
    _render_tags(update.message)

# ─── Scene / Tags renderers (shared by commands + callbacks) ──────────────────

def _render_scene(message, artist_query, chat_id):
    try:
        data = requests.get(
            "https://api.discogs.com/database/search",
            params={"artist": artist_query, "type": "release", "per_page": 100, "token": DISCOGS_TOKEN},
            timeout=15
        ).json()
    except Exception:
        message.reply_text("⚠️ Discogs request failed. Try again.")
        return

    counter = {}
    for rel in data.get("results", []):
        for s in rel.get("style", []):
            counter[s] = counter.get(s, 0) + 1
            tag_index[s] = tag_index.get(s, 0) + 1
        for g in rel.get("genre", []):
            tag_index[g] = tag_index.get(g, 0) + 1

    sorted_styles = sorted(counter.items(), key=lambda x: x[1], reverse=True)[:15]
    scene_memory[chat_id] = {"artist": artist_query, "styles": sorted_styles}

    if not sorted_styles:
        message.reply_text(f"🧠 No styles found for "{artist_query}".")
        return

    buttons = [
        [InlineKeyboardButton(f"{style}  ({count})", callback_data=f"scene_style|{style}")]
        for style, count in sorted_styles
    ]
    buttons.append([InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")])

    message.reply_text(
        f"{BOT_VERSION}\n\n🧠 {artist_query}\n\nChoose a style:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

def _render_tags(message):
    if not tag_index:
        message.reply_text("No tags collected yet.\n\nUse /scene <artist> first to start building your tag library.")
        return

    sorted_tags = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)[:48]

    # 2-column button grid
    buttons = []
    row = []
    for tag, count in sorted_tags:
        row.append(InlineKeyboardButton(f"{tag} ({count})", callback_data=f"scene_style|{tag}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")])

    tag_list = "\n".join(f"• {tag}  ×{count}" for tag, count in sorted_tags)
    message.reply_text(
        f"🏷️ Tag Library — {len(sorted_tags)} genres\n\n{tag_list}",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# ─── Callback router ──────────────────────────────────────────────────────────

def handle_buttons(update, context):
    query = update.callback_query
    query.answer()

    chat_id = query.message.chat.id
    message = query.message          # use .reply_text() on this
    raw = query.data
    parts = raw.split("|", 1)
    action = parts[0]
    value = parts[1] if len(parts) > 1 else ""

    # ── cmd actions ────────────────────────────────────────────────────────────
    if action == "cmd":

        if value == "menu":
            query.edit_message_text(
                f"{BOT_VERSION}\n\nTap a command to begin:",
                reply_markup=main_menu_markup()
            )

        elif value == "playlist":
            query.edit_message_text("📀 Building playlist…")
            send_playlist(message, select_tracks(expand_artist_graph(extract_seed_artists())))

        elif value == "dig":
            query.edit_message_text("🕳️ Digging deep…")
            send_playlist(message, select_tracks(expand_artist_graph(extract_seed_artists())), "🕳️ Dig")

        elif value == "rare":
            query.edit_message_text("🧪 Searching rare artists…")
            send_playlist(
                message,
                select_tracks(expand_artist_graph(extract_seed_artists(), max_listeners=RARE_MAX_LISTENERS)),
                "🧪 Rare"
            )

        elif value == "trail_prompt":
            query.edit_message_text("🔗 Trail\n\nSend:\n/trail <artist>")

        elif value == "scene_prompt":
            query.edit_message_text("🧠 Scene\n\nSend:\n/scene <artist>")

        elif value == "tags":
            if not tag_index:
                query.edit_message_text(
                    "No tags yet.\n\nUse /scene <artist> first.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="cmd|menu")]])
                )
                return
            sorted_tags = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)[:48]
            buttons = []
            row = []
            for tag, count in sorted_tags:
                row.append(InlineKeyboardButton(f"{tag} ({count})", callback_data=f"scene_style|{tag}"))
                if len(row) == 2:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)
            buttons.append([InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")])
            tag_list = "\n".join(f"• {tag}  ×{count}" for tag, count in sorted_tags)
            query.edit_message_text(
                f"🏷️ Tag Library — {len(sorted_tags)} genres\n\n{tag_list}",
                reply_markup=InlineKeyboardMarkup(buttons)
            )

        elif value == "help":
            query.edit_message_text(
                """❓ Help

📀 Playlist — Curated from your Last.fm history
🕳️ Dig — Deep discovery, less mainstream
🔗 Trail — Artists similar to one you name
🧠 Scene — Styles and subgenres via Discogs
🏷️ Tags — All collected genres, clickable
🧪 Rare — Artists under 500K listeners

Some responses may take a few seconds.
""",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")]
                ])
            )

    # ── scene_style: show style detail + actions ───────────────────────────────
    elif action == "scene_style":
        mem = scene_memory.get(chat_id, {})
        artist = mem.get("artist", "")
        back_label = f"⬅ Back to {artist}" if artist else "⬅ Back"

        buttons = [
            [InlineKeyboardButton("✅ Generate playlist", callback_data=f"build|{value}")],
            [InlineKeyboardButton(back_label, callback_data=f"scene_back|{chat_id}")],
            [InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")],
        ]
        query.edit_message_text(
            f"🎧 {value}\n\nGenerate a playlist for this style?",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    # ── scene_back: restore scene list from memory ────────────────────────────
    elif action == "scene_back":
        mem = scene_memory.get(chat_id, {})
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

    # ── build: generate playlist for a tag/style ──────────────────────────────
    elif action == "build":
        query.edit_message_text(f"📀 Building {value} playlist…")
        data = lastfm("tag.gettopartists", tag=value, limit=50)
        names = [a["name"] for a in data.get("topartists", {}).get("artist", [])]
        send_playlist(message, select_tracks(names), f"📀 {value}")

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
dp.add_handler(CallbackQueryHandler(handle_buttons))

print(BOT_VERSION)
updater.start_polling()
updater.idle()
