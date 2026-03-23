 import os
import json
import logging
import random
import time
import tempfile
import itertools
import threading
import secrets
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote, urlencode

import requests
from flask import Flask, request as flask_request
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler, CommandHandler, Updater

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)

# ─── Version ──────────────────────────────────────────────────────────────────
BOT_VERSION = "Kurator 📀 Music Discovery Engine (v3.4.0)"

# ─── Environment ──────────────────────────────────────────────────────────────
LASTFM_USER    = "burbq"
LASTFM_API     = os.environ["LASTFM_API_KEY"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
DISCOGS_TOKEN  = os.environ["DISCOGS_TOKEN"]

# Spotify OAuth — set these as env vars in Railway
SPOTIFY_CLIENT_ID     = os.environ.get("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
SPOTIFY_REDIRECT_URI  = os.environ.get(
    "SPOTIFY_REDIRECT_URI",
    "https://kurator-telegram-bot.railway.app/callback/spotify"
)
SPOTIFY_SCOPES   = "playlist-modify-public playlist-modify-private"
CALLBACK_PORT    = int(os.environ.get("PORT", 8080))

# ─── Constants ────────────────────────────────────────────────────────────────
SCROBBLE_LIMIT      = 600
SEED_ARTISTS        = 35
SIMILAR_EXPANSION   = 60
PLAYLIST_SIZE       = 30
RARE_MAX_LISTENERS  = 500_000
RARE_CANDIDATE_CAP  = 150
TAGS_PAGE_SIZE      = 24
CALLBACK_DATA_MAX   = 60
SPOTIFY_STORE_MAX   = 20
TRACK_FETCH_LIMIT   = 50
TRACK_SKIP_TOP      = 5
TRACK_PLAYCOUNT_MAX = 500_000

# ─── File paths ───────────────────────────────────────────────────────────────
HISTORY_FILE       = "history.json"
TAG_INDEX_FILE     = "tag_index.json"
SCENE_FILE         = "scene_memory.json"
SPOTIFY_TOKENS_FILE = "spotify_tokens.json"

# ─── Cache ────────────────────────────────────────────────────────────────────
_cache = {}

def cache_get(key):
    entry = _cache.get(key)
    if entry and time.time() - entry["ts"] < 600:
        return entry["value"]
    return None

def cache_set(key, value):
    _cache[key] = {"value": value, "ts": time.time()}

# ─── JSON helpers — atomic write ──────────────────────────────────────────────

def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return default

def save_json(path, data):
    dir_ = os.path.dirname(os.path.abspath(path))
    try:
        fd, tmp = tempfile.mkstemp(dir=dir_)
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
        os.replace(tmp, path)
    except Exception as e:
        log.error(f"save_json failed for {path}: {e}")

# ─── Persistent history ───────────────────────────────────────────────────────

def load_history():
    d = load_json(HISTORY_FILE, {})
    return {"tracks": set(d.get("tracks", []))}

def save_history():
    save_json(HISTORY_FILE, {"tracks": list(history["tracks"])})

history = load_history()

# ─── Persistent tag_index + scene_memory ─────────────────────────────────────

tag_index  = load_json(TAG_INDEX_FILE, {})
_scene_raw = load_json(SCENE_FILE, {})
scene_memory = {int(k): v for k, v in _scene_raw.items()}

def save_tag_index():
    save_json(TAG_INDEX_FILE, tag_index)

def save_scene_memory():
    save_json(SCENE_FILE, {str(k): v for k, v in scene_memory.items()})

# ─── Spotify token storage ────────────────────────────────────────────────────

_spotify_tokens = load_json(SPOTIFY_TOKENS_FILE, {})  # {str(chat_id): token_dict}
_pending_auth   = {}  # state -> chat_id (in-memory, short-lived)

def save_spotify_tokens():
    save_json(SPOTIFY_TOKENS_FILE, _spotify_tokens)

def get_spotify_token(chat_id):
    """Returns a valid access token for chat_id, refreshing if needed."""
    key  = str(chat_id)
    data = _spotify_tokens.get(key)
    if not data:
        return None

    # Refresh if expired (with 60s buffer)
    if time.time() > data.get("expires_at", 0) - 60:
        refreshed = _spotify_refresh(data["refresh_token"])
        if refreshed:
            _spotify_tokens[key] = refreshed
            save_spotify_tokens()
            return refreshed["access_token"]
        else:
            return None

    return data["access_token"]

def _spotify_refresh(refresh_token):
    try:
        r = requests.post(
            "https://accounts.spotify.com/api/token",
            data={
                "grant_type":    "refresh_token",
                "refresh_token": refresh_token,
            },
            auth=(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET),
            timeout=10
        )
        if r.status_code != 200:
            log.warning(f"Spotify refresh failed: {r.status_code}")
            return None
        d = r.json()
        return {
            "access_token":  d["access_token"],
            "refresh_token": d.get("refresh_token", refresh_token),
            "expires_at":    time.time() + d["expires_in"],
        }
    except Exception as e:
        log.error(f"Spotify refresh error: {e}")
        return None

# ─── Spotify API helpers ──────────────────────────────────────────────────────

def spotify_search_track_id(token, query):
    """Returns Spotify track URI for a given 'Artist - Track' string."""
    try:
        r = requests.get(
            "https://api.spotify.com/v1/search",
            headers={"Authorization": f"Bearer {token}"},
            params={"q": query, "type": "track", "limit": 1},
            timeout=8
        )
        items = r.json().get("tracks", {}).get("items", [])
        if items:
            return items[0]["uri"]
    except Exception as e:
        log.error(f"Spotify search error for '{query}': {e}")
    return None

def spotify_get_user_id(token):
    try:
        r = requests.get(
            "https://api.spotify.com/v1/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=8
        )
        return r.json().get("id")
    except Exception as e:
        log.error(f"Spotify get user error: {e}")
        return None

def spotify_create_playlist(token, user_id, title):
    try:
        r = requests.post(
            f"https://api.spotify.com/v1/users/{user_id}/playlists",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"name": title, "description": "Created by Kurator 📀", "public": True},
            timeout=10
        )
        d = r.json()
        return d.get("id"), d.get("external_urls", {}).get("spotify")
    except Exception as e:
        log.error(f"Spotify create playlist error: {e}")
        return None, None

def spotify_add_tracks(token, playlist_id, uris):
    """Add up to 100 tracks to a playlist."""
    try:
        for i in range(0, len(uris), 100):
            requests.post(
                f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"uris": uris[i:i+100]},
                timeout=10
            )
    except Exception as e:
        log.error(f"Spotify add tracks error: {e}")

def spotify_build_playlist(chat_id, tracks, title):
    """
    Full flow: search all tracks, create playlist, add them.
    Returns playlist URL or None.
    """
    token = get_spotify_token(chat_id)
    if not token:
        return None

    user_id = spotify_get_user_id(token)
    if not user_id:
        return None

    # Search track URIs in parallel
    uris = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(spotify_search_track_id, token, t): t for t in tracks}
        for f in as_completed(futures):
            uri = f.result()
            if uri:
                uris.append(uri)

    if not uris:
        return None

    playlist_id, playlist_url = spotify_create_playlist(token, user_id, title)
    if not playlist_id:
        return None

    spotify_add_tracks(token, playlist_id, uris)
    return playlist_url

# ─── Flask OAuth callback server ──────────────────────────────────────────────

flask_app = Flask(__name__)
_bot_ref   = None  # set after Updater is created

@flask_app.route("/callback/spotify")
def spotify_callback():
    code  = flask_request.args.get("code")
    state = flask_request.args.get("state")
    error = flask_request.args.get("error")

    chat_id = _pending_auth.pop(state, None)
    if not chat_id:
        return "❌ Invalid or expired session. Please try /connect again.", 400

    if error or not code:
        if _bot_ref:
            _bot_ref.send_message(chat_id, "❌ Spotify authorization cancelled.")
        return "Authorization cancelled.", 400

    # Exchange code for tokens
    try:
        r = requests.post(
            "https://accounts.spotify.com/api/token",
            data={
                "grant_type":   "authorization_code",
                "code":         code,
                "redirect_uri": SPOTIFY_REDIRECT_URI,
            },
            auth=(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET),
            timeout=10
        )
        d = r.json()
        if "access_token" not in d:
            raise ValueError(d)

        _spotify_tokens[str(chat_id)] = {
            "access_token":  d["access_token"],
            "refresh_token": d["refresh_token"],
            "expires_at":    time.time() + d["expires_in"],
        }
        save_spotify_tokens()

        if _bot_ref:
            _bot_ref.send_message(
                chat_id,
                "✅ Spotify connected!\n\nYour next playlists will be created directly in your account.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")
                ]])
            )
        return "<h2>✅ Kurator connected to Spotify!</h2><p>You can close this tab.</p>"

    except Exception as e:
        log.error(f"Spotify token exchange error: {e}")
        if _bot_ref:
            _bot_ref.send_message(chat_id, "❌ Spotify connection failed. Try /connect again.")
        return "Connection failed.", 500

def _run_flask():
    flask_app.run(host="0.0.0.0", port=CALLBACK_PORT, use_reloader=False)

# ─── Last.fm helpers ──────────────────────────────────────────────────────────

def spotify_url(track):
    return f"https://open.spotify.com/search/{quote(track)}"

def lastfm(method, **params):
    payload = {"method": method, "api_key": LASTFM_API, "format": "json", **params}
    try:
        r = requests.get("https://ws.audioscrobbler.com/2.0/", params=payload, timeout=10)
        if r.status_code != 200:
            log.warning(f"Last.fm {method} returned HTTP {r.status_code}")
            return {}
        return r.json()
    except Exception as e:
        log.error(f"Last.fm error ({method}): {e}")
        return {}

def normalize(name):
    return name.lower().strip()

def safe_callback(value):
    if len(value.encode("utf-8")) > CALLBACK_DATA_MAX:
        value = value.encode("utf-8")[:CALLBACK_DATA_MAX].decode("utf-8", errors="ignore")
    return value

def get_recent_tracks():
    cached = cache_get("recent_tracks")
    if cached is not None:
        return cached
    data   = lastfm("user.getrecenttracks", user=LASTFM_USER, limit=SCROBBLE_LIMIT)
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
    data = lastfm("artist.getsimilar", artist=artist, limit=SIMILAR_EXPANSION)
    return [s["name"] for s in data.get("similarartists", {}).get("artist", [])]

def _fetch_listeners(artist):
    data = lastfm("artist.getinfo", artist=artist)
    try:
        listeners = int(data["artist"]["stats"]["listeners"])
    except (KeyError, ValueError):
        listeners = 0
    return artist, listeners

def expand_artist_graph(seed_artists):
    pool = set()
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(_fetch_similar_names, a) for a in seed_artists]
        for f in as_completed(futures):
            try:
                pool.update(f.result())
            except Exception as e:
                log.error(f"expand L1 error: {e}")
    return list(pool)

def expand_artist_graph_deep(seed_artists):
    level1 = set(expand_artist_graph(seed_artists))
    sample = random.sample(list(level1), min(len(level1), 30))
    level2 = set()
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(_fetch_similar_names, a) for a in sample]
        for f in as_completed(futures):
            try:
                level2.update(f.result())
            except Exception as e:
                log.error(f"expand L2 error: {e}")
    return list(level2 - level1 - set(seed_artists))

def expand_artist_graph_rare(seed_artists):
    candidates = expand_artist_graph(seed_artists)
    if len(candidates) > RARE_CANDIDATE_CAP:
        candidates = random.sample(candidates, RARE_CANDIDATE_CAP)
    filtered = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(_fetch_listeners, a) for a in candidates]
        for f in as_completed(futures):
            try:
                artist, listeners = f.result()
                if 0 < listeners < RARE_MAX_LISTENERS:
                    filtered.append(artist)
            except Exception as e:
                log.error(f"expand_rare error: {e}")
    return filtered

# ─── Track selection ──────────────────────────────────────────────────────────

def _fetch_top_track(artist):
    data = lastfm("artist.gettoptracks", artist=artist, limit=TRACK_FETCH_LIMIT)
    top  = data.get("toptracks", {}).get("track", [])
    pool = top[TRACK_SKIP_TOP:] or top
    filtered = [t for t in pool
                if int(t.get("playcount", 0) or 0) < TRACK_PLAYCOUNT_MAX] or pool
    random.shuffle(filtered)
    for t in filtered:
        key = f"{normalize(artist)}-{normalize(t['name'])}"
        if key not in history["tracks"]:
            return (artist, t["name"], key)
    return None

def select_tracks(artists):
    tracks     = []
    keys_added = set()
    random.shuffle(artists)
    candidates = artists[:PLAYLIST_SIZE * 7]

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

# ─── Track store (for expand fallback) ───────────────────────────────────────

_track_store    = {}
_track_counter  = itertools.count()

def _store_tracks(tracks):
    key = str(next(_track_counter))
    _track_store[key] = tracks
    if len(_track_store) > SPOTIFY_STORE_MAX:
        oldest = sorted(_track_store.keys(), key=lambda k: int(k))
        for old in oldest[:len(_track_store) - SPOTIFY_STORE_MAX]:
            del _track_store[old]
    return key

# ─── Playlist sender ──────────────────────────────────────────────────────────

def send_playlist(message, tracks, title="✦ Kurator's Pick", branded=True, chat_id=None):
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

    short_warning = ""
    if len(tracks) < PLAYLIST_SIZE * 0.7:
        short_warning = f"\n⚠️ Only {len(tracks)} tracks found — history filling up. /reset to refresh.\n"

    track_list = "\n".join(f"{i+1:02d}. {t}" for i, t in enumerate(tracks))
    subtitle   = "✦ Selected from Kurator's collection" if branded else "🔍 Open discovery"

    # Try to create Spotify playlist if user is connected
    spotify_url_playlist = None
    if chat_id and get_spotify_token(chat_id):
        message.reply_text("🎧 Creating your Spotify playlist…")
        spotify_url_playlist = spotify_build_playlist(chat_id, tracks, title)

    key = _store_tracks(tracks)

    # Build export buttons
    if spotify_url_playlist:
        export_buttons = [
            [InlineKeyboardButton("🎧 Open playlist in Spotify", url=spotify_url_playlist)],
            [InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")],
        ]
    else:
        export_buttons = [
            [InlineKeyboardButton("🎧 Expand in Spotify", callback_data=f"sp_expand|{key}")],
            [InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")],
        ]
        if chat_id and not get_spotify_token(chat_id):
            export_buttons.insert(1, [
                InlineKeyboardButton("🔗 Connect Spotify", callback_data="cmd|connect")
            ])

    message.reply_text(
        f"{title} — {len(tracks)} tracks\n"
        f"{subtitle}{short_warning}\n\n"
        f"{track_list}\n\n"
        "Import plain text at soundiiz.com",
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup(export_buttons)
    )

# ─── Main menu ────────────────────────────────────────────────────────────────

def main_menu_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("── ✦ KURATOR'S PICKS ──────────────", callback_data="noop")],
        [InlineKeyboardButton("📀 Playlist — Kurator's selection",    callback_data="cmd|playlist")],
        [InlineKeyboardButton("⛏️ Dig — Deeper into Kurator's taste", callback_data="cmd|dig")],
        [InlineKeyboardButton("💎 Rare — Kurator's hidden gems",      callback_data="cmd|rare")],
        [InlineKeyboardButton("── 🔍 EXPLORE ──────────────────────", callback_data="noop")],
        [InlineKeyboardButton("🧬 Trail — Follow an artist",           callback_data="cmd|trail_prompt")],
        [InlineKeyboardButton("🌐 Scene — Map styles via Discogs",     callback_data="cmd|scene_prompt")],
        [InlineKeyboardButton("🗂️ Tags — Browse collected genres",    callback_data="cmd|tags")],
        [
            InlineKeyboardButton("📊 Status",  callback_data="cmd|status"),
            InlineKeyboardButton("🗑️ Reset",   callback_data="cmd|reset"),
            InlineKeyboardButton("❓ Help",    callback_data="cmd|help"),
        ],
    ])

# ─── Help text ────────────────────────────────────────────────────────────────

def _help_text():
    return """❓ Help

✦ KURATOR'S PICKS
Selections drawn from Kurator's own listening history.

📀 /playlist — Hand-picked by Kurator.
⛏️ /dig — Two degrees from Kurator's taste.
💎 /rare — Hidden gems under 500K listeners.

🔍 EXPLORE
Open-ended discovery — not tied to Kurator's taste.

🧬 /trail <artist> — Follow an artist's DNA.
🌐 /scene <artist> — Map styles via Discogs.
🗂️ /tags — Browse collected genres.
📀 /playlist <tag> — e.g. /playlist jazz

🔗 /connect — Link your Spotify account.
📊 /status — History and tag stats.
🗑️ /reset  — Clear history and start fresh.

Kurator is built around taste, not algorithms.
"""

# ─── Commands ─────────────────────────────────────────────────────────────────

def start(update, context):
    update.message.reply_text(
        f"{BOT_VERSION}\n\nTap a command to begin:",
        reply_markup=main_menu_markup()
    )

def help_command(update, context):
    update.message.reply_text(_help_text())

def connect(update, context):
    """Generate Spotify OAuth URL and send to user."""
    if not SPOTIFY_CLIENT_ID:
        update.message.reply_text("⚠️ Spotify integration not configured.")
        return

    chat_id = update.effective_chat.id
    state   = secrets.token_urlsafe(16)
    _pending_auth[state] = chat_id

    params = {
        "client_id":     SPOTIFY_CLIENT_ID,
        "response_type": "code",
        "redirect_uri":  SPOTIFY_REDIRECT_URI,
        "scope":         SPOTIFY_SCOPES,
        "state":         state,
    }
    auth_url = "https://accounts.spotify.com/authorize?" + urlencode(params)

    update.message.reply_text(
        "🎧 Connect your Spotify account to Kurator.\n\n"
        "Tap the button below, authorize, and your playlists will be created directly in your account.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔗 Connect Spotify", url=auth_url)
        ]])
    )

def disconnect_spotify(update, context):
    chat_id = str(update.effective_chat.id)
    if chat_id in _spotify_tokens:
        del _spotify_tokens[chat_id]
        save_spotify_tokens()
        update.message.reply_text("✅ Spotify disconnected.")
    else:
        update.message.reply_text("You don't have a Spotify account connected.")

def playlist(update, context):
    msg     = update.message
    chat_id = update.effective_chat.id
    if context.args:
        tag = " ".join(context.args)
        msg.reply_text(f"🔍 Searching tracks tagged \"{tag}\"…")
        data  = lastfm("tag.gettoptracks", tag=tag, limit=100)
        items = data.get("tracks", {}).get("track", [])
        names = list({t["artist"]["name"] for t in items if t.get("artist")})
        random.shuffle(names)
        send_playlist(msg, select_tracks(names), title=f"🔍 {tag}", branded=False, chat_id=chat_id)
    else:
        msg.reply_text("📀 Kurator is selecting tracks for you… ⏳")
        seeds = extract_seed_artists()
        msg.reply_text("🔍 Expanding the collection…")
        send_playlist(msg, select_tracks(expand_artist_graph(seeds)),
                      title="✦ Kurator's Pick", branded=True, chat_id=chat_id)

def dig(update, context):
    msg     = update.message
    chat_id = update.effective_chat.id
    msg.reply_text("⛏️ Going deeper into Kurator's taste… ⏳")
    seeds = extract_seed_artists()
    msg.reply_text("🔍 Mapping two degrees of separation…")
    send_playlist(msg, select_tracks(expand_artist_graph_deep(seeds)),
                  title="✦ Kurator's Dig", branded=True, chat_id=chat_id)

def rare(update, context):
    msg     = update.message
    chat_id = update.effective_chat.id
    msg.reply_text("💎 Searching Kurator's hidden gems… ⏳\nThis may take a moment.")
    seeds = extract_seed_artists()
    send_playlist(msg, select_tracks(expand_artist_graph_rare(seeds)),
                  title="✦ Kurator's Rare", branded=True, chat_id=chat_id)

def trail(update, context):
    msg     = update.message
    chat_id = update.effective_chat.id
    if not context.args:
        msg.reply_text("🧬 Trail\n\nType:\n/trail <artist>")
        return
    artist = " ".join(context.args)
    msg.reply_text(f"🧬 Following trail from {artist}…")
    data  = lastfm("artist.getsimilar", artist=artist, limit=60)
    names = [a["name"] for a in data.get("similarartists", {}).get("artist", [])]
    send_playlist(msg, select_tracks(names), title=f"🧬 {artist}", branded=False, chat_id=chat_id)

def scene(update, context):
    msg = update.message
    if not context.args:
        msg.reply_text("🌐 Scene\n\nType:\n/scene <artist>")
        return
    artist_query = " ".join(context.args)
    msg.reply_text("🌐 Mapping scene…")
    _render_scene(msg, artist_query, update.effective_chat.id)

def tags(update, context):
    _render_tags(update.message, page=0)

def status(update, context):
    _render_status(update.message, update.effective_chat.id)

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

    if not sorted_styles:
        message.reply_text(f'🌐 No styles found for "{artist_query}".')
        return

    scene_memory[chat_id] = {"artist": artist_query, "styles": sorted_styles}
    save_scene_memory()

    buttons = [
        [InlineKeyboardButton(f"{style}  ({count})", callback_data=safe_callback(f"scene_style|{style}"))]
        for style, count in sorted_styles
    ]
    buttons.append([InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")])

    message.reply_text(
        f"🌐 {artist_query}\n\nChoose a style:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# ─── Tags renderer — paginated + edit mode ───────────────────────────────────

def _build_tags_buttons(sorted_tags, page, edit_mode=False):
    start     = page * TAGS_PAGE_SIZE
    end       = start + TAGS_PAGE_SIZE
    page_tags = sorted_tags[start:end]

    buttons = []
    if edit_mode:
        for tag, count in page_tags:
            cb_del = safe_callback(f"tag_del|{tag}")
            buttons.append([
                InlineKeyboardButton(f"❌ {tag} ({count})", callback_data=cb_del)
            ])
        buttons.append([InlineKeyboardButton("✅ Done", callback_data=f"tags_page|{page}")])
    else:
        row = []
        for tag, count in page_tags:
            cb = safe_callback(f"scene_style|{tag}")
            row.append(InlineKeyboardButton(f"{tag} ({count})", callback_data=cb))
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

        buttons.append([
            InlineKeyboardButton("✏️ Edit",    callback_data=f"tags_edit|{page}"),
            InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu"),
        ])

    return buttons

def _render_tags(message, page=0, edit_mode=False):
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
    mode_label  = "  ✏️ Edit mode — tap a tag to delete" if edit_mode else ""

    message.reply_text(
        f"🗂️ Tag Library — {len(sorted_tags)} genres (page {page + 1}/{total_pages}){mode_label}\n\n{tag_list}",
        reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page, edit_mode))
    )

# ─── Status + Reset ───────────────────────────────────────────────────────────

def _render_status(message, chat_id=None):
    spotify_status = "✅ Connected" if (chat_id and get_spotify_token(chat_id)) else "❌ Not connected — /connect"
    message.reply_text(
        f"📊 Kurator Status\n\n"
        f"🎵 Tracks in history: {len(history['tracks'])}\n"
        f"🗂️ Tags collected: {len(tag_index)}\n"
        f"🌐 Scene sessions: {len(scene_memory)}\n"
        f"🎧 Spotify: {spotify_status}\n\n"
        f"Use /reset to clear history and start fresh.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🗑️ Reset", callback_data="cmd|reset"),
            InlineKeyboardButton("⬅ Menu",  callback_data="cmd|menu"),
        ]])
    )

def _do_reset(message):
    history["tracks"].clear()
    save_history()
    message.reply_text(
        "🗑️ History cleared!\n\nYou'll start seeing fresh artists and tracks again.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")
        ]])
    )

# ─── Callback router ──────────────────────────────────────────────────────────

def handle_buttons(update, context):
    query   = update.callback_query
    chat_id = query.message.chat.id
    message = query.message
    parts   = query.data.split("|", 1)
    action  = parts[0]
    value   = parts[1] if len(parts) > 1 else ""

    if action == "noop":
        query.answer()
        return

    query.answer()

    # ── cmd ────────────────────────────────────────────────────────────────────
    if action == "cmd":

        if value == "menu":
            query.edit_message_text(
                f"{BOT_VERSION}\n\nTap a command to begin:",
                reply_markup=main_menu_markup()
            )

        elif value == "playlist":
            query.edit_message_text("📀 Kurator is selecting tracks… ⏳")
            seeds = extract_seed_artists()
            send_playlist(message, select_tracks(expand_artist_graph(seeds)),
                          title="✦ Kurator's Pick", branded=True, chat_id=chat_id)

        elif value == "dig":
            query.edit_message_text("⛏️ Going deeper into Kurator's taste… ⏳")
            seeds = extract_seed_artists()
            send_playlist(message, select_tracks(expand_artist_graph_deep(seeds)),
                          title="✦ Kurator's Dig", branded=True, chat_id=chat_id)

        elif value == "rare":
            query.edit_message_text("💎 Searching Kurator's hidden gems… ⏳")
            seeds = extract_seed_artists()
            send_playlist(message, select_tracks(expand_artist_graph_rare(seeds)),
                          title="✦ Kurator's Rare", branded=True, chat_id=chat_id)

        elif value == "trail_prompt":
            query.edit_message_text("🧬 Trail\n\nSend:\n/trail <artist>")

        elif value == "scene_prompt":
            query.edit_message_text("🌐 Scene\n\nSend:\n/scene <artist>")

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
                f"🗂️ Tag Library — {len(sorted_tags)} genres (page 1/{total_pages})\n\n{tag_list}",
                reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, 0))
            )

        elif value == "connect":
            if not SPOTIFY_CLIENT_ID:
                query.answer("Spotify not configured.", show_alert=True)
                return
            state = secrets.token_urlsafe(16)
            _pending_auth[state] = chat_id
            params = {
                "client_id":     SPOTIFY_CLIENT_ID,
                "response_type": "code",
                "redirect_uri":  SPOTIFY_REDIRECT_URI,
                "scope":         SPOTIFY_SCOPES,
                "state":         state,
            }
            auth_url = "https://accounts.spotify.com/authorize?" + urlencode(params)
            query.edit_message_text(
                "🎧 Connect your Spotify account to Kurator.\n\n"
                "Tap below, authorize, and your playlists will be created directly in your account.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔗 Connect Spotify", url=auth_url)
                ]])
            )

        elif value == "status":
            spotify_status = "✅ Connected" if get_spotify_token(chat_id) else "❌ Not connected — /connect"
            query.edit_message_text(
                f"📊 Kurator Status\n\n"
                f"🎵 Tracks in history: {len(history['tracks'])}\n"
                f"🗂️ Tags collected: {len(tag_index)}\n"
                f"🌐 Scene sessions: {len(scene_memory)}\n"
                f"🎧 Spotify: {spotify_status}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🗑️ Reset", callback_data="cmd|reset"),
                    InlineKeyboardButton("⬅ Menu",  callback_data="cmd|menu"),
                ]])
            )

        elif value == "reset":
            history["tracks"].clear()
            save_history()
            query.edit_message_text(
                "🗑️ History cleared!\n\nYou'll start seeing fresh artists and tracks again.",
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
            f"🗂️ Tag Library — {len(sorted_tags)} genres (page {page + 1}/{total_pages})\n\n{tag_list}",
            reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page))
        )

    # ── tags edit mode ────────────────────────────────────────────────────────
    elif action == "tags_edit":
        page        = int(value)
        sorted_tags = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
        total_pages = max(1, (len(sorted_tags) - 1) // TAGS_PAGE_SIZE + 1)
        start       = page * TAGS_PAGE_SIZE
        page_tags   = sorted_tags[start:start + TAGS_PAGE_SIZE]
        tag_list    = "\n".join(f"• {tag}  ×{count}" for tag, count in page_tags)
        query.edit_message_text(
            f"🗂️ Tag Library — {len(sorted_tags)} genres (page {page + 1}/{total_pages})  ✏️ Edit mode\n\n{tag_list}",
            reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page, edit_mode=True))
        )

    # ── tag delete ────────────────────────────────────────────────────────────
    elif action == "tag_del":
        tag = value
        if tag in tag_index:
            del tag_index[tag]
            save_tag_index()
        sorted_tags = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
        if not sorted_tags:
            query.edit_message_text(
                "🗂️ Tag library is empty.\n\nUse /scene <artist> to build it up.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")
                ]])
            )
            return
        total_pages = max(1, (len(sorted_tags) - 1) // TAGS_PAGE_SIZE + 1)
        page        = 0
        page_tags   = sorted_tags[:TAGS_PAGE_SIZE]
        tag_list    = "\n".join(f"• {t}  ×{c}" for t, c in page_tags)
        query.edit_message_text(
            f"🗂️ Tag Library — {len(sorted_tags)} genres (page 1/{total_pages})  ✏️ Edit mode\n\n{tag_list}",
            reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page, edit_mode=True))
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
            f"🌐 {value}\n\nGenerate a playlist for this style?",
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
            [InlineKeyboardButton(f"{style}  ({count})", callback_data=safe_callback(f"scene_style|{style}"))]
            for style, count in styles
        ]
        buttons.append([InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")])
        query.edit_message_text(
            f"🌐 {artist}\n\nChoose a style:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    # ── sp_expand: expand individual Spotify search links (fallback) ──────────
    elif action == "sp_expand":
        tracks = _track_store.get(value, [])
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

    # ── build: playlist from scene style ─────────────────────────────────────
    elif action == "build":
        query.edit_message_text(f"🔍 Building {value} playlist… ⏳")
        data  = lastfm("tag.gettoptracks", tag=value, limit=100)
        items = data.get("tracks", {}).get("track", [])
        names = list({t["artist"]["name"] for t in items if t.get("artist")})
        random.shuffle(names)
        send_playlist(message, select_tracks(names), title=f"🔍 {value}", branded=False, chat_id=chat_id)

# ─── Boot ─────────────────────────────────────────────────────────────────────

updater  = Updater(TELEGRAM_TOKEN)
dp       = updater.dispatcher
_bot_ref = updater.bot  # used by Flask callback to send messages

dp.add_handler(CommandHandler("start",       start))
dp.add_handler(CommandHandler("help",        help_command))
dp.add_handler(CommandHandler("playlist",    playlist))
dp.add_handler(CommandHandler("scene",       scene))
dp.add_handler(CommandHandler("tags",        tags))
dp.add_handler(CommandHandler("dig",         dig))
dp.add_handler(CommandHandler("trail",       trail))
dp.add_handler(CommandHandler("rare",        rare))
dp.add_handler(CommandHandler("status",      status))
dp.add_handler(CommandHandler("reset",       reset))
dp.add_handler(CommandHandler("connect",     connect))
dp.add_handler(CommandHandler("disconnect",  disconnect_spotify))
dp.add_handler(CallbackQueryHandler(handle_buttons))

# Start Flask in background thread
threading.Thread(target=_run_flask, daemon=True).start()
log.info(f"Flask OAuth server running on port {CALLBACK_PORT}")

log.info(BOT_VERSION)
print(BOT_VERSION)
updater.start_polling()
updater.idle()
