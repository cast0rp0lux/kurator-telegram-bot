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
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler, CommandHandler, Updater

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)

# ─── Version ──────────────────────────────────────────────────────────────────
BOT_VERSION = "Kurator 📀 Music Discovery Engine (v3.6.0)"

# ─── Environment ──────────────────────────────────────────────────────────────
LASTFM_USER    = "burbq"
LASTFM_API     = os.environ["LASTFM_API_KEY"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
DISCOGS_TOKEN  = os.environ["DISCOGS_TOKEN"]

# Spotify OAuth
SPOTIFY_CLIENT_ID     = os.environ.get("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
SPOTIFY_REDIRECT_URI  = os.environ.get(
    "SPOTIFY_REDIRECT_URI",
    "https://kurator-telegram-bot-production.up.railway.app/callback/spotify"
)
SPOTIFY_SCOPES = "playlist-modify-public playlist-modify-private"

# Qobuz OAuth — register at https://www.qobuz.com/us-en/discover/developer
# Set QOBUZ_CLIENT_ID and QOBUZ_CLIENT_SECRET as env vars in Railway when ready
QOBUZ_CLIENT_ID     = os.environ.get("QOBUZ_CLIENT_ID", "")
QOBUZ_CLIENT_SECRET = os.environ.get("QOBUZ_CLIENT_SECRET", "")
QOBUZ_REDIRECT_URI  = os.environ.get(
    "QOBUZ_REDIRECT_URI",
    "https://kurator-telegram-bot-production.up.railway.app/callback/qobuz"
)

CALLBACK_PORT = int(os.environ.get("PORT", 8080))

# ─── Constants ────────────────────────────────────────────────────────────────
SCROBBLE_LIMIT      = 600
SEED_ARTISTS        = 35
SIMILAR_EXPANSION   = 60
PLAYLIST_SIZE       = 30
GENRE_PLAYLIST_SIZE = 50
RARE_MAX_LISTENERS  = 500_000
RARE_CANDIDATE_CAP  = 150
TAGS_PAGE_SIZE      = 24
CALLBACK_DATA_MAX   = 60
SPOTIFY_STORE_MAX   = 20
TRACK_FETCH_LIMIT   = 50
TRACK_SKIP_TOP      = 5
TRACK_PLAYCOUNT_MAX = 500_000
HISTORY_EXPIRY_DAYS = 90        # tracks expire after this many days

# ─── File paths ───────────────────────────────────────────────────────────────
HISTORY_FILE        = "history.json"
TAG_INDEX_FILE      = "tag_index.json"
SCENE_FILE          = "scene_memory.json"
SPOTIFY_TOKENS_FILE = "spotify_tokens.json"
QOBUZ_TOKENS_FILE   = "qobuz_tokens.json"
ONBOARDED_FILE      = "onboarded.json"

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

# ─── Persistent history — with timestamps + auto-expiry ──────────────────────

def load_history():
    d = load_json(HISTORY_FILE, {})
    # Support both old format (set of strings) and new format (dict with timestamps)
    raw = d.get("tracks", {})
    if isinstance(raw, list):
        # Migrate from old format — assign current time to all existing tracks
        now = time.time()
        tracks = {t: now for t in raw}
    else:
        tracks = raw
    return {"tracks": tracks}

def save_history():
    save_json(HISTORY_FILE, {"tracks": history["tracks"]})

def expire_history():
    """Remove tracks older than HISTORY_EXPIRY_DAYS. Called at boot."""
    cutoff = time.time() - (HISTORY_EXPIRY_DAYS * 86400)
    before = len(history["tracks"])
    history["tracks"] = {k: v for k, v in history["tracks"].items() if v > cutoff}
    expired = before - len(history["tracks"])
    if expired > 0:
        log.info(f"Expired {expired} tracks from history.")
        save_history()

def track_in_history(key):
    return key in history["tracks"]

def add_to_history(key):
    history["tracks"][key] = time.time()

def history_oldest_expiry():
    """Returns days until the oldest track expires, or None if empty."""
    if not history["tracks"]:
        return None
    oldest_ts  = min(history["tracks"].values())
    expires_at = oldest_ts + (HISTORY_EXPIRY_DAYS * 86400)
    days_left  = max(0, int((expires_at - time.time()) / 86400))
    return days_left

history = load_history()
expire_history()

# ─── Onboarded users ──────────────────────────────────────────────────────────

_onboarded = set(load_json(ONBOARDED_FILE, []))

def mark_onboarded(chat_id):
    _onboarded.add(str(chat_id))
    save_json(ONBOARDED_FILE, list(_onboarded))

def is_onboarded(chat_id):
    return str(chat_id) in _onboarded

# ─── Persistent tag_index + scene_memory ─────────────────────────────────────

tag_index    = load_json(TAG_INDEX_FILE, {})
_scene_raw   = load_json(SCENE_FILE, {})
scene_memory = {int(k): v for k, v in _scene_raw.items()}

def save_tag_index():
    save_json(TAG_INDEX_FILE, tag_index)

def save_scene_memory():
    save_json(SCENE_FILE, {str(k): v for k, v in scene_memory.items()})

# ─── Spotify token storage ────────────────────────────────────────────────────

_spotify_tokens = load_json(SPOTIFY_TOKENS_FILE, {})
_qobuz_tokens   = load_json(QOBUZ_TOKENS_FILE, {})
_pending_auth   = {}  # state -> {chat_id, service}

def save_spotify_tokens():
    save_json(SPOTIFY_TOKENS_FILE, _spotify_tokens)

def save_qobuz_tokens():
    save_json(QOBUZ_TOKENS_FILE, _qobuz_tokens)

def get_spotify_token(chat_id):
    key  = str(chat_id)
    data = _spotify_tokens.get(key)
    if not data:
        return None
    if time.time() > data.get("expires_at", 0) - 60:
        refreshed = _spotify_refresh(data["refresh_token"])
        if refreshed:
            _spotify_tokens[key] = refreshed
            save_spotify_tokens()
            return refreshed["access_token"]
        return None
    return data["access_token"]

def _spotify_refresh(refresh_token):
    try:
        r = requests.post(
            "https://accounts.spotify.com/api/token",
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
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

# ─── Spotify API ──────────────────────────────────────────────────────────────

def spotify_search_track_id(token, query):
    try:
        r = requests.get(
            "https://api.spotify.com/v1/search",
            headers={"Authorization": f"Bearer {token}"},
            params={"q": query, "type": "track", "limit": 1},
            timeout=8
        )
        if r.status_code != 200:
            log.warning(f"Spotify search HTTP {r.status_code} for '{query}'")
            return None
        items = r.json().get("tracks", {}).get("items", [])
        if items:
            return items[0]["uri"]
        log.warning(f"Spotify search: no results for '{query}'")
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
        if r.status_code != 200:
            log.error(f"Spotify get user HTTP {r.status_code}: {r.text[:200]}")
            return None
        if not r.content:
            log.error("Spotify get user: empty response body")
            return None
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
    token = get_spotify_token(chat_id)
    if not token:
        return None
    user_id = spotify_get_user_id(token)
    if not user_id:
        return None
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

# ─── OAuth callback server ────────────────────────────────────────────────────

_bot_ref = None

class _OAuthHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        code   = params.get("code",  [None])[0]
        state  = params.get("state", [None])[0]
        error  = params.get("error", [None])[0]

        if parsed.path == "/callback/spotify":
            self._handle_spotify(code, state, error)
        elif parsed.path == "/callback/qobuz":
            self._handle_qobuz(code, state, error)
        else:
            self._respond(404, "Not found")

    def _handle_spotify(self, code, state, error):
        auth_info = _pending_auth.pop(state, None)
        if not auth_info:
            self._respond(400, "Invalid or expired session. Try /connect again.")
            return
        chat_id = auth_info["chat_id"]
        if error or not code:
            if _bot_ref:
                _bot_ref.send_message(chat_id, "Spotify authorization cancelled.")
            self._respond(400, "Authorization cancelled.")
            return
        try:
            r = requests.post(
                "https://accounts.spotify.com/api/token",
                data={"grant_type": "authorization_code", "code": code, "redirect_uri": SPOTIFY_REDIRECT_URI},
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
                    "Spotify connected.\n\nYour next playlists will be created directly in your account.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")
                    ]])
                )
            self._respond(200, "<h2>Kurator × Spotify</h2><p>Connected. You can close this tab.</p>")
        except Exception as e:
            log.error(f"Spotify token exchange error: {e}")
            if _bot_ref:
                _bot_ref.send_message(chat_id, "Spotify connection failed. Try /connect again.")
            self._respond(500, "Connection failed.")

    def _handle_qobuz(self, code, state, error):
        # Qobuz OAuth — structure ready, activate when CLIENT_ID is set
        auth_info = _pending_auth.pop(state, None)
        if not auth_info:
            self._respond(400, "Invalid or expired session.")
            return
        chat_id = auth_info["chat_id"]
        if error or not code:
            if _bot_ref:
                _bot_ref.send_message(chat_id, "Qobuz authorization cancelled.")
            self._respond(400, "Authorization cancelled.")
            return
        # TODO: exchange code for token when Qobuz API access is confirmed
        if _bot_ref:
            _bot_ref.send_message(chat_id, "Qobuz connected. (coming soon)")
        self._respond(200, "<h2>Kurator × Qobuz</h2><p>Connected. You can close this tab.</p>")

    def _respond(self, code, body):
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

def _run_http_server():
    server = HTTPServer(("0.0.0.0", CALLBACK_PORT), _OAuthHandler)
    log.info(f"OAuth server listening on port {CALLBACK_PORT}")
    server.serve_forever()

# ─── Helpers ──────────────────────────────────────────────────────────────────

def spotify_url(track):
    return f"https://open.spotify.com/search/{quote(track)}"

def qobuz_url(track):
    return f"https://www.qobuz.com/search?q={quote(track)}"

def lastfm(method, **params):
    payload = {"method": method, "api_key": LASTFM_API, "format": "json", **params}
    for attempt in range(2):
        try:
            r = requests.get("https://ws.audioscrobbler.com/2.0/", params=payload, timeout=20)
            if r.status_code == 200:
                return r.json()
            log.warning(f"Last.fm {method} returned HTTP {r.status_code}")
        except Exception as e:
            log.error(f"Last.fm error ({method}) attempt {attempt+1}: {e}")
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
        if not track_in_history(key):
            return (artist, t["name"], key)
    return None

def select_tracks(artists, size=None):
    """Parallel track selection. size overrides PLAYLIST_SIZE."""
    target     = size or PLAYLIST_SIZE
    tracks     = []
    keys_added = set()
    random.shuffle(artists)
    candidates = artists[:target * 7]

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(_fetch_top_track, a) for a in candidates]
        for f in as_completed(futures):
            if len(tracks) >= target:
                break
            try:
                result = f.result()
                if result:
                    artist, track_name, key = result
                    if not track_in_history(key) and key not in keys_added:
                        tracks.append(f"{artist} - {track_name}")
                        keys_added.add(key)
            except Exception as e:
                log.error(f"select_tracks error: {e}")

    for key in keys_added:
        add_to_history(key)
    save_history()
    return tracks

# ─── Trail multi-hop expansion ────────────────────────────────────────────────

def _expand_trail(artist, hops):
    """
    Expand artist similarity graph to N hops.
    1 hop = direct neighbours
    2 hops = neighbours of neighbours
    3 hops = one level deeper, excludes closer artists
    """
    level1 = set(s["name"] for s in
                 lastfm("artist.getsimilar", artist=artist, limit=60)
                 .get("similarartists", {}).get("artist", []))

    if hops == 1:
        return list(level1)

    level2 = set()
    sample1 = random.sample(list(level1), min(len(level1), 20))
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(_fetch_similar_names, a) for a in sample1]
        for f in as_completed(futures):
            try:
                level2.update(f.result())
            except Exception:
                pass
    level2 -= level1 | {artist}

    if hops == 2:
        return list(level1 | level2)

    # 3 hops — pure level3, exclude level1 and level2
    level3 = set()
    sample2 = random.sample(list(level2), min(len(level2), 20))
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(_fetch_similar_names, a) for a in sample2]
        for f in as_completed(futures):
            try:
                level3.update(f.result())
            except Exception:
                pass
    level3 -= level1 | level2 | {artist}
    return list(level3)

_track_store   = {}
_track_counter = itertools.count()

def _store_tracks(tracks):
    key = str(next(_track_counter))
    _track_store[key] = tracks
    if len(_track_store) > SPOTIFY_STORE_MAX:
        oldest = sorted(_track_store.keys(), key=lambda k: int(k))
        for old in oldest[:len(_track_store) - SPOTIFY_STORE_MAX]:
            del _track_store[old]
    return key

# ─── Export buttons ───────────────────────────────────────────────────────────

def _export_buttons(key, spotify_url_playlist=None, chat_id=None):
    buttons = [
        [InlineKeyboardButton("📤 Export via Soundiiz", url="https://soundiiz.com")],
    ]
    if spotify_url_playlist:
        buttons.append([InlineKeyboardButton("🎧 Open Spotify playlist", url=spotify_url_playlist)])
    else:
        buttons.append([InlineKeyboardButton("🎧 Export to Spotify", callback_data=f"sp_build|{key}")])
    buttons.append([InlineKeyboardButton("🔗 Open track links", callback_data=f"sp_expand|{key}")])
    buttons.append([InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")])
    return buttons

# ─── Playlist sender ──────────────────────────────────────────────────────────

def send_playlist(message, tracks, title="✦ Kurator's Pick", branded=True, chat_id=None, size=None):
    target = size or PLAYLIST_SIZE
    if not tracks:
        message.reply_text(
            f"{title}\n\nNo new tracks found.\n\nYour history may be full.\nUse /reset to start fresh.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🗑️ Reset", callback_data="cmd|reset")],
                [InlineKeyboardButton("⬅ Menu",  callback_data="cmd|menu")],
            ])
        )
        return

    short_warning = ""
    if len(tracks) < target * 0.7:
        short_warning = f"\nOnly {len(tracks)} tracks found — history is getting full. /reset to refresh.\n"

    key        = _store_tracks(tracks)
    track_list = "\n".join(tracks)

    message.reply_text(
        f"{title} — {len(tracks)} tracks\n"
        f"{BOT_VERSION}{short_warning}\n\n"
        f"{track_list}",
        disable_web_page_preview=True,
    )

    spotify_url_playlist = None
    if chat_id and get_spotify_token(chat_id):
        message.reply_text("Creating your Spotify playlist…")
        spotify_url_playlist = spotify_build_playlist(chat_id, tracks, title)
        if spotify_url_playlist:
            log.info(f"Spotify playlist created: {spotify_url_playlist}")
        else:
            log.warning(f"Spotify playlist creation failed for chat_id {chat_id}")
            message.reply_text(
                "Playlist creation needs Spotify Premium.\n\n"
                "You can still export via Soundiiz or browse track links below."
            )

    message.reply_text(
        "Export options:",
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup(_export_buttons(key, spotify_url_playlist, chat_id))
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
            InlineKeyboardButton("📊 Status", callback_data="cmd|status"),
            InlineKeyboardButton("🗑️ Reset",  callback_data="cmd|reset"),
            InlineKeyboardButton("❓ Help",   callback_data="cmd|help"),
        ],
    ])

# ─── Help text ────────────────────────────────────────────────────────────────

def _help_text():
    return """Kurator is built around taste, not algorithms.

✦ KURATOR'S PICKS
Selections from Kurator's own listening history.

📀 /playlist — Kurator's cut
⛏️ /dig — Two degrees from Kurator's taste
💎 /rare — Artists under 500K listeners

🔍 EXPLORE
Open-ended discovery tools.

🧬 /trail <artist> — Follow an artist's DNA
🌐 /scene <artist> — Map styles via Discogs
🗂️ /tags — Browse collected genres
📀 /playlist <tag> — 50 tracks, e.g. /playlist jazz

/connect — Link your Spotify account
/status — History and tag stats
/reset — Clear history and start fresh
"""

# ─── Commands ─────────────────────────────────────────────────────────────────

def start(update, context):
    chat_id = update.effective_chat.id
    if is_onboarded(chat_id):
        update.message.reply_text(
            f"{BOT_VERSION}\n\nTap a command to begin.",
            reply_markup=main_menu_markup()
        )
    else:
        update.message.reply_text(
            "Kurator 📀\n\n"
            "A music discovery engine built around taste, not algorithms.\n\n"
            "Curated selections drawn from a real listening history — not what's trending.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("→ What can Kurator do?", callback_data="onboard|2")
            ]])
        )

def help_command(update, context):
    update.message.reply_text(_help_text())

def connect(update, context):
    if not SPOTIFY_CLIENT_ID:
        update.message.reply_text("Spotify integration not configured.")
        return
    chat_id = update.effective_chat.id
    state   = secrets.token_urlsafe(16)
    _pending_auth[state] = {"chat_id": chat_id, "service": "spotify"}
    params = {
        "client_id":     SPOTIFY_CLIENT_ID,
        "response_type": "code",
        "redirect_uri":  SPOTIFY_REDIRECT_URI,
        "scope":         SPOTIFY_SCOPES,
        "state":         state,
        "show_dialog":   "true",
    }
    auth_url = "https://accounts.spotify.com/authorize?" + urlencode(params)
    update.message.reply_text(
        "Kurator × Spotify\n\n"
        "Connect your account and playlists will be created automatically — ready to play in one tap.\n\n"
        "Playlist creation requires Spotify Premium.\n"
        "No Premium? You can still export via Soundiiz or browse tracks one by one.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔗 Connect Spotify", url=auth_url)],
            [InlineKeyboardButton("⬅ Back", callback_data="cmd|menu")],
        ])
    )

def disconnect_spotify(update, context):
    chat_id = str(update.effective_chat.id)
    if chat_id in _spotify_tokens:
        del _spotify_tokens[chat_id]
        save_spotify_tokens()
        update.message.reply_text("Spotify disconnected.")
    else:
        update.message.reply_text("No Spotify account connected.")

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
        # Genre playlists use larger size (50)
        send_playlist(msg, select_tracks(names, size=GENRE_PLAYLIST_SIZE),
                      title=f"🔍 {tag}", branded=False, chat_id=chat_id, size=GENRE_PLAYLIST_SIZE)
    else:
        msg.reply_text("📀 Selecting tracks…")
        seeds = extract_seed_artists()
        msg.reply_text("Expanding the collection…")
        send_playlist(msg, select_tracks(expand_artist_graph(seeds)),
                      title="✦ Kurator's Pick", branded=True, chat_id=chat_id)

def dig(update, context):
    msg     = update.message
    chat_id = update.effective_chat.id
    msg.reply_text("⛏️ Digging deeper…")
    seeds = extract_seed_artists()
    msg.reply_text("Mapping two degrees of separation…")
    send_playlist(msg, select_tracks(expand_artist_graph_deep(seeds)),
                  title="✦ Kurator's Dig", branded=True, chat_id=chat_id)

def rare(update, context):
    msg     = update.message
    chat_id = update.effective_chat.id
    msg.reply_text("💎 Searching for hidden gems…")
    seeds = extract_seed_artists()
    send_playlist(msg, select_tracks(expand_artist_graph_rare(seeds)),
                  title="✦ Kurator's Rare", branded=True, chat_id=chat_id)

def trail(update, context):
    msg     = update.message
    chat_id = update.effective_chat.id
    if not context.args:
        msg.reply_text("🧬 Trail\n\nSend: /trail <artist>")
        return
    artist = " ".join(context.args)
    # Show depth selector instead of generating immediately
    msg.reply_text(
        f"🧬 {artist}\n\nHow far do you want to go?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("1 hop — Direct neighbours",  callback_data=safe_callback(f"trail_go|1|{artist}"))],
            [InlineKeyboardButton("2 hops — Wider connections", callback_data=safe_callback(f"trail_go|2|{artist}"))],
            [InlineKeyboardButton("3 hops — Deep exploration",  callback_data=safe_callback(f"trail_go|3|{artist}"))],
            [InlineKeyboardButton("⬅ Back", callback_data="cmd|menu")],
        ])
    )

def scene(update, context):
    msg = update.message
    if not context.args:
        msg.reply_text("🌐 Scene\n\nSend: /scene <artist>")
        return
    artist_query = " ".join(context.args)
    msg.reply_text("🌐 Mapping the scene…")
    _render_scene(msg, artist_query, update.effective_chat.id)


def tags(update, context):
    _render_tags(update.message, page=0)

def status(update, context):
    _render_status(update.message, update.effective_chat.id)

def reset(update, context):
    _do_reset(update.message)

# ─── Artist info helpers ──────────────────────────────────────────────────────

def _get_artist_info(artist_query, sorted_styles):
    """
    Build artist card from Discogs results + Last.fm bio.
    Returns dict with country, years, top_genre, bio.
    """
    # Country + first year from Discogs results
    country    = None
    first_year = None
    try:
        data = requests.get(
            "https://api.discogs.com/database/search",
            params={"artist": artist_query, "type": "release",
                    "per_page": 50, "token": DISCOGS_TOKEN},
            timeout=15
        ).json()
        for rel in data.get("results", []):
            if not country and rel.get("country"):
                country = rel["country"]
            year = rel.get("year")
            if year and (not first_year or int(year) < int(first_year)):
                first_year = year
            if country and first_year:
                break
    except Exception:
        pass

    # Top genre from sorted_styles
    top_genre = sorted_styles[0][0] if sorted_styles else None
    second_genre = sorted_styles[1][0] if len(sorted_styles) > 1 else None

    # Bio from Last.fm
    bio = None
    try:
        data = lastfm("artist.getinfo", artist=artist_query)
        raw_bio = data.get("artist", {}).get("bio", {}).get("summary", "")
        if raw_bio:
            # Strip the Last.fm "Read more" HTML link at the end
            import re
            clean = re.sub(r"<a href.*?</a>", "", raw_bio, flags=re.DOTALL).strip()
            # Truncate to ~300 chars at sentence boundary
            if len(clean) > 300:
                clean = clean[:300].rsplit(".", 1)[0] + "."
            bio = clean if len(clean) > 20 else None
    except Exception:
        pass

    return {
        "country":      country,
        "first_year":   first_year,
        "top_genre":    top_genre,
        "second_genre": second_genre,
        "bio":          bio,
    }

def _format_artist_card(artist_query, info):
    """Format the artist info card text."""
    lines = [f"🌐 {artist_query}"]
    meta = []
    if info.get("country"):
        meta.append(info["country"])
    if info.get("first_year"):
        meta.append(f"{info['first_year']}–present")
    if meta:
        lines.append(" · ".join(meta))
    genres = []
    if info.get("top_genre"):
        genres.append(info["top_genre"])
    if info.get("second_genre"):
        genres.append(info["second_genre"])
    if genres:
        lines.append(" · ".join(genres))
    return "\n".join(lines)

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
        message.reply_text("Discogs request failed. Try again.")
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
        message.reply_text(f'No styles found for "{artist_query}".\nTry a different artist or spelling.')
        return

    # Get artist info card
    info = _get_artist_info(artist_query, sorted_styles)
    scene_memory[chat_id] = {
        "artist": artist_query,
        "styles": sorted_styles,
        "info":   info,
    }
    save_scene_memory()

    card_text = _format_artist_card(artist_query, info)

    buttons = []
    if info.get("bio"):
        buttons.append([InlineKeyboardButton("ℹ️ Artist bio", callback_data=safe_callback(f"artist_bio|{artist_query}"))])
    for style, count in sorted_styles:
        buttons.append([InlineKeyboardButton(f"{style}  ({count})", callback_data=safe_callback(f"scene_style|{style}"))])
    buttons.append([InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")])

    message.reply_text(
        f"{card_text}\n\nChoose a style:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# ─── Tags renderer ────────────────────────────────────────────────────────────

def _build_tags_buttons(sorted_tags, page, edit_mode=False):
    start     = page * TAGS_PAGE_SIZE
    end       = start + TAGS_PAGE_SIZE
    page_tags = sorted_tags[start:end]
    buttons   = []

    if edit_mode:
        for tag, count in page_tags:
            buttons.append([InlineKeyboardButton(f"❌ {tag} ({count})",
                                                 callback_data=safe_callback(f"tag_del|{tag}"))])
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
            InlineKeyboardButton("✏️ Edit",     callback_data=f"tags_edit|{page}"),
            InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu"),
        ])

    return buttons

def _render_tags(message, page=0, edit_mode=False):
    if not tag_index:
        message.reply_text("No tags collected yet.\n\nUse /scene <artist> to start building your library.")
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
    spotify_status = "Connected" if (chat_id and get_spotify_token(chat_id)) else "Not connected — /connect"
    days_left = history_oldest_expiry()
    expiry_line = f"  Oldest track expires in {days_left} days\n" if days_left is not None else ""
    message.reply_text(
        f"📊 Status\n\n"
        f"Tracks in history — {len(history['tracks'])}\n"
        f"{expiry_line}"
        f"Tags collected — {len(tag_index)}\n"
        f"Scene sessions — {len(scene_memory)}\n"
        f"Spotify — {spotify_status}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑️ Reset", callback_data="cmd|reset")],
            [InlineKeyboardButton("⬅ Menu",  callback_data="cmd|menu")],
        ])
    )

def _do_reset(message):
    history["tracks"].clear()
    save_history()
    message.reply_text(
        "History cleared.\n\nFresh artists and tracks on your next request.",
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
            message.reply_text(
                f"{BOT_VERSION}\n\nTap a command to begin.",
                reply_markup=main_menu_markup()
            )

        elif value == "playlist":
            query.edit_message_text("📀 Selecting tracks…")
            seeds = extract_seed_artists()
            send_playlist(message, select_tracks(expand_artist_graph(seeds)),
                          title="✦ Kurator's Pick", branded=True, chat_id=chat_id)

        elif value == "dig":
            query.edit_message_text("⛏️ Digging deeper…")
            seeds = extract_seed_artists()
            send_playlist(message, select_tracks(expand_artist_graph_deep(seeds)),
                          title="✦ Kurator's Dig", branded=True, chat_id=chat_id)

        elif value == "rare":
            query.edit_message_text("💎 Searching for hidden gems…")
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
            _pending_auth[state] = {"chat_id": chat_id, "service": "spotify"}
            params = {
                "client_id":     SPOTIFY_CLIENT_ID,
                "response_type": "code",
                "redirect_uri":  SPOTIFY_REDIRECT_URI,
                "scope":         SPOTIFY_SCOPES,
                "state":         state,
                "show_dialog":   "true",
            }
            auth_url = "https://accounts.spotify.com/authorize?" + urlencode(params)
            query.edit_message_text(
                "Kurator × Spotify\n\n"
                "Connect your account and playlists will be created automatically.\n\n"
                "Playlist creation requires Spotify Premium.\n"
                "No Premium? You can still export via Soundiiz or browse tracks one by one.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔗 Connect Spotify", url=auth_url)],
                    [InlineKeyboardButton("⬅ Back", callback_data="cmd|menu")],
                ])
            )

        elif value == "status":
            spotify_status = "Connected" if get_spotify_token(chat_id) else "Not connected — /connect"
            query.edit_message_text(
                f"📊 Status\n\n"
                f"Tracks in history — {len(history['tracks'])}\n"
                f"Tags collected — {len(tag_index)}\n"
                f"Scene sessions — {len(scene_memory)}\n"
                f"Spotify — {spotify_status}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🗑️ Reset", callback_data="cmd|reset")],
                    [InlineKeyboardButton("⬅ Menu",  callback_data="cmd|menu")],
                ])
            )

        elif value == "reset":
            history["tracks"].clear()
            save_history()
            query.edit_message_text(
                "History cleared.\n\nFresh artists and tracks on your next request.",
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
                "Tag library is empty.\n\nUse /scene <artist> to build it up.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")
                ]])
            )
            return
        total_pages = max(1, (len(sorted_tags) - 1) // TAGS_PAGE_SIZE + 1)
        page_tags   = sorted_tags[:TAGS_PAGE_SIZE]
        tag_list    = "\n".join(f"• {t}  ×{c}" for t, c in page_tags)
        query.edit_message_text(
            f"🗂️ Tag Library — {len(sorted_tags)} genres (page 1/{total_pages})  ✏️ Edit mode\n\n{tag_list}",
            reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, 0, edit_mode=True))
        )

    # ── onboarding steps ──────────────────────────────────────────────────────
    elif action == "onboard":
        step = int(value)
        if step == 2:
            query.edit_message_text(
                "✦ KURATOR'S PICKS\n"
                "Playlists, deep cuts and hidden gems drawn from Kurator's own collection.\n\n"
                "🔍 EXPLORE\n"
                "Follow an artist's DNA, map styles via Discogs, browse by genre.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("→ How do I start?", callback_data="onboard|3")
                ]])
            )
        elif step == 3:
            mark_onboarded(chat_id)
            query.edit_message_text(
                "Tap 📀 Playlist for your first selection.\n\n"
                "Connect Spotify to get playlists created automatically in your account.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📀 Get my first playlist", callback_data="cmd|playlist")],
                    [InlineKeyboardButton("🔗 Connect Spotify",        callback_data="cmd|connect")],
                    [InlineKeyboardButton("Skip → Main menu",          callback_data="onboard|0")],
                ])
            )
        elif step == 0:  # skip
            mark_onboarded(chat_id)
            query.edit_message_text(
                f"{BOT_VERSION}\n\nTap a command to begin.",
                reply_markup=main_menu_markup()
            )

    # ── trail_go: execute trail at chosen depth ───────────────────────────────
    elif action == "trail_go":
        # value format: "hops|artist"
        parts_trail = value.split("|", 1)
        hops   = int(parts_trail[0])
        artist = parts_trail[1] if len(parts_trail) > 1 else ""
        hop_labels = {1: "1 hop", 2: "2 hops", 3: "deep"}
        query.edit_message_text(f"🧬 Following the trail — {hop_labels.get(hops, '')}…")
        names = _expand_trail(artist, hops)
        send_playlist(message, select_tracks(names),
                      title=f"🧬 {artist} — {hop_labels.get(hops, '')}",
                      branded=False, chat_id=chat_id)

    # ── artist_bio ────────────────────────────────────────────────────────────
    elif action == "artist_bio":
        artist = value
        mem    = scene_memory.get(chat_id, {})
        info   = mem.get("info", {})
        bio    = info.get("bio", "No bio available.")
        query.edit_message_text(
            f"🌐 {artist}\n\n{bio}",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(f"⬅ Back to {artist[:25]}", callback_data=f"scene_back|{chat_id}")
            ]])
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
        info   = mem.get("info", {})
        if not artist or not styles:
            query.edit_message_text(
                f"{BOT_VERSION}\n\nTap a command to begin.",
                reply_markup=main_menu_markup()
            )
            return
        card_text = _format_artist_card(artist, info)
        buttons   = []
        if info.get("bio"):
            buttons.append([InlineKeyboardButton("ℹ️ Artist bio", callback_data=safe_callback(f"artist_bio|{artist}"))])
        for style, count in styles:
            buttons.append([InlineKeyboardButton(f"{style}  ({count})", callback_data=safe_callback(f"scene_style|{style}"))])
        buttons.append([InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")])
        query.edit_message_text(
            f"{card_text}\n\nChoose a style:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    # ── sp_build: create Spotify playlist on demand ───────────────────────────
    elif action == "sp_build":
        key = value
        if not get_spotify_token(chat_id):
            query.edit_message_text(
                "Connect your Spotify account first.\n\nPlaylist creation requires Spotify Premium.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔗 Connect Spotify", callback_data="cmd|connect")],
                    [InlineKeyboardButton("⬅ Back",             callback_data=f"export_back|{key}")],
                ])
            )
            return
        tracks = _track_store.get(key, [])
        if not tracks:
            query.answer("Playlist expired. Generate a new one.", show_alert=True)
            return
        query.edit_message_text("Creating your Spotify playlist…")
        url = spotify_build_playlist(chat_id, tracks, "Kurator playlist")
        if url:
            query.edit_message_text(
                "Playlist created.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🎧 Open in Spotify", url=url)],
                    [InlineKeyboardButton("⬅ Back",             callback_data=f"export_back|{key}")],
                ])
            )
        else:
            query.edit_message_text(
                "Playlist creation needs Spotify Premium.\n\nYou can still export via Soundiiz or browse track links.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⬅ Back", callback_data=f"export_back|{key}")
                ]])
            )

    # ── export_back: restore export options ───────────────────────────────────
    elif action == "export_back":
        key = value
        query.edit_message_text(
            "Export options:",
            reply_markup=InlineKeyboardMarkup(_export_buttons(key))
        )

    # ── sp_expand: open individual track links ────────────────────────────────
    elif action == "sp_expand":
        tracks = _track_store.get(value, [])
        if not tracks:
            query.answer("Links expired. Generate a new playlist.", show_alert=True)
            return
        buttons = []
        for t in tracks:
            parts = t.split(" - ", 1)
            label = f"{parts[0][:28]} – {parts[1][:20]}" if len(parts) == 2 else t[:48]
            buttons.append([InlineKeyboardButton(label, callback_data="noop")])
            buttons.append([
                InlineKeyboardButton("🎧 Spotify", url=spotify_url(t)),
                InlineKeyboardButton("🎵 Qobuz",   url=qobuz_url(t)),
            ])
        buttons.append([InlineKeyboardButton("⬅ Back",      callback_data=f"export_back|{value}")])
        buttons.append([InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")])
        query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(buttons))

    # ── build: playlist from scene style ─────────────────────────────────────
    elif action == "build":
        query.edit_message_text(f"🔍 Building {value} playlist…")
        data  = lastfm("tag.gettoptracks", tag=value, limit=100)
        items = data.get("tracks", {}).get("track", [])
        names = list({t["artist"]["name"] for t in items if t.get("artist")})
        random.shuffle(names)
        send_playlist(message, select_tracks(names, size=GENRE_PLAYLIST_SIZE),
                      title=f"🔍 {value}", branded=False, chat_id=chat_id, size=GENRE_PLAYLIST_SIZE)

# ─── Boot ─────────────────────────────────────────────────────────────────────

updater  = Updater(TELEGRAM_TOKEN)
dp       = updater.dispatcher
_bot_ref = updater.bot

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

threading.Thread(target=_run_http_server, daemon=True).start()

log.info(BOT_VERSION)
print(BOT_VERSION)
updater.start_polling()
updater.idle()
