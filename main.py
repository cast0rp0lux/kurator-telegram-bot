import os
import re
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
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# ─── Version ──────────────────────────────────────────────────────────────────
BOT_VERSION = "Kurator 📀 Music Discovery Engine (v3.7.3)"

# ─── Environment ──────────────────────────────────────────────────────────────
LASTFM_USER    = "burbq"
LASTFM_API     = os.environ["LASTFM_API_KEY"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
DISCOGS_TOKEN  = os.environ["DISCOGS_TOKEN"]

SPOTIFY_CLIENT_ID     = os.environ.get("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
SPOTIFY_REDIRECT_URI  = os.environ.get(
    "SPOTIFY_REDIRECT_URI",
    "https://kurator-telegram-bot-production.up.railway.app/callback/spotify"
)
SPOTIFY_SCOPES = "playlist-modify-public playlist-modify-private"

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
HISTORY_EXPIRY_DAYS = 90
TRACK_LINKS_PAGE    = 10   # tracks per page in sp_expand

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

# ─── Persistent history — timestamps + auto-expiry ───────────────────────────

def load_history():
    d = load_json(HISTORY_FILE, {})
    raw = d.get("tracks", {})
    if isinstance(raw, list):
        # Migrate old format (plain set) to new format (dict with timestamps)
        now = time.time()
        tracks = {t: now for t in raw}
        log.info(f"Migrated {len(tracks)} tracks to timestamped history.")
    else:
        tracks = {k: float(v) for k, v in raw.items()}
    return {"tracks": tracks}

def save_history():
    save_json(HISTORY_FILE, {"tracks": history["tracks"]})

def expire_history():
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
_pending_auth   = {}

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
            log.warning(f"Spotify refresh failed: {r.status_code} {r.text[:200]}")
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

def spotify_get_user_id(token):
    """Returns user_id or None. No premium check — let the API return 403 if needed."""
    try:
        r = requests.get(
            "https://api.spotify.com/v1/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=8
        )
        if r.status_code != 200:
            log.error(f"Spotify /me HTTP {r.status_code}: {r.text[:300]}")
            return None
        data    = r.json()
        user_id = data.get("id")
        product = data.get("product", "unknown")
        log.info(f"Spotify user: {user_id}, product: {product}")
        return user_id
    except Exception as e:
        log.error(f"Spotify /me error: {e}")
        return None

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
        return items[0]["uri"] if items else None
    except Exception as e:
        log.error(f"Spotify search error for '{query}': {e}")
    return None

def spotify_create_playlist(token, user_id, title):
    try:
        r = requests.post(
            f"https://api.spotify.com/v1/users/{user_id}/playlists",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"name": title, "description": "Created by Kurator 📀", "public": True},
            timeout=10
        )
        if r.status_code == 403:
            log.warning(f"Spotify create playlist 403 — likely not Premium: {r.text[:200]}")
            return None, None, "not_premium"
        if r.status_code not in (200, 201):
            log.error(f"Spotify create playlist HTTP {r.status_code}: {r.text[:300]}")
            return None, None, "failed"
        d = r.json()
        return d.get("id"), d.get("external_urls", {}).get("spotify"), None
    except Exception as e:
        log.error(f"Spotify create playlist error: {e}")
        return None, None, "failed"

def spotify_add_tracks(token, playlist_id, uris):
    try:
        for i in range(0, len(uris), 100):
            r = requests.post(
                f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"uris": uris[i:i+100]},
                timeout=10
            )
            if r.status_code not in (200, 201):
                log.warning(f"Spotify add tracks HTTP {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log.error(f"Spotify add tracks error: {e}")

def spotify_build_playlist(chat_id, tracks, title):
    """
    Full flow: get user_id, search URIs, create playlist, add tracks.
    Returns (playlist_url, error_code).
    error_code: None = success, 'not_premium', 'no_token', 'failed', 'no_uris'
    """
    token = get_spotify_token(chat_id)
    if not token:
        return None, "no_token"

    user_id = spotify_get_user_id(token)
    if not user_id:
        return None, "failed"

    # Search track URIs in parallel
    uris = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(spotify_search_track_id, token, t): t for t in tracks}
        for f in as_completed(futures):
            uri = f.result()
            if uri:
                uris.append(uri)

    log.info(f"Spotify: found {len(uris)}/{len(tracks)} URIs")
    if not uris:
        return None, "no_uris"

    playlist_id, playlist_url, err = spotify_create_playlist(token, user_id, title)
    if err:
        return None, err
    if not playlist_id:
        return None, "failed"

    spotify_add_tracks(token, playlist_id, uris)
    log.info(f"Spotify: playlist created → {playlist_url}")
    return playlist_url, None

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
            if _bot_ref: _bot_ref.send_message(chat_id, "Spotify authorization cancelled.")
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
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")]])
                )
            self._respond(200, "<h2>Kurator × Spotify</h2><p>Connected. You can close this tab.</p>")
        except Exception as e:
            log.error(f"Spotify token exchange error: {e}")
            if _bot_ref: _bot_ref.send_message(chat_id, "Spotify connection failed. Try /connect again.")
            self._respond(500, "Connection failed.")

    def _handle_qobuz(self, code, state, error):
        auth_info = _pending_auth.pop(state, None)
        if not auth_info:
            self._respond(400, "Invalid or expired session.")
            return
        chat_id = auth_info["chat_id"]
        if error or not code:
            if _bot_ref: _bot_ref.send_message(chat_id, "Qobuz authorization cancelled.")
            self._respond(400, "Authorization cancelled.")
            return
        if _bot_ref: _bot_ref.send_message(chat_id, "Qobuz connected. (coming soon)")
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

# ─── URL helpers ──────────────────────────────────────────────────────────────

def spotify_url(track):
    return f"https://open.spotify.com/search/{quote(track)}"

def qobuz_url(track):
    # open.qobuz.com triggers the app on mobile, falls back to web on desktop
    return f"https://open.qobuz.com/search?q={quote(track)}"

# ─── Last.fm helper ───────────────────────────────────────────────────────────

def lastfm(method, **params):
    payload = {"method": method, "api_key": LASTFM_API, "format": "json", **params}
    for attempt in range(2):
        try:
            r = requests.get("https://ws.audioscrobbler.com/2.0/", params=payload, timeout=20)
            if r.status_code == 200:
                return r.json()
            log.warning(f"Last.fm {method} HTTP {r.status_code}")
        except Exception as e:
            log.error(f"Last.fm error ({method}) attempt {attempt+1}: {e}")
    return {}

def normalize(name):
    return name.lower().strip()

def safe_callback(value):
    encoded = value.encode("utf-8")
    if len(encoded) > CALLBACK_DATA_MAX:
        value = encoded[:CALLBACK_DATA_MAX].decode("utf-8", errors="ignore")
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
            try: pool.update(f.result())
            except Exception as e: log.error(f"expand L1: {e}")
    return list(pool)

def expand_artist_graph_deep(seed_artists):
    level1 = set(expand_artist_graph(seed_artists))
    sample = random.sample(list(level1), min(len(level1), 30))
    level2 = set()
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(_fetch_similar_names, a) for a in sample]
        for f in as_completed(futures):
            try: level2.update(f.result())
            except Exception as e: log.error(f"expand L2: {e}")
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
            except Exception as e: log.error(f"expand_rare: {e}")
    return filtered

# ─── Trail multi-hop ──────────────────────────────────────────────────────────

def _expand_trail(artist, hops):
    level1 = set(s["name"] for s in
                 lastfm("artist.getsimilar", artist=artist, limit=60)
                 .get("similarartists", {}).get("artist", []))
    if hops == 1:
        return list(level1)

    level2 = set()
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(_fetch_similar_names, a)
                   for a in random.sample(list(level1), min(len(level1), 20))]
        for f in as_completed(futures):
            try: level2.update(f.result())
            except: pass
    level2 -= level1 | {artist}

    if hops == 2:
        return list(level1 | level2)

    level3 = set()
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(_fetch_similar_names, a)
                   for a in random.sample(list(level2), min(len(level2), 20))]
        for f in as_completed(futures):
            try: level3.update(f.result())
            except: pass
    level3 -= level1 | level2 | {artist}
    return list(level3)

# ─── Track selection ──────────────────────────────────────────────────────────

def _fetch_top_track(artist):
    data = lastfm("artist.gettoptracks", artist=artist, limit=TRACK_FETCH_LIMIT)
    top  = data.get("toptracks", {}).get("track", [])
    pool = top[TRACK_SKIP_TOP:] or top
    filtered = [t for t in pool if int(t.get("playcount", 0) or 0) < TRACK_PLAYCOUNT_MAX] or pool
    random.shuffle(filtered)
    for t in filtered:
        key = f"{normalize(artist)}-{normalize(t['name'])}"
        if not track_in_history(key):
            return (artist, t["name"], key)
    return None

def select_tracks(artists, size=None):
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
            except Exception as e: log.error(f"select_tracks: {e}")

    for key in keys_added:
        add_to_history(key)
    save_history()
    return tracks

# ─── Track store ──────────────────────────────────────────────────────────────

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

# ─── Artist info — MusicBrainz + Last.fm ─────────────────────────────────────

def _country_flag(code):
    """Convert ISO 3166-1 alpha-2 country code to flag emoji."""
    if not code or len(code) != 2:
        return ""
    return chr(0x1F1E0 + ord(code[0].upper()) - ord('A')) + \
           chr(0x1F1E0 + ord(code[1].upper()) - ord('A'))

def _get_artist_info(artist_query):
    """
    Fetch artist info from MusicBrainz (country, years, genres) + Last.fm (bio).
    Returns dict: country_code, country_name, flag, begin_year, end_year,
                  genres (list), bio, mb_url, lastfm_url
    """
    info = {
        "country_code": None, "country_name": None, "flag": "",
        "begin_year": None, "end_year": None,
        "genres": [], "bio": None,
        "mb_url": None, "lastfm_url": None,
    }

    # ── MusicBrainz ───────────────────────────────────────────────────────────
    try:
        r = requests.get(
            "https://musicbrainz.org/ws/2/artist/",
            params={"query": f'artist:"{artist_query}"', "fmt": "json", "limit": 3},
            headers={"User-Agent": "Kurator/3.7.2 (telegram bot)"},
            timeout=10
        )
        if r.status_code == 200:
            artists = r.json().get("artists", [])
            # Pick the best match — require score >= 85 and name similarity
            mb = None
            for candidate in artists:
                score = int(candidate.get("score", 0))
                name  = candidate.get("name", "").lower()
                if score >= 85 and name == artist_query.lower():
                    mb = candidate
                    break
            # Fallback: take top result if score >= 90
            if not mb and artists and int(artists[0].get("score", 0)) >= 90:
                mb = artists[0]

            if mb:
                # Use only the direct 'country' field — area ISO codes are unreliable
                code = mb.get("country")
                if code:
                    info["country_code"] = code
                    info["flag"]         = _country_flag(code)
                    country_names = {
                        "GB": "UK", "US": "USA", "DE": "Germany", "FR": "France",
                        "JP": "Japan", "KR": "Korea", "SE": "Sweden", "NO": "Norway",
                        "DK": "Denmark", "FI": "Finland", "IS": "Iceland", "NL": "Netherlands",
                        "BE": "Belgium", "AU": "Australia", "CA": "Canada", "IT": "Italy",
                        "ES": "Spain", "PT": "Portugal", "BR": "Brazil", "AR": "Argentina",
                        "MX": "Mexico", "PL": "Poland", "CZ": "Czech Republic", "AT": "Austria",
                        "CH": "Switzerland", "RU": "Russia", "UA": "Ukraine", "TR": "Turkey",
                        "ZA": "South Africa", "NG": "Nigeria", "GH": "Ghana", "JM": "Jamaica",
                        "XE": "Europe", "XW": "Worldwide",
                    }
                    info["country_name"] = country_names.get(code, code)
                ls = mb.get("life-span", {})
                begin = ls.get("begin", "")
                end   = ls.get("end", "")
                if begin:
                    info["begin_year"] = begin[:4]
                if end:
                    info["end_year"] = end[:4]
                tags = sorted(mb.get("tags", []), key=lambda t: t.get("count", 0), reverse=True)
                info["genres"] = [t["name"].title() for t in tags[:4]]
                info["mb_url"] = f"https://musicbrainz.org/artist/{mb.get('id', '')}"
                log.info(f"MusicBrainz match: '{mb.get('name')}' score={mb.get('score')} country={code}")
    except Exception as e:
        log.error(f"MusicBrainz error for '{artist_query}': {e}")

    # ── Last.fm bio ───────────────────────────────────────────────────────────
    try:
        data = lastfm("artist.getinfo", artist=artist_query)
        artist_data = data.get("artist", {})
        # Last.fm URL
        lfm_url = artist_data.get("url")
        if lfm_url:
            info["lastfm_url"] = lfm_url
        # Bio — use "content" (full) or "summary" (short)
        raw_bio = artist_data.get("bio", {}).get("content", "") or \
                  artist_data.get("bio", {}).get("summary", "")
        if raw_bio:
            clean = re.sub(r"<a href[^>]*>.*?</a>", "", raw_bio, flags=re.DOTALL)
            clean = re.sub(r"\s+", " ", clean).strip()
            # Truncate to ~500 chars at sentence boundary
            if len(clean) > 500:
                clean = clean[:500].rsplit(".", 1)[0] + "."
            if len(clean) > 30:
                info["bio"] = clean
        # Fill genres from Last.fm tags if MB didn't return any
        if not info["genres"]:
            lfm_tags = artist_data.get("tags", {}).get("tag", [])
            info["genres"] = [t["name"].title() for t in lfm_tags[:4]]
    except Exception as e:
        log.error(f"Last.fm artist info error for '{artist_query}': {e}")

    return info

def _format_artist_card(artist_query, info):
    """Format the artist info card — clean, concise."""
    lines = [f"🌐 {artist_query}"]

    # Country + years
    meta = []
    if info.get("flag") and info.get("country_name"):
        meta.append(f"{info['flag']} {info['country_name']}")
    elif info.get("country_name"):
        meta.append(info["country_name"])

    begin = info.get("begin_year")
    end   = info.get("end_year")
    if begin:
        period = f"{begin}–{end}" if end else f"{begin}–present"
        meta.append(period)
    if meta:
        lines.append("  ".join(meta))

    return "\n".join(lines)

# ─── Export buttons ───────────────────────────────────────────────────────────

def _export_buttons(key, spotify_url_playlist=None):
    buttons = [
        [InlineKeyboardButton("📤 Export via Soundiiz", url="https://soundiiz.com")],
    ]
    if spotify_url_playlist:
        buttons.append([InlineKeyboardButton("🎧 Open Spotify playlist", url=spotify_url_playlist)])
    else:
        buttons.append([InlineKeyboardButton("🎧 Export to Spotify", callback_data=f"sp_build|{key}")])
    buttons.append([InlineKeyboardButton("🔗 Open track links", callback_data=f"sp_expand|{key}|0")])
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

    # Playlist message — plain text, no buttons (scrollable, copyable)
    message.reply_text(
        f"{title} — {len(tracks)} tracks\n"
        f"{BOT_VERSION}{short_warning}\n\n"
        f"{track_list}",
        disable_web_page_preview=True,
    )

    # Build Spotify playlist if connected
    spotify_url_playlist = None
    if chat_id and get_spotify_token(chat_id):
        message.reply_text("Creating your Spotify playlist…")
        spotify_url_playlist, err = spotify_build_playlist(chat_id, tracks, title)
        if err == "not_premium":
            message.reply_text(
                "Playlist creation needs Spotify Premium.\n\n"
                "You can still export via Soundiiz or browse track links below."
            )
        elif err and err not in ("no_token",):
            message.reply_text("Could not create Spotify playlist. Try again or export via Soundiiz.")

    # Export options — always visible, no scroll needed
    message.reply_text(
        "Export options:",
        reply_markup=InlineKeyboardMarkup(_export_buttons(key, spotify_url_playlist))
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
        "client_id": SPOTIFY_CLIENT_ID, "response_type": "code",
        "redirect_uri": SPOTIFY_REDIRECT_URI, "scope": SPOTIFY_SCOPES,
        "state": state, "show_dialog": "true",
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
    msg = update.message
    chat_id = update.effective_chat.id
    if context.args:
        tag = " ".join(context.args)
        msg.reply_text(f"🔍 Searching tracks tagged \"{tag}\"…")
        data  = lastfm("tag.gettoptracks", tag=tag, limit=100)
        items = data.get("tracks", {}).get("track", [])
        names = list({t["artist"]["name"] for t in items if t.get("artist")})
        random.shuffle(names)
        send_playlist(msg, select_tracks(names, size=GENRE_PLAYLIST_SIZE),
                      title=f"🔍 {tag}", branded=False, chat_id=chat_id, size=GENRE_PLAYLIST_SIZE)
    else:
        msg.reply_text("📀 Selecting tracks…")
        seeds = extract_seed_artists()
        msg.reply_text("Expanding the collection…")
        send_playlist(msg, select_tracks(expand_artist_graph(seeds)),
                      title="✦ Kurator's Pick", branded=True, chat_id=chat_id)

def dig(update, context):
    msg = update.message
    chat_id = update.effective_chat.id
    msg.reply_text("⛏️ Digging deeper…")
    seeds = extract_seed_artists()
    msg.reply_text("Mapping two degrees of separation…")
    send_playlist(msg, select_tracks(expand_artist_graph_deep(seeds)),
                  title="✦ Kurator's Dig", branded=True, chat_id=chat_id)

def rare(update, context):
    msg = update.message
    chat_id = update.effective_chat.id
    msg.reply_text("💎 Searching for hidden gems…")
    seeds = extract_seed_artists()
    send_playlist(msg, select_tracks(expand_artist_graph_rare(seeds)),
                  title="✦ Kurator's Rare", branded=True, chat_id=chat_id)

def trail(update, context):
    msg = update.message
    if not context.args:
        msg.reply_text("🧬 Trail\n\nSend: /trail <artist>")
        return
    artist = " ".join(context.args)
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
    sorted_styles = sorted(counter.items(), key=lambda x: x[1], reverse=True)[:12]

    if not sorted_styles:
        message.reply_text(f'No styles found for "{artist_query}".\nTry a different artist or spelling.')
        return

    # Get artist info from MusicBrainz + Last.fm
    info = _get_artist_info(artist_query)
    # Fill genres from sorted_styles if MB/Last.fm returned none
    if not info["genres"]:
        info["genres"] = [s for s, _ in sorted_styles[:4]]

    scene_memory[chat_id] = {"artist": artist_query, "styles": sorted_styles, "info": info}
    save_scene_memory()

    card_text = _format_artist_card(artist_query, info)

    # Genre buttons (clickable, 2 per row)
    genre_buttons = []
    if info["genres"]:
        row = []
        for g in info["genres"]:
            row.append(InlineKeyboardButton(g, callback_data=safe_callback(f"scene_style|{g}")))
            if len(row) == 2:
                genre_buttons.append(row)
                row = []
        if row:
            genre_buttons.append(row)

    buttons = []
    if info.get("bio"):
        buttons.append([InlineKeyboardButton("ℹ️ Artist bio", callback_data=safe_callback(f"artist_bio|{artist_query}"))])
    buttons.extend(genre_buttons)
    buttons.append([InlineKeyboardButton("── All styles ──────────────────", callback_data="noop")])
    for style, count in sorted_styles[:8]:  # limit to 8 to avoid scroll
        buttons.append([InlineKeyboardButton(f"{style}  ({count})", callback_data=safe_callback(f"scene_style|{style}"))])
    if len(sorted_styles) > 8:
        buttons.append([InlineKeyboardButton(f"+ {len(sorted_styles)-8} more styles", callback_data=f"scene_more|{chat_id}")])
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
    mode_label  = "  ✏️ Edit mode" if edit_mode else ""

    message.reply_text(
        f"🗂️ Tag Library — {len(sorted_tags)} genres (page {page + 1}/{total_pages}){mode_label}\n\n{tag_list}",
        reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page, edit_mode))
    )

# ─── Status + Reset ───────────────────────────────────────────────────────────

def _render_status(message, chat_id=None):
    days_left     = history_oldest_expiry()
    expiry_line   = f"  Oldest track expires in {days_left} days\n" if days_left is not None else ""
    spotify_status = "Connected" if (chat_id and get_spotify_token(chat_id)) else "Not connected — /connect"
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
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")]])
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
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="cmd|menu")]])
                )
                return
            sorted_tags = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
            total_pages = max(1, (len(sorted_tags) - 1) // TAGS_PAGE_SIZE + 1)
            tag_list    = "\n".join(f"• {tag}  ×{count}" for tag, count in sorted_tags[:TAGS_PAGE_SIZE])
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
                "client_id": SPOTIFY_CLIENT_ID, "response_type": "code",
                "redirect_uri": SPOTIFY_REDIRECT_URI, "scope": SPOTIFY_SCOPES,
                "state": state, "show_dialog": "true",
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
            days_left      = history_oldest_expiry()
            expiry_line    = f"  Oldest track expires in {days_left} days\n" if days_left is not None else ""
            spotify_status = "Connected" if get_spotify_token(chat_id) else "Not connected — /connect"
            query.edit_message_text(
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

        elif value == "reset":
            history["tracks"].clear()
            save_history()
            query.edit_message_text(
                "History cleared.\n\nFresh artists and tracks on your next request.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")]])
            )

        elif value == "help":
            query.edit_message_text(
                _help_text(),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")]])
            )

    # ── onboarding ────────────────────────────────────────────────────────────
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
                    [InlineKeyboardButton("🔗 Connect Spotify",       callback_data="cmd|connect")],
                    [InlineKeyboardButton("Skip → Main menu",         callback_data="onboard|0")],
                ])
            )
        elif step == 0:
            mark_onboarded(chat_id)
            query.edit_message_text(
                f"{BOT_VERSION}\n\nTap a command to begin.",
                reply_markup=main_menu_markup()
            )

    # ── trail depth selector ──────────────────────────────────────────────────
    elif action == "trail_go":
        sub    = value.split("|", 1)
        hops   = int(sub[0])
        artist = sub[1] if len(sub) > 1 else ""
        labels = {1: "1 hop", 2: "2 hops", 3: "deep"}
        query.edit_message_text(f"🧬 Following the trail — {labels.get(hops, '')}…")
        names = _expand_trail(artist, hops)
        send_playlist(message, select_tracks(names),
                      title=f"🧬 {artist} — {labels.get(hops, '')}",
                      branded=False, chat_id=chat_id)

    # ── artist bio ────────────────────────────────────────────────────────────
    elif action == "artist_bio":
        artist = value
        mem    = scene_memory.get(chat_id, {})
        info   = mem.get("info", {})
        bio    = info.get("bio", "No bio available.")
        # Add Last.fm link if available
        lfm    = info.get("lastfm_url")
        link   = f"\n\n→ {lfm}" if lfm else ""
        query.edit_message_text(
            f"🌐 {artist}\n\n{bio}{link}",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(f"⬅ Back to {artist[:20]}", callback_data=f"scene_back|{chat_id}")
            ]])
        )

    # ── scene_more: show remaining styles ─────────────────────────────────────
    elif action == "scene_more":
        mem    = scene_memory.get(chat_id, {})
        artist = mem.get("artist", "")
        styles = mem.get("styles", [])
        remaining = styles[8:]
        buttons = [
            [InlineKeyboardButton(f"{style}  ({count})", callback_data=safe_callback(f"scene_style|{style}"))]
            for style, count in remaining
        ]
        buttons.append([InlineKeyboardButton(f"⬅ Back to {artist[:20]}", callback_data=f"scene_back|{chat_id}")])
        query.edit_message_text(
            f"🌐 {artist} — all styles:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    # ── scene_style ───────────────────────────────────────────────────────────
    elif action == "scene_style":
        mem    = scene_memory.get(chat_id, {})
        artist = mem.get("artist", "")
        back_label = f"⬅ Back to {artist[:20]}" if artist else "⬅ Back"
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
            query.edit_message_text(f"{BOT_VERSION}\n\nTap a command to begin.", reply_markup=main_menu_markup())
            return

        card_text = _format_artist_card(artist, info)
        genre_buttons = []
        if info.get("genres"):
            row = []
            for g in info["genres"]:
                row.append(InlineKeyboardButton(g, callback_data=safe_callback(f"scene_style|{g}")))
                if len(row) == 2:
                    genre_buttons.append(row)
                    row = []
            if row:
                genre_buttons.append(row)

        buttons = []
        if info.get("bio"):
            buttons.append([InlineKeyboardButton("ℹ️ Artist bio", callback_data=safe_callback(f"artist_bio|{artist}"))])
        buttons.extend(genre_buttons)
        buttons.append([InlineKeyboardButton("── All styles ──────────────────", callback_data="noop")])
        for style, count in styles[:8]:
            buttons.append([InlineKeyboardButton(f"{style}  ({count})", callback_data=safe_callback(f"scene_style|{style}"))])
        if len(styles) > 8:
            buttons.append([InlineKeyboardButton(f"+ {len(styles)-8} more styles", callback_data=f"scene_more|{chat_id}")])
        buttons.append([InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")])
        query.edit_message_text(f"{card_text}\n\nChoose a style:", reply_markup=InlineKeyboardMarkup(buttons))

    # ── tags pagination ────────────────────────────────────────────────────────
    elif action == "tags_page":
        page        = int(value)
        sorted_tags = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
        total_pages = max(1, (len(sorted_tags) - 1) // TAGS_PAGE_SIZE + 1)
        start       = page * TAGS_PAGE_SIZE
        tag_list    = "\n".join(f"• {t}  ×{c}" for t, c in sorted_tags[start:start + TAGS_PAGE_SIZE])
        query.edit_message_text(
            f"🗂️ Tag Library — {len(sorted_tags)} genres (page {page + 1}/{total_pages})\n\n{tag_list}",
            reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page))
        )

    # ── tags edit mode ─────────────────────────────────────────────────────────
    elif action == "tags_edit":
        page        = int(value)
        sorted_tags = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
        total_pages = max(1, (len(sorted_tags) - 1) // TAGS_PAGE_SIZE + 1)
        start       = page * TAGS_PAGE_SIZE
        tag_list    = "\n".join(f"• {t}  ×{c}" for t, c in sorted_tags[start:start + TAGS_PAGE_SIZE])
        query.edit_message_text(
            f"🗂️ Tag Library — {len(sorted_tags)} genres (page {page + 1}/{total_pages})  ✏️ Edit mode\n\n{tag_list}",
            reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page, edit_mode=True))
        )

    # ── tag delete ────────────────────────────────────────────────────────────
    elif action == "tag_del":
        if value in tag_index:
            del tag_index[value]
            save_tag_index()
        sorted_tags = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
        if not sorted_tags:
            query.edit_message_text(
                "Tag library is empty.\n\nUse /scene <artist> to build it up.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")]])
            )
            return
        total_pages = max(1, (len(sorted_tags) - 1) // TAGS_PAGE_SIZE + 1)
        tag_list    = "\n".join(f"• {t}  ×{c}" for t, c in sorted_tags[:TAGS_PAGE_SIZE])
        query.edit_message_text(
            f"🗂️ Tag Library — {len(sorted_tags)} genres (page 1/{total_pages})  ✏️ Edit mode\n\n{tag_list}",
            reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, 0, edit_mode=True))
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
        url, err = spotify_build_playlist(chat_id, tracks, "Kurator playlist")
        if url:
            query.edit_message_text(
                "Playlist created.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🎧 Open in Spotify", url=url)],
                    [InlineKeyboardButton("⬅ Back",             callback_data=f"export_back|{key}")],
                ])
            )
        elif err == "not_premium":
            query.edit_message_text(
                "Playlist creation needs Spotify Premium.\n\nYou can still export via Soundiiz or browse track links.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data=f"export_back|{key}")]])
            )
        else:
            query.edit_message_text(
                "Could not create playlist. Try again.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data=f"export_back|{key}")]])
            )

    # ── export_back ───────────────────────────────────────────────────────────
    elif action == "export_back":
        query.edit_message_text(
            "Export options:",
            reply_markup=InlineKeyboardMarkup(_export_buttons(value))
        )

    # ── sp_expand: paginated track links (Spotify + Qobuz) ───────────────────
    elif action == "sp_expand":
        sub    = value.split("|", 1)
        key    = sub[0]
        page   = int(sub[1]) if len(sub) > 1 else 0
        tracks = _track_store.get(key, [])
        if not tracks:
            query.answer("Links expired. Generate a new playlist.", show_alert=True)
            return

        total_pages = max(1, (len(tracks) - 1) // TRACK_LINKS_PAGE + 1)
        start       = page * TRACK_LINKS_PAGE
        page_tracks = tracks[start:start + TRACK_LINKS_PAGE]

        buttons = []
        for t in page_tracks:
            parts = t.split(" - ", 1)
            label = f"{parts[0][:26]} – {parts[1][:18]}" if len(parts) == 2 else t[:46]
            buttons.append([InlineKeyboardButton(label, callback_data="noop")])
            buttons.append([
                InlineKeyboardButton("🎧 Spotify", url=spotify_url(t)),
                InlineKeyboardButton("🎵 Qobuz",   url=qobuz_url(t)),
            ])

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("◀ Prev", callback_data=f"sp_expand|{key}|{page-1}"))
        if (page + 1) < total_pages:
            nav.append(InlineKeyboardButton("Next ▶", callback_data=f"sp_expand|{key}|{page+1}"))
        if nav:
            buttons.append(nav)

        buttons.append([InlineKeyboardButton("⬅ Back",      callback_data=f"export_back|{key}")])
        buttons.append([InlineKeyboardButton("⬅ Main menu", callback_data="cmd|menu")])

        query.edit_message_text(
            f"Track links (page {page+1}/{total_pages}):",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

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
