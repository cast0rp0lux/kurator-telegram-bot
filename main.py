import os
import re
import json
import logging
import random
import time
import tempfile
import itertools
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

import requests
from flask import Flask, request as flask_request
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import CallbackQueryHandler, CommandHandler, MessageHandler, Filters, Updater

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# ─── Version ──────────────────────────────────────────────────────────────────
BOT_VERSION = "Kurator 📀 Music Discovery Engine (v6.5)"

# ─── Changelog ────────────────────────────────────────────────────────────────
CHANGELOG = {
    "6.5": {
        "date": "2026-04-14",
        "changes": [
            "Filtros underground más permisivos para géneros nicho",
            "500k-1M listeners: penalización -3 (antes -4), score=1 pasa threshold 1",
            "Hard cap subido de 750k a 1M listeners",
            "Expansión de pool niche: umbral subido de 60 a 80 artistas"
        ],
        "technical": [
            "_compute_underground_score: listeners 500k-1M → -3 (era -4)",
            "_filter_underground_artists: hard cap 750k→1M en threshold <=2",
            "_filter_underground_artists: expansión niche si pool < 80 (era 60)"
        ]
    },
    "6.4": {
        "date": "2026-04-14",
        "changes": [
            "/cancel ahora para también el mensaje 'Still building...' inmediatamente"
        ],
        "technical": [
            "_active_timers dict registra working message timer por chat_id",
            "_set_cancel para el timer activo vía _active_timers[chat_id]",
            "_register_timer llamado en los 4 handlers de generación principales"
        ]
    },
    "6.3": {
        "date": "2026-04-14",
        "changes": [
            "Comando /cancel oculto: cancela operación en curso y limpia estado"
        ],
        "technical": [
            "_cancel_flags dict + _is_cancelled/_set_cancel/_clear_cancel helpers",
            "Check de cancel al inicio de build| y tras obtener pool de artistas",
            "_clear_cancel al arrancar nueva generación (decade_confirm)"
        ]
    },
    "6.2": {
        "date": "2026-04-14",
        "changes": [
            "Fix mainstream en géneros: safety fallback con cap 750k listeners",
            "Fix fallback genre-match: usa attempted_names (no scored_names)",
            "Botón ☰ hamburguesa nativo de Telegram vía set_my_commands",
            "/explore como nuevo nombre de /map (🔍), /map sigue como alias"
        ],
        "technical": [
            "ThreadPoolExecutor workers 6→4 en _filter_underground_artists",
            "attempted_names trackea artistas procesados (incluso score -999)",
            "Safety fallback aplica _artist_listeners_cache cap antes de restaurar pool",
            "set_my_commands + setChatMenuButton type=commands al boot"
        ]
    },
    "6.1": {
        "date": "2026-04-13",
        "changes": [
            "Botón persistente 🍌 Menú en barra inferior de Telegram",
            "Fix /changelog: ADMIN_CHAT_ID actualizado",
            "Fix /changelog: html.escape() en entradas para evitar crash HTML"
        ],
        "technical": [
            "_persistent_keyboard(): ReplyKeyboardMarkup con botón Menú",
            "handle_text_reply: intercepta '🍌 Menú' antes de ForceReply logic",
            "start(): envía _persistent_keyboard() en primer uso post-onboarding"
        ]
    },
    "6.0": {
        "date": "2026-04-13",
        "changes": [
            "Near-genre base score reducido a 2 (antes 4): artistas sin género exacto penalizados",
            "Split rango listeners 300k-1M: 300k-500k(-2), 500k-1M(-4)",
            "Hard cap 750k listeners cuando threshold <= 2 en filtrado underground",
            "Seeds de Last.fm desde posición 30 en vez de 20 (evita semi-mainstream)"
        ],
        "technical": [
            "_compute_underground_score: near-genre score=2, exact score=4",
            "_compute_underground_score: listeners 500k-1M → score=0 (rechazado)",
            "_filter_underground_artists: hard cap activo en threshold <=2",
            "_get_era_artists_from_lastfm: seed_pool top_names[30:150]"
        ]
    },
    "5.9": {
        "date": "2026-04-13",
        "changes": [
            "FIX CRÍTICO: Fallback genre-match ahora filtra score >= 1",
            "Eliminados artistas mega-mainstream del fallback (Yes, Steely Dan, ELO)",
            "Eliminado texto 'Export:' duplicado, solo botón"
        ],
        "technical": [
            "Fallback nunca usa artistas con score negativo",
            "positive_scored = [(a, s) for a, s in scored if s >= 1]",
            "Fix UX: mensaje Export simplificado"
        ]
    },
    "5.8": {
        "date": "2026-04-13",
        "changes": [
            "Simplificación a regla universal de filtrado underground",
            "Threshold progresivo [5,4,3,2,1] aplicado a TODOS los géneros",
            "Eliminado filtro selectivo por pool size (≥80)"
        ],
        "technical": [
            "Código simplificado (-16 líneas en _filter_underground_artists)",
            "Una sola ruta de ejecución, sin casos especiales",
            "Escalabilidad mejorada para Oracle migration"
        ]
    },
    "5.7": {
        "date": "2026-04-13",
        "changes": [
            "Filtro selectivo por pool size implementado",
            "Pools ≥80 artistas exigen score ≥1",
            "Pools <80 artistas usan todos los scored"
        ],
        "technical": [
            "Protege géneros nicho (Power Pop, Pub Rock, Rockabilly)",
            "Filtra mainstream en géneros genéricos (Rock, Pop)"
        ]
    },
    "5.6": {
        "date": "2026-04-13",
        "changes": [
            "Penalización escalonada para mega-mainstream",
            "Listeners: <50K:+3, <100K:+2, <300K:+1",
            "Listeners: <1M:-2, <3M:-5, ≥3M:-8",
            "Elimina Beatles, Pink Floyd, Queen de playlists"
        ],
        "technical": [
            "Ajuste en _compute_underground_score()",
            "Sin cambios en lógica, solo thresholds de listeners"
        ]
    },
    "5.4": {
        "date": "2026-04-13",
        "changes": [
            "Normalización extendida de géneros (power pop/power-pop/powerpop)",
            "Pool expansion automático para géneros nicho (<60 artistas)",
            "Thresholds dinámicos según pool size",
            "Underground filtering en Genre+Era"
        ],
        "technical": [
            "normalize_tag_extended() retorna versiones base+compact",
            "_get_artist_tags_listeners() cacheado",
            "Incremento pool 20-40% en géneros con variantes"
        ]
    }
}

ADMIN_CHAT_ID = 939175455  # cast0rp0lux chat_id (only user who can see /changelog)

# ─── Environment ──────────────────────────────────────────────────────────────
LASTFM_USER    = "burbq"
LASTFM_API     = os.environ["LASTFM_API_KEY"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
DISCOGS_TOKEN  = os.environ["DISCOGS_TOKEN"]


# ─── Constants ────────────────────────────────────────────────────────────────
SCROBBLE_LIMIT      = 600
SEED_ARTISTS        = 35
SIMILAR_EXPANSION   = 60
PLAYLIST_SIZE       = 50
GENRE_PLAYLIST_SIZE = 50
RARE_PLAYLIST_SIZE  = 50
RARE_MAX_LISTENERS             = 500_000
SINGLES_FALLBACK_LISTENER_CAP  = 1_000_000  # only artists below this get the singles/EPs fallback
RARE_CANDIDATE_CAP  = 150
TAGS_PAGE_SIZE      = 10
CALLBACK_DATA_MAX   = 60
TRACK_STORE_MAX     = 20
TRACK_FETCH_LIMIT   = 50
TRACK_SKIP_TOP      = 5
TRACK_PLAYCOUNT_MAX = 500_000
HISTORY_EXPIRY_DAYS = 90
TRACK_LINKS_PAGE    = 5
MB_USER_AGENT       = "Kurator/4.8.10 (telegram bot)"
DECADES             = ["50s", "60s", "70s", "80s", "90s", "00s", "10s", "20s"]
DECADE_YEARS        = {
    "50s": (1950, 1959), "60s": (1960, 1969), "70s": (1970, 1979),
    "80s": (1980, 1989), "90s": (1990, 1999), "00s": (2000, 2009),
    "10s": (2010, 2019), "20s": (2020, 2029),
}

# Pending decade selections per chat_id: {chat_id: set of selected decades}
_pending_decades      = {}
# Pending tag multi-select state per chat_id
_pending_tag_deletes  = {}   # chat_id → set of tags selected for deletion
_pending_tag_restores = {}   # chat_id → set of tags selected for restore
# Pending generation actions per chat_id: {chat_id: {"action": str, "data": dict}}
_pending_gen       = {}
# Navigation history per chat_id: [{artist, display_name, styles, info}]
_nav_history       = {}
# Last "Mapping…" message per chat_id — deleted when new /map is called
_pending_map_msgs  = {}
# Recently used artists — prevents repetition across consecutive playlists
_recent_artists    = []
RECENT_ARTISTS_MAX = 60

# ─── Cancel flags ─────────────────────────────────────────────────────────────
_cancel_flags   = {}  # chat_id → bool
_active_timers  = {}  # chat_id → (sent_dict, timer) — working message activo
_progress_msgs  = {}  # chat_id → [Message] — intermediate messages to delete after playlist

def _is_cancelled(chat_id):
    return _cancel_flags.get(chat_id, False)

def _set_cancel(chat_id):
    _cancel_flags[chat_id] = True
    # Parar el working message timer si hay uno activo para este chat
    state = _active_timers.pop(chat_id, None)
    if state:
        sent, timer = state
        sent["done"] = True
        timer.cancel()

def _clear_cancel(chat_id):
    _cancel_flags.pop(chat_id, None)

def _register_timer(chat_id, sent, timer):
    """Registra el working message timer para que /cancel pueda pararlo."""
    _active_timers[chat_id] = (sent, timer)

def _unregister_timer(chat_id):
    _active_timers.pop(chat_id, None)

# ─── File paths ───────────────────────────────────────────────────────────────
HISTORY_FILE   = "history.json"
TAG_INDEX_FILE = "tag_index.json"
MAP_FILE       = "map_memory.json"
ONBOARDED_FILE = "onboarded.json"

# ─── Cache ────────────────────────────────────────────────────────────────────
_cache = {}

def cache_get(key):
    entry = _cache.get(key)
    if entry and time.time() - entry["ts"] < 600:
        return entry["value"]
    return None

def cache_set(key, value):
    _cache[key] = {"value": value, "ts": time.time()}

# ─── JSON helpers ─────────────────────────────────────────────────────────────

MONGO_URI      = os.environ.get("MONGO_URI", "")

# ─── MongoDB persistence ──────────────────────────────────────────────────────

_mongo_client = None
_mongo_db     = None

def _get_db():
    global _mongo_client, _mongo_db
    if _mongo_db is not None:
        return _mongo_db
    if not MONGO_URI:
        log.warning("MONGO_URI not set — using local JSON fallback")
        return None
    try:
        from pymongo import MongoClient
        _mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        _mongo_db     = _mongo_client["kurator"]
        log.info("MongoDB connected")
        return _mongo_db
    except Exception as e:
        log.error(f"MongoDB connection failed: {e}")
        return None

def _mongo_get(collection, key, default):
    db = _get_db()
    if db is None:
        return load_json_file(key, default)
    try:
        doc = db[collection].find_one({"_id": key})
        return doc["data"] if doc else default
    except Exception as e:
        log.error(f"MongoDB get {collection}/{key}: {e}")
        return default

def _mongo_set(collection, key, data):
    db = _get_db()
    if db is None:
        save_json_file(key, data)
        return
    try:
        db[collection].replace_one({"_id": key}, {"_id": key, "data": data}, upsert=True)
    except Exception as e:
        log.error(f"MongoDB set {collection}/{key}: {e}")

# ─── JSON file fallback (used when MONGO_URI not set) ─────────────────────────

def load_json_file(path, default):
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return default

def save_json_file(path, data):
    dir_ = os.path.dirname(os.path.abspath(path))
    try:
        fd, tmp = tempfile.mkstemp(dir=dir_)
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
        os.replace(tmp, path)
    except Exception as e:
        log.error(f"save_json failed for {path}: {e}")

# Keep backward compat aliases
def load_json(path, default):
    return load_json_file(path, default)

def save_json(path, data):
    save_json_file(path, data)

# ─── Persistent history ───────────────────────────────────────────────────────

def load_history():
    d   = _mongo_get("store", HISTORY_FILE, {})
    raw = d.get("tracks", {})
    if isinstance(raw, list):
        now    = time.time()
        tracks = {t: now for t in raw}
        log.info(f"Migrated {len(tracks)} tracks to timestamped history.")
    else:
        tracks = {k: float(v) for k, v in raw.items()}
    return {"tracks": tracks}

def save_history():
    _mongo_set("store", HISTORY_FILE, {"tracks": history["tracks"]})

def expire_history():
    cutoff  = time.time() - (HISTORY_EXPIRY_DAYS * 86400)
    before  = len(history["tracks"])
    history["tracks"] = {k: v for k, v in history["tracks"].items() if v > cutoff}
    expired = before - len(history["tracks"])
    if expired > 0:
        log.info(f"Expired {expired} tracks from history.")
        save_history()

def track_in_history(key):
    return key in history["tracks"]

def add_to_history(key):
    history["tracks"][key] = time.time()

history = load_history()
expire_history()

# ─── Onboarded users ──────────────────────────────────────────────────────────

_onboarded = set(_mongo_get("store", ONBOARDED_FILE, []))

def mark_onboarded(chat_id):
    _onboarded.add(str(chat_id))
    _mongo_set("store", ONBOARDED_FILE, list(_onboarded))

def is_onboarded(chat_id):
    return str(chat_id) in _onboarded

# ─── Persistent tag_index + map_memory ───────────────────────────────────────

tag_index      = _mongo_get("store", TAG_INDEX_FILE, {})
tag_blacklist  = set(_mongo_get("store", "tag_blacklist.json", []))
_map_raw       = _mongo_get("store", MAP_FILE, {})
map_memory     = {int(k): v for k, v in _map_raw.items()}

def save_tag_index():
    _mongo_set("store", TAG_INDEX_FILE, tag_index)

def save_tag_blacklist():
    _mongo_set("store", "tag_blacklist.json", list(tag_blacklist))

def save_map_memory():
    _mongo_set("store", MAP_FILE, {str(k): v for k, v in map_memory.items()})

# ─── URL helpers ──────────────────────────────────────────────────────────────

def spotify_url(track):
    return f"https://open.spotify.com/search/{quote(track)}"

def spotify_album_url(artist, album):
    return f"https://open.spotify.com/search/{quote(artist + ' ' + album)}"

# ─── Last.fm ──────────────────────────────────────────────────────────────────

def lastfm(method, **params):
    payload = {"method": method, "api_key": LASTFM_API, "format": "json", **params}
    for attempt in range(2):
        try:
            r = requests.get("https://ws.audioscrobbler.com/2.0/", params=payload, timeout=20)
            if r.status_code == 200:
                return r.json()
            log.warning(f"Last.fm {method} HTTP {r.status_code}")
        except Exception as e:
            log.error(f"Last.fm ({method}) attempt {attempt+1}: {e}")
    return {}

def normalize(name):
    return name.lower().strip()

def normalize_artist(name):
    """Normalize artist name for deduplication — strips accents, suffixes like '& The X'."""
    import re, unicodedata
    n = unicodedata.normalize("NFD", name.lower().strip())
    n = "".join(c for c in n if unicodedata.category(c) != "Mn")  # strip accents
    n = re.sub(r"\s*(&|and)\s+(the|his|her|their)\s+\w+.*$", '', n).strip()
    n = re.sub(r'\s*with\s+.*$', '', n).strip()
    return n

def _clean_artist_name(name):
    """
    Clean Discogs-style artist names:
    - Remove asterisks: 'Tony Williams*' → 'Tony Williams'
    - Remove disambiguation numbers: 'Solution (4)' → 'Solution'
    - Fix broken UTF-8 encoding: 'WuyÃ©' → 'Wuyé'
    """
    import re
    # Fix broken UTF-8 (latin1 misread as utf-8)
    try:
        cleaned = name.encode("latin1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        cleaned = name
    # Remove trailing asterisk
    cleaned = re.sub(r'\*+$', '', cleaned).strip()
    # Remove Discogs disambiguation numbers like (4), (19)
    cleaned = re.sub(r'\s*\(\d+\)\s*$', '', cleaned).strip()
    return cleaned

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

def _get_listeners_cached(artist):
    """Return cached listener count, fetching from Last.fm on first access."""
    if artist not in _artist_listeners_cache:
        _, count = _fetch_listeners(artist)
        _artist_listeners_cache[artist] = count
    return _artist_listeners_cache[artist]

def expand_artist_graph(seed_artists):
    pool = set()
    with ThreadPoolExecutor(max_workers=8) as ex:
        for f in as_completed([ex.submit(_fetch_similar_names, a) for a in seed_artists]):
            try: pool.update(f.result())
            except Exception as e: log.error(f"expand L1: {e}")
    return list(pool)

def expand_artist_graph_deep(seed_artists):
    level1 = set(expand_artist_graph(seed_artists))
    level2 = set()
    with ThreadPoolExecutor(max_workers=8) as ex:
        for f in as_completed([ex.submit(_fetch_similar_names, a)
                               for a in random.sample(list(level1), min(len(level1), 30))]):
            try: level2.update(f.result())
            except Exception: pass
    return list(level2 - level1 - set(seed_artists))

def expand_artist_graph_rare(seed_artists):
    candidates = expand_artist_graph(seed_artists)
    if len(candidates) > RARE_CANDIDATE_CAP:
        candidates = random.sample(candidates, RARE_CANDIDATE_CAP)
    filtered = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        for f in as_completed([ex.submit(_fetch_listeners, a) for a in candidates]):
            try:
                artist, listeners = f.result()
                if 0 < listeners < RARE_MAX_LISTENERS:
                    filtered.append(artist)
            except Exception: pass
    return filtered

# ─── Similar artists (trail) ─────────────────────────────────────────────────

def _expand_trail(artist, hops):
    level1 = set(s["name"] for s in
                 lastfm("artist.getsimilar", artist=artist, limit=60)
                 .get("similarartists", {}).get("artist", []))
    if hops == 1:
        return list(level1)
    level2 = set()
    with ThreadPoolExecutor(max_workers=8) as ex:
        for f in as_completed([ex.submit(_fetch_similar_names, a)
                               for a in random.sample(list(level1), min(len(level1), 20))]):
            try: level2.update(f.result())
            except: pass
    level2 -= level1 | {artist}
    return list(level1 | level2)

# ─── Track selection ──────────────────────────────────────────────────────────

def _clean_track_title(title):
    """Remove all noise from track titles for cleaner Soundiiz/Qobuz matching."""
    import re, html
    cleaned = html.unescape(title)  # Fix HTML entities: &#039; → ' &amp; → & etc.
    # Remove numeric prefixes like "03-", "06 - "
    cleaned = re.sub(r'^\d{1,3}\s*[-\.]\s*', '', cleaned).strip()
    # Remove = Translation suffix
    cleaned = re.sub(r'\s*=\s*.+$', '', cleaned).strip()
    # Remove ALL content in parentheses and brackets
    cleaned = re.sub(r'\s*[\(\[][^\)\]]*[\)\]]', '', cleaned).strip()
    # Remove dash-based suffixes — covers remaster, mix, version, edit variants
    cleaned = re.sub(r'\s*-\s*\d{4}\s+.*$', '', cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r'\s*-\s*digitally\s+remaster(?:ed)?\s*\d*', '', cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"\s*-\s*\d{1,2}[''\"]\s*.*$", '', cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r'\s*-\s*(remaster(?:ed)?|digital remaster|alternative mix|alt\.? mix|radio edit|single version|album version|extended|mono|stereo|demo|live).*$', '', cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"\s*-\s*\d{2,4}\s*$", '', cleaned, flags=re.IGNORECASE).strip()
    # Remove trailing " - " artifacts
    cleaned = re.sub(r'\s*-\s*$', '', cleaned).strip()
    # Capitalize only if title is fully lowercase — preserve ALL CAPS and Mixed Case
    if cleaned and cleaned == cleaned.lower():
        cleaned = cleaned.title()
    return cleaned or title

def _is_live_track(name):
    """Return True if track name indicates a live recording."""
    low = name.lower()
    return ("live" in low)

OBSCURE_LISTENER_THRESHOLD = 100_000  # below this → skip TOP filter disabled

SEASONAL_TRACK_BLACKLIST = {
    "have yourself a merry little christmas",
    "white christmas", "jingle bells", "silent night",
    "rudolph the red nosed reindeer", "santa claus is coming to town",
    "o come all ye faithful", "deck the halls", "o holy night",
    "happy christmas", "happy xmas", "feliz navidad", "feliz navidad",
    "frosty the snowman", "winter wonderland", "let it snow",
    "the christmas song", "chestnuts roasting on an open fire",
    "away in a manger", "hark the herald angels sing",
    "we wish you a merry christmas", "auld lang syne",
}

def _fetch_track_from_early_albums(artist, decade_years=None):
    """
    Fetch a track from the artist's early studio albums via MusicBrainz + Last.fm.
    When decade_years is specified (era mode):
    - Only uses albums within that era range
    - Returns None if no era-matching albums found (no fallback — Discogs is source of truth)
    When no decade_years:
    - Falls back to _fetch_top_track if no MB data
    """
    try:
        # Step 1: find MBID
        mbid, _ = _mb_find_artist(artist)
        if not mbid:
            # No MB data — only fallback if no era constraint
            return None if decade_years else _fetch_top_track(artist)

        # Step 2: get studio albums ordered by year
        albums = _mb_studio_albums(mbid)
        if not albums:
            if decade_years:
                lo, hi = decade_years
                return _try_track_from_singles_eps(artist, mbid, lo, hi, SINGLES_FALLBACK_LISTENER_CAP)
            return _fetch_top_track(artist)

        # Step 3: filter by era if specified, else take first 4
        if decade_years:
            lo, hi = decade_years
            era_albums = [a for a in albums if a.get("year") and lo <= int(a["year"]) <= hi]
            if not era_albums:
                return _try_track_from_singles_eps(artist, mbid, lo, hi, SINGLES_FALLBACK_LISTENER_CAP)
            candidate_albums = era_albums[:5]
        else:
            candidate_albums = albums[:4]

        if not candidate_albums:
            return None if decade_years else _fetch_top_track(artist)

        # Step 4: try each album for a valid track
        random.shuffle(candidate_albums)
        for album in candidate_albums:
            try:
                data   = lastfm("album.getinfo", artist=artist, album=album["title"])
                tracks = data.get("album", {}).get("tracks", {}).get("track", [])
                if not tracks:
                    continue
                if isinstance(tracks, dict):
                    tracks = [tracks]
                # Filter live tracks
                valid = [t for t in tracks if not _is_live_track(t.get("name", ""))]
                if not valid:
                    valid = tracks
                random.shuffle(valid)
                for t in valid:
                    name  = t.get("name", "")
                    clean = _clean_track_title(name)
                    if clean.lower() in SEASONAL_TRACK_BLACKLIST:
                        continue
                    key   = f"{normalize(artist)}-{normalize(clean)}"
                    if not track_in_history(key):
                        return (artist, clean, key)
            except Exception:
                continue

    except Exception as e:
        log.error(f"_fetch_track_from_early_albums {artist}: {e}")

    # Final fallback — only when no era constraint
    return None if decade_years else _fetch_top_track(artist)

def _fetch_top_track(artist):
    """
    Fetch top track from Last.fm. For obscure artists (<100K listeners)
    disables the TRACK_SKIP_TOP filter so their best track is always reachable.
    """
    # Check if artist is obscure — skip the top-track skip for them
    _, listeners = _fetch_listeners(artist)
    is_obscure   = 0 < listeners < OBSCURE_LISTENER_THRESHOLD
    if is_obscure:
        log.info(f"Obscure artist '{artist}' ({listeners:,} listeners) — skip filter disabled")

    data = lastfm("artist.gettoptracks", artist=artist, limit=TRACK_FETCH_LIMIT)
    top  = data.get("toptracks", {}).get("track", [])

    if is_obscure:
        # Don't skip top tracks for obscure artists — any track is a discovery
        pool = top
    else:
        pool = top[TRACK_SKIP_TOP:] or top

    filtered = [t for t in pool
                if int(t.get("playcount", 0) or 0) < TRACK_PLAYCOUNT_MAX
                and not _is_live_track(t["name"])] or pool
    random.shuffle(filtered)
    for t in filtered:
        clean_name = _clean_track_title(t["name"])
        if clean_name.lower() in SEASONAL_TRACK_BLACKLIST:
            continue
        key = f"{normalize(artist)}-{normalize(clean_name)}"
        if not track_in_history(key):
            return (artist, clean_name, key)
    return None

def select_tracks(artists, size=None, skip_recent=True):
    global _recent_artists
    target     = size or PLAYLIST_SIZE
    tracks     = []
    keys_added = set()
    artists_used = []
    recent_set = {normalize(r) for r in _recent_artists} if skip_recent else set()
    artists = [a for a in artists
               if not _is_blacklisted(a)
               and normalize(a) not in recent_set]
    random.shuffle(artists)
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(_fetch_track_from_early_albums, a) for a in artists[:target * 7]]
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
                        artists_used.append(artist)
            except Exception as e: log.error(f"select_tracks: {e}")
    for key in keys_added:
        add_to_history(key)
    save_history()
    _recent_artists.extend(artists_used)
    _recent_artists = _recent_artists[-RECENT_ARTISTS_MAX:]
    return tracks

# ─── Era-based artist pool — Discogs approach ────────────────────────────────

# Artists to always exclude
ARTIST_BLACKLIST = {
    "various", "various artists", "various interprets", "various interpretations",
    "unknown artist", "unknown", "va", "v.a.", "v.a", "soundtrack",
    "original soundtrack", "ost", "cast recording", "anonymous"
}

def _is_blacklisted(name):
    return name.lower().strip() in ARTIST_BLACKLIST

# Curated styles per decade — balanced across genres/origins
DECADE_STYLES = {
    "50s": ["Rockabilly", "Rock & Roll", "Doo Wop", "Jump Blues", "Western Swing",
            "Country", "Exotica", "Easy Listening", "Rhythm & Blues",
            "Cool Jazz", "Hard Bop", "Mambo"],
    "60s": ["Psychedelic Rock", "Garage Rock", "Beat", "Freakbeat", "Mod",
            "Surf", "Folk Rock", "Prog Rock", "Krautrock", "Art Rock",
            "British Blues", "Baroque Pop", "Chamber Pop",
            "Soul", "Funk", "Bossa Nova", "Latin",
            "Acid Rock", "Merseybeat", "Sunshine Pop", "Raga Rock",
            "Free Jazz", "Spaghetti Western"],
    "70s": ["Krautrock", "Prog Rock", "Canterbury Scene", "Jazz-Rock",
            "Glam", "Proto-Punk", "Cosmic Rock", "Fusion", "Ambient",
            "Folk Rock", "Art Rock", "Experimental", "Space Rock",
            "Disco", "Reggae", "Dub", "Kosmische", "Minimal",
            "Neue Deutsche Welle", "Stoner Rock"],
    "80s": ["Post-Punk", "Cold Wave", "Minimal Wave", "Darkwave", "Synthpop",
            "No Wave", "Industrial", "Dream Pop", "Gothic Rock",
            "Indie Rock", "New Wave", "Noise Rock", "EBM",
            "New Romantic", "Hardcore", "Jangle Pop",
            "Ethereal Wave", "C86", "Paisley Underground", "Thrash Metal"],
    "90s": ["Shoegaze", "Dream Pop", "Slowcore", "Math Rock", "Post-Rock",
            "Lo-Fi", "Noise Pop", "Emo", "Post-Hardcore", "Indie Pop",
            "Alternative Rock", "Britpop", "Grunge", "Trip-Hop",
            "Drum n Bass", "Stoner Rock", "Ambient Techno", "Noise Rock"],
    "00s": ["Post-Punk Revival", "Noise Pop", "Freak Folk", "Chamber Pop",
            "Indie Folk", "Math Rock", "Neo-Psychedelia", "Experimental Rock",
            "New Weird America", "Free Folk", "Post-Metal", "Drone Metal"],
    "10s": ["Chillwave", "Witch House", "Ambient Pop", "Indie Folk",
            "Post-Rock", "Drone", "Synth-Pop", "Vaporwave", "Hauntology"],
    "20s": ["Lo-Fi", "Bedroom Pop", "Ambient", "Indie Pop", "Experimental",
            "Neo-Soul", "Hyperpop"],
}

# community.have thresholds per mode
ERA_HAVE_RANGES = {
    "playlist": (10,  15000),  # lowered to catch obscure era releases
    "dig":      (5,   5000),   # less obvious
    "rare":     (3,   500),    # very obscure releases
}

# Genre relationship graph — near/mid/far distances for expansion
GENRE_GRAPH = {'rock': {'near': ['alternative rock', 'hard rock', 'indie rock', 'classic rock'], 'mid': ['punk', 'metal', 'pop rock', 'psychedelic rock'], 'far': ['folk', 'electronic', 'blues']}, 'classic rock': {'near': ['rock', 'hard rock', 'blues rock'], 'mid': ['heavy metal', 'psychedelic rock'], 'far': ['folk rock', 'country rock']}, 'alternative rock': {'near': ['indie rock', 'grunge', 'post-rock', 'noise rock'], 'mid': ['shoegaze', 'post-punk', 'emo'], 'far': ['electronic rock', 'folk rock']}, 'indie rock': {'near': ['alternative rock', 'dream pop', 'lo-fi', 'indie pop'], 'mid': ['post-punk', 'folk', 'jangle pop'], 'far': ['electronic', 'ambient']}, 'grunge': {'near': ['alternative rock', 'indie rock', 'noise rock'], 'mid': ['heavy metal', 'punk'], 'far': ['post-punk']}, 'post-rock': {'near': ['ambient', 'experimental', 'indie rock', 'math rock'], 'mid': ['shoegaze', 'minimalism', 'krautrock'], 'far': ['classical', 'electronic']}, 'math rock': {'near': ['post-rock', 'indie rock', 'noise rock'], 'mid': ['experimental', 'progressive rock'], 'far': ['jazz']}, 'shoegaze': {'near': ['dream pop', 'noise pop', 'indie rock', 'slowcore'], 'mid': ['post-rock', 'ambient'], 'far': ['electronic']}, 'dream pop': {'near': ['shoegaze', 'indie pop', 'ambient pop'], 'mid': ['post-punk', 'slowcore'], 'far': ['ambient']}, 'slowcore': {'near': ['dream pop', 'shoegaze', 'indie rock'], 'mid': ['post-rock', 'folk'], 'far': ['ambient']}, 'noise rock': {'near': ['post-punk', 'alternative rock', 'no wave'], 'mid': ['noise', 'experimental rock'], 'far': ['industrial']}, 'no wave': {'near': ['noise rock', 'post-punk', 'avant-garde'], 'mid': ['noise', 'experimental'], 'far': ['industrial']}, 'punk': {'near': ['hardcore punk', 'post-punk', 'garage rock', 'pop punk'], 'mid': ['alternative rock', 'oi'], 'far': ['metal']}, 'pop punk': {'near': ['punk', 'power pop', 'alternative rock'], 'mid': ['emo'], 'far': ['indie rock']}, 'oi': {'near': ['punk', 'hardcore punk', 'street punk'], 'mid': ['ska punk'], 'far': ['metal']}, 'street punk': {'near': ['oi', 'punk', 'hardcore punk'], 'mid': ['crust punk'], 'far': ['metal']}, 'crust punk': {'near': ['d-beat', 'hardcore punk', 'street punk'], 'mid': ['grindcore', 'noise'], 'far': ['metal']}, 'hardcore punk': {'near': ['punk', 'post-hardcore', 'd-beat', 'powerviolence'], 'mid': ['metalcore', 'crossover thrash'], 'far': ['noise rock']}, 'd-beat': {'near': ['hardcore punk', 'crust punk'], 'mid': ['grindcore'], 'far': ['metal']}, 'powerviolence': {'near': ['hardcore punk', 'grindcore'], 'mid': ['noise', 'sludge metal'], 'far': ['metal']}, 'post-hardcore': {'near': ['hardcore punk', 'emo', 'noise rock'], 'mid': ['metalcore', 'math rock'], 'far': ['alternative rock']}, 'emo': {'near': ['post-hardcore', 'indie rock', 'pop punk'], 'mid': ['alternative rock', 'screamo'], 'far': ['folk']}, 'screamo': {'near': ['emo', 'post-hardcore', 'hardcore punk'], 'mid': ['noise rock'], 'far': ['black metal']}, 'post-punk': {'near': ['new wave', 'gothic rock', 'indie rock', 'cold wave'], 'mid': ['experimental', 'noise rock'], 'far': ['electronic']}, 'cold wave': {'near': ['post-punk', 'gothic rock', 'darkwave'], 'mid': ['synth pop', 'industrial'], 'far': ['ambient']}, 'gothic rock': {'near': ['post-punk', 'darkwave', 'cold wave'], 'mid': ['industrial', 'death rock'], 'far': ['metal']}, 'death rock': {'near': ['gothic rock', 'punk', 'post-punk'], 'mid': ['darkwave'], 'far': ['metal']}, 'garage rock': {'near': ['punk', 'rock', 'blues rock', 'surf rock'], 'mid': ['psychedelic rock', 'alternative rock'], 'far': ['folk rock']}, 'surf rock': {'near': ['garage rock', 'rock', 'instrumental rock'], 'mid': ['psychedelic rock'], 'far': ['folk']}, 'psychedelic rock': {'near': ['garage rock', 'krautrock', 'folk rock', 'space rock'], 'mid': ['progressive rock', 'experimental rock'], 'far': ['ambient']}, 'space rock': {'near': ['psychedelic rock', 'krautrock', 'post-rock'], 'mid': ['ambient', 'progressive rock'], 'far': ['electronic']}, 'krautrock': {'near': ['psychedelic rock', 'space rock', 'electronic'], 'mid': ['post-rock', 'ambient', 'experimental'], 'far': ['minimalism']}, 'progressive rock': {'near': ['art rock', 'psychedelic rock', 'symphonic rock'], 'mid': ['jazz fusion', 'math rock'], 'far': ['classical']}, 'art rock': {'near': ['progressive rock', 'experimental rock', 'glam rock'], 'mid': ['post-punk', 'avant-garde'], 'far': ['classical']}, 'symphonic rock': {'near': ['progressive rock', 'art rock'], 'mid': ['classical', 'power metal'], 'far': ['folk']}, 'glam rock': {'near': ['art rock', 'rock', 'hard rock'], 'mid': ['pop rock', 'punk'], 'far': ['electronic']}, 'experimental rock': {'near': ['noise rock', 'art rock', 'avant-garde'], 'mid': ['post-rock', 'electronic'], 'far': ['ambient']}, 'metal': {'near': ['heavy metal', 'doom metal', 'thrash metal', 'hard rock'], 'mid': ['industrial metal', 'black metal', 'death metal'], 'far': ['punk', 'classical']}, 'heavy metal': {'near': ['metal', 'hard rock', 'doom metal'], 'mid': ['thrash metal', 'speed metal'], 'far': ['punk']}, 'hard rock': {'near': ['rock', 'heavy metal', 'blues rock'], 'mid': ['glam rock', 'classic rock'], 'far': ['punk']}, 'blues rock': {'near': ['hard rock', 'classic rock', 'blues'], 'mid': ['rock', 'garage rock'], 'far': ['folk']}, 'doom metal': {'near': ['stoner rock', 'sludge metal', 'death doom'], 'mid': ['post-metal', 'drone'], 'far': ['ambient']}, 'stoner rock': {'near': ['doom metal', 'sludge metal', 'psychedelic rock'], 'mid': ['hard rock', 'desert rock'], 'far': ['blues']}, 'desert rock': {'near': ['stoner rock', 'psychedelic rock', 'hard rock'], 'mid': ['garage rock'], 'far': ['blues']}, 'sludge metal': {'near': ['doom metal', 'post-metal', 'hardcore punk'], 'mid': ['noise rock', 'powerviolence'], 'far': ['ambient']}, 'post-metal': {'near': ['sludge metal', 'doom metal', 'post-rock'], 'mid': ['ambient', 'experimental'], 'far': ['drone']}, 'death doom': {'near': ['doom metal', 'death metal'], 'mid': ['gothic metal'], 'far': ['ambient']}, 'gothic metal': {'near': ['doom metal', 'death doom', 'gothic rock'], 'mid': ['symphonic metal'], 'far': ['darkwave']}, 'symphonic metal': {'near': ['gothic metal', 'power metal', 'symphonic rock'], 'mid': ['progressive metal'], 'far': ['classical']}, 'thrash metal': {'near': ['speed metal', 'crossover thrash', 'heavy metal'], 'mid': ['death metal', 'black metal'], 'far': ['hardcore punk']}, 'speed metal': {'near': ['thrash metal', 'heavy metal', 'power metal'], 'mid': ['black metal'], 'far': ['hardcore punk']}, 'power metal': {'near': ['speed metal', 'heavy metal', 'symphonic metal'], 'mid': ['progressive metal', 'folk metal'], 'far': ['classical']}, 'progressive metal': {'near': ['power metal', 'technical death metal', 'math rock'], 'mid': ['jazz fusion', 'progressive rock'], 'far': ['classical']}, 'death metal': {'near': ['thrash metal', 'black metal', 'grindcore'], 'mid': ['doom metal', 'technical death metal'], 'far': ['noise']}, 'technical death metal': {'near': ['death metal', 'progressive metal'], 'mid': ['math rock', 'grindcore'], 'far': ['jazz']}, 'black metal': {'near': ['death metal', 'thrash metal', 'atmospheric black metal'], 'mid': ['post-metal', 'gothic metal'], 'far': ['ambient']}, 'atmospheric black metal': {'near': ['black metal', 'post-black metal', 'post-metal'], 'mid': ['ambient', 'shoegaze'], 'far': ['folk']}, 'post-black metal': {'near': ['atmospheric black metal', 'black metal', 'post-rock'], 'mid': ['shoegaze', 'post-metal'], 'far': ['ambient']}, 'folk metal': {'near': ['power metal', 'heavy metal', 'folk rock'], 'mid': ['pagan metal'], 'far': ['folk']}, 'pagan metal': {'near': ['folk metal', 'black metal'], 'mid': ['viking metal'], 'far': ['folk']}, 'viking metal': {'near': ['pagan metal', 'black metal'], 'mid': ['folk metal'], 'far': ['folk']}, 'metalcore': {'near': ['post-hardcore', 'hardcore punk', 'thrash metal'], 'mid': ['deathcore', 'emo'], 'far': ['alternative rock']}, 'deathcore': {'near': ['metalcore', 'death metal', 'hardcore punk'], 'mid': ['grindcore'], 'far': ['noise']}, 'crossover thrash': {'near': ['thrash metal', 'hardcore punk', 'speed metal'], 'mid': ['punk', 'death metal'], 'far': ['noise rock']}, 'grindcore': {'near': ['death metal', 'hardcore punk', 'powerviolence'], 'mid': ['noise', 'sludge metal'], 'far': ['experimental']}, 'industrial metal': {'near': ['industrial', 'metal', 'noise rock'], 'mid': ['electronic', 'hardcore punk'], 'far': ['ambient']}, 'electronic': {'near': ['techno', 'house', 'ambient', 'synth pop'], 'mid': ['idm', 'electro', 'experimental'], 'far': ['pop', 'jazz']}, 'techno': {'near': ['minimal techno', 'industrial techno', 'acid techno', 'detroit techno'], 'mid': ['electro', 'trance', 'hard techno'], 'far': ['house']}, 'detroit techno': {'near': ['techno', 'electro'], 'mid': ['minimal techno', 'house'], 'far': ['electronic']}, 'minimal techno': {'near': ['techno', 'deep tech', 'microhouse'], 'mid': ['house', 'ambient'], 'far': ['idm']}, 'microhouse': {'near': ['minimal techno', 'tech house', 'house'], 'mid': ['idm', 'ambient'], 'far': ['electronic']}, 'acid techno': {'near': ['techno', 'acid house', 'hard techno'], 'mid': ['trance'], 'far': ['industrial']}, 'industrial techno': {'near': ['techno', 'industrial', 'hard techno'], 'mid': ['noise', 'ebm'], 'far': ['ambient']}, 'hard techno': {'near': ['industrial techno', 'schranz', 'acid techno'], 'mid': ['hard trance', 'techno'], 'far': ['industrial']}, 'schranz': {'near': ['hard techno', 'industrial techno'], 'mid': ['techno', 'noise'], 'far': ['industrial']}, 'ebm': {'near': ['industrial', 'synth pop', 'darkwave'], 'mid': ['industrial techno', 'cold wave'], 'far': ['electronic']}, 'house': {'near': ['deep house', 'tech house', 'acid house', 'chicago house'], 'mid': ['disco', 'garage', 'afro house'], 'far': ['techno']}, 'chicago house': {'near': ['house', 'deep house', 'acid house'], 'mid': ['disco', 'soul'], 'far': ['funk']}, 'deep house': {'near': ['house', 'soulful house', 'organic house'], 'mid': ['techno', 'afro house'], 'far': ['jazz']}, 'soulful house': {'near': ['deep house', 'gospel house', 'house'], 'mid': ['soul', 'rnb'], 'far': ['disco']}, 'afro house': {'near': ['deep house', 'house', 'afrobeat'], 'mid': ['amapiano'], 'far': ['tribal']}, 'amapiano': {'near': ['afro house', 'deep house'], 'mid': ['afrobeat'], 'far': ['house']}, 'tech house': {'near': ['house', 'minimal techno', 'microhouse'], 'mid': ['techno'], 'far': ['electro']}, 'acid house': {'near': ['house', 'acid techno', 'chicago house'], 'mid': ['rave'], 'far': ['trance']}, 'rave': {'near': ['acid house', 'hardcore rave', 'techno'], 'mid': ['drum and bass', 'trance'], 'far': ['ambient']}, 'hardcore rave': {'near': ['rave', 'gabber', 'happy hardcore'], 'mid': ['drum and bass'], 'far': ['techno']}, 'gabber': {'near': ['hardcore rave', 'industrial techno'], 'mid': ['hard techno'], 'far': ['noise']}, 'happy hardcore': {'near': ['hardcore rave', 'trance'], 'mid': ['eurodance'], 'far': ['pop']}, 'eurodance': {'near': ['dance pop', 'techno', 'happy hardcore'], 'mid': ['trance', 'synth pop'], 'far': ['pop']}, 'ambient': {'near': ['drone', 'minimalism', 'ambient techno', 'dark ambient'], 'mid': ['post-rock', 'new age'], 'far': ['classical', 'noise']}, 'dark ambient': {'near': ['ambient', 'drone', 'industrial'], 'mid': ['noise', 'post-industrial'], 'far': ['black metal']}, 'ambient techno': {'near': ['ambient', 'techno', 'idm'], 'mid': ['minimal techno'], 'far': ['new age']}, 'drone': {'near': ['ambient', 'doom metal', 'noise'], 'mid': ['minimalism', 'dark ambient'], 'far': ['experimental']}, 'new age': {'near': ['ambient', 'new age classical'], 'mid': ['minimalism', 'world'], 'far': ['classical']}, 'idm': {'near': ['glitch', 'ambient', 'experimental', 'braindance'], 'mid': ['techno', 'electroacoustic'], 'far': ['electro']}, 'glitch': {'near': ['idm', 'experimental', 'microsound'], 'mid': ['ambient', 'noise'], 'far': ['electronic']}, 'braindance': {'near': ['idm', 'experimental', 'electronic'], 'mid': ['glitch'], 'far': ['ambient']}, 'electro': {'near': ['techno', 'breakbeat', 'electro funk'], 'mid': ['hip hop', 'idm'], 'far': ['house']}, 'electro funk': {'near': ['electro', 'funk', 'hip hop'], 'mid': ['disco', 'synth pop'], 'far': ['soul']}, 'trance': {'near': ['goa trance', 'psytrance', 'hard trance', 'progressive trance'], 'mid': ['techno', 'eurodance'], 'far': ['house']}, 'goa trance': {'near': ['psytrance', 'trance'], 'mid': ['techno'], 'far': ['ambient']}, 'psytrance': {'near': ['goa trance', 'trance', 'full on'], 'mid': ['techno'], 'far': ['ambient']}, 'hard trance': {'near': ['trance', 'hard techno', 'hardcore rave'], 'mid': ['techno'], 'far': ['house']}, 'progressive trance': {'near': ['trance', 'progressive house'], 'mid': ['techno'], 'far': ['ambient']}, 'progressive house': {'near': ['house', 'progressive trance', 'deep house'], 'mid': ['techno'], 'far': ['ambient']}, 'drum and bass': {'near': ['jungle', 'neurofunk', 'liquid dnb', 'techstep'], 'mid': ['breakbeat', 'rave'], 'far': ['dubstep']}, 'jungle': {'near': ['drum and bass', 'rave', 'hardcore rave'], 'mid': ['reggae', 'dub'], 'far': ['hip hop']}, 'neurofunk': {'near': ['drum and bass', 'techstep'], 'mid': ['industrial', 'experimental'], 'far': ['techno']}, 'liquid dnb': {'near': ['drum and bass', 'soulful dnb'], 'mid': ['deep house'], 'far': ['ambient']}, 'techstep': {'near': ['neurofunk', 'drum and bass'], 'mid': ['industrial techno'], 'far': ['techno']}, 'dubstep': {'near': ['uk garage', 'drum and bass', 'dub'], 'mid': ['electronic', 'grime'], 'far': ['trap']}, 'grime': {'near': ['uk garage', 'hip hop', 'dubstep'], 'mid': ['drill', 'electronic'], 'far': ['dance pop']}, 'disco': {'near': ['italo disco', 'euro disco', 'funk', 'soul'], 'mid': ['house'], 'far': ['pop']}, 'italo disco': {'near': ['euro disco', 'space disco', 'synth pop', 'disco'], 'mid': ['house', 'new wave'], 'far': ['pop']}, 'euro disco': {'near': ['italo disco', 'disco', 'synth pop'], 'mid': ['eurodance'], 'far': ['pop']}, 'space disco': {'near': ['italo disco', 'cosmic disco', 'disco'], 'mid': ['house'], 'far': ['electronic']}, 'cosmic disco': {'near': ['space disco', 'italo disco', 'krautrock'], 'mid': ['house'], 'far': ['ambient']}, 'synth pop': {'near': ['new wave', 'electropop', 'darkwave'], 'mid': ['italo disco', 'dream pop'], 'far': ['pop']}, 'electropop': {'near': ['synth pop', 'pop', 'dance pop'], 'mid': ['electronic', 'indie pop'], 'far': ['house']}, 'new wave': {'near': ['post-punk', 'synth pop', 'power pop'], 'mid': ['cold wave', 'electronic'], 'far': ['rock']}, 'power pop': {'near': ['new wave', 'pop rock', 'punk'], 'mid': ['indie pop', 'jangle pop'], 'far': ['alternative rock']}, 'jangle pop': {'near': ['indie pop', 'power pop', 'dream pop'], 'mid': ['indie rock', 'folk rock'], 'far': ['folk']}, 'indie pop': {'near': ['indie rock', 'dream pop', 'jangle pop'], 'mid': ['lo-fi', 'baroque pop'], 'far': ['electronic']}, 'baroque pop': {'near': ['indie pop', 'chamber pop', 'folk pop'], 'mid': ['art rock'], 'far': ['classical']}, 'chamber pop': {'near': ['baroque pop', 'indie pop', 'orchestral pop'], 'mid': ['art rock', 'folk'], 'far': ['classical']}, 'lo-fi': {'near': ['indie rock', 'bedroom pop', 'noise pop'], 'mid': ['alternative rock', 'folk'], 'far': ['ambient']}, 'bedroom pop': {'near': ['lo-fi', 'indie pop', 'dream pop'], 'mid': ['chillwave'], 'far': ['electronic']}, 'chillwave': {'near': ['bedroom pop', 'dream pop', 'synth pop'], 'mid': ['lo-fi', 'ambient'], 'far': ['electronic']}, 'noise pop': {'near': ['shoegaze', 'lo-fi', 'indie rock'], 'mid': ['noise rock'], 'far': ['noise']}, 'darkwave': {'near': ['post-punk', 'gothic rock', 'cold wave', 'ebm'], 'mid': ['synth pop', 'industrial'], 'far': ['ambient']}, 'industrial': {'near': ['industrial techno', 'noise', 'ebm', 'post-industrial'], 'mid': ['dark ambient', 'metal'], 'far': ['experimental']}, 'post-industrial': {'near': ['industrial', 'noise', 'experimental'], 'mid': ['dark ambient'], 'far': ['ambient']}, 'hip hop': {'near': ['boom bap', 'trap', 'lo-fi hip hop', 'conscious hip hop'], 'mid': ['rnb', 'funk'], 'far': ['electronic', 'jazz']}, 'boom bap': {'near': ['hip hop', 'conscious hip hop', 'east coast hip hop'], 'mid': ['jazz rap', 'lo-fi hip hop'], 'far': ['jazz']}, 'east coast hip hop': {'near': ['boom bap', 'hip hop', 'gangsta rap'], 'mid': ['jazz rap'], 'far': ['soul']}, 'west coast hip hop': {'near': ['gangsta rap', 'hip hop', 'g-funk'], 'mid': ['funk'], 'far': ['soul']}, 'gangsta rap': {'near': ['west coast hip hop', 'trap', 'hip hop'], 'mid': ['g-funk'], 'far': ['electronic']}, 'g-funk': {'near': ['west coast hip hop', 'gangsta rap', 'funk'], 'mid': ['hip hop'], 'far': ['soul']}, 'conscious hip hop': {'near': ['boom bap', 'hip hop', 'jazz rap'], 'mid': ['spoken word', 'soul'], 'far': ['folk']}, 'jazz rap': {'near': ['boom bap', 'conscious hip hop', 'hip hop'], 'mid': ['jazz', 'soul'], 'far': ['funk']}, 'lo-fi hip hop': {'near': ['lo-fi', 'hip hop', 'boom bap'], 'mid': ['jazz rap', 'ambient'], 'far': ['jazz']}, 'trap': {'near': ['hip hop', 'drill', 'mumble rap'], 'mid': ['rnb', 'electronic'], 'far': ['pop']}, 'drill': {'near': ['trap', 'hip hop', 'grime'], 'mid': ['electronic'], 'far': ['pop']}, 'mumble rap': {'near': ['trap', 'hip hop'], 'mid': ['pop rap'], 'far': ['pop']}, 'pop rap': {'near': ['hip hop', 'rnb', 'dance pop'], 'mid': ['trap'], 'far': ['pop']}, 'southern hip hop': {'near': ['hip hop', 'trap', 'crunk'], 'mid': ['bounce'], 'far': ['soul']}, 'crunk': {'near': ['southern hip hop', 'trap', 'hip hop'], 'mid': ['electronic'], 'far': ['dance pop']}, 'jazz': {'near': ['bebop', 'fusion', 'cool jazz', 'swing'], 'mid': ['blues', 'funk', 'soul'], 'far': ['classical', 'electronic']}, 'swing': {'near': ['jazz', 'big band', 'bebop'], 'mid': ['blues', 'soul'], 'far': ['folk']}, 'big band': {'near': ['swing', 'jazz', 'orchestral jazz'], 'mid': ['blues'], 'far': ['classical']}, 'bebop': {'near': ['jazz', 'hard bop', 'cool jazz'], 'mid': ['post-bop', 'free jazz'], 'far': ['classical']}, 'hard bop': {'near': ['bebop', 'jazz', 'soul jazz'], 'mid': ['modal jazz', 'blues'], 'far': ['funk']}, 'soul jazz': {'near': ['hard bop', 'jazz', 'funk'], 'mid': ['rnb', 'blues'], 'far': ['soul']}, 'cool jazz': {'near': ['bebop', 'jazz', 'west coast jazz'], 'mid': ['bossa nova'], 'far': ['classical']}, 'west coast jazz': {'near': ['cool jazz', 'jazz', 'chamber jazz'], 'mid': ['bossa nova'], 'far': ['classical']}, 'modal jazz': {'near': ['bebop', 'hard bop', 'avant-garde jazz'], 'mid': ['free jazz'], 'far': ['world']}, 'post-bop': {'near': ['bebop', 'modal jazz', 'free jazz'], 'mid': ['avant-garde jazz', 'fusion'], 'far': ['experimental']}, 'free jazz': {'near': ['avant-garde jazz', 'post-bop', 'experimental'], 'mid': ['noise', 'contemporary classical'], 'far': ['ambient']}, 'avant-garde jazz': {'near': ['free jazz', 'post-bop', 'experimental'], 'mid': ['contemporary classical'], 'far': ['noise']}, 'fusion': {'near': ['jazz', 'jazz rock', 'funk', 'latin jazz'], 'mid': ['progressive rock', 'electronic'], 'far': ['world']}, 'jazz rock': {'near': ['fusion', 'jazz', 'rock'], 'mid': ['progressive rock'], 'far': ['blues']}, 'latin jazz': {'near': ['fusion', 'jazz', 'bossa nova', 'salsa'], 'mid': ['afrobeat', 'cumbia'], 'far': ['folk']}, 'bossa nova': {'near': ['samba', 'latin jazz', 'cool jazz'], 'mid': ['mpb', 'jazz'], 'far': ['folk']}, 'mpb': {'near': ['bossa nova', 'samba', 'tropicalia'], 'mid': ['folk', 'latin jazz'], 'far': ['pop']}, 'tropicalia': {'near': ['mpb', 'bossa nova', 'psychedelic rock'], 'mid': ['experimental'], 'far': ['folk']}, 'samba': {'near': ['bossa nova', 'mpb', 'afrobeat'], 'mid': ['latin jazz'], 'far': ['folk']}, 'funk': {'near': ['soul', 'disco', 'rnb', 'p-funk'], 'mid': ['hip hop', 'jazz'], 'far': ['rock']}, 'p-funk': {'near': ['funk', 'soul', 'disco'], 'mid': ['hip hop'], 'far': ['electronic']}, 'soul': {'near': ['rnb', 'funk', 'gospel', 'motown'], 'mid': ['blues', 'jazz'], 'far': ['pop']}, 'motown': {'near': ['soul', 'rnb', 'pop'], 'mid': ['funk', 'gospel'], 'far': ['jazz']}, 'gospel': {'near': ['soul', 'rnb', 'blues'], 'mid': ['funk', 'folk'], 'far': ['classical']}, 'neo soul': {'near': ['soul', 'rnb', 'hip hop'], 'mid': ['jazz rap', 'funk'], 'far': ['electronic']}, 'rnb': {'near': ['soul', 'hip hop', 'neo soul'], 'mid': ['pop', 'dance pop'], 'far': ['electronic']}, 'contemporary rnb': {'near': ['rnb', 'pop', 'hip hop'], 'mid': ['trap', 'electronic'], 'far': ['soul']}, 'blues': {'near': ['blues rock', 'soul', 'jazz', 'delta blues'], 'mid': ['rock', 'country'], 'far': ['folk']}, 'delta blues': {'near': ['blues', 'country blues', 'acoustic blues'], 'mid': ['folk', 'country'], 'far': ['gospel']}, 'country blues': {'near': ['delta blues', 'blues', 'folk'], 'mid': ['country'], 'far': ['acoustic']}, 'chicago blues': {'near': ['blues', 'electric blues', 'soul'], 'mid': ['rnb', 'rock'], 'far': ['jazz']}, 'electric blues': {'near': ['chicago blues', 'blues', 'blues rock'], 'mid': ['rock', 'soul'], 'far': ['jazz']}, 'reggae': {'near': ['dub', 'ska', 'rocksteady', 'dancehall'], 'mid': ['roots reggae', 'lovers rock'], 'far': ['soul']}, 'dub': {'near': ['reggae', 'dubstep', 'ambient dub'], 'mid': ['electronic', 'drum and bass'], 'far': ['ambient']}, 'ambient dub': {'near': ['dub', 'ambient', 'electronic'], 'mid': ['chill out'], 'far': ['reggae']}, 'ska': {'near': ['reggae', 'ska punk', 'two tone'], 'mid': ['punk', 'soul'], 'far': ['jazz']}, 'ska punk': {'near': ['ska', 'punk', 'hardcore punk'], 'mid': ['two tone'], 'far': ['reggae']}, 'two tone': {'near': ['ska', 'punk', 'new wave'], 'mid': ['ska punk'], 'far': ['soul']}, 'rocksteady': {'near': ['reggae', 'ska', 'dub'], 'mid': ['soul'], 'far': ['jazz']}, 'dancehall': {'near': ['reggae', 'dub', 'soca'], 'mid': ['hip hop', 'electronic'], 'far': ['pop']}, 'roots reggae': {'near': ['reggae', 'dub', 'rocksteady'], 'mid': ['gospel', 'soul'], 'far': ['folk']}, 'lovers rock': {'near': ['reggae', 'soul', 'rnb'], 'mid': ['pop'], 'far': ['dance pop']}, 'soca': {'near': ['dancehall', 'calypso', 'carnival'], 'mid': ['afrobeat'], 'far': ['pop']}, 'calypso': {'near': ['soca', 'caribbean'], 'mid': ['jazz', 'soul'], 'far': ['folk']}, 'afrobeat': {'near': ['afro house', 'funk', 'jazz', 'highlife'], 'mid': ['soul', 'world'], 'far': ['reggae']}, 'highlife': {'near': ['afrobeat', 'jazz', 'palm wine'], 'mid': ['soul'], 'far': ['folk']}, 'afropop': {'near': ['afrobeat', 'highlife', 'pop'], 'mid': ['dancehall', 'rnb'], 'far': ['electronic']}, 'pop': {'near': ['electropop', 'dance pop', 'indie pop', 'synth pop'], 'mid': ['rnb', 'country pop'], 'far': ['rock', 'electronic']}, 'dance pop': {'near': ['pop', 'electropop', 'eurodance'], 'mid': ['house', 'rnb'], 'far': ['techno']}, 'country pop': {'near': ['country', 'pop', 'folk pop'], 'mid': ['americana'], 'far': ['rock']}, 'pop rock': {'near': ['rock', 'power pop', 'soft rock'], 'mid': ['indie pop', 'alternative rock'], 'far': ['country']}, 'soft rock': {'near': ['pop rock', 'adult contemporary', 'folk rock'], 'mid': ['rnb', 'country'], 'far': ['electronic']}, 'adult contemporary': {'near': ['soft rock', 'pop', 'rnb'], 'mid': ['jazz'], 'far': ['country']}, 'folk': {'near': ['folk rock', 'acoustic', 'singer-songwriter', 'traditional folk'], 'mid': ['country', 'blues'], 'far': ['rock']}, 'traditional folk': {'near': ['folk', 'world', 'acoustic'], 'mid': ['celtic', 'bluegrass'], 'far': ['classical']}, 'folk rock': {'near': ['folk', 'rock', 'country rock', 'indie folk'], 'mid': ['psychedelic folk', 'baroque pop'], 'far': ['country']}, 'indie folk': {'near': ['folk rock', 'folk', 'indie rock'], 'mid': ['singer-songwriter', 'lo-fi'], 'far': ['ambient']}, 'psychedelic folk': {'near': ['folk rock', 'folk', 'psychedelic rock'], 'mid': ['experimental folk'], 'far': ['ambient']}, 'experimental folk': {'near': ['psychedelic folk', 'folk', 'avant-folk'], 'mid': ['experimental', 'drone'], 'far': ['ambient']}, 'avant-folk': {'near': ['experimental folk', 'folk', 'avant-garde'], 'mid': ['experimental'], 'far': ['noise']}, 'acoustic': {'near': ['folk', 'singer-songwriter', 'fingerpicking'], 'mid': ['blues', 'country'], 'far': ['classical']}, 'singer-songwriter': {'near': ['folk', 'acoustic', 'indie folk'], 'mid': ['country', 'pop'], 'far': ['electronic']}, 'country': {'near': ['folk', 'americana', 'country pop', 'outlaw country'], 'mid': ['bluegrass', 'rock'], 'far': ['blues']}, 'americana': {'near': ['country', 'folk', 'roots rock'], 'mid': ['bluegrass', 'blues'], 'far': ['rock']}, 'roots rock': {'near': ['americana', 'rock', 'folk rock'], 'mid': ['country', 'blues rock'], 'far': ['country']}, 'outlaw country': {'near': ['country', 'americana', 'folk'], 'mid': ['blues'], 'far': ['rock']}, 'bluegrass': {'near': ['country', 'folk', 'traditional folk'], 'mid': ['americana'], 'far': ['blues']}, 'celtic': {'near': ['traditional folk', 'folk', 'world'], 'mid': ['acoustic'], 'far': ['classical']}, 'classical': {'near': ['romantic', 'contemporary classical', 'baroque'], 'mid': ['minimalism', 'modern classical'], 'far': ['ambient', 'jazz']}, 'baroque': {'near': ['classical', 'early music'], 'mid': ['romantic', 'modern classical'], 'far': ['folk']}, 'early music': {'near': ['baroque', 'classical', 'medieval'], 'mid': ['traditional folk'], 'far': ['world']}, 'romantic': {'near': ['classical', 'baroque', 'opera'], 'mid': ['modern classical'], 'far': ['folk']}, 'opera': {'near': ['romantic', 'classical', 'baroque'], 'mid': ['contemporary classical'], 'far': ['folk']}, 'modern classical': {'near': ['contemporary classical', 'classical', 'minimalism'], 'mid': ['experimental', 'electroacoustic'], 'far': ['ambient']}, 'contemporary classical': {'near': ['modern classical', 'minimalism', 'avant-garde'], 'mid': ['experimental', 'electroacoustic'], 'far': ['ambient']}, 'minimalism': {'near': ['contemporary classical', 'ambient', 'post-minimalism'], 'mid': ['post-rock', 'drone'], 'far': ['electronic']}, 'post-minimalism': {'near': ['minimalism', 'contemporary classical', 'ambient'], 'mid': ['experimental'], 'far': ['electronic']}, 'electroacoustic': {'near': ['contemporary classical', 'experimental', 'idm'], 'mid': ['ambient', 'noise'], 'far': ['electronic']}, 'experimental': {'near': ['noise', 'avant-garde', 'electroacoustic', 'free improvisation'], 'mid': ['electronic', 'contemporary classical'], 'far': ['rock']}, 'avant-garde': {'near': ['experimental', 'noise', 'free jazz'], 'mid': ['contemporary classical', 'electroacoustic'], 'far': ['ambient']}, 'free improvisation': {'near': ['free jazz', 'experimental', 'avant-garde'], 'mid': ['noise', 'contemporary classical'], 'far': ['ambient']}, 'noise': {'near': ['noise rock', 'industrial', 'power electronics'], 'mid': ['experimental', 'harsh noise'], 'far': ['ambient']}, 'harsh noise': {'near': ['noise', 'power electronics', 'industrial'], 'mid': ['grindcore'], 'far': ['experimental']}, 'power electronics': {'near': ['harsh noise', 'noise', 'industrial'], 'mid': ['dark ambient'], 'far': ['experimental']}, 'garage': {'near': ['uk garage', 'house', '2-step'], 'mid': ['dubstep', 'grime'], 'far': ['electronic']}, 'uk garage': {'near': ['garage', '2-step', 'speed garage'], 'mid': ['dubstep', 'grime'], 'far': ['house']}, '2-step': {'near': ['uk garage', 'garage', 'speed garage'], 'mid': ['house'], 'far': ['electronic']}, 'speed garage': {'near': ['uk garage', '2-step', 'house'], 'mid': ['techno'], 'far': ['dubstep']}, 'breakbeat': {'near': ['electro', 'big beat', 'drum and bass'], 'mid': ['hip hop', 'house'], 'far': ['techno']}, 'big beat': {'near': ['breakbeat', 'electronic', 'trip hop'], 'mid': ['house', 'hip hop'], 'far': ['alternative rock']}, 'trip hop': {'near': ['downtempo', 'hip hop', 'electronic'], 'mid': ['ambient', 'soul'], 'far': ['jazz']}, 'downtempo': {'near': ['trip hop', 'ambient', 'chill out'], 'mid': ['electronic', 'lo-fi'], 'far': ['jazz']}, 'chill out': {'near': ['ambient', 'downtempo', 'new age'], 'mid': ['trip hop', 'electronic'], 'far': ['jazz']}, 'vaporwave': {'near': ['chillwave', 'lo-fi', 'synthwave'], 'mid': ['electronic', 'ambient'], 'far': ['pop']}, 'synthwave': {'near': ['synth pop', 'electropop', 'italo disco'], 'mid': ['new wave', 'vaporwave'], 'far': ['electronic']}, 'world': {'near': ['folk', 'traditional folk', 'afrobeat'], 'mid': ['jazz', 'latin'], 'far': ['classical']}, 'latin': {'near': ['salsa', 'cumbia', 'latin jazz', 'bossa nova'], 'mid': ['reggaeton', 'afrobeat'], 'far': ['pop']}, 'salsa': {'near': ['latin', 'latin jazz', 'cumbia'], 'mid': ['afrobeat', 'soul'], 'far': ['jazz']}, 'cumbia': {'near': ['latin', 'salsa', 'vallenato'], 'mid': ['reggaeton', 'tropical'], 'far': ['folk']}, 'reggaeton': {'near': ['latin', 'dancehall', 'trap'], 'mid': ['hip hop', 'pop'], 'far': ['electronic']}, 'flamenco': {'near': ['world', 'latin', 'folk'], 'mid': ['classical', 'jazz'], 'far': ['blues']}, 'fado': {'near': ['world', 'folk', 'latin'], 'mid': ['traditional folk'], 'far': ['classical']}, 'chanson': {'near': ['folk', 'singer-songwriter', 'world'], 'mid': ['pop'], 'far': ['classical']}, 'canzone': {'near': ['chanson', 'folk', 'world'], 'mid': ['pop'], 'far': ['classical']}, 'j-pop': {'near': ['pop', 'electropop', 'city pop'], 'mid': ['rnb'], 'far': ['electronic']}, 'city pop': {'near': ['j-pop', 'funk', 'soul'], 'mid': ['disco', 'pop'], 'far': ['jazz']}, 'k-pop': {'near': ['pop', 'dance pop', 'electropop'], 'mid': ['rnb', 'hip hop'], 'far': ['electronic']}}

# Generic genres that span multiple decades — era selector is mandatory
GENRES_NEED_ERA = {
    "rock", "pop", "jazz", "blues", "funk", "soul", "country",
    "folk", "metal", "punk", "electronic", "dance", "hip hop",
    "hip-hop", "classical", "reggae", "r&b", "rnb",
}

# Map generic genre + decade → specific Discogs styles
GENRE_TO_DISCOGS_STYLES = {
    "rock": {
        "50s": ["Rock & Roll", "Rockabilly", "Rhythm & Blues", "Boogie Woogie"],
        "60s": ["Garage Rock", "Psychedelic Rock", "Surf", "Beat", "Freakbeat",
                "Acid Rock", "British Blues", "Art Rock", "Mod", "Merseybeat",
                "Raga Rock", "Sunshine Pop", "Folk Rock"],
        "70s": ["Hard Rock", "Prog Rock", "Glam", "Proto-Punk", "Space Rock",
                "Krautrock", "Art Rock", "Folk Rock", "Cosmic Rock", "Canterbury Scene",
                "Jazz-Rock", "Funk Rock", "Southern Rock", "Stoner Rock"],
        "80s": ["Post-Punk", "Indie Rock", "New Wave", "Hardcore", "Noise Rock",
                "Jangle Pop", "C86", "Paisley Underground", "Gothic Rock",
                "Dream Pop", "Thrash Metal", "College Rock"],
        "90s": ["Grunge", "Alternative Rock", "Britpop", "Stoner Rock", "Math Rock",
                "Shoegaze", "Post-Rock", "Noise Rock", "Lo-Fi", "Slowcore",
                "Post-Hardcore", "Emo"],
        "00s": ["Post-Punk Revival", "Indie Rock", "Experimental Rock", "Post-Rock",
                "Math Rock", "Noise Rock", "Lo-Fi"],
        "10s": ["Indie Rock", "Post-Rock", "Drone", "Noise Rock", "Math Rock",
                "Shoegaze", "Lo-Fi"],
    },
    "pop": {
        "60s": ["Beat", "Sunshine Pop", "Baroque Pop", "Chamber Pop", "Merseybeat",
                "Bossa Nova", "Soft Rock", "Folk Rock"],
        "70s": ["Glam", "Soft Rock", "Disco", "Folk Rock", "Sunshine Pop",
                "Baroque Pop", "Chamber Pop"],
        "80s": ["Synth-Pop", "New Romantic", "New Wave", "Dream Pop", "Jangle Pop",
                "C86", "Indie Pop", "Sophisti-Pop"],
        "90s": ["Dream Pop", "Indie Pop", "Britpop", "Noise Pop", "Chamber Pop",
                "Twee Pop", "Slowcore", "Lo-Fi"],
        "00s": ["Indie Pop", "Chamber Pop", "Neo-Psychedelia", "Twee Pop",
                "Dream Pop", "Freak Folk"],
        "10s": ["Synth-Pop", "Indie Pop", "Ambient Pop", "Chillwave", "Bedroom Pop",
                "Dream Pop", "Lo-Fi"],
        "20s": ["Bedroom Pop", "Indie Pop", "Hyperpop", "Lo-Fi"],
    },
    "jazz": {
        "50s": ["Cool Jazz", "Hard Bop", "Bebop", "Swing", "Modal"],
        "60s": ["Free Jazz", "Jazz-Rock", "Post Bop", "Soul Jazz", "Modal",
                "Avant-garde Jazz", "Bossa Nova", "Latin Jazz"],
        "70s": ["Fusion", "Jazz-Rock", "Jazz-Funk", "Free Jazz", "Avant-garde Jazz",
                "Latin Jazz", "Post Bop"],
        "80s": ["Contemporary Jazz", "Smooth Jazz", "Fusion", "Free Jazz"],
        "90s": ["Future Jazz", "Acid Jazz", "Nu Jazz", "Contemporary Jazz"],
        "00s": ["Future Jazz", "Contemporary Jazz", "Nu Jazz", "Free Jazz"],
    },
    "blues": {
        "50s": ["Jump Blues", "Rhythm & Blues", "Electric Blues", "Delta Blues",
                "Country Blues", "Boogie Woogie"],
        "60s": ["British Blues", "Chicago Blues", "Blues Rock", "Electric Blues",
                "Piedmont Blues"],
        "70s": ["Blues Rock", "Electric Blues", "Chicago Blues"],
        "80s": ["Blues Rock", "Electric Blues"],
    },
    "funk": {
        "60s": ["Soul", "Funk", "Rhythm & Blues", "Gospel"],
        "70s": ["Funk", "Soul", "Disco", "Jazz-Funk", "P.Funk", "Afrobeat"],
        "80s": ["Funk", "Soul", "Electro", "Go-Go"],
        "90s": ["Funk", "Soul", "Neo Soul"],
    },
    "soul": {
        "60s": ["Soul", "Rhythm & Blues", "Northern Soul", "Gospel", "Doo Wop",
                "Motown", "Boogaloo"],
        "70s": ["Soul", "Funk", "Disco", "Philadelphia Soul", "Southern Soul"],
        "80s": ["Soul", "Funk", "Contemporary R&B", "New Jack Swing"],
        "90s": ["Neo Soul", "Contemporary R&B", "Soul"],
    },
    "electronic": {
        "60s": ["Experimental", "Musique Concrète", "Avant-garde", "Electronic"],
        "70s": ["Krautrock", "Kosmische", "Minimal", "Ambient", "Experimental",
                "Synth", "Electronic", "Neue Deutsche Welle", "Disco",
                "Electro", "Funk"],
        "80s": ["Synth-Pop", "EBM", "Industrial", "Minimal Wave", "New Wave",
                "Darkwave", "Ambient", "Experimental", "Cold Wave", "New Age",
                "Hi NRG", "Electro", "Italo-Disco"],
        "90s": ["Trip-Hop", "Ambient Techno", "Drum n Bass", "Ambient",
                "Experimental", "Minimal", "IDM", "Breakbeat", "Acid"],
        "00s": ["Minimal", "Experimental", "Ambient", "IDM", "Glitch",
                "Drone", "Microhouse"],
        "10s": ["Chillwave", "Witch House", "Drone", "Vaporwave", "Ambient",
                "Hauntology", "Hypnagogic Pop"],
        "20s": ["Ambient", "Experimental", "Hyperpop"],
    },
    "metal": {
        "70s": ["Hard Rock", "Heavy Metal", "Proto-Metal"],
        "80s": ["Thrash Metal", "Heavy Metal", "Hardcore", "Speed Metal",
                "Death Metal", "Black Metal"],
        "90s": ["Doom Metal", "Stoner Rock", "Noise Rock", "Death Metal",
                "Black Metal", "Post-Metal", "Sludge Metal"],
        "00s": ["Post-Metal", "Drone Metal", "Sludge Metal", "Mathcore"],
        "10s": ["Post-Metal", "Drone Metal", "Blackgaze"],
    },
    "punk": {
        "70s": ["Proto-Punk", "Punk", "New Wave"],
        "80s": ["Hardcore", "Post-Punk", "No Wave", "Noise Rock", "Anarcho-Punk",
                "Oi", "Skate Punk"],
        "90s": ["Post-Hardcore", "Emo", "Hardcore", "Pop Punk", "Riot Grrrl"],
        "00s": ["Post-Hardcore", "Emo", "Pop Punk"],
    },
    "folk": {
        "60s": ["Folk Rock", "Acoustic", "Baroque Pop", "Protest", "Psychedelic Folk"],
        "70s": ["Folk Rock", "Canterbury Scene", "Acoustic", "Singer/Songwriter",
                "Prog Folk", "Psychedelic Folk"],
        "80s": ["Folk Rock", "Indie Folk", "Acoustic"],
        "90s": ["Indie Folk", "Lo-Fi", "Singer/Songwriter"],
        "00s": ["Freak Folk", "New Weird America", "Indie Folk", "Free Folk",
                "Psychedelic Folk", "Singer/Songwriter"],
        "10s": ["Indie Folk", "Freak Folk", "Singer/Songwriter"],
    },
    "country": {
        "50s": ["Country", "Western Swing", "Rockabilly", "Honky Tonk"],
        "60s": ["Country", "Folk Rock", "Honky Tonk", "Outlaw Country"],
        "70s": ["Country Rock", "Outlaw Country", "Country Soul"],
        "80s": ["Country Rock", "Country"],
    },
    "reggae": {
        "60s": ["Ska", "Rocksteady", "Mento"],
        "70s": ["Reggae", "Dub", "Roots Reggae", "Lovers Rock"],
        "80s": ["Dub", "Reggae", "Dancehall", "Lovers Rock"],
        "90s": ["Reggae", "Dub", "Dancehall"],
    },
    "hip hop": {
        "80s": ["Hip Hop", "Electro"],
        "90s": ["Hip Hop", "Gangsta", "Conscious", "Boom Bap", "Instrumental"],
        "00s": ["Hip Hop", "Crunk", "Dirty South", "Boom Bap"],
        "10s": ["Hip Hop", "Trap", "Cloud Rap", "Conscious"],
    },
    "hip-hop": {
        "80s": ["Hip Hop", "Electro"],
        "90s": ["Hip Hop", "Gangsta", "Conscious", "Boom Bap", "Instrumental"],
        "00s": ["Hip Hop", "Crunk", "Dirty South", "Boom Bap"],
        "10s": ["Hip Hop", "Trap", "Cloud Rap", "Conscious"],
    },
    "rap": {
        "80s": ["Hip Hop", "Electro"],
        "90s": ["Hip Hop", "Gangsta", "Conscious", "Boom Bap", "Instrumental"],
        "00s": ["Hip Hop", "Crunk", "Dirty South", "Boom Bap"],
        "10s": ["Hip Hop", "Trap", "Cloud Rap", "Conscious"],
    },
    "dance": {
        "70s": ["Disco", "Funk"],
        "80s": ["Italo-Disco", "Hi NRG", "Electro", "Synth-Pop", "House"],
        "90s": ["House", "Techno", "Trance", "Drum n Bass", "Garage"],
        "00s": ["House", "Techno", "Minimal", "Electro"],
        "10s": ["House", "Techno", "Minimal", "Tropical House"],
    },
    "classical": {
        "50s": ["Contemporary", "Modern Classical", "Orchestral"],
        "60s": ["Contemporary", "Modern Classical", "Minimalism", "Avant-garde"],
        "70s": ["Minimalism", "Contemporary", "Experimental", "Avant-garde"],
        "80s": ["Minimalism", "Contemporary", "New Age"],
        "90s": ["Minimalism", "Contemporary", "Modern Classical"],
        "00s": ["Contemporary", "Modern Classical", "Minimalism"],
    },
}

def _expand_genre(genre, level="near"):
    """Return related genres at the given distance level (near/mid/far)."""
    node = GENRE_GRAPH.get(genre.lower().strip(), {})
    return node.get(level, [])

def _expand_genre(genre, level="near"):
    """Return related genres at the given distance level (near/mid/far)."""
    node = GENRE_GRAPH.get(genre.lower().strip(), {})
    return node.get(level, [])

def _resolve_genre_styles(genre, decades):
    """Return Discogs styles for a genre + set of decades.
    If genre is in GENRE_TO_DISCOGS_STYLES, uses mapped styles per decade.
    If not mapped, uses the genre name directly as a Discogs style — works for
    most specific genres like 'Hard Techno', 'Cumbia', 'Drone Metal', etc.
    """
    genre_map = GENRE_TO_DISCOGS_STYLES.get(genre.lower().strip(), {})
    if genre_map:
        styles = []
        for d in decades:
            styles.extend(genre_map.get(d, []))
        # Deduplicate preserving order
        seen = set()
        return [s for s in styles if not (s in seen or seen.add(s))]
    else:
        # Not mapped — use genre literal as Discogs style
        # Capitalize properly: "hard techno" → "Hard Techno"
        return [genre.strip().title()]


GENRE_INCOMPATIBLE = {
    "reggaeton", "trap", "drill", "k-pop", "kpop", "j-pop", "jpop",
    "corridos", "banda", "norteño", "cumbia villera", "dembow",
    "phonk", "mumble rap", "edm", "dubstep", "hardstyle", "gabber",
    "christmas", "holiday", "children", "kids", "nursery",
}

def _is_valid_artist_name(artist):
    """Basic quality filter for artist names — rejects corrupt or non-music entries."""
    import re
    if not artist or len(artist) > 60:
        return False
    # Reject non-ASCII characters in name
    if re.search(r"[^\x00-\x7F]", artist):
        return False
    # Reject abnormally long words
    if any(len(w) > 20 for w in artist.split()):
        return False
    # Reject obvious non-artist patterns
    if any(p in artist.lower() for p in ["official", "records", "promo", "vevo",
                                          "in this video", "tutorial", "feat.", "ft."]):
        return False
    return True


def _get_valid_genres_for(genre):
    """Return set of acceptable genre tags for a given genre using GENRE_GRAPH."""
    genre = genre.lower().strip()
    valid = {genre}
    node  = GENRE_GRAPH.get(genre, {})
    valid.update(g.lower() for g in node.get("near", []))
    valid.update(g.lower() for g in node.get("mid", []))
    return valid

# ─── Genre/Tag Normalization ──────────────────────────────────────────────────

def normalize_tag(tag):
    """
    Normalize tag/genre for consistent matching.
    Handles hyphens, underscores, and multiple spaces.
    """
    tag = tag.lower()
    tag = tag.replace("-", " ")
    tag = tag.replace("_", " ")
    tag = re.sub(r"\s+", " ", tag)
    return tag.strip()

def normalize_tag_extended(tag):
    """
    Return both base normalized and compact (no spaces) versions.
    Critical for matching variants like 'power pop' / 'power-pop' / 'powerpop'.
    """
    base = normalize_tag(tag)
    compact = base.replace(" ", "")
    return base, compact

_artist_listeners_cache = {}  # artist → listener count
_artist_tags_cache = {}       # artist → (normalized_tags, compact_tags)
_artist_tags_compact_cache = {}  # artist → compact tags

def _get_artist_tags_listeners(artist):
    """
    Single Last.fm call returning (normalized_tags, compact_tags, listeners) — cached.
    Returns two tag lists: base normalized and compact (no spaces) versions.
    """
    if artist in _artist_tags_cache:
        tags_norm = _artist_tags_cache[artist]
        tags_comp = _artist_tags_compact_cache.get(artist, [])
        listeners = _artist_listeners_cache.get(artist, 0)
        return tags_norm, tags_comp, listeners
    
    try:
        data      = lastfm("artist.getinfo", artist=artist)
        listeners = int(data.get("artist", {}).get("stats", {}).get("listeners", 0) or 0)
        raw_tags  = data.get("artist", {}).get("tags", {}).get("tag", [])
        if isinstance(raw_tags, dict):
            raw_tags = [raw_tags]
        
        # Normalize tags: both base and compact versions
        tags_normalized = []
        tags_compact = []
        for t in raw_tags:
            if t.get("name"):
                norm, comp = normalize_tag_extended(t["name"])
                tags_normalized.append(norm)
                tags_compact.append(comp)
        
        _artist_listeners_cache[artist] = listeners
        _artist_tags_cache[artist] = tags_normalized
        _artist_tags_compact_cache[artist] = tags_compact
        
        return tags_normalized, tags_compact, listeners
    except Exception:
        return [], [], 0

def _artist_matches_genre(artist, valid_genres):
    """Return True if artist tags don't contain incompatible genres."""
    tags, _, _ = _get_artist_tags_listeners(artist)
    if not tags:
        return True
    artist_tags = set(tags)
    if artist_tags & GENRE_INCOMPATIBLE:
        return False
    return True

# Underground scoring constants
_UNDERGROUND_RARE_TAGS = {
    "obscure", "rare", "garage punk", "psychedelic garage",
    "lo fi", "lofi", "underground", "proto punk", "freakbeat",
    "raw", "primitive", "nuggets"
}
_UNDERGROUND_BAD_CONTEXT = {
    "classic rock", "arena rock", "hard rock", "progressive rock",
    "pop", "mainstream", "adult contemporary", "soft rock"
}

def _compute_underground_score(artist, target_genre):
    """
    Score artist for underground/discovery quality (hybrid strategy + normalization).
    Single Last.fm call via _get_artist_tags_listeners (cached).
    Returns score int — higher = better underground candidate.
    Returns -999 if artist should be discarded.
    """
    tags_norm, tags_comp, listeners = _get_artist_tags_listeners(artist)

    if not tags_norm:
        return -999

    # Normalize target genre: both base and compact versions
    target_norm, target_comp = normalize_tag_extended(target_genre)

    # Genre must be present (match either normalized or compact)
    genre_matched = target_norm in tags_norm or target_comp in tags_comp
    
    if not genre_matched:
        # Allow near genres as weak match
        near = [g.lower().replace("-", " ").strip()
                for g in GENRE_GRAPH.get(target_genre.lower(), {}).get("near", [])]
        if not any(t in near for t in tags_norm):
            return -999
        score = 2  # near-genre: base reducida — no compite igual que match exacto
    else:
        score = 4  # exact genre match

    # Boost for rare/underground tags
    if any(t in _UNDERGROUND_RARE_TAGS for t in tags_norm):
        score += 2

    # Listener-based scoring (aggressive thresholds with mega-mainstream penalty)
    if 0 < listeners < 50_000:
        score += 3  # very underground
    elif listeners < 100_000:
        score += 2  # underground
    elif listeners < 300_000:
        score += 1  # semi-underground
    elif listeners < 500_000:
        score -= 2  # moderado (300k-500k): score = 2, pasa threshold 2
    elif listeners < 1_000_000:
        score -= 3  # semi-mainstream (500k-1M): score = 1, pasa threshold 1
    elif listeners < 3_000_000:
        score -= 5  # mainstream (1M-3M): score = -1, rechazado
    else:
        score -= 8  # mega mainstream (Beatles, Floyd, Queen level)

    # Penalize mainstream context tags
    if any(t in _UNDERGROUND_BAD_CONTEXT for t in tags_norm):
        score -= 3

    # Reward rich tagging (proxy for community knowledge)
    if len(tags_norm) >= 5:
        score += 1

    return score


def _filter_underground_artists(artists, genre, decades=None):
    """
    Score and filter artists by underground quality with adaptive thresholds.
    For niche genres (pool < 60), expands pool with Last.fm seeds.
    Progressive relaxation with dynamic thresholds based on pool size.
    Uses concurrent calls via ThreadPoolExecutor.
    """
    pool_size = len(artists)
    log.info(f"[Underground] Initial pool: {pool_size} artists for '{genre}'")
    
    # Detect niche genre and expand pool if needed (umbral subido a 80 para géneros nicho)
    if pool_size < 80:
        log.info(f"[Niche genre detected] Expanding pool for '{genre}'")
        try:
            extra = _get_era_artists_from_lastfm(genre, decades, max_artists=150)
            # Merge without duplicates
            artists_set = set(artists)
            for artist in extra:
                if artist not in artists_set:
                    artists.append(artist)
                    artists_set.add(artist)
            log.info(f"[Underground] After Last.fm expansion: {len(artists)} artists (added {len(artists) - pool_size})")
            # Second pass: Discogs if pool still small (requires decades — skip for All Time)
            if len(artists) < 30 and decades:
                log.info(f"[Underground] Pool still small ({len(artists)}) — trying Discogs")
                try:
                    discogs_extra = _get_era_artists_from_discogs(
                        decades, mode="playlist", max_artists=60,
                        style_override=_resolve_genre_styles(genre, decades)
                    )
                    for a in discogs_extra:
                        if a not in artists_set:
                            artists.append(a)
                            artists_set.add(a)
                    log.info(f"[Underground] After Discogs: {len(artists)} artists")
                except Exception as e2:
                    log.error(f"[Underground] Discogs expansion failed: {e2}")
        except Exception as e:
            log.error(f"[Underground] Pool expansion failed: {e}")
    
    # Score all artists once
    # attempted_names: todos los artistas procesados (score cualquiera, incluido -999)
    # Solo los artistas con excepción real quedan fuera, para el fallback genre-match
    attempted_names = set()
    scored = []
    with ThreadPoolExecutor(max_workers=4) as ex:  # 4 workers: menos rate limiting en Last.fm
        futures = {ex.submit(_compute_underground_score, a, genre): a for a in artists}
        for f in as_completed(futures):
            artist = futures[f]
            try:
                score = f.result()
                attempted_names.add(artist)  # procesado correctamente (cualquier score)
                if score > -999:
                    scored.append((artist, score))
            except Exception:
                pass  # excepción real → no en attempted_names → elegible para fallback

    scored.sort(key=lambda x: x[1], reverse=True)
    
    # Dynamic threshold based on pool size
    current_pool = len(scored)
    if current_pool >= 80:
        min_required = 40
    elif current_pool >= 50:
        min_required = 30
    elif current_pool >= 30:
        min_required = 20
    else:
        min_required = 8   # small niche pool — take what we have

    log.info(f"[Underground] Pool size: {current_pool}, min required: {min_required}")
    
    # Universal rule: progressive thresholds [5,4,3,2,1] → always underground first
    # Relax only if needed, never use artists with score < 1 directly
    thresholds = [5, 4, 3, 2, 1]
    for threshold in thresholds:
        if threshold <= 2:
            # Hard cap: cuando el threshold relaja, bloquear artistas con >750k listeners
            # _artist_listeners_cache ya fue poblado durante el scoring (sin llamadas extra)
            filtered = [(a, s) for a, s in scored
                        if s >= threshold
                        and _artist_listeners_cache.get(a, 0) <= 1_000_000]
        else:
            filtered = [(a, s) for a, s in scored if s >= threshold]
        if len(filtered) >= min_required:
            result = [a for a, _ in filtered[:100]]  # cap at 100
            log.info(f"[Underground] {len(result)} artists passed (threshold={threshold})")
            return result
    
    # Final fallback — keep only positive scores, then genre-match only for un-attempted artists
    log.info(f"[Underground] Insufficient scored ({len(scored)}) — genre-match fallback")
    # CRITICAL: Never use artists with negative score
    positive_scored = [(a, s) for a, s in scored if s >= 1]
    result = [a for a, _ in positive_scored]  # start with positive scores only
    target_norm, target_comp = normalize_tag_extended(genre)

    # Solo añadir artistas que fallaron con excepción real (no los que obtuvieron score -999)
    # attempted_names incluye todos los procesados correctamente (incluso score -999)
    for artist in artists:
        if len(result) >= 100:
            break
        if artist in attempted_names:  # ya fue procesado (score -999 o mayor) → skip
            continue
        # Solo llega aquí si hubo excepción durante scoring — re-intentar
        tags_norm, tags_comp, _ = _get_artist_tags_listeners(artist)
        if target_norm in tags_norm or target_comp in tags_comp:
            result.append(artist)

    log.info(f"[Underground] Final pool: {len(result)} artists (fallback with score >= 1)")
    return result[:100]


def _get_era_artists_from_lastfm(genre, decades, max_artists=150):
    """
    Seed-based artist discovery for genre playlists.
    1. Get top artists for the genre tag from Last.fm
    2. Pick 5 random seeds from the top 50 (representative but varied)
    3. Expand each seed via artist.getsimilar
    4. Filter expanded pool by era via MB album years
    """
    # Step 1: get top artists for genre tag
    data  = lastfm("tag.gettopartists", tag=genre, limit=100)
    items = data.get("topartists", {}).get("artist", [])
    if not items:
        log.info(f"Last.fm tag '{genre}' returned no artists")
        return []

    top_names = [a["name"] for a in items if a.get("name")]
    if not top_names:
        return []

    # Step 2: pick 5 random seeds from positions 30-150 — skip top 30 (likely 500k+ listeners)
    # Filter out invalid seeds (song titles, lowercase usernames, single words that look like titles)
    seed_pool = [a for a in top_names[30:150]
                 if len(a) > 2
                 and not a[0].islower()
                 and not any(c in a for c in ["@", "/", "http"])
                 and len(a.split()) >= 1]
    if not seed_pool:
        seed_pool = top_names[:50]
    seeds = random.sample(seed_pool, min(5, len(seed_pool)))
    log.info(f"Genre '{genre}' seeds: {seeds}")

    # Step 3: expand each seed via artist.getsimilar
    expanded = set()
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(_fetch_similar_names, seed): seed for seed in seeds}
        for f in as_completed(futures):
            try:
                similar = f.result()
                expanded.update(similar)
            except Exception:
                pass

    # Add seeds themselves to pool
    expanded.update(seeds)
    candidates = [a for a in expanded if _is_valid_artist_name(a)]
    random.shuffle(candidates)

    # Step 4: filter by genre tags — reject artists whose tags don't match genre
    valid_genres = _get_valid_genres_for(genre)
    pool = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_artist_matches_genre, a, valid_genres): a for a in candidates}
        for f in as_completed(futures):
            artist = futures[f]
            try:
                if f.result():
                    pool.append(artist)
            except Exception:
                pool.append(artist)  # benefit of the doubt

    random.shuffle(pool)
    log.info(f"Genre '{genre}' pool: {len(pool)}/{len(candidates)} passed genre filter")
    return pool[:max_artists]


def _is_valid_fallback_track(artist, track_name):
    """Generic quality filter for fallback tracks from _fetch_top_track."""
    import re
    # Artist name validation
    if len(artist) > 60:
        return False
    if any(p in artist.lower() for p in ["official", "records", "promo", "vevo"]):
        return False
    # Reject artists with non-ASCII characters in name (likely corrupt data)
    if re.search(r"[^\x00-\x7F]", artist):
        return False
    # Reject artists with abnormally long words (corrupt names)
    if any(len(word) > 20 for word in artist.split()):
        return False
    # Track title validation
    if len(track_name) > 80:
        return False
    if any(p in track_name.lower() for p in ["download", "itunes", "http", "www", ".com"]):
        return False
    return True


def _fetch_track_for_genre(artist, decade_year_range, min_listeners=0):
    """
    Fetch a track for Free Explore genre playlists.
    MB is optional — if no era album found, fallback to _fetch_top_track.
    min_listeners: minimum listener threshold for fallback tracks.
    """
    try:
        mbid, _ = _mb_find_artist(artist)
        if mbid:
            albums = _mb_studio_albums(mbid)
            if albums and decade_year_range:
                lo, hi = decade_year_range
                era_albums = [a for a in albums if a.get("year") and lo <= int(a["year"]) <= hi]
                if not era_albums:
                    return _try_track_from_singles_eps(artist, mbid, lo, hi, SINGLES_FALLBACK_LISTENER_CAP)
                random.shuffle(era_albums)
                for album in era_albums[:3]:
                    try:
                        data   = lastfm("album.getinfo", artist=artist, album=album["title"])
                        tracks = data.get("album", {}).get("tracks", {}).get("track", [])
                        if not tracks:
                            continue
                        if isinstance(tracks, dict):
                            tracks = [tracks]
                        valid = [t for t in tracks if not _is_live_track(t.get("name", ""))]
                        if not valid:
                            valid = tracks
                        random.shuffle(valid)
                        for t in valid:
                            name  = t.get("name", "")
                            clean = _clean_track_title(name)
                            if clean.lower() in SEASONAL_TRACK_BLACKLIST:
                                continue
                            key = f"{normalize(artist)}-{normalize(clean)}"
                            if not track_in_history(key):
                                return (artist, clean, key)
                    except Exception:
                        continue
    except Exception:
        pass

    # MB didn't work — fallback to top track with quality filter
    # Apply listener threshold if set
    listeners = _artist_listeners_cache.get(artist, 0)
    if min_listeners > 0 and listeners > 0 and listeners < min_listeners:
        return None

    result = _fetch_top_track(artist)
    if result:
        _, track_name, _ = result
        if not _is_valid_fallback_track(artist, track_name):
            return None
    return result


def _get_era_artists_from_discogs(decades, mode="playlist", max_artists=80, style_override=None):
    """
    Use Discogs to find artists from a specific era filtered by community.have.
    style_override: list of specific Discogs styles (for generic genre + era combos).
    Only includes artists with at least 2 releases matching the style — filters noise.
    """
    have_min, have_max = ERA_HAVE_RANGES.get(mode, (30, 5000))

    # Use style_override if provided (generic genre + era), else fall back to DECADE_STYLES
    if style_override:
        styles = list(style_override)
    else:
        styles = []
        for d in decades:
            styles.extend(DECADE_STYLES.get(d, []))
    random.shuffle(styles)

    year_lo = min(DECADE_YEARS[d][0] for d in decades)
    year_hi = max(DECADE_YEARS[d][1] for d in decades)
    all_years = list(range(year_lo, year_hi + 1))

    tasks = []
    max_styles = 15 if style_override else 12
    for style in styles[:max_styles]:
        years = random.sample(all_years, min(8, len(all_years)))
        for year in years:
            tasks.append((style, year))

    def _fetch_one(style, year):
        try:
            r = requests.get(
                "https://api.discogs.com/database/search",
                params={
                    "style":    style,
                    "year":     str(year),
                    "type":     "release",
                    "format":   "Vinyl",
                    "per_page": 50,
                    "page":     random.randint(1, 3),
                    "token":    DISCOGS_TOKEN,
                },
                timeout=10
            ).json()
            results = []
            for rel in r.get("results", []):
                have = rel.get("community", {}).get("have", 0)
                if not (have_min <= have <= have_max):
                    continue
                title = rel.get("title", "")
                if " - " in title:
                    artist = _clean_artist_name(title.split(" - ")[0].strip())
                    if artist and not _is_blacklisted(artist) and len(artist) > 1:
                        results.append(artist)
            return results
        except Exception as e:
            log.error(f"Discogs era search: {e}")
            return []

    artist_counts = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(_fetch_one, style, year) for style, year in tasks]
        for f in as_completed(futures):
            for artist in f.result():
                artist_counts[artist] = artist_counts.get(artist, 0) + 1
            if len([a for a, c in artist_counts.items() if c >= 2]) >= max_artists:
                break

    qualified = [a for a, c in artist_counts.items() if c >= 2]
    if len(qualified) < 15:
        qualified = list(artist_counts.keys())

    log.info(f"Discogs era pool: {len(qualified)} artists for {decades} mode={mode}")
    return qualified


def _expand_era_pool_dig(seed_artists):
    """Expand via Last.fm similar — for Dig mode."""
    pool = set()
    sample = random.sample(seed_artists, min(40, len(seed_artists)))
    with ThreadPoolExecutor(max_workers=8) as ex:
        for f in as_completed([ex.submit(_fetch_similar_names, a) for a in sample]):
            try: pool.update(f.result())
            except Exception: pass
    pool -= set(seed_artists)
    pool = {a for a in pool if not _is_blacklisted(a)}
    return list(pool) if pool else seed_artists

_mb_decade_cache = {}  # artist_name → set of decades (e.g. {"60s", "70s"})

def _artist_decade_from_mb(artist):
    """
    Get artist's decade from MusicBrainz begin_year.
    Cached. Returns set of decades the artist was active in.
    Uses _mb_get which respects rate limiting.
    """
    key = artist.lower().strip()
    if key in _mb_decade_cache:
        return _mb_decade_cache[key]

    try:
        # Step 1: find MBID
        data = _mb_get("artist/", {"query": f'artist:"{artist}"', "limit": 3})
        candidates = data.get("artists", [])
        mbid = None
        for c in candidates:
            if int(c.get("score", 0)) >= 85 and c.get("name", "").lower() == key:
                mbid = c.get("id")
                break
        if not mbid and candidates and int(candidates[0].get("score", 0)) >= 90:
            mbid = candidates[0].get("id")

        if not mbid:
            _mb_decade_cache[key] = set()
            return set()

        # Step 2: get life-span
        mb = _mb_get(f"artist/{mbid}")
        ls = mb.get("life-span", {})
        begin = ls.get("begin", "") or ""
        end   = ls.get("end",   "") or ""
        begin_year = int(begin[:4]) if begin[:4].isdigit() else None
        end_year   = int(end[:4])   if end[:4].isdigit()   else 9999

        if begin_year is None:
            _mb_decade_cache[key] = set()
            return set()

        active = set()
        for d, (lo, hi) in DECADE_YEARS.items():
            if begin_year <= hi and end_year >= lo:
                active.add(d)

        _mb_decade_cache[key] = active
        return active

    except Exception:
        _mb_decade_cache[key] = set()
        return set()

def _filter_artists_by_decade(artists, decades, message=None):
    """
    Filter artists by era using MusicBrainz begin_year.
    Reliable, cached, with early stop and periodic messages.
    """
    if not decades:
        return artists

    pool = artists[:120] if len(artists) > 120 else artists
    random.shuffle(pool)

    if message:
        _safe_reply(message, "📅 Filtering by era…")

    # Periodic messages
    import threading
    _timers = []
    if message:
        for delay, text in [(12, "⏳ Still filtering…"), (25, "⏳ Cross-checking artists…"),
                            (40, "⏳ Almost there…"), (55, "⏳ Last few checks…")]:
            t = threading.Timer(delay, lambda m=message, tx=text: _safe_reply(m, tx))
            t.start()
            _timers.append(t)

    passed  = []
    unknown = []
    TARGET  = 150  # enough for 50 tracks with margin

    # MusicBrainz rate limit: max 1 req/s — use sequential with small delay
    for artist in pool:
        try:
            active = _artist_decade_from_mb(artist)
            if not active:
                unknown.append(artist)
            elif active & decades:
                passed.append(artist)
                if len(passed) >= TARGET:
                    break
            time.sleep(1.5)  # stay well within 1 req/s limit (2 calls per artist)
        except Exception:
            unknown.append(artist)

    for t in _timers:
        t.cancel()

    log.info(f"MB era filter: {len(pool)} pool → {len(passed)} passed")

    return passed if passed else []

def _safe_reply(message, text):
    try:
        m = message.reply_text(text)
        _progress_msgs.setdefault(message.chat_id, []).append(m)
    except Exception:
        pass

def select_tracks_with_decades(artists, size=None, decades=None, message=None, mode="playlist", style_filter=None, use_mb=False, genre=None):
    """
    When decades selected: use Discogs as source, ignore user history pool.
    genre: if set and in GENRES_NEED_ERA, uses GENRE_TO_DISCOGS_STYLES for targeted search.
    """
    if not decades:
        return select_tracks(artists, size=size)

    chat_id = message.chat_id if message else None

    # Resolve style_override from genre — always, even if not in map
    style_override = None
    if genre:
        style_override = _resolve_genre_styles(genre, decades)
        log.info(f"Genre '{genre}' → styles: {style_override}")

    if message:
        _safe_reply(message, "📅 Building era pool…")

    if mode == "dig":
        seeds = _get_era_artists_from_discogs(decades, mode="dig", max_artists=80, style_override=style_override)
        if chat_id and _is_cancelled(chat_id): return []
        if message:
            _safe_reply(message, "🔗 Expanding connections…")
        pool = _expand_era_pool_dig(seeds)
        if len(pool) < 15:
            pool = seeds
        # Cap before verification
        if len(pool) > 200:
            random.shuffle(pool)
            pool = pool[:200]
    elif mode == "rare":
        pool = _get_era_artists_from_discogs(decades, mode="rare", max_artists=120, style_override=style_override)
        if chat_id and _is_cancelled(chat_id): return []
        if message:
            _safe_reply(message, "💎 Filtering for obscure artists…")
        rare_pool = []
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(_fetch_listeners, a): a for a in pool}
            for f in as_completed(futures):
                try:
                    artist, listeners = f.result()
                    if 0 < listeners < RARE_MAX_LISTENERS:
                        rare_pool.append(artist)
                except Exception:
                    pass
        pool = rare_pool if rare_pool else pool[:20]
        log.info(f"Rare era: {len(pool)} after listeners filter")
    else:
        pool = _get_era_artists_from_discogs(decades, mode="playlist", max_artists=100, style_override=style_override)

    if chat_id and _is_cancelled(chat_id): return []

    # Expand pool using genre graph if too small
    if genre and len(pool) < 50:
        log.info(f"Genre '{genre}' pool too small ({len(pool)}) — expanding to near genres")
        for related in _expand_genre(genre, "near"):
            if len(pool) >= 80:
                break
            rel_styles = _resolve_genre_styles(related, decades)
            extra = _get_era_artists_from_discogs(decades, mode=mode,
                                                   max_artists=40,
                                                   style_override=rel_styles if rel_styles else [related.title()])
            pool = list(set(pool) | set(extra))
            log.info(f"After near '{related}': {len(pool)} artists")

    if genre and len(pool) < 30:
        log.info(f"Genre '{genre}' pool still small ({len(pool)}) — expanding to mid genres")
        for related in _expand_genre(genre, "mid"):
            if len(pool) >= 50:
                break
            rel_styles = _resolve_genre_styles(related, decades)
            extra = _get_era_artists_from_discogs(decades, mode=mode,
                                                   max_artists=30,
                                                   style_override=rel_styles if rel_styles else [related.title()])
            pool = list(set(pool) | set(extra))
            log.info(f"After mid '{related}': {len(pool)} artists")

    if genre and len(pool) < 15:
        log.info(f"Genre '{genre}' pool very small ({len(pool)}) — expanding to far genres")
        for related in _expand_genre(genre, "far"):
            if len(pool) >= 30:
                break
            rel_styles = _resolve_genre_styles(related, decades)
            extra = _get_era_artists_from_discogs(decades, mode=mode,
                                                   max_artists=20,
                                                   style_override=rel_styles if rel_styles else [related.title()])
            pool = list(set(pool) | set(extra))
            log.info(f"After far '{related}': {len(pool)} artists")

    if not pool:
        log.info("Era pool empty — falling back to user history pool")
        return select_tracks(artists, size=size)

    if chat_id and _is_cancelled(chat_id): return []

    # Verify artist names against Last.fm for track lookup compatibility
    if message:
        _safe_reply(message, "🎵 Selecting tracks…")

    verified = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        def _check_artist(name):
            try:
                data = lastfm("artist.getinfo", artist=name)
                lfm_name = data.get("artist", {}).get("name", "")
                if lfm_name:
                    return lfm_name
            except Exception:
                pass
            return None

        futures = {ex.submit(_check_artist, a): a for a in pool}
        for f in as_completed(futures):
            try:
                result = f.result()
                if result:
                    verified.append(result)
            except Exception:
                pass

    log.info(f"Era pool ({mode}): {len(pool)} → {len(verified)} verified for {decades}")

    # No MB validation — Discogs is source of truth for era
    final_pool = verified if len(verified) >= 10 else pool

    # Compute decade year range for album filtering
    decade_year_range = None
    if decades:
        lo = min(DECADE_YEARS[d][0] for d in decades)
        hi = max(DECADE_YEARS[d][1] for d in decades)
        decade_year_range = (lo, hi)

    # Select tracks
    global _recent_artists
    target        = size or PLAYLIST_SIZE
    recent_set    = {normalize(r) for r in _recent_artists}
    filtered_pool = [a for a in final_pool if normalize(a) not in recent_set]
    if len(filtered_pool) < 10:
        filtered_pool = final_pool

    random.shuffle(filtered_pool)
    tracks       = []
    keys_added   = set()
    seen_artists = set()
    artists_used = []
    BATCH_SIZE   = 20  # submit in small batches, stop when target reached

    for i in range(0, min(len(filtered_pool), target * 4), BATCH_SIZE):
        if len(tracks) >= target:
            break
        if chat_id and _is_cancelled(chat_id):
            return []
        batch = filtered_pool[i:i + BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=4) as ex:
            futures = [ex.submit(_fetch_track_from_early_albums, a, decade_year_range)
                       for a in batch]
            for f in as_completed(futures):
                if len(tracks) >= target:
                    break
                try:
                    result = f.result()
                    if result:
                        artist, track_name, key = result
                        artist_norm = normalize_artist(artist)
                        if artist_norm in seen_artists:
                            continue
                        if not track_in_history(key) and key not in keys_added:
                            tracks.append(f"{artist} - {track_name}")
                            keys_added.add(key)
                            seen_artists.add(artist_norm)
                            artists_used.append(artist)
                except Exception as e:
                    log.error(f"select_tracks_with_decades: {e}")

    for key in keys_added:
        add_to_history(key)
    save_history()
    _recent_artists.extend(artists_used)
    _recent_artists = _recent_artists[-RECENT_ARTISTS_MAX:]
    return tracks

def _decade_label_from_set(decades):
    """Human-readable label from a set of decades."""
    if not decades:
        return ""
    return " · ".join(sorted(decades, key=lambda d: DECADES.index(d) if d in DECADES else 99))

def _decade_label(chat_id):
    """Human-readable label for selected decades."""
    decades = _pending_decades.get(chat_id, set())
    return _decade_label_from_set(decades)

_track_store   = {}  # key → {"tracks": [...], "title": "..."}
_track_counter = itertools.count()

def _store_tracks(tracks, title="Kurator Playlist", map_chat_id=None, text=None):
    key = str(next(_track_counter))
    _track_store[key] = {"tracks": tracks, "title": title, "map_chat_id": map_chat_id, "text": text}
    if len(_track_store) > TRACK_STORE_MAX:
        for old in sorted(_track_store.keys(), key=int)[:len(_track_store) - TRACK_STORE_MAX]:
            del _track_store[old]
    return key

# ─── Decade selector ──────────────────────────────────────────────────────────

def _get_relevant_decades(genre):
    """Always return all decades — genre should not restrict era choice."""
    return DECADES

def _decade_selector_buttons(chat_id, genre=None):
    """Build decade toggle buttons — shows only relevant decades for genre if provided."""
    selected  = _pending_decades.get(chat_id, set())
    available = _get_relevant_decades(genre) if genre else DECADES
    buttons   = []
    row       = []
    for d in available:
        tick = "🟡" if d in selected else "⚪"
        row.append(InlineKeyboardButton(f"{tick} {d}", callback_data=f"decade_toggle|{d}"))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    label = f"🍌 GENERATE PLAYLIST — {_decade_label(chat_id)}" if selected else "🍌 GENERATE PLAYLIST"
    buttons.append([InlineKeyboardButton(label, callback_data="decade_confirm")])
    buttons.append([InlineKeyboardButton("← Back", callback_data="decade_back")])
    return buttons

def _show_era_choice(query, chat_id, title, gen_action, back_cb):
    """Show Any era / Select decade. Stores gen_action in _pending_gen."""
    _pending_gen[chat_id] = {"action": gen_action, "back": back_cb}
    query.edit_message_text(
        f"{title}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🍌 GENERATE PLAYLIST", callback_data="decade_confirm")],
            [InlineKeyboardButton("📅 Select a decade",   callback_data="decade_open")],
            [InlineKeyboardButton("← Back",               callback_data=back_cb)],
        ])
    )

# ─── Working message — appears at 8s if still generating ─────────────────────

def _render_similar(query, artist, similar, page, chat_id):
    """Render paginated similar artists with 2-column layout."""
    PAGE_SIZE   = 8
    total_pages = max(1, (len(similar) - 1) // PAGE_SIZE + 1)
    start       = page * PAGE_SIZE
    page_items  = sorted(similar[start:start + PAGE_SIZE], key=lambda x: len(x))

    buttons = [
        [InlineKeyboardButton("🔗 1 hop — Direct neighbours",  callback_data=safe_callback(f"trail_go|1|{artist}"))],
        [InlineKeyboardButton("🔗 2 hops — Wider connections", callback_data=safe_callback(f"trail_go|2|{artist}"))],
    ]

    i = 0
    while i < len(page_items):
        name = page_items[i]
        if i + 1 < len(page_items) and len(name) + len(page_items[i+1]) <= 28:
            buttons.append([
                InlineKeyboardButton(name.upper(), callback_data=safe_callback(f"explore_artist|{name}|{artist}")),
                InlineKeyboardButton(page_items[i+1].upper(), callback_data=safe_callback(f"explore_artist|{page_items[i+1]}|{artist}")),
            ])
            i += 2
        else:
            buttons.append([InlineKeyboardButton(name.upper(), callback_data=safe_callback(f"explore_artist|{name}|{artist}"))])
            i += 1

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("← Prev", callback_data=safe_callback(f"similar_page|{artist}|{page-1}")))
    if page + 1 < total_pages:
        nav.append(InlineKeyboardButton("Next →", callback_data=safe_callback(f"similar_page|{artist}|{page+1}")))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton(f"← Back to {artist[:20]}", callback_data=f"card_back|{chat_id}")])

    page_label = f" (page {page+1}/{total_pages})" if total_pages > 1 else ""
    query.edit_message_text(
        f"🔗 Similar Artists — {artist}{page_label}",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# ─── Working message — appears at 8s if still generating ─────────────────────

def _working_message(message, text, delay=8):
    """Send a 'still working' message after delay seconds using a background timer."""
    import threading
    sent = {"done": False}

    def _send():
        if not sent["done"]:
            try:
                m = message.reply_text(text)
                _progress_msgs.setdefault(message.chat_id, []).append(m)
            except Exception:
                pass

    timer = threading.Timer(delay, _send)
    timer.start()
    return sent, timer

def _cancel_working(sent, timer):
    sent["done"] = True
    timer.cancel()

def _clear_progress_msgs(chat_id):
    """Delete all intermediate progress messages accumulated during generation."""
    for m in _progress_msgs.pop(chat_id, []):
        try:
            m.delete()
        except Exception:
            pass

COUNTRY_FLAGS = {
    "GB": "🇬🇧", "US": "🇺🇸", "DE": "🇩🇪", "FR": "🇫🇷",
    "JP": "🇯🇵", "KR": "🇰🇷", "SE": "🇸🇪", "NO": "🇳🇴",
    "DK": "🇩🇰", "FI": "🇫🇮", "IS": "🇮🇸", "NL": "🇳🇱",
    "BE": "🇧🇪", "AU": "🇦🇺", "CA": "🇨🇦", "IT": "🇮🇹",
    "ES": "🇪🇸", "PT": "🇵🇹", "BR": "🇧🇷", "AR": "🇦🇷",
    "MX": "🇲🇽", "PL": "🇵🇱", "CZ": "🇨🇿", "AT": "🇦🇹",
    "CH": "🇨🇭", "RU": "🇷🇺", "UA": "🇺🇦", "TR": "🇹🇷",
    "ZA": "🇿🇦", "NG": "🇳🇬", "GH": "🇬🇭", "JM": "🇯🇲",
    "IE": "🇮🇪", "NZ": "🇳🇿", "GR": "🇬🇷", "HU": "🇭🇺",
    "RO": "🇷🇴", "SK": "🇸🇰", "HR": "🇭🇷", "RS": "🇷🇸",
    "CL": "🇨🇱", "CO": "🇨🇴", "PE": "🇵🇪", "VE": "🇻🇪",
    "IN": "🇮🇳", "CN": "🇨🇳", "TW": "🇹🇼", "TH": "🇹🇭",
    "ID": "🇮🇩", "MY": "🇲🇾", "SG": "🇸🇬", "PH": "🇵🇭",
    "IL": "🇮🇱", "EG": "🇪🇬", "MA": "🇲🇦", "ET": "🇪🇹",
    "XE": "🇪🇺", "XW": "🌍",
}

COUNTRY_NAMES = {
    "GB": "UK", "US": "USA", "DE": "Germany", "FR": "France",
    "JP": "Japan", "KR": "Korea", "SE": "Sweden", "NO": "Norway",
    "DK": "Denmark", "FI": "Finland", "IS": "Iceland", "NL": "Netherlands",
    "BE": "Belgium", "AU": "Australia", "CA": "Canada", "IT": "Italy",
    "ES": "Spain", "PT": "Portugal", "BR": "Brazil", "AR": "Argentina",
    "MX": "Mexico", "PL": "Poland", "CZ": "Czech Republic", "AT": "Austria",
    "CH": "Switzerland", "RU": "Russia", "UA": "Ukraine", "TR": "Turkey",
    "ZA": "South Africa", "NG": "Nigeria", "GH": "Ghana", "JM": "Jamaica",
    "IE": "Ireland", "NZ": "New Zealand", "GR": "Greece", "HU": "Hungary",
    "RO": "Romania", "SK": "Slovakia", "HR": "Croatia", "RS": "Serbia",
    "CL": "Chile", "CO": "Colombia", "PE": "Peru", "VE": "Venezuela",
    "IN": "India", "CN": "China", "TW": "Taiwan", "TH": "Thailand",
    "ID": "Indonesia", "MY": "Malaysia", "SG": "Singapore", "PH": "Philippines",
    "IL": "Israel", "EG": "Egypt", "MA": "Morocco", "ET": "Ethiopia",
    "XE": "Europe", "XW": "Worldwide",
}

def _country_flag(code):
    return COUNTRY_FLAGS.get(code, "")

_mb_lock      = threading.Lock()
_mb_last_call = 0.0

def _mb_get(path, params=None):
    """Generic MusicBrainz GET — globally rate-limited to 1 req/s with retry."""
    global _mb_last_call
    for attempt in range(3):
        try:
            with _mb_lock:
                # Enforce 1 req/s globally across all threads
                now = time.time()
                wait = 1.1 - (now - _mb_last_call)
                if wait > 0:
                    time.sleep(wait)
                _mb_last_call = time.time()

            r = requests.get(
                f"https://musicbrainz.org/ws/2/{path}",
                params={**(params or {}), "fmt": "json"},
                headers={"User-Agent": MB_USER_AGENT},
                timeout=10
            )
            if r.status_code == 200:
                return r.json()
            if r.status_code == 503:
                log.warning(f"MusicBrainz 503 on {path} — retrying (attempt {attempt+1})")
                time.sleep(2 ** attempt)
                continue
            log.warning(f"MusicBrainz {path} HTTP {r.status_code}")
            return {}
        except Exception as e:
            log.error(f"MusicBrainz error {path} (attempt {attempt+1}): {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
    return {}


def _mb_get_artists_by_genre(genre, decades, limit=200):
    """
    POC — simulates future Oracle query using MusicBrainz API.
    Searches artists by genre tag, filters by era using begin/end dates.
    In production: SELECT * FROM music_artists JOIN artist_genres WHERE genre=X AND year BETWEEN Y AND Z
    Returns list of artist names sorted by tag weight (relevance).
    """
    lo = min(DECADE_YEARS[d][0] for d in decades) if decades else 0
    hi = max(DECADE_YEARS[d][1] for d in decades) if decades else 9999

    log.info(f"[Oracle-POC] Querying genre=\'{genre}\' era={lo}-{hi}")

    all_artists = []
    for offset in range(0, 400, 100):
        data = _mb_get("artist/", {
            "query":  f'tag:"{genre}"',
            "limit":  100,
            "offset": offset,
        })
        candidates = data.get("artists", [])
        if not candidates:
            break
        for a in candidates:
            name  = a.get("name", "")
            score = int(a.get("score", 0))
            if not name or score < 50:
                continue
            if not _is_valid_artist_name(name):
                continue
            begin_area = a.get("life-span", {}) or {}
            begin_str  = (begin_area.get("begin") or "")[:4]
            end_str    = (begin_area.get("end")   or "")[:4]
            begin_year = int(begin_str) if begin_str.isdigit() else None
            end_year   = int(end_str)   if end_str.isdigit()   else None
            if decades:
                if begin_year and begin_year > hi:
                    continue
                if end_year and end_year < lo:
                    continue
            all_artists.append((name, score))
        if len(all_artists) >= limit:
            break

    all_artists.sort(key=lambda x: x[1], reverse=True)
    result = [name for name, _ in all_artists[:limit]]
    log.info(f"[Oracle-POC] \'{genre}\' {lo}-{hi}: {len(result)} artists found")
    return result


def _update_user_genre_profile(chat_id, genre, decades=None):
    """Silently track genre interactions per user — simulates Oracle user_genre_profile table."""
    try:
        key     = f"user_profile_{chat_id}"
        profile = _mongo_get("store", key, {"genres": {}, "decades": {}})
        genre_lower = genre.lower().strip()
        profile["genres"][genre_lower] = profile["genres"].get(genre_lower, 0) + 1
        if decades:
            for d in decades:
                profile["decades"][d] = profile["decades"].get(d, 0) + 1
        _mongo_set("store", key, profile)
    except Exception as e:
        log.error(f"_update_user_genre_profile: {e}")


def _get_user_genre_profile(chat_id):
    """Get user genre profile — simulates SELECT FROM user_genre_profile ORDER BY interactions DESC."""
    try:
        key     = f"user_profile_{chat_id}"
        profile = _mongo_get("store", key, {"genres": {}, "decades": {}})
        top_genres  = sorted(profile["genres"].items(),  key=lambda x: x[1], reverse=True)
        top_decades = sorted(profile["decades"].items(), key=lambda x: x[1], reverse=True)
        return {"genres": top_genres, "decades": top_decades}
    except Exception as e:
        log.error(f"_get_user_genre_profile: {e}")
        return {"genres": [], "decades": []}


def _mb_find_artist(artist_query):
    """
    Search MusicBrainz for an artist. Returns (mbid, official_name) or (None, None).
    Tries exact name match first, then falls back to top result with score >= 90.
    """
    data = _mb_get("artist/", {"query": f'artist:"{artist_query}"', "limit": 5})
    candidates = data.get("artists", [])
    for c in candidates:
        if int(c.get("score", 0)) >= 80 and \
           c.get("name", "").lower() == artist_query.lower():
            return c.get("id"), c.get("name")
    if candidates and int(candidates[0].get("score", 0)) >= 90:
        return candidates[0].get("id"), candidates[0].get("name")
    return None, None

def _mb_artist_full(mbid):
    """Full artist lookup with tags and area."""
    return _mb_get(f"artist/{mbid}", {"inc": "tags+area-rels"})

def _mb_studio_albums(mbid):
    """
    Fetch studio albums and EPs ordered by year asc.
    Includes: Album (no secondary types), Album+EP
    Excludes: Live, Compilation, Soundtrack, Remix, Interview, Single, etc.
    Returns list of {title, year, is_ep}.
    """
    data = _mb_get(f"release-group/", {
        "artist": mbid,
        "type":   "album",
        "limit":  100,
    })
    albums = []
    for rg in data.get("release-groups", []):
        primary   = rg.get("primary-type", "")
        secondary = [s.lower() for s in rg.get("secondary-types", [])]

        if primary != "Album":
            continue
        # Exclude anything with unwanted secondary types
        excluded = {"live", "compilation", "soundtrack", "remix",
                    "interview", "spokenword", "audiobook", "mixtape/street"}
        if any(s in excluded for s in secondary):
            continue

        is_ep = "ep" in secondary
        year  = (rg.get("first-release-date") or "")[:4]
        title = rg.get("title", "")
        if title:
            albums.append({"title": title, "year": year, "is_ep": is_ep})

    albums.sort(key=lambda a: a["year"] or "9999")
    return albums

def _mb_singles_eps(mbid):
    """
    Fetch singles and EPs for an artist from MusicBrainz.
    Returns [{title, year}] sorted by year asc, deduplicated.
    """
    results = []
    seen    = set()
    for mb_type, primary_label in [("single", "Single"), ("ep", "EP")]:
        data = _mb_get("release-group/", {"artist": mbid, "type": mb_type, "limit": 100})
        for rg in data.get("release-groups", []):
            if rg.get("primary-type", "") != primary_label:
                continue
            secondary = [s.lower() for s in rg.get("secondary-types", [])]
            if any(s in {"live", "compilation", "remix", "interview"} for s in secondary):
                continue
            title = rg.get("title", "")
            if not title:
                continue
            key = normalize(title)
            if key in seen:
                continue
            seen.add(key)
            year = (rg.get("first-release-date") or "")[:4]
            results.append({"title": title, "year": year})
    results.sort(key=lambda r: r["year"] or "9999")
    return results

def _try_track_from_singles_eps(artist, mbid, lo, hi, listener_cap):
    """
    Fallback for artists with no studio albums in the target era.
    Guards with listener_cap — skips mainstream artists (> 1M listeners).
    Tries singles/EPs from MusicBrainz filtered to [lo, hi].
    """
    listeners = _get_listeners_cached(artist)
    if listeners > listener_cap:
        log.debug(f"Singles fallback skipped for '{artist}': {listeners:,} > cap")
        return None

    singles    = _mb_singles_eps(mbid)
    era_singles = [s for s in singles if s.get("year") and lo <= int(s["year"]) <= hi]
    if not era_singles:
        return None

    random.shuffle(era_singles)
    for single in era_singles[:8]:
        title = single["title"]
        try:
            data   = lastfm("album.getinfo", artist=artist, album=title)
            tracks = data.get("album", {}).get("tracks", {}).get("track", [])
            if tracks:
                if isinstance(tracks, dict):
                    tracks = [tracks]
                valid = [t for t in tracks if not _is_live_track(t.get("name", ""))]
                if not valid:
                    valid = tracks
                # Singles (≤2 tracks): B-side first, then A-side
                # EPs (3+ tracks): full shuffle — no A/B distinction
                if len(valid) <= 2:
                    b_sides = valid[1:]
                    a_sides = valid[:1]
                    random.shuffle(b_sides)
                    ordered = b_sides + a_sides
                else:
                    ordered = valid[:]
                    random.shuffle(ordered)
                for t in ordered:
                    name  = t.get("name", "")
                    clean = _clean_track_title(name)
                    if clean.lower() in SEASONAL_TRACK_BLACKLIST:
                        continue
                    key = f"{normalize(artist)}-{normalize(clean)}"
                    if not track_in_history(key):
                        log.info(f"Singles fallback: '{artist}' → '{clean}' (via {title})")
                        return (artist, clean, key)
        except Exception:
            pass
        # Last.fm had no tracklist — use single title as A-side (lowest priority)
        clean = _clean_track_title(title)
        if clean.lower() not in SEASONAL_TRACK_BLACKLIST:
            key = f"{normalize(artist)}-{normalize(clean)}"
            if not track_in_history(key):
                log.info(f"Singles fallback (A-side title): '{artist}' → '{clean}'")
                return (artist, clean, key)
    return None

def _mb_label_artists(label_name):
    """
    Search MusicBrainz for artists on a label.
    Step 1: find the label MBID.
    Step 2: fetch releases on that label and extract unique artists.
    Returns list of artist names.
    """
    # Step 1 — find label MBID
    label_data = _mb_get("label/", {"query": f'label:"{label_name}"', "limit": 1})
    labels     = label_data.get("labels", [])
    if not labels:
        log.warning(f"MusicBrainz: no label found for '{label_name}'")
        return []
    label_mbid = labels[0].get("id")
    if not label_mbid:
        return []
    log.info(f"MusicBrainz: label '{label_name}' → {label_mbid}")

    # Step 2 — fetch releases on that label and extract artists
    artists = set()
    for offset in range(0, 200, 100):
        rel_data = _mb_get("release/", {
            "label":  label_mbid,
            "limit":  100,
            "offset": offset,
            "inc":    "artist-credits",
        })
        releases = rel_data.get("releases", [])
        if not releases:
            break
        for release in releases:
            for credit in release.get("artist-credit", []):
                if isinstance(credit, dict) and "artist" in credit:
                    name = credit["artist"].get("name")
                    if name:
                        artists.add(name)
        if len(releases) < 100:
            break

    log.info(f"MusicBrainz: found {len(artists)} artists for label '{label_name}'")
    return list(artists)

# ─── Artist info ──────────────────────────────────────────────────────────────

def _get_artist_full_info(artist_query):
    """
    Fetch full artist info. Returns dict with:
    official_name, country_code, country_name, flag,
    begin_year, end_year, genres, bio, lastfm_url, label, albums
    """
    info = {
        "official_name": None,
        "country_code":  None, "country_name": None, "flag": "",
        "city":          None,
        "begin_year":    None, "end_year": None,
        "genres":        [], "bio": None, "lastfm_url": None,
        "label":         None, "albums": [],
    }

    # ── MusicBrainz ──────────────────────────────────────────────────────────
    mbid, official_name = _mb_find_artist(artist_query)
    if official_name:
        info["official_name"] = official_name
        log.info(f"MusicBrainz: matched '{artist_query}' → '{official_name}' ({mbid})")

    if mbid:
        mb = _mb_artist_full(mbid)
        code = mb.get("country")
        if code:
            info["country_code"] = code
            info["flag"]         = _country_flag(code)
            info["country_name"] = COUNTRY_NAMES.get(code, code)
        # Extract city/area
        area = mb.get("begin-area") or mb.get("area")
        if area:
            city = area.get("name")
            # Don't show city if it's the same as the country name
            if city and city != info.get("country_name") and city != mb.get("country"):
                info["city"] = city
        ls    = mb.get("life-span", {})
        begin = ls.get("begin", "")
        end   = ls.get("end", "")
        if begin: info["begin_year"] = begin[:4]
        if end:   info["end_year"]   = end[:4]
        tags = sorted(mb.get("tags", []), key=lambda t: t.get("count", 0), reverse=True)
        info["genres"] = [t["name"].title() for t in tags[:4]]
        # Add MusicBrainz tags to tag index — skip blacklisted tags
        for t in tags[:8]:
            tag_name = t["name"].title()
            if not _is_valid_tag(tag_name):
                continue
            tag_index[tag_name] = tag_index.get(tag_name, 0) + t.get("count", 1)
        save_tag_index()
        info["albums"] = _mb_studio_albums(mbid)

    # ── Discogs — label extraction ────────────────────────────────────────────
    try:
        name_for_discogs = info["official_name"] or artist_query
        r = requests.get(
            "https://api.discogs.com/database/search",
            params={"artist": name_for_discogs, "type": "release",
                    "per_page": 100, "token": DISCOGS_TOKEN},
            timeout=15
        ).json()
        label_counter = {}
        for rel in r.get("results", []):
            for lbl in rel.get("label", []):
                if lbl and lbl.lower() not in ("not on label", "unknown", "self-released"):
                    label_counter[lbl] = label_counter.get(lbl, 0) + 1
            for s in rel.get("style", []):
                if _is_valid_tag(s):
                    tag_index[s] = tag_index.get(s, 0) + 1
            for g in rel.get("genre", []):
                if _is_valid_tag(g):
                    tag_index[g] = tag_index.get(g, 0) + 1
        save_tag_index()
        if label_counter:
            info["label"] = max(label_counter, key=label_counter.get)
    except Exception as e:
        log.error(f"Discogs error for '{artist_query}': {e}")

    # ── Last.fm — bio + URL + fallback genres ─────────────────────────────────
    try:
        name_for_lastfm = info["official_name"] or artist_query
        data        = lastfm("artist.getinfo", artist=name_for_lastfm)
        artist_data = data.get("artist", {})
        lfm_url     = artist_data.get("url")
        if lfm_url:
            info["lastfm_url"] = lfm_url
        raw_bio = (artist_data.get("bio", {}).get("content", "") or
                   artist_data.get("bio", {}).get("summary", ""))
        if raw_bio:
            clean = re.sub(r"<a href[^>]*>.*?</a>", "", raw_bio, flags=re.DOTALL)
            clean = re.sub(r"\s+", " ", clean).strip()
            if len(clean) > 500:
                clean = clean[:500].rsplit(".", 1)[0] + "."
            if len(clean) > 30:
                info["bio"] = clean
        if not info["genres"]:
            lfm_tags = artist_data.get("tags", {}).get("tag", [])
            info["genres"] = [t["name"].title() for t in lfm_tags[:4]]
    except Exception as e:
        log.error(f"Last.fm artist info for '{artist_query}': {e}")

    return info

def _artist_display_name(info, fallback):
    """Returns official name in UPPERCASE, falling back to input."""
    return (info.get("official_name") or fallback).upper()

def _format_artist_card(artist_query, info):
    """Build the artist card text — name, city, country, years, genres, label."""
    name  = _artist_display_name(info, artist_query)
    lines = [name, ""]
    meta  = []
    flag  = info.get("flag", "")
    city  = info.get("city")
    country = info.get("country_name")
    if flag and city and country:
        meta.append(f"{flag} {city}, {country}")
    elif flag and country:
        meta.append(f"{flag} {country}")
    elif country:
        meta.append(country)
    begin = info.get("begin_year")
    end   = info.get("end_year")
    if begin:
        meta.append(f"{begin}–{end if end else 'present'}")
    if meta:
        lines.append("  ·  ".join(meta))
    if info.get("genres"):
        lines.append("🏷️ " + "  ·  ".join(info["genres"][:4]))
    if info.get("label"):
        lines.append(f"🎙️ {info['label']}")
    return "\n".join(lines)

# ─── Export buttons ───────────────────────────────────────────────────────────

def _export_collapsed_buttons(key, map_chat_id=None):
    """Single Export button — expands when tapped."""
    buttons = [[InlineKeyboardButton("Export ▼", callback_data=f"export_expand|{key}")]]
    if map_chat_id:
        mem          = map_memory.get(map_chat_id, {})
        display_name = mem.get("display_name", "")
        if display_name:
            buttons.append([InlineKeyboardButton(
                f"🗺️ Back to {display_name[:22]}",
                callback_data=f"map_back|{map_chat_id}"
            )])
    return buttons

def _export_buttons(key, map_chat_id=None):
    """Expanded export options."""
    buttons = [
        [InlineKeyboardButton("🟣 Export via Soundiiz",      callback_data=f"soundiiz_help|{key}")],

        [InlineKeyboardButton("🟢 Open Spotify links",       callback_data=f"sp_expand|{key}|0")],
        [InlineKeyboardButton("← Back",                      callback_data=f"export_collapse|{key}")],
    ]
    return buttons

# ─── Playlist sender ──────────────────────────────────────────────────────────

def send_playlist(message, tracks, title="✦ Kurator's Playlist", branded=True, chat_id=None, size=None, map_chat_id=None, suppress_warning=False):
    target = size or PLAYLIST_SIZE
    if not tracks:
        message.reply_text(
            f"{title}\n\nNo new tracks found.\n\nYour history may be full.\nUse /clear to start fresh.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🗑️ Clear", callback_data="cmd|clear_confirm")],
                [InlineKeyboardButton("🍌 Main menu",  callback_data="cmd|menu")],
            ])
        )
        return
    short_warning = ""
    if not suppress_warning and len(tracks) < target * 0.7:
        history_size = len(history.get("tracks", {}))
        if history_size > 600:
            short_warning = f"\nHistory is getting full — use /clear to start fresh.\n"
        elif len(tracks) >= 25:
            short_warning = f"\nDeep cut selection — {len(tracks)} tracks found.\n"
        elif len(tracks) < 25:
            short_warning = f"\nTight scene — {len(tracks)} essential tracks.\n"
    track_list   = "\n".join(tracks)
    playlist_text = (
        f"{title} — {len(tracks)} tracks\n"
        f"{BOT_VERSION}{short_warning}\n\n"
        f"{track_list}"
    )
    key = _store_tracks(tracks, title=title, map_chat_id=map_chat_id, text=playlist_text)

    # Single message: playlist text + Export button at the bottom
    message.reply_text(
        playlist_text,
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup(_export_collapsed_buttons(key, map_chat_id=map_chat_id))
    )
    log.info(f"[Export] OK (key={key[:20]})")

# ─── Persistent bottom keyboard ───────────────────────────────────────────────

def _persistent_keyboard():
    """Botón fijo en la barra inferior de Telegram para acceder al menú principal."""
    return ReplyKeyboardMarkup([["🍌 Menú"]], resize_keyboard=True, input_field_placeholder="Escribe o pulsa Menú…")

# ─── Main menu ────────────────────────────────────────────────────────────────

def main_menu_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✦ Kurator's Picks", callback_data="cmd|picks_menu")],
        [InlineKeyboardButton("🧭 Free Explore",   callback_data="cmd|explore_menu")],
        [InlineKeyboardButton("────────────────────────────────────", callback_data="noop")],
        [
            InlineKeyboardButton("📊 Status", callback_data="cmd|status"),
            InlineKeyboardButton("❓ Help",   callback_data="cmd|help"),
        ],
    ])

# ─── Help text ────────────────────────────────────────────────────────────────

def _help_text():
    return f"""{BOT_VERSION}
Built around taste, not algorithms.

────────────────────────
<b>✦ KURATOR'S PICKS</b>
────────────────────────
Playlists curated from the creator's own listening history. A real person's taste.

Choose how deep you go: Playlist, Dig or Rare. Filter by decade before generating.

────────────────────────
<b>🧭 FREE EXPLORE</b>
────────────────────────
Navigate the music world freely.

Search any artist and explore their world. Discover similar artists, styles and albums.

Browse by genre or tap your tag collection to generate a playlist.

────────────────────────

📊 /status — see your stats

<b>📅 ERA FILTERING</b>

Decade filtering takes up to 60 seconds. Kurator cross-checks artists against multiple sources. Be patient.
"""

# ─── Commands ─────────────────────────────────────────────────────────────────

def start(update, context):
    chat_id = update.effective_chat.id
    if is_onboarded(chat_id):
        update.message.reply_text("🍌", reply_markup=_persistent_keyboard())
        update.message.reply_text(
            f"{BOT_VERSION}\n\n<b>✦ Kurator's Picks</b> — Playlists from a real listening history.\n\n<b>🧭 Free Explore</b> — Navigate the music map freely.",
            parse_mode="HTML",
            reply_markup=main_menu_markup()
        )
    else:
        # Send logo first
        logo_path = "kurator_logo.png"
        if os.path.exists(logo_path):
            try:
                with open(logo_path, "rb") as f:
                    update.message.reply_photo(photo=f)
            except Exception as e:
                log.error(f"Failed to send logo: {e}")
        update.message.reply_text(
            "Kurator 📀\n\n"
            "A music discovery engine built around taste, not algorithms. 🍌\n\n"
            "Curated selections drawn from a real listening history (not what's trending).",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("→ Next", callback_data="onboard|2")
            ]])
        )

def help_command(update, context):
    update.message.reply_text(_help_text(), parse_mode="HTML")

def changelog_command(update, context):
    """Show development changelog, newest versions first, split if needed."""
    import html as _html

    sorted_versions = sorted(CHANGELOG.keys(), key=lambda x: float(x), reverse=True)

    chunks   = []
    current  = "📀 <b>Kurator Development Log</b>\n\n"

    for version in sorted_versions:
        entry  = CHANGELOG[version]
        block  = f"━━━ <b>v{version}</b> ({entry['date']}) ━━━\n"
        if entry.get('changes'):
            block += "✨ <b>Cambios:</b>\n"
            for change in entry['changes']:
                block += f"  • {_html.escape(change)}\n"
        if entry.get('technical'):
            block += "\n⚙️ <b>Técnico:</b>\n"
            for tech in entry['technical']:
                block += f"  • {_html.escape(tech)}\n"
        block += "\n"

        if len(current) + len(block) > 3800:
            chunks.append(current)
            current = block
        else:
            current += block

    if current:
        chunks.append(current)

    for chunk in chunks:
        update.message.reply_text(chunk, parse_mode="HTML")

def _genre_era_prompt(responder, chat_id, style, back_cb):
    """Show era-selection screen for a genre. responder is a callable(text, reply_markup=...)."""
    _pending_decades.pop(chat_id, None)
    _pending_gen[chat_id] = {"action": f"build|{style}", "back": back_cb}
    responder(
        f"🎸 {style.title()}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("∞ All Time",         callback_data="decade_alltime")],
            [InlineKeyboardButton("📅 Select a decade", callback_data="decade_open")],
            [InlineKeyboardButton("← Back",             callback_data=back_cb)],
        ])
    )

def genre_command(update, context):
    msg     = update.message
    chat_id = update.effective_chat.id
    if context.args:
        tag = " ".join(context.args)
        _genre_era_prompt(msg.reply_text, chat_id, tag, "cmd|explore_menu")
    else:
        _pending_gen[chat_id] = {"action": "awaiting_genre", "back": "cmd|explore_menu"}
        msg.reply_text(
            "🎸 Play a genre\n\nType a genre name:\n\ne.g. Rock, Jazz, Electronic, Post-Punk",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("← Back", callback_data="cmd|explore_menu")],
            ])
        )

def playlist(update, context):
    msg     = update.message
    chat_id = update.effective_chat.id
    if context.args:
        msg.reply_text(f"For genre playlists use /genre {' '.join(context.args)}")
        return
    _pending_gen[chat_id] = {"action": "playlist", "back": "cmd|picks_menu"}
    msg.reply_text(
        "🎧 Kurator's Playlist",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🍌 GENERATE PLAYLIST", callback_data="decade_confirm")],
            [InlineKeyboardButton("📅 Select a decade",   callback_data="decade_open")],
            [InlineKeyboardButton("← Back",               callback_data="cmd|picks_menu")],
        ])
    )

def dig(update, context):
    chat_id = update.effective_chat.id
    _pending_gen[chat_id] = {"action": "dig", "back": "cmd|picks_menu"}
    update.message.reply_text(
        "⛏️ Kurator's Dig",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🍌 GENERATE PLAYLIST", callback_data="decade_confirm")],
            [InlineKeyboardButton("📅 Select a decade",   callback_data="decade_open")],
            [InlineKeyboardButton("← Back",               callback_data="cmd|picks_menu")],
        ])
    )

def rare(update, context):
    chat_id = update.effective_chat.id
    _pending_gen[chat_id] = {"action": "rare", "back": "cmd|picks_menu"}
    update.message.reply_text(
        "💎 Kurator's Rare",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🍌 GENERATE PLAYLIST", callback_data="decade_confirm")],
            [InlineKeyboardButton("📅 Select a decade",   callback_data="decade_open")],
            [InlineKeyboardButton("← Back",               callback_data="cmd|picks_menu")],
        ])
    )

def map_command(update, context):
    msg     = update.message
    chat_id = update.effective_chat.id
    if not context.args:
        _pending_gen[chat_id] = {"action": "awaiting_map", "back": "cmd|explore_menu"}
        msg.reply_text(
            "🧑‍🎤 Artist\n\nType an artist name:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("← Back", callback_data="cmd|explore_menu")],
            ])
        )
        return
    artist_query = " ".join(context.args)

    # Delete previous "Mapping…" message if exists
    prev_id = _pending_map_msgs.pop(chat_id, None)
    if prev_id:
        try:
            msg.bot.delete_message(chat_id=chat_id, message_id=prev_id)
        except Exception:
            pass

    mapping_msg = msg.reply_text(f"🧑‍🎤 Exploring {artist_query.upper()}…")
    _pending_map_msgs[chat_id] = mapping_msg.message_id

    sent, timer = _working_message(msg, "🧑‍🎤 Still exploring…")
    _nav_history.pop(chat_id, None)
    _render_map(msg, artist_query, chat_id)
    _cancel_working(sent, timer)
    _pending_map_msgs.pop(chat_id, None)

def tags(update, context):
    _render_tags(update.message, page=0)

def status(update, context):
    _render_status(update.message)

def reset(update, context):
    _do_reset(update.message)

def cancel_command(update, context):
    """Para cualquier generación en curso."""
    chat_id = update.effective_chat.id
    was_generating = bool(_active_timers.get(chat_id) or _progress_msgs.get(chat_id))
    _set_cancel(chat_id)
    _clear_progress_msgs(chat_id)
    _pending_gen.pop(chat_id, None)
    _pending_decades.pop(chat_id, None)
    msg = "⏹ Generation stopped." if was_generating else "Nothing in progress."
    update.message.reply_text(msg, reply_markup=main_menu_markup())

# ─── Map renderer ─────────────────────────────────────────────────────────────

def _render_map(message, artist_query, chat_id):
    # Fetch Discogs styles
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

    if not counter:
        message.reply_text(f'No styles found for "{artist_query}".\nTry a different artist or spelling.')
        return

    sorted_styles = sorted(
        [(s, c) for s, c in counter.items() if c >= 5],
        key=lambda x: x[1], reverse=True
    )[:12]
    # Fallback if too strict
    if not sorted_styles:
        sorted_styles = sorted(counter.items(), key=lambda x: x[1], reverse=True)[:12]

    info = _get_artist_full_info(artist_query)
    if not info["genres"]:
        info["genres"] = [s for s, _ in sorted_styles[:4]]

    display_name = _artist_display_name(info, artist_query)

    map_memory[chat_id] = {
        "artist":       artist_query,
        "display_name": display_name,
        "styles":       sorted_styles,
        "info":         info,
    }
    save_map_memory()

    card_text = _format_artist_card(artist_query, info)
    buttons   = _build_map_buttons(display_name, sorted_styles, info, chat_id)

    message.reply_text(
        f"{card_text}\n\nExplore:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

def _build_map_buttons(display_name, sorted_styles, info, chat_id):
    buttons = []

    # Artist Bio — left-aligned single button
    if info.get("bio"):
        buttons.append([InlineKeyboardButton(
            "📋 Artist Bio",
            callback_data=safe_callback(f"map_bio|{chat_id}")
        )])

    # Studio Albums
    if info.get("albums"):
        count = len(info["albums"])
        buttons.append([InlineKeyboardButton(
            f"💿 Studio Albums ({count})",
            callback_data=f"map_albums|{chat_id}"
        )])

    # Similar Artists
    buttons.append([InlineKeyboardButton(
        "🔗 Similar Artists",
        callback_data=safe_callback(f"map_similar|{display_name}")
    )])

    # Styles button
    if sorted_styles:
        buttons.append([InlineKeyboardButton(
            f"🏷️ Styles ({len(sorted_styles)})",
            callback_data=f"map_styles|{chat_id}|0"
        )])

    buttons.append([InlineKeyboardButton("🍌 Main menu", callback_data="cmd|menu")])
    return buttons

# ─── Tags renderer ────────────────────────────────────────────────────────────

TAGS_BLACKLIST = {
    "seen live", "favourite", "favorites", "favourite albums", "albums i own",
    "british", "american", "english", "german", "french", "swedish", "canadian",
    "female vocalists", "male vocalists", "female vocalist", "male vocalist",
    "00s", "90s", "80s", "70s", "60s", "50s",
    "classic", "beautiful", "cool", "awesome", "love", "good", "great",
    "music", "all", "other", "misc", "various",
    "rock", "pop", "electronic", "metal", "jazz", "classical", "folk",
    "dance", "punk", "soul", "blues", "country", "reggae", "hip hop", "hip-hop",
}
TAGS_MIN_CHARS = 4

def _is_valid_tag(tag):
    """Return True if tag is a real genre worth showing."""
    t = tag.lower().strip()
    if t in TAGS_BLACKLIST:
        return False
    if t in tag_blacklist:
        return False
    if len(t) < TAGS_MIN_CHARS:
        return False
    return True

def _tags_nav_row(page, total_pages, prev_cb, next_cb):
    """Always returns a 2-button [Prev, Next] row.
    Missing side shows 'page / total' as a noop label so alignment stays fixed."""
    label = InlineKeyboardButton(f"{page+1} / {total_pages}", callback_data="noop")
    left  = InlineKeyboardButton("← Prev", callback_data=prev_cb) if page > 0          else label
    right = InlineKeyboardButton("Next →",  callback_data=next_cb) if page < total_pages - 1 else label
    return [left, right]


def _build_tags_buttons(sorted_tags, page, edit_mode=False, chat_id=None):
    start     = page * TAGS_PAGE_SIZE
    end       = start + TAGS_PAGE_SIZE
    page_tags = sorted_tags[start:end]
    buttons   = []
    total_pages = max(1, (len(sorted_tags)-1) // TAGS_PAGE_SIZE + 1)
    if edit_mode:
        # Two columns — 🟡/⚪ multiselect instead of immediate ❌ delete
        sel = _pending_tag_deletes.get(chat_id, set()) if chat_id else set()
        row = []
        for tag, count in page_tags:
            tick = "🟡" if tag in sel else "⚪"
            row.append(InlineKeyboardButton(
                f"{tick} {tag.title()} ({count})",
                callback_data=safe_callback(f"tag_toggle|{page}|{tag}")
            ))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append(_tags_nav_row(page, total_pages,
                                     prev_cb=f"tags_edit|{page-1}",
                                     next_cb=f"tags_edit|{page+1}"))
        if sel:
            buttons.append([InlineKeyboardButton(
                f"🗑️ Delete {len(sel)} selected",
                callback_data=f"tag_del_confirm|{page}"
            )])
        if tag_blacklist:
            buttons.append([InlineKeyboardButton(
                f"🔄 Restore {len(tag_blacklist)} hidden tag(s)",
                callback_data="tags_restore_open"
            )])
        buttons.append([InlineKeyboardButton("✅ Done", callback_data=f"tags_page|{page}")])
    else:
        row = []
        for tag, count in page_tags:
            row.append(InlineKeyboardButton(f"{tag.title()} ({count})", callback_data=safe_callback(f"map_style|{tag}")))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append(_tags_nav_row(page, total_pages,
                                     prev_cb=f"tags_page|{page-1}",
                                     next_cb=f"tags_page|{page+1}"))
        buttons.append([
            InlineKeyboardButton("✏️ Edit", callback_data=f"tags_edit|{page}"),
            InlineKeyboardButton("← Back",  callback_data="cmd|explore_menu"),
        ])
    return buttons


def _build_restore_buttons(chat_id, page=0):
    """Multiselect view for restoring hidden tags (🟡/⚪ per tag), paginated."""
    sel       = _pending_tag_restores.get(chat_id, set())
    all_tags  = sorted(tag_blacklist)
    start     = page * TAGS_PAGE_SIZE
    end       = start + TAGS_PAGE_SIZE
    page_tags = all_tags[start:end]
    total_pages = max(1, (len(all_tags) - 1) // TAGS_PAGE_SIZE + 1)
    buttons = []
    row     = []
    for tag in page_tags:
        tick = "🟡" if tag in sel else "⚪"
        row.append(InlineKeyboardButton(
            f"{tick} {tag.title()}",
            callback_data=safe_callback(f"tag_restore_toggle|{page}|{tag}")
        ))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append(_tags_nav_row(page, total_pages,
                                 prev_cb=f"tags_restore_page|{page-1}",
                                 next_cb=f"tags_restore_page|{page+1}"))
    if sel:
        buttons.append([InlineKeyboardButton(
            f"🔄 Restore {len(sel)} selected",
            callback_data=f"tag_restore_confirm|{page}"
        )])
    buttons.append([InlineKeyboardButton("← Back", callback_data="tags_edit|0")])
    return buttons

def _render_tags(message, page=0, edit_mode=False, chat_id=None):
    if not tag_index:
        message.reply_text("No tags collected yet.\n\nUse /map <artist> to start building your library.")
        return
    all_tags    = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
    sorted_tags = [(t, c) for t, c in all_tags if _is_valid_tag(t)]
    total_pages = max(1, (len(sorted_tags)-1) // TAGS_PAGE_SIZE + 1)
    mode_label  = "  ✏️ Edit mode" if edit_mode else ""
    message.reply_text(
        f"🏷️ Tag collection — {len(sorted_tags)} genres (page {page+1}/{total_pages}){mode_label}",
        reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page, edit_mode, chat_id=chat_id))
    )

# ─── Status + Reset ───────────────────────────────────────────────────────────

def _render_status(message):
    visible = [(t, c) for t, c in tag_index.items() if _is_valid_tag(t)]
    hidden  = len(tag_blacklist)
    message.reply_text(
        f"📊 Status\n\n"
        f"🎵 Tracks in history — {len(history['tracks'])}\n"
        f"🏷️ Visible tags — {len(visible)}\n"
        f"🙈 Hidden tags — {hidden}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑️ Clear history",       callback_data="cmd|clear_confirm")],
            [InlineKeyboardButton("🔄 Restore hidden tags", callback_data="tags_restore_open")],
            [InlineKeyboardButton("🍌 Main menu",           callback_data="cmd|menu")],
        ])
    )

def _do_reset(message):
    global _recent_artists
    history["tracks"].clear()
    save_history()
    _recent_artists = []
    message.reply_text(
        "History cleared.\n\nTags are kept.\nFresh tracks on your next request.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🍌 Main menu", callback_data="cmd|menu")]])
    )

def _show_clear_confirm(query):
    query.edit_message_text(
        "🗑️ Clear history?\n\n"
        "Resets which tracks Kurator has already\n"
        "played for you. Start getting fresh\n"
        "recommendations again.\n\n"
        "Tags are kept.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Yes, clear it", callback_data="cmd|reset")],
            [InlineKeyboardButton("← Cancel",         callback_data="cmd|status")],
        ])
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
            map_memory.pop(chat_id, None)
            query.edit_message_text(f"{BOT_VERSION}\n\n<b>✦ Kurator's Picks</b> — Playlists from a real listening history.\n\n<b>🧭 Free Explore</b> — Navigate the music map freely.", parse_mode="HTML", reply_markup=main_menu_markup())

        elif value == "picks_menu":
            map_memory.pop(chat_id, None)
            query.edit_message_text(
                "<b>✦ Kurator's Picks</b>\n\n"
                "A music discovery engine built around taste, not algorithms.\n"
                "Curated from Kurator's own listening history.\n\n"
                "▼ How deep do you want to go?",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🎧 Playlist — A curated selection", callback_data="cmd|playlist")],
                    [InlineKeyboardButton("⛏️ Dig — Deeper cuts",              callback_data="cmd|dig")],
                    [InlineKeyboardButton("💎 Rare — Hidden gems only",        callback_data="cmd|rare")],
                    [InlineKeyboardButton("← Back", callback_data="cmd|menu")],
                ])
            )

        elif value == "explore_menu":
            _pending_gen.pop(chat_id, None)
            query.edit_message_text(
                "<b>🧭 Free Explore</b>\n\n"
                "Navigate the music world freely.\n"
                "Start from an artist, a genre, or your own tag library.\n\n"
                "▼ Where do you want to start?",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🧑‍🎤 Artist",              callback_data="cmd|map_prompt")],
                    [InlineKeyboardButton("🎸 Play a genre",       callback_data="cmd|genre_prompt")],
                    [InlineKeyboardButton("🏷️ My tag collection",  callback_data="cmd|tags")],
                    [InlineKeyboardButton("← Back", callback_data="cmd|menu")],
                ])
            )

        elif value == "playlist":
            _pending_decades.pop(chat_id, None)
            _show_era_choice(query, chat_id, "🎧 Kurator's Playlist", "playlist", "cmd|picks_menu")

        elif value == "dig":
            _pending_decades.pop(chat_id, None)
            _show_era_choice(query, chat_id, "⛏️ Kurator's Dig", "dig", "cmd|picks_menu")

        elif value == "rare":
            _pending_decades.pop(chat_id, None)
            _show_era_choice(query, chat_id, "💎 Kurator's Rare", "rare", "cmd|picks_menu")

        elif value == "map_prompt":
            _nav_history.pop(chat_id, None)
            _pending_gen[chat_id] = {"action": "awaiting_map", "back": "cmd|explore_menu"}
            query.edit_message_text(
                "🧑‍🎤 Artist\n\nType an artist name:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("← Back", callback_data="cmd|explore_menu")],
                ])
            )

        elif value == "genre_prompt":
            _pending_gen[chat_id] = {"action": "awaiting_genre", "back": "cmd|explore_menu"}
            query.edit_message_text(
                "🎸 Play a genre\n\nType a genre name:\n\ne.g. Rock, Jazz, Electronic, Post-Punk",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("← Back", callback_data="cmd|explore_menu")],
                ])
            )

        elif value == "tags":
            _render_tags(message, page=0)

        elif value == "status":
            visible = [(t, c) for t, c in tag_index.items() if _is_valid_tag(t)]
            hidden  = len(tag_blacklist)
            query.edit_message_text(
                f"📊 Status\n\n"
                f"🎵 Tracks in history — {len(history['tracks'])}\n"
                f"🏷️ Visible tags — {len(visible)}\n"
                f"🙈 Hidden tags — {hidden}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🗑️ Clear history",       callback_data="cmd|clear_confirm")],
                    [InlineKeyboardButton("🔄 Restore hidden tags", callback_data="tags_restore_open")],
                    [InlineKeyboardButton("🍌 Main menu",           callback_data="cmd|menu")],
                ])
            )

        elif value == "clear_confirm":
            _show_clear_confirm(query)

        elif value == "reset":
            _do_reset(message)
        elif value == "help":
            query.edit_message_text(_help_text(), parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🍌 Main menu", callback_data="cmd|menu")]]))

    # ── decade_open: show decade toggle grid ─────────────────────────────────
    elif action == "decade_open":
        _pending_decades.setdefault(chat_id, set())
        # Mark that user opened the decade selector
        if chat_id in _pending_gen:
            _pending_gen[chat_id]["decade_opened"] = True
        pending_action = _pending_gen.get(chat_id, {}).get("action", "")
        genre = pending_action.split("|", 1)[1] if pending_action.startswith("build|") else None
        query.edit_message_text(
            "📅 Select a decade:",
            reply_markup=InlineKeyboardMarkup(_decade_selector_buttons(chat_id, genre=genre))
        )

    # ── decade_toggle: toggle a decade on/off ────────────────────────────────
    elif action == "decade_toggle":
        decade   = value
        selected = _pending_decades.setdefault(chat_id, set())
        if decade in selected:
            selected.discard(decade)
        else:
            selected.add(decade)
        pending_action = _pending_gen.get(chat_id, {}).get("action", "")
        genre = pending_action.split("|", 1)[1] if pending_action.startswith("build|") else None
        query.edit_message_text(
            "📅 Select a decade:",
            reply_markup=InlineKeyboardMarkup(_decade_selector_buttons(chat_id, genre=genre))
        )

    # ── decade_confirm: execute pending generation ────────────────────────────
    elif action == "decade_confirm":
        _clear_cancel(chat_id)  # limpiar cualquier cancel previo al arrancar nueva generación
        pending    = _pending_gen.pop(chat_id, {})
        gen_action = pending.get("action", "")
        decades    = _pending_decades.pop(chat_id, set()) or None
        decade_opened = pending.get("decade_opened", False)

        if not gen_action:
            message.reply_text("Session expired. Please try again.")
            return

        # Warn if user opened selector but didn't pick any decade
        if decade_opened and not decades:
            _pending_gen[chat_id] = pending  # restore pending
            _pending_gen[chat_id]["decade_opened"] = False
            back_cb = pending.get("back", "cmd|menu")
            query.edit_message_text(
                "No decade selected.\n\n"
                "Kurator will search across all eras.\n"
                "Want to pick one first?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📅 Select a decade",   callback_data="decade_open")],
                    [InlineKeyboardButton("🍌 Generate anyway",   callback_data="decade_confirm")],
                    [InlineKeyboardButton("← Back",               callback_data=back_cb)],
                ])
            )
            return

        era_tag = f" — {_decade_label_from_set(decades)}" if decades else ""

        if gen_action == "playlist":
            map_memory.pop(chat_id, None)
            query.edit_message_text(f"🎵 Selecting tracks{era_tag}…")
            sent, timer = _working_message(message, "🎵 Still selecting…", delay=50)
            _register_timer(chat_id, sent, timer)
            if decades:
                result = select_tracks_with_decades([], decades=decades, message=message, mode="playlist")
            else:
                result = select_tracks(expand_artist_graph(extract_seed_artists()))
            _cancel_working(sent, timer)
            _unregister_timer(chat_id)
            if _is_cancelled(chat_id): _clear_progress_msgs(chat_id); return
            send_playlist(message, result, title=f"✦ Kurator's Playlist{era_tag}", branded=True, chat_id=chat_id)
            _clear_progress_msgs(chat_id)
            era_label = _decade_label_from_set(decades) if decades else "∞ All Time"
            try: query.edit_message_text(f"✦ Kurator's Playlist — {era_label}")
            except Exception: pass

        elif gen_action == "dig":
            query.edit_message_text(f"⛏️ Digging{era_tag}…")
            sent, timer = _working_message(message, "⛏️ Going deeper…", delay=50)
            _register_timer(chat_id, sent, timer)
            if decades:
                result = select_tracks_with_decades([], decades=decades, message=message, mode="dig")
            else:
                result = select_tracks(expand_artist_graph_deep(extract_seed_artists()))
            _cancel_working(sent, timer)
            _unregister_timer(chat_id)
            if _is_cancelled(chat_id): _clear_progress_msgs(chat_id); return
            send_playlist(message, result, title=f"✦ Kurator's Dig{era_tag}", branded=True, chat_id=chat_id)
            _clear_progress_msgs(chat_id)
            era_label = _decade_label_from_set(decades) if decades else "∞ All Time"
            try: query.edit_message_text(f"⛏️ Kurator's Dig — {era_label}")
            except Exception: pass

        elif gen_action == "rare":
            query.edit_message_text(f"💎 Searching for hidden gems{era_tag}…")
            sent, timer = _working_message(message, "💎 Hunting for gems…", delay=50)
            _register_timer(chat_id, sent, timer)
            if decades:
                result = select_tracks_with_decades([], size=RARE_PLAYLIST_SIZE, decades=decades, message=message, mode="rare")
            else:
                result = select_tracks(expand_artist_graph_rare(extract_seed_artists()), size=RARE_PLAYLIST_SIZE)
            _cancel_working(sent, timer)
            _unregister_timer(chat_id)
            if _is_cancelled(chat_id): _clear_progress_msgs(chat_id); return
            send_playlist(message, result, title=f"✦ Kurator's Rare{era_tag}", branded=True, chat_id=chat_id, size=RARE_PLAYLIST_SIZE)
            _clear_progress_msgs(chat_id)
            era_label = _decade_label_from_set(decades) if decades else "∞ All Time"
            try: query.edit_message_text(f"💎 Kurator's Rare — {era_label}")
            except Exception: pass

        elif gen_action.startswith("build|"):
            style = gen_action.split("|", 1)[1]

            if _is_cancelled(chat_id):
                return

            query.edit_message_text(f"🎸 Building {style.title()}{era_tag} playlist…")
            sent, timer = _working_message(message, "🎸 Still building…", delay=50)
            _register_timer(chat_id, sent, timer)

            # decade_year_range=None means All Time (no year filter)
            decade_year_range = (
                min(DECADE_YEARS[d][0] for d in decades),
                max(DECADE_YEARS[d][1] for d in decades)
            ) if decades else None

            pool = _mb_get_artists_by_genre(style, decades, limit=200)

            # Fallback to Last.fm seeds if MB tag returns nothing
            if not pool:
                log.info(f"[Oracle-POC] No results for '{style}' — falling back to Last.fm seeds")
                pool = _get_era_artists_from_lastfm(style, decades, max_artists=150)

            # Fallback to Discogs if both empty
            if not pool:
                log.info(f"[Oracle-POC] Last.fm empty — falling back to Discogs")
                pool = _get_era_artists_from_discogs(decades, mode="playlist",
                                                      max_artists=100,
                                                      style_override=_resolve_genre_styles(style, decades))

            if _is_cancelled(chat_id):
                _cancel_working(sent, timer)
                return

            random.shuffle(pool)
            global _recent_artists
            target        = GENRE_PLAYLIST_SIZE
            recent_set    = {normalize(r) for r in _recent_artists}
            filtered_pool = [a for a in pool if normalize(a) not in recent_set]
            if len(filtered_pool) < 10:
                filtered_pool = pool

            # Apply underground scoring filter
            if message:
                _safe_reply(message, "🔍 Filtering for quality…")
            filtered_pool = _filter_underground_artists(filtered_pool, style, decades)
            if len(filtered_pool) < 10:
                # Safety fallback — cap at 750k; listeners==0 means unscored, skip (could be mainstream)
                cap_fallback = [a for a in pool
                                if 0 < _artist_listeners_cache.get(a, 0) <= 750_000]
                filtered_pool = cap_fallback if len(cap_fallback) >= 5 else pool
                log.info(f"[Oracle-POC] Safety fallback: {len(filtered_pool)} artists (cap applied)")

            log.info(f"[Oracle-POC] Final pool: {len(filtered_pool)} artists for '{style}' {decades}")

            tracks       = []
            keys_added   = set()
            seen_artists = set()
            artists_used = []

            BATCH_SIZE = 20
            for i in range(0, min(len(filtered_pool), target * 4), BATCH_SIZE):
                if len(tracks) >= target:
                    break
                batch = filtered_pool[i:i + BATCH_SIZE]
                with ThreadPoolExecutor(max_workers=4) as ex:
                    futures = [ex.submit(_fetch_track_for_genre, a, decade_year_range)
                               for a in batch]
                    for f in as_completed(futures):
                        if len(tracks) >= target:
                            break
                        try:
                            result = f.result()
                            if result:
                                artist, track_name, key = result
                                artist_norm = normalize_artist(artist)
                                if artist_norm in seen_artists:
                                    continue
                                if not track_in_history(key) and key not in keys_added:
                                    tracks.append(f"{artist} - {track_name}")
                                    keys_added.add(key)
                                    seen_artists.add(artist_norm)
                                    artists_used.append(artist)
                        except Exception as e:
                            log.error(f"build genre track: {e}")

            for key in keys_added:
                add_to_history(key)
            save_history()
            _recent_artists.extend(artists_used)
            _recent_artists = _recent_artists[-RECENT_ARTISTS_MAX:]

            # Track user genre profile silently
            _update_user_genre_profile(chat_id, style, decades)

            _cancel_working(sent, timer)
            era_tag2 = f" — {_decade_label_from_set(decades)}" if decades else " — ∞ All Time"
            from_map = chat_id in map_memory
            send_playlist(message, tracks, title=f"🎸 {style.title()}{era_tag2}",
                          branded=False, chat_id=chat_id, size=GENRE_PLAYLIST_SIZE,
                          map_chat_id=chat_id if from_map else None)
            _clear_progress_msgs(chat_id)
            try: query.edit_message_text(f"🎸 {style.title()}{era_tag2}")
            except Exception: pass

    # ── decade_back: restore era choice screen ────────────────────────────────
    elif action == "decade_back":
        pending = _pending_gen.get(chat_id, {})
        back_cb = pending.get("back", "cmd|menu")
        _pending_decades.pop(chat_id, None)
        gen_action = pending.get("action", "")
        if gen_action.startswith("build|"):
            # Genre flow — restore ∞ All Time / 📅 decade screen
            style = gen_action.split("|", 1)[1]
            _genre_era_prompt(query.edit_message_text, chat_id, style, back_cb)
        else:
            # Picks flow — restore 🍌 GENERATE PLAYLIST screen
            parts = gen_action.split("|", 1)
            title_map = {"playlist": "🎧 Kurator's Playlist", "dig": "⛏️ Kurator's Dig", "rare": "💎 Kurator's Rare"}
            title = title_map.get(parts[0], "Generate playlist")
            _show_era_choice(query, chat_id, title, gen_action, back_cb)

    # ── decade_alltime: All Time confirmation screen ──────────────────────────
    elif action == "decade_alltime":
        _pending_decades.pop(chat_id, None)  # clear any selected decades → All Time
        pending = _pending_gen.get(chat_id, {})
        gen_action = pending.get("action", "")
        back_cb = pending.get("back", "cmd|explore_menu")
        if gen_action.startswith("build|"):
            style = gen_action.split("|", 1)[1]
            header = f"🎸 {style.title()} — ∞ All Time"
        else:
            header = "∞ All Time"
        query.edit_message_text(
            header,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🍌 GENERATE PLAYLIST", callback_data="decade_confirm")],
                [InlineKeyboardButton("← Back",               callback_data="decade_era_back")],
            ])
        )

    # ── decade_era_back: return to era choice (∞ / 📅) ───────────────────────
    elif action == "decade_era_back":
        pending = _pending_gen.get(chat_id, {})
        gen_action = pending.get("action", "")
        back_cb = pending.get("back", "cmd|explore_menu")
        if gen_action.startswith("build|"):
            style = gen_action.split("|", 1)[1]
            _genre_era_prompt(query.edit_message_text, chat_id, style, back_cb)
        else:
            parts = gen_action.split("|", 1)
            title_map = {"playlist": "🎧 Kurator's Playlist", "dig": "⛏️ Kurator's Dig", "rare": "💎 Kurator's Rare"}
            title = title_map.get(parts[0], "Generate playlist")
            _show_era_choice(query, chat_id, title, gen_action, back_cb)

    # ── onboarding ────────────────────────────────────────────────────────────
    elif action == "onboard":
        step = int(value)
        if step == 1:
            query.edit_message_text(
                "Kurator 📀\n\n"
                "A music discovery engine built around taste, not algorithms.\n\n"
                "Curated selections drawn from a real listening history — not what's trending.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("→ Next", callback_data="onboard|2")
                ]])
            )
        elif step == 2:
            query.edit_message_text(
                "2 / 3  —  TWO WAYS TO DISCOVER\n\n"
                "🍌 KURATOR'S PICKS\n"
                "Playlists curated from Kurator's own listening history. "
                "A real person's taste — not an algorithm.\n\n"
                "🔍 FREE EXPLORE\n"
                "Navigate the music map freely. Start from any artist, "
                "explore by genre, or browse your own tag library.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("← Back", callback_data="onboard|1"),
                    InlineKeyboardButton("→ Next", callback_data="onboard|3"),
                ]])
            )
        elif step == 3:
            query.edit_message_text(
                "3 / 3  —  HOW TO EXPORT\n\n"
                "Once you have a playlist you like, export it to any "
                "streaming platform via Soundiiz — Spotify, Qobuz, "
                "Apple Music and 40+ more.\n\n"
                "Tap the 🟣 Export via Soundiiz button after any playlist.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("← Back",   callback_data="onboard|2"),
                    InlineKeyboardButton("Let's go →", callback_data="onboard|0"),
                ]])
            )
        elif step == 0:
            mark_onboarded(chat_id)
            query.edit_message_text(
                "Kurator 📀\n\nTap a command to begin.",
                reply_markup=main_menu_markup()
            )

    # ── map_bio ───────────────────────────────────────────────────────────────
    elif action == "map_bio":
        mem          = map_memory.get(chat_id, {})
        info         = mem.get("info", {})
        display_name = mem.get("display_name", "")
        bio          = info.get("bio", "No bio available.")
        lfm          = info.get("lastfm_url", "")
        link         = f"\n\n↗ Full profile\n{lfm}" if lfm else ""
        query.edit_message_text(
            f"{display_name}\n\n{bio}{link}",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(f"← Back to {display_name[:20]}", callback_data=f"map_back|{chat_id}")
            ]])
        )

    # ── map_albums ────────────────────────────────────────────────────────────
    elif action == "map_albums":
        mem          = map_memory.get(chat_id, {})
        info         = mem.get("info", {})
        display_name = mem.get("display_name", "")
        albums       = info.get("albums", [])
        if not albums:
            message.reply_text("No studio albums found.")
            return
        buttons = []
        for album in albums:
            ep_tag = " EP" if album.get("is_ep") else ""
            year   = f"({album['year']}) " if album.get("year") else ""
            label  = f"{year}{album['title'][:36]}{ep_tag}"
            buttons.append([InlineKeyboardButton(
                label,
                callback_data=safe_callback(f"album_select|{chat_id}|{album['title'][:20]}")
            )])
        buttons.append([InlineKeyboardButton(f"← Back to {display_name[:20]}", callback_data=f"card_back|{chat_id}")])
        query.edit_message_text(
            f"{display_name} — Studio Albums",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    # ── album_select ──────────────────────────────────────────────────────────
    elif action == "album_select":
        sub          = value.split("|", 2)
        album_chat   = int(sub[0]) if len(sub) > 0 else chat_id
        album_title  = sub[1] if len(sub) > 1 else ""
        mem          = map_memory.get(album_chat, {})
        info         = mem.get("info", {})
        display_name = mem.get("display_name", "")
        # Find full title from albums list
        full_title = album_title
        for a in info.get("albums", []):
            if a["title"].startswith(album_title):
                full_title = a["title"]
                break
        query.edit_message_text(
            f"{display_name} — {full_title}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎧 Generate playlist", callback_data=safe_callback(f"album_playlist|{display_name}|{full_title}"))],
                [InlineKeyboardButton("🟢 Open in Spotify",   url=spotify_album_url(display_name, full_title))],
                [InlineKeyboardButton(f"← Back",             callback_data=f"map_albums|{album_chat}")],
            ])
        )

    # ── album_playlist ────────────────────────────────────────────────────────
    elif action == "album_playlist":
        sub    = value.split("|", 2)
        artist = sub[0] if len(sub) > 0 else ""
        album  = sub[1] if len(sub) > 1 else ""
        query.edit_message_text(f"📀 Building {album.upper()} playlist…")
        data   = lastfm("album.getinfo", artist=artist, album=album)
        tracks = [t.get("name") for t in data.get("album", {}).get("tracks", {}).get("track", []) if t.get("name")]
        track_strings = [f"{artist} - {t}" for t in tracks]
        if not track_strings:
            query.edit_message_text(
                f"No tracks found for {album}.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Back", callback_data=f"map_back|{chat_id}")]])
            )
            return
        send_playlist(message, track_strings, title=f"💿 {album.upper()}", branded=False, chat_id=chat_id, map_chat_id=chat_id, suppress_warning=True)

    # ── map_similar ───────────────────────────────────────────────────────────
    elif action == "map_similar":
        artist = value
        query.edit_message_text(f"🔗 Fetching similar artists for {artist}…")
        similar = [s["name"] for s in
                   lastfm("artist.getsimilar", artist=artist, limit=60)
                   .get("similarartists", {}).get("artist", [])]

        if chat_id in map_memory:
            map_memory[chat_id]["similar"] = similar
            save_map_memory()

        _render_similar(query, artist, similar, page=0, chat_id=chat_id)

    # ── similar_page: paginate similar artists ────────────────────────────────
    elif action == "similar_page":
        sub    = value.split("|", 1)
        artist = sub[0]
        page   = int(sub[1]) if len(sub) > 1 else 0
        similar = map_memory.get(chat_id, {}).get("similar", [])
        _render_similar(query, artist, similar, page=page, chat_id=chat_id)

    # ── explore_artist: navigate to a similar artist's map card ──────────────
    elif action == "explore_artist":
        sub         = value.split("|", 1)
        new_artist  = sub[0]
        from_artist = sub[1] if len(sub) > 1 else ""

        if chat_id not in _nav_history:
            _nav_history[chat_id] = []
        if from_artist:
            _nav_history[chat_id].append(from_artist)
        _nav_history[chat_id] = _nav_history[chat_id][-10:]

        # Just send new card — don't touch the current message
        message.reply_text(f"🧑‍🎤 Exploring {new_artist.upper()}…")
        _render_map(message, new_artist, chat_id)

    # ── trail_go ──────────────────────────────────────────────────────────────
    elif action == "trail_go":
        sub    = value.split("|", 1)
        hops   = int(sub[0])
        artist = sub[1] if len(sub) > 1 else ""
        labels = {1: "🔗 1 hop — Direct neighbours", 2: "🔗 2 hops — Wider connections"}
        query.edit_message_text(
            f"🧬 {artist}\n{labels.get(hops, '')}\n\nGenerate playlist?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🍌 GENERATE PLAYLIST", callback_data=safe_callback(f"trail_confirm|{hops}|{artist}"))],
                [InlineKeyboardButton(f"← Back to {artist[:20]}", callback_data=f"card_back|{chat_id}")],
            ])
        )

    # ── trail_confirm ─────────────────────────────────────────────────────────
    elif action == "trail_confirm":
        sub    = value.split("|", 1)
        hops   = int(sub[0])
        artist = sub[1] if len(sub) > 1 else ""
        hop_labels = {1: "1 hop", 2: "2 hops"}
        query.edit_message_text(f"🔗 {artist} — Following the trail ({hop_labels.get(hops, '')})…")
        sent, timer = _working_message(message, "🧑‍🎤 Still exploring…")
        if hops == 1:
            stored = map_memory.get(chat_id, {}).get("similar")
            names  = stored if stored else _expand_trail(artist, 1)
            history_tracks = history.get("tracks") or {}
            exhausted = sum(1 for n in names if any(k.startswith(normalize(n)+"-") for k in history_tracks))
            if names and exhausted / len(names) > 0.6:
                log.info(f"Trail auto-expanding to 2 hops ({exhausted}/{len(names)} exhausted)")
                names = _expand_trail(artist, 2)
        else:
            names = _expand_trail(artist, hops)
        result = select_tracks(names, skip_recent=False)
        _cancel_working(sent, timer)
        send_playlist(message, result,
                      title=f"🔗 {artist} — {hop_labels.get(hops, '')}",
                      branded=False, chat_id=chat_id, map_chat_id=chat_id)

    # ── map_styles: paginated styles view ────────────────────────────────────
    elif action == "map_styles":
        sub          = value.split("|", 1)
        page         = int(sub[1]) if len(sub) > 1 else 0
        mem          = map_memory.get(chat_id, {})
        display_name = mem.get("display_name", "")
        styles       = mem.get("styles", [])
        PAGE_SIZE    = 8
        total_pages  = max(1, (len(styles) - 1) // PAGE_SIZE + 1)
        page_styles  = styles[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
        buttons      = []
        style_row    = []
        for style, count in page_styles:
            style_row.append(InlineKeyboardButton(
                f"{style.title()}  ({count})",
                callback_data=safe_callback(f"map_style|{style}")
            ))
            if len(style_row) == 2:
                buttons.append(style_row)
                style_row = []
        if style_row:
            buttons.append(style_row)
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("← Prev", callback_data=f"map_styles|{chat_id}|{page-1}"))
        if page + 1 < total_pages:
            nav.append(InlineKeyboardButton("Next →", callback_data=f"map_styles|{chat_id}|{page+1}"))
        if nav:
            buttons.append(nav)
        buttons.append([InlineKeyboardButton(f"← Back to {display_name[:20]}", callback_data=f"card_back|{chat_id}")])
        page_label = f" (page {page+1}/{total_pages})" if total_pages > 1 else ""
        query.edit_message_text(
            f"🏷️ {display_name} — Styles{page_label}",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    # ── map_style ─────────────────────────────────────────────────────────────
    elif action == "map_style":
        mem          = map_memory.get(chat_id, {})
        display_name = mem.get("display_name", "")
        back_cb      = f"map_styles|{chat_id}|0" if display_name else "cmd|explore_menu"
        _genre_era_prompt(query.edit_message_text, chat_id, value, back_cb)

    # ── map_back ──────────────────────────────────────────────────────────────
    # ── card_back: return to current card without touching nav history ─────────
    elif action == "card_back":
        mem          = map_memory.get(chat_id, {})
        artist       = mem.get("artist", "")
        display_name = mem.get("display_name", "")
        styles       = mem.get("styles", [])
        info         = mem.get("info", {})
        if not artist or not styles:
            query.edit_message_text(f"{BOT_VERSION}\n\n<b>✦ Kurator's Picks</b> — Playlists from a real listening history.\n\n<b>🧭 Free Explore</b> — Navigate the music map freely.", parse_mode="HTML", reply_markup=main_menu_markup())
            return
        card_text = _format_artist_card(artist, info)
        buttons   = _build_map_buttons(display_name, styles, info, chat_id)
        query.edit_message_text(f"{card_text}\n\nExplore:", reply_markup=InlineKeyboardMarkup(buttons))

    elif action == "map_back":
        history_stack = _nav_history.get(chat_id, [])
        if history_stack:
            prev_artist = history_stack.pop()
            _nav_history[chat_id] = history_stack
            message.reply_text(f"🧑‍🎤 Exploring {prev_artist.upper()}…")
            _render_map(message, prev_artist, chat_id)
            return

        mem          = map_memory.get(chat_id, {})
        artist       = mem.get("artist", "")
        display_name = mem.get("display_name", "")
        styles       = mem.get("styles", [])
        info         = mem.get("info", {})
        if not artist or not styles:
            query.edit_message_text(f"{BOT_VERSION}\n\n<b>✦ Kurator's Picks</b> — Playlists from a real listening history.\n\n<b>🧭 Free Explore</b> — Navigate the music map freely.", parse_mode="HTML", reply_markup=main_menu_markup())
            return
        card_text = _format_artist_card(artist, info)
        buttons   = _build_map_buttons(display_name, styles, info, chat_id)
        query.edit_message_text(f"{card_text}\n\nExplore:", reply_markup=InlineKeyboardMarkup(buttons))

    # ── tags ──────────────────────────────────────────────────────────────────
    elif action == "tags_page":
        page        = int(value)
        # Clear pending state when leaving edit mode
        _pending_tag_deletes.pop(chat_id, None)
        _pending_tag_restores.pop(chat_id, None)
        all_tags    = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
        sorted_tags = [(t, c) for t, c in all_tags if _is_valid_tag(t)]
        total_pages = max(1, (len(sorted_tags)-1) // TAGS_PAGE_SIZE + 1)
        query.edit_message_text(
            f"🏷️ Tag collection — {len(sorted_tags)} genres (page {page+1}/{total_pages})",
            reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page, chat_id=chat_id))
        )

    elif action == "tags_edit":
        page        = int(value)
        all_tags    = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
        sorted_tags = [(t, c) for t, c in all_tags if _is_valid_tag(t)]
        total_pages = max(1, (len(sorted_tags)-1) // TAGS_PAGE_SIZE + 1)
        query.edit_message_text(
            f"🏷️ Tag collection — {len(sorted_tags)} genres (page {page+1}/{total_pages})  ✏️ Edit mode",
            reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page, edit_mode=True, chat_id=chat_id))
        )

    elif action == "tag_toggle":
        # Multiselect: toggle a tag in/out of pending delete set
        parts_val = value.split("|", 1)
        page      = int(parts_val[0]) if len(parts_val) > 1 else 0
        tag       = parts_val[1] if len(parts_val) > 1 else value
        sel = _pending_tag_deletes.setdefault(chat_id, set())
        if tag in sel:
            sel.discard(tag)
        else:
            sel.add(tag)
        all_tags    = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
        sorted_tags = [(t, c) for t, c in all_tags if _is_valid_tag(t)]
        total_pages = max(1, (len(sorted_tags)-1) // TAGS_PAGE_SIZE + 1)
        query.edit_message_text(
            f"🏷️ Tag collection — {len(sorted_tags)} genres (page {page+1}/{total_pages})  ✏️ Edit mode",
            reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page, edit_mode=True, chat_id=chat_id))
        )

    elif action == "tag_del_confirm":
        # Show confirmation before deleting selected tags
        page   = int(value) if value else 0
        to_del = _pending_tag_deletes.get(chat_id, set())
        if not to_del:
            query.answer("No tags selected.")
            return
        names = ", ".join(sorted(t.title() for t in to_del))
        query.edit_message_text(
            f"🗑️ Delete {len(to_del)} tag(s)?\n\n{names}\n\nThey'll move to hidden — recoverable from Restore.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"✅ Yes, delete {len(to_del)}", callback_data=f"tag_del_exec|{page}")],
                [InlineKeyboardButton("← Cancel", callback_data=f"tags_edit|{page}")],
            ])
        )

    elif action == "tag_del_exec":
        # Execute deletion after confirmation
        page   = int(value) if value else 0
        to_del = _pending_tag_deletes.pop(chat_id, set())
        for tag in to_del:
            tag_index.pop(tag, None)
            tag_blacklist.add(tag.lower())
        if to_del:
            save_tag_index()
            save_tag_blacklist()
        all_tags    = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
        sorted_tags = [(t, c) for t, c in all_tags if _is_valid_tag(t)]
        if not sorted_tags:
            query.edit_message_text("Tag collection is empty.\n\nUse /map <artist> to build it up.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🍌 Main menu", callback_data="cmd|menu")]]))
            return
        total_pages = max(1, (len(sorted_tags)-1) // TAGS_PAGE_SIZE + 1)
        page = min(page, total_pages - 1)
        query.edit_message_text(
            f"🏷️ Tag collection — {len(sorted_tags)} genres (page {page+1}/{total_pages})  ✏️ Edit mode",
            reply_markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page, edit_mode=True, chat_id=chat_id))
        )

    elif action == "tags_restore_open":
        # Open multiselect view for restoring hidden tags
        _pending_tag_restores[chat_id] = set()
        if not tag_blacklist:
            query.answer("No hidden tags to restore.")
            return
        query.edit_message_text(
            "🔄 Select tags to restore:",
            reply_markup=InlineKeyboardMarkup(_build_restore_buttons(chat_id, page=0))
        )

    elif action == "tags_restore_page":
        # Navigate pages within restore view (keeps selection)
        page = int(value)
        query.edit_message_text(
            "🔄 Select tags to restore:",
            reply_markup=InlineKeyboardMarkup(_build_restore_buttons(chat_id, page=page))
        )

    elif action == "tag_restore_toggle":
        # Toggle a hidden tag in/out of pending restore set
        parts_val = value.split("|", 1)
        page      = int(parts_val[0]) if len(parts_val) > 1 else 0
        tag       = parts_val[1] if len(parts_val) > 1 else value
        sel = _pending_tag_restores.setdefault(chat_id, set())
        if tag in sel:
            sel.discard(tag)
        else:
            sel.add(tag)
        query.edit_message_text(
            "🔄 Select tags to restore:",
            reply_markup=InlineKeyboardMarkup(_build_restore_buttons(chat_id, page=page))
        )

    elif action == "tag_restore_confirm":
        # Show confirmation before restoring selected hidden tags
        back_page  = int(value) if value else 0
        to_restore = _pending_tag_restores.get(chat_id, set())
        if not to_restore:
            query.answer("No tags selected.")
            return
        names = ", ".join(sorted(t.title() for t in to_restore))
        query.edit_message_text(
            f"🔄 Restore {len(to_restore)} tag(s)?\n\n{names}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"✅ Yes, restore {len(to_restore)}", callback_data="tag_restore_exec")],
                [InlineKeyboardButton("← Cancel", callback_data=f"tags_restore_page|{back_page}")],
            ])
        )

    elif action == "tag_restore_exec":
        # Execute restoration after confirmation
        to_restore = _pending_tag_restores.pop(chat_id, set())
        for tag in to_restore:
            tag_blacklist.discard(tag.lower())
            if tag.lower() not in tag_index:
                tag_index[tag.lower()] = 1
        if to_restore:
            save_tag_index()
            save_tag_blacklist()
        _render_tags(message, page=0, chat_id=chat_id)

    # ── soundiiz_help ─────────────────────────────────────────────────────────
    elif action == "soundiiz_help":
        key = value
        query.edit_message_text(
            "📡 Export your playlist\n\n"
            "1. Go to Soundiiz — log in or create a free account\n\n"
            "2. Tap ··· top right — select \"Import playlist\"\n\n"
            "3. Select \"From plain text\"\n\n"
            "4. Paste your playlist — tap \"Send text\"\n\n"
            "5. Choose your platform: Spotify, Qobuz, Apple Music and more\n\n"
            "↗ soundiiz.com",
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("← Back", callback_data=f"export_back|{key}")],
            ])
        )

    # ── export_expand: show full export options ───────────────────────────────
    elif action == "export_expand":
        query.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup(_export_buttons(value))
        )

    # ── export_collapse: back to single Export button ─────────────────────────
    elif action == "export_collapse":
        stored = _track_store.get(value, {})
        mcid   = stored.get("map_chat_id") if isinstance(stored, dict) else None
        text   = stored.get("text") if isinstance(stored, dict) else None
        if text:
            query.edit_message_text(
                text,
                disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup(_export_collapsed_buttons(value, map_chat_id=mcid))
            )
        else:
            query.edit_message_reply_markup(
                reply_markup=InlineKeyboardMarkup(_export_collapsed_buttons(value, map_chat_id=mcid))
            )

    # ── export_back ───────────────────────────────────────────────────────────
    elif action == "export_back":
        stored = _track_store.get(value, {})
        text   = stored.get("text") if isinstance(stored, dict) else None
        if text:
            query.edit_message_text(
                text,
                disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup(_export_buttons(value))
            )
        else:
            query.edit_message_reply_markup(
                reply_markup=InlineKeyboardMarkup(_export_buttons(value))
            )

    # ── sp_expand ─────────────────────────────────────────────────────────────
    elif action == "sp_expand":
        sub    = value.split("|", 1)
        key    = sub[0]
        page   = int(sub[1]) if len(sub) > 1 else 0
        stored = _track_store.get(key, {})
        tracks = stored.get("tracks", []) if isinstance(stored, dict) else stored
        if not tracks:
            message.reply_text("Links expired. Generate a new playlist.")
            return
        total_pages = max(1, (len(tracks)-1) // TRACK_LINKS_PAGE + 1)
        start       = page * TRACK_LINKS_PAGE
        page_tracks = tracks[start:start+TRACK_LINKS_PAGE]
        buttons = []
        for t in page_tracks:
            parts = t.split(" - ", 1)
            label = f"{parts[0][:18]} – {parts[1][:32]}" if len(parts) == 2 else t[:52]
            buttons.append([InlineKeyboardButton(f"‣ {label}", url=spotify_url(t))])
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("← Prev", callback_data=f"sp_expand|{key}|{page-1}"))
        if (page+1) < total_pages:
            nav.append(InlineKeyboardButton("Next →", callback_data=f"sp_expand|{key}|{page+1}"))
        if nav:
            buttons.append(nav)
        buttons.append([InlineKeyboardButton("← Back", callback_data=f"export_back|{key}")])
        query.edit_message_text(
            f"Track links — page {page+1}/{total_pages}",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

# ─── Flask server ─────────────────────────────────────────────────────────────

flask_app = Flask(__name__)

@flask_app.route("/health")
def health():
    return "OK", 200

def _start_flask():
    flask_app.run(host="0.0.0.0", port=8080, use_reloader=False)

# ─── Text reply handler ───────────────────────────────────────────────────────

def handle_text_reply(update, context):
    """Handle text input for map and genre searches."""
    msg     = update.message
    chat_id = update.effective_chat.id
    text    = msg.text.strip() if msg.text else ""

    if not text:
        return

    # Botón persistente de Menú — tiene prioridad sobre cualquier estado pendiente
    if text in ("🍌 Menú", "Menú", "Menu", "menú", "menu"):
        _pending_gen.pop(chat_id, None)
        msg.reply_text(
            f"{BOT_VERSION}\n\n<b>✦ Kurator's Picks</b> — Playlists from a real listening history.\n\n<b>🧭 Free Explore</b> — Navigate the music map freely.",
            parse_mode="HTML",
            reply_markup=main_menu_markup()
        )
        return

    pending = _pending_gen.get(chat_id, {})
    action  = pending.get("action", "")

    if action == "awaiting_map":
        _pending_gen.pop(chat_id, None)
        prev_id = _pending_map_msgs.pop(chat_id, None)
        if prev_id:
            try:
                msg.bot.delete_message(chat_id=chat_id, message_id=prev_id)
            except Exception:
                pass
        mapping_msg = msg.reply_text(f"🧑‍🎤 Exploring {text.upper()}…")
        _pending_map_msgs[chat_id] = mapping_msg.message_id
        sent, timer = _working_message(msg, "🧑‍🎤 Still exploring…")
        _nav_history.pop(chat_id, None)
        _render_map(msg, text, chat_id)
        _cancel_working(sent, timer)
        _pending_map_msgs.pop(chat_id, None)

    elif action == "awaiting_genre":
        _pending_gen.pop(chat_id, None)
        tag = text.lower().strip()
        _genre_era_prompt(msg.reply_text, chat_id, tag, "cmd|explore_menu")

# ─── Boot ─────────────────────────────────────────────────────────────────────

updater  = Updater(TELEGRAM_TOKEN)
dp       = updater.dispatcher

# Start Flask OAuth server in background
threading.Thread(target=_start_flask, daemon=True).start()
log.info("Flask OAuth server started on port 8080")

dp.add_handler(CommandHandler("start",     start))
dp.add_handler(CommandHandler("help",      help_command))
dp.add_handler(CommandHandler("changelog", changelog_command))
dp.add_handler(CommandHandler("genre",     genre_command))
dp.add_handler(CommandHandler("playlist",  playlist))
dp.add_handler(CommandHandler("dig",      dig))
dp.add_handler(CommandHandler("rare",     rare))
dp.add_handler(CommandHandler("artist",   map_command))  # nombre principal
dp.add_handler(CommandHandler("explore",  map_command))  # alias
dp.add_handler(CommandHandler("map",      map_command))  # alias
dp.add_handler(CommandHandler("tags",     tags))
dp.add_handler(CommandHandler("status",   status))
dp.add_handler(CommandHandler("reset",    reset))
dp.add_handler(CommandHandler("cancel",   cancel_command))  # oculto — no en set_my_commands
dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_text_reply))
dp.add_handler(CallbackQueryHandler(handle_buttons))

# Registrar comandos para activar el botón ☰ nativo de Telegram
from telegram import BotCommand
try:
    updater.bot.set_my_commands([
        BotCommand("start",    "🍌 Main menu"),
        BotCommand("playlist", "🎧 Kurator's Playlist"),
        BotCommand("dig",      "⛏️ Dig deeper"),
        BotCommand("rare",     "💎 Rare finds"),
        BotCommand("artist",   "🧑‍🎤 Explore an artist"),
        BotCommand("genre",    "🎸 Genre playlist"),
        BotCommand("tags",     "🏷️ Browse tags"),
        BotCommand("status",   "📊 My stats"),
        BotCommand("help",     "❓ Help"),
    ])
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setChatMenuButton",
        json={"menu_button": {"type": "commands"}},
        timeout=10
    )
    log.info("Telegram commands registered")
except Exception as e:
    log.warning(f"Failed to register commands: {e}")

log.info(BOT_VERSION)
print(BOT_VERSION)
updater.start_polling()
updater.idle()
