import os
import re
import json
import logging
import random
import time
import tempfile
import itertools
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

import requests
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler, CommandHandler, Updater

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# ─── Version ──────────────────────────────────────────────────────────────────
BOT_VERSION = "Kurator 📀 Music Discovery Engine (v4.5.8)"

# ─── Environment ──────────────────────────────────────────────────────────────
LASTFM_USER    = "burbq"
LASTFM_API     = os.environ["LASTFM_API_KEY"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
DISCOGS_TOKEN  = os.environ["DISCOGS_TOKEN"]

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
TRACK_STORE_MAX     = 20
TRACK_FETCH_LIMIT   = 50
TRACK_SKIP_TOP      = 5
TRACK_PLAYCOUNT_MAX = 500_000
HISTORY_EXPIRY_DAYS = 90
TRACK_LINKS_PAGE    = 10
MB_USER_AGENT       = "Kurator/4.5.4 (telegram bot)"
DECADES             = ["50s", "60s", "70s", "80s", "90s", "00s", "10s", "20s"]
DECADE_YEARS        = {
    "50s": (1950, 1959), "60s": (1960, 1969), "70s": (1970, 1979),
    "80s": (1980, 1989), "90s": (1990, 1999), "00s": (2000, 2009),
    "10s": (2010, 2019), "20s": (2020, 2029),
}

# Pending decade selections per chat_id: {chat_id: set of selected decades}
_pending_decades   = {}
# Pending generation actions per chat_id: {chat_id: {"action": str, "data": dict}}
_pending_gen       = {}
# Navigation history per chat_id: [{artist, display_name, styles, info}]
_nav_history       = {}

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

# ─── Navigation helper ────────────────────────────────────────────────────────

def _nav(query, text, markup=None, disable_preview=False):
    """
    Delete the current message and send a new one.
    Forces Telegram to scroll to the bottom automatically.
    Falls back to edit if delete fails.
    """
    try:
        query.message.delete()
        query.message.reply_text(
            text,
            reply_markup=markup,
            disable_web_page_preview=disable_preview
        )
    except Exception:
        try:
            query.message.reply_text(
                text,
                reply_markup=markup,
                disable_web_page_preview=disable_preview
            )
        except Exception as e:
            log.error(f"_nav failed: {e}")

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
    d   = load_json(HISTORY_FILE, {})
    raw = d.get("tracks", {})
    if isinstance(raw, list):
        now    = time.time()
        tracks = {t: now for t in raw}
        log.info(f"Migrated {len(tracks)} tracks to timestamped history.")
    else:
        tracks = {k: float(v) for k, v in raw.items()}
    return {"tracks": tracks}

def save_history():
    save_json(HISTORY_FILE, {"tracks": history["tracks"]})

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

_onboarded = set(load_json(ONBOARDED_FILE, []))

def mark_onboarded(chat_id):
    _onboarded.add(str(chat_id))
    save_json(ONBOARDED_FILE, list(_onboarded))

def is_onboarded(chat_id):
    return str(chat_id) in _onboarded

# ─── Persistent tag_index + map_memory ───────────────────────────────────────

tag_index   = load_json(TAG_INDEX_FILE, {})
_map_raw    = load_json(MAP_FILE, {})
map_memory  = {int(k): v for k, v in _map_raw.items()}

def save_tag_index():
    save_json(TAG_INDEX_FILE, tag_index)

def save_map_memory():
    save_json(MAP_FILE, {str(k): v for k, v in map_memory.items()})

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
    if hops == 2:
        return list(level1 | level2)
    level3 = set()
    with ThreadPoolExecutor(max_workers=8) as ex:
        for f in as_completed([ex.submit(_fetch_similar_names, a)
                               for a in random.sample(list(level2), min(len(level2), 20))]):
            try: level3.update(f.result())
            except: pass
    return list(level3 - level1 - level2 - {artist})

# ─── Track selection ──────────────────────────────────────────────────────────

def _fetch_top_track(artist):
    data     = lastfm("artist.gettoptracks", artist=artist, limit=TRACK_FETCH_LIMIT)
    top      = data.get("toptracks", {}).get("track", [])
    pool     = top[TRACK_SKIP_TOP:] or top
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
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(_fetch_top_track, a) for a in artists[:target * 7]]
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

def _artist_decade_from_lastfm(artist):
    """
    Get artist's decade tags from Last.fm.
    Returns set of decade strings like {'60s', '70s'} or empty set.
    """
    try:
        data = lastfm("artist.getinfo", artist=artist)
        tags = data.get("artist", {}).get("tags", {}).get("tag", [])
        decades = set()
        for t in tags:
            name = t.get("name", "").lower().strip()
            # Match patterns: "60s", "1960s", "1960's", "sixties" etc.
            for d in DECADES:
                decade_num = d[:2]  # "60", "70" etc
                century    = "19" + decade_num if int(decade_num) >= 50 else "20" + decade_num
                if (d in name or
                    century + "s" in name or
                    century + "'s" in name):
                    decades.add(d)
        # Also check bio published year as fallback
        if not decades:
            bio = data.get("artist", {}).get("bio", {})
            pub = bio.get("published", "") or bio.get("formed", "")
            if pub:
                for chunk in pub.split():
                    if chunk.isdigit() and len(chunk) == 4:
                        year = int(chunk)
                        for d, (lo, hi) in DECADE_YEARS.items():
                            if lo <= year <= hi:
                                decades.add(d)
                                break
        return decades
    except Exception:
        return set()

def _filter_artists_by_decade(artists, decades, message=None):
    """
    Filter artists by era using Last.fm tags.
    Optimized: caps pool, stops early when enough found, periodic messages.
    """
    if not decades:
        return artists

    # Cap pool to avoid too many API calls
    pool = artists[:80] if len(artists) > 80 else artists
    random.shuffle(pool)

    if message:
        try: message.reply_text("🔍 Filtering by era…")
        except Exception: pass

    # Periodic messages every 10s
    import threading
    _periodic_msgs = [
        (10, "Still filtering…"),
        (20, "Cross-checking artists…"),
        (30, "Almost there…"),
        (45, "Last few checks…"),
    ]
    _timers = []
    if message:
        for delay, text in _periodic_msgs:
            t = threading.Timer(delay, lambda m=message, tx=text: m.reply_text(tx))
            t.start()
            _timers.append(t)

    passed  = []
    unknown = []
    TARGET  = 25  # stop early when we have enough

    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = {ex.submit(_artist_decade_from_lastfm, a): a for a in pool}
        for f in as_completed(futures):
            artist = futures[f]
            try:
                artist_decades = f.result()
                if not artist_decades:
                    unknown.append(artist)
                elif artist_decades & decades:
                    passed.append(artist)
                    if len(passed) >= TARGET:
                        # Cancel remaining futures and exit early
                        for remaining in futures:
                            remaining.cancel()
                        break
            except Exception:
                unknown.append(artist)

    # Cancel all pending timers
    for t in _timers:
        t.cancel()

    log.info(f"Era filter: {len(pool)} pool → {len(passed)} passed → {len(unknown)} unknown")

    if len(passed) >= 15:
        return passed

    if len(passed) + len(unknown) >= 10:
        return passed + unknown[:max(0, 30 - len(passed))]

    log.info("Era filter too restrictive — using full pool")
    return pool

def select_tracks_with_decades(artists, size=None, decades=None, message=None):
    """Select tracks from era-filtered artists."""
    if not decades:
        return select_tracks(artists, size=size)
    filtered = _filter_artists_by_decade(artists, decades, message=message)
    return select_tracks(filtered, size=size)

def _decade_label_from_set(decades):
    """Human-readable label from a set of decades."""
    if not decades:
        return ""
    return " · ".join(sorted(decades, key=lambda d: DECADES.index(d) if d in DECADES else 99))

def _decade_label(chat_id):
    """Human-readable label for selected decades."""
    decades = _pending_decades.get(chat_id, set())
    return _decade_label_from_set(decades)

_track_store   = {}
_track_counter = itertools.count()

def _store_tracks(tracks):
    key = str(next(_track_counter))
    _track_store[key] = tracks
    if len(_track_store) > TRACK_STORE_MAX:
        for old in sorted(_track_store.keys(), key=int)[:len(_track_store) - TRACK_STORE_MAX]:
            del _track_store[old]
    return key

# ─── Decade selector ──────────────────────────────────────────────────────────

def _decade_selector_buttons(chat_id):
    """Build decade toggle buttons — action stored in _pending_gen."""
    selected = _pending_decades.get(chat_id, set())
    buttons  = []
    row      = []
    for d in DECADES:
        tick = "🟡" if d in selected else "⚪"
        row.append(InlineKeyboardButton(f"{tick} {d}", callback_data=f"decade_toggle|{d}"))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    label = f"🍌 Generate — {_decade_label(chat_id)}" if selected else "🍌 Generate (any era)"
    buttons.append([InlineKeyboardButton(label, callback_data="decade_confirm")])
    buttons.append([InlineKeyboardButton("← Back", callback_data="decade_back")])
    return buttons

def _show_era_choice(query, chat_id, title, gen_action, back_cb):
    """Show Any era / Select decade. Stores gen_action in _pending_gen."""
    _pending_gen[chat_id] = {"action": gen_action, "back": back_cb}
    _nav(query,
        f"{title}\n\nSelect era:\n\n"
        f"📡 Filtering by era takes a little longer —\n"
        f"Kurator cross-checks artists across multiple sources.\n"
        f"Usually under 60 seconds.",
        markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("·  Any era",       callback_data="decade_confirm")],
            [InlineKeyboardButton("📅 Select decade", callback_data="decade_open")],
            [InlineKeyboardButton("← Back",           callback_data=back_cb)],
        ])
    )

# ─── Working message — appears at 8s if still generating ─────────────────────

def _working_message(message, text, delay=8):
    """Send a 'still working' message after delay seconds using a background timer."""
    import threading
    sent = {"done": False}

    def _send():
        if not sent["done"]:
            try:
                message.reply_text(text)
            except Exception:
                pass

    timer = threading.Timer(delay, _send)
    timer.start()
    return sent, timer

def _cancel_working(sent, timer):
    sent["done"] = True
    timer.cancel()

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

def _mb_get(path, params=None):
    """Generic MusicBrainz GET with rate-limit awareness."""
    try:
        r = requests.get(
            f"https://musicbrainz.org/ws/2/{path}",
            params={**(params or {}), "fmt": "json"},
            headers={"User-Agent": MB_USER_AGENT},
            timeout=10
        )
        if r.status_code == 200:
            return r.json()
        log.warning(f"MusicBrainz {path} HTTP {r.status_code}")
    except Exception as e:
        log.error(f"MusicBrainz error {path}: {e}")
    return {}

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

def _get_artist_info(artist_query):
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
        # Add MusicBrainz tags to tag index
        for t in tags[:8]:
            tag_name = t["name"].title()
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
                tag_index[s] = tag_index.get(s, 0) + 1
            for g in rel.get("genre", []):
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

def _export_buttons(key, map_chat_id=None):
    buttons = [
        [InlineKeyboardButton("🟣 Export via Soundiiz", callback_data=f"soundiiz_help|{key}")],
        [InlineKeyboardButton("🟢 Open Spotify links",  callback_data=f"sp_expand|{key}|0")],
    ]
    if map_chat_id:
        mem          = map_memory.get(map_chat_id, {})
        display_name = mem.get("display_name", "")
        if display_name:
            buttons.append([InlineKeyboardButton("─────────────────────", callback_data="noop")])
            buttons.append([InlineKeyboardButton(
                f"🗺️ Back to {display_name[:22]}",
                callback_data=f"map_back|{map_chat_id}"
            )])
    else:
        buttons.append([InlineKeyboardButton("─────────────────────", callback_data="noop")])
    buttons.append([InlineKeyboardButton("📀 Main menu", callback_data="cmd|menu")])
    return buttons

# ─── Playlist sender ──────────────────────────────────────────────────────────

def send_playlist(message, tracks, title="✦ Kurator's Playlist", branded=True, chat_id=None, size=None, map_chat_id=None, suppress_warning=False):
    target = size or PLAYLIST_SIZE
    if not tracks:
        message.reply_text(
            f"{title}\n\nNo new tracks found.\n\nYour history may be full.\nUse /reset to start fresh.",
            markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🗑️ Reset", callback_data="cmd|reset")],
                [InlineKeyboardButton("📀 Main menu", callback_data="cmd|menu")],
            ])
        )
        return
    short_warning = ""
    if not suppress_warning and len(tracks) < target * 0.7:
        short_warning = f"\nOnly {len(tracks)} tracks found — history is getting full. /reset to refresh.\n"
    key        = _store_tracks(tracks)
    track_list = "\n".join(tracks)

    # Message 1 — playlist only, clean and copyable
    message.reply_text(
        f"{title} — {len(tracks)} tracks\n"
        f"{BOT_VERSION}{short_warning}\n\n"
        f"{track_list}",
        disable_web_page_preview=True,
    )

    # Message 2 — export options separately
    message.reply_text(
        "Export options:",
        markup=InlineKeyboardMarkup(_export_buttons(key, map_chat_id=map_chat_id))
    )

# ─── Main menu ────────────────────────────────────────────────────────────────

def main_menu_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("── 🍌 KURATOR'S PICKS ──────────────", callback_data="noop")],
        [InlineKeyboardButton("🎵 Playlist — Kurator's selection",   callback_data="cmd|playlist")],
        [InlineKeyboardButton("⛏️ Dig — Deeper into the taste",      callback_data="cmd|dig")],
        [InlineKeyboardButton("💎 Rare — Hidden gems",               callback_data="cmd|rare")],
        [InlineKeyboardButton("── 🔍 FREE EXPLORE ─────────────────", callback_data="noop")],
        [InlineKeyboardButton("🗺️ Map — Explore an artist's world",  callback_data="cmd|map_prompt")],
        [InlineKeyboardButton("🏷️ Tags — Your genre library",        callback_data="cmd|tags")],
        [InlineKeyboardButton("────────────────────────────────────", callback_data="noop")],
        [
            InlineKeyboardButton("📊 Status", callback_data="cmd|status"),
            InlineKeyboardButton("🗑️ Reset",  callback_data="cmd|reset"),
            InlineKeyboardButton("❓ Help",   callback_data="cmd|help"),
        ],
    ])

# ─── Help text ────────────────────────────────────────────────────────────────

def _help_text():
    return """Kurator is built around taste, not algorithms.

🍌 KURATOR'S PICKS
Selections drawn from Kurator's own listening history.

🎵 /playlist — Kurator's cut
⛏️ /dig — Two degrees from Kurator's taste
💎 /rare — Artists under 500K listeners

🔍 FREE EXPLORE

🗺️ /map <artist> — Full artist breakdown.
  Labels, albums, styles, similar artists.

🏷️ /tags — Your personal genre library.
  Built from your Map sessions.

🔖 /playlist <tag> — Genre playlists.
  Example: /playlist post-punk

⚙️ OTHER

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
                InlineKeyboardButton("→ Next", callback_data="onboard|2")
            ]])
        )

def help_command(update, context):
    update.message.reply_text(_help_text())

def playlist(update, context):
    msg     = update.message
    chat_id = update.effective_chat.id
    if context.args:
        tag = " ".join(context.args)
        msg.reply_text(f"🔖 Searching \"{tag.upper()}\"…")
        data  = lastfm("tag.gettoptracks", tag=tag, limit=100)
        items = data.get("tracks", {}).get("track", [])
        names = list({t["artist"]["name"] for t in items if t.get("artist")})
        random.shuffle(names)
        sent, timer = _working_message(msg, "Still building…")
        result = select_tracks(names, size=GENRE_PLAYLIST_SIZE)
        _cancel_working(sent, timer)
        send_playlist(msg, result, title=f"🎵🏷️ {tag.upper()}", branded=False,
                      chat_id=chat_id, size=GENRE_PLAYLIST_SIZE)
    else:
        _pending_gen[chat_id] = {"action": "playlist", "back": "cmd|menu"}
        msg.reply_text(
            "🎵 Kurator's Playlist\n\nSelect era:\n\n📡 Filtering by era takes a little longer —\nKurator cross-checks artists across multiple sources.\nUsually under 60 seconds.",
            markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("·  Any era",       callback_data="decade_confirm")],
                [InlineKeyboardButton("📅 Select decade", callback_data="decade_open")],
                [InlineKeyboardButton("📀 Main menu",     callback_data="cmd|menu")],
            ])
        )

def dig(update, context):
    chat_id = update.effective_chat.id
    _pending_gen[chat_id] = {"action": "dig", "back": "cmd|menu"}
    update.message.reply_text(
        "⛏️ Kurator's Dig\n\nSelect era:\n\n📡 Filtering by era takes a little longer —\nKurator cross-checks artists across multiple sources.\nUsually under 60 seconds.",
        markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("·  Any era",       callback_data="decade_confirm")],
            [InlineKeyboardButton("📅 Select decade", callback_data="decade_open")],
            [InlineKeyboardButton("📀 Main menu",     callback_data="cmd|menu")],
        ])
    )

def rare(update, context):
    chat_id = update.effective_chat.id
    _pending_gen[chat_id] = {"action": "rare", "back": "cmd|menu"}
    update.message.reply_text(
        "💎 Kurator's Rare\n\nSelect era:\n\n📡 Filtering by era takes a little longer —\nKurator cross-checks artists across multiple sources.\nUsually under 60 seconds.",
        markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("·  Any era",       callback_data="decade_confirm")],
            [InlineKeyboardButton("📅 Select decade", callback_data="decade_open")],
            [InlineKeyboardButton("📀 Main menu",     callback_data="cmd|menu")],
        ])
    )

def map_command(update, context):
    msg = update.message
    if not context.args:
        msg.reply_text("🗺️ Map\n\nSend: /map <artist>")
        return
    artist_query = " ".join(context.args)
    msg.reply_text(f"🔭 Mapping {artist_query.upper()}…")
    sent, timer  = _working_message(msg, "Still mapping…")
    # Clear navigation history on fresh /map
    _nav_history.pop(update.effective_chat.id, None)
    _render_map(msg, artist_query, update.effective_chat.id)
    _cancel_working(sent, timer)

def tags(update, context):
    _render_tags(update.message, page=0)

def status(update, context):
    _render_status(update.message)

def reset(update, context):
    _do_reset(update.message)

# ─── Map renderer ─────────────────────────────────────────────────────────────

def _render_map(message, artist_query, chat_id, query=None):
    # Fetch artist info first — use official MusicBrainz name for Discogs
    info         = _get_artist_info(artist_query)
    discogs_name = info.get("official_name") or artist_query

    # Fetch Discogs styles using official name
    try:
        data = requests.get(
            "https://api.discogs.com/database/search",
            params={"artist": discogs_name, "type": "release",
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
        message.reply_text(f'No styles found for "{discogs_name}".\nTry a different artist or spelling.')
        return

    sorted_styles = sorted(counter.items(), key=lambda x: x[1], reverse=True)[:12]

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

    # Edit existing message when navigating, reply when fresh
    if query:
        try:
            _nav(query,
                f"{card_text}\n\nExplore:",
                markup=InlineKeyboardMarkup(buttons)
            )
            return
        except Exception:
            pass
    message.reply_text(
        f"{card_text}\n\nExplore:",
        markup=InlineKeyboardMarkup(buttons)
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
        "🧬 Similar Artists",
        callback_data=safe_callback(f"map_similar|{display_name}")
    )])

    # Label — after Similar Artists
    label = info.get("label")
    if label:
        buttons.append([InlineKeyboardButton(
            f"🎙️ Label: {label[:24]}",
            callback_data=safe_callback(f"label_menu|{chat_id}")
        )])

    # Separator + top 4 styles in 2 columns
    buttons.append([InlineKeyboardButton("─────────────────────", callback_data="noop")])
    style_row = []
    for style, count in sorted_styles[:4]:
        style_row.append(InlineKeyboardButton(
            f"{style}  ({count})",
            callback_data=safe_callback(f"map_style|{style}")
        ))
        if len(style_row) == 2:
            buttons.append(style_row)
            style_row = []
    if style_row:
        buttons.append(style_row)

    # More styles
    if len(sorted_styles) > 4:
        buttons.append([InlineKeyboardButton(
            f"+ {len(sorted_styles)-4} more styles…",
            callback_data=f"map_more|{chat_id}"
        )])

    buttons.append([InlineKeyboardButton("📀 Main menu", callback_data="cmd|menu")])
    return buttons

# ─── Tags renderer ────────────────────────────────────────────────────────────

def _build_tags_buttons(sorted_tags, page, edit_mode=False):
    start     = page * TAGS_PAGE_SIZE
    end       = start + TAGS_PAGE_SIZE
    page_tags = sorted_tags[start:end]
    buttons   = []
    if edit_mode:
        for tag, count in page_tags:
            buttons.append([InlineKeyboardButton(
                f"❌ {tag} ({count})",
                callback_data=safe_callback(f"tag_del|{tag}")
            )])
        buttons.append([InlineKeyboardButton("✅ Done", callback_data=f"tags_page|{page}")])
    else:
        row = []
        for tag, count in page_tags:
            row.append(InlineKeyboardButton(f"{tag} ({count})", callback_data=safe_callback(f"map_style|{tag}")))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("← Prev", callback_data=f"tags_page|{page-1}"))
        if end < len(sorted_tags):
            nav.append(InlineKeyboardButton("Next →", callback_data=f"tags_page|{page+1}"))
        if nav:
            buttons.append(nav)
        buttons.append([
            InlineKeyboardButton("✏️ Edit",     callback_data=f"tags_edit|{page}"),
            InlineKeyboardButton("📀 Main menu", callback_data="cmd|menu"),
        ])
    return buttons

def _render_tags(message, page=0, edit_mode=False):
    if not tag_index:
        message.reply_text("No tags collected yet.\n\nUse /map <artist> to start building your library.")
        return
    sorted_tags = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
    total_pages = max(1, (len(sorted_tags)-1) // TAGS_PAGE_SIZE + 1)
    start       = page * TAGS_PAGE_SIZE
    tag_list    = "\n".join(f"• {t}  ×{c}" for t, c in sorted_tags[start:start+TAGS_PAGE_SIZE])
    mode_label  = "  ✏️ Edit mode" if edit_mode else ""
    message.reply_text(
        f"🏷️ Tag Library — {len(sorted_tags)} genres (page {page+1}/{total_pages}){mode_label}\n\n{tag_list}",
        markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page, edit_mode))
    )

# ─── Status + Reset ───────────────────────────────────────────────────────────

def _render_status(message):
    message.reply_text(
        f"📊 Status\n\n"
        f"Tracks in history — {len(history['tracks'])}\n"
        f"Tags collected — {len(tag_index)}\n"
        f"Map sessions — {len(map_memory)}",
        markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑️ Reset", callback_data="cmd|reset")],
            [InlineKeyboardButton("📀 Main menu", callback_data="cmd|menu")],
        ])
    )

def _do_reset(message):
    history["tracks"].clear()
    save_history()
    message.reply_text(
        "History cleared.\n\nFresh artists and tracks on your next request.",
        markup=InlineKeyboardMarkup([[InlineKeyboardButton("📀 Main menu", callback_data="cmd|menu")]])
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
            message.reply_text(f"{BOT_VERSION}\n\nTap a command to begin.", reply_markup=main_menu_markup())

        elif value == "playlist":
            _pending_decades.pop(chat_id, None)
            _show_era_choice(query, chat_id, "🎵 Kurator's Playlist", "playlist", "cmd|menu")

        elif value == "dig":
            _pending_decades.pop(chat_id, None)
            _show_era_choice(query, chat_id, "⛏️ Kurator's Dig", "dig", "cmd|menu")

        elif value == "rare":
            _pending_decades.pop(chat_id, None)
            _show_era_choice(query, chat_id, "💎 Kurator's Rare", "rare", "cmd|menu")

        elif value == "map_prompt":
            _nav_history.pop(chat_id, None)
            _nav(query, "🗺️ Map\n\nSend:\n/map <artist>")

        elif value == "tags":
            if not tag_index:
                _nav(query, "No tags yet.\n\nUse /map <artist> first.",
                    markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Back", callback_data="cmd|menu")]]))
                return
            sorted_tags = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
            total_pages = max(1, (len(sorted_tags)-1) // TAGS_PAGE_SIZE + 1)
            tag_list    = "\n".join(f"• {t}  ×{c}" for t, c in sorted_tags[:TAGS_PAGE_SIZE])
            _nav(query, 
                f"🏷️ Tag Library — {len(sorted_tags)} genres (page 1/{total_pages})\n\n{tag_list}",
                markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, 0))
            )

        elif value == "status":
            _nav(query, 
                f"📊 Status\n\n"
                f"Tracks in history — {len(history['tracks'])}\n"
                f"Tags collected — {len(tag_index)}\n"
                f"Map sessions — {len(map_memory)}",
                markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🗑️ Reset", callback_data="cmd|reset")],
                    [InlineKeyboardButton("📀 Main menu", callback_data="cmd|menu")],
                ])
            )

        elif value == "reset":
            history["tracks"].clear()
            save_history()
            _nav(query, "History cleared.\n\nFresh artists and tracks on your next request.",
                markup=InlineKeyboardMarkup([[InlineKeyboardButton("📀 Main menu", callback_data="cmd|menu")]]))

        elif value == "help":
            _nav(query, _help_text(),
                markup=InlineKeyboardMarkup([[InlineKeyboardButton("📀 Main menu", callback_data="cmd|menu")]]))

    # ── decade_open: show decade toggle grid ─────────────────────────────────
    elif action == "decade_open":
        _pending_decades.setdefault(chat_id, set())
        _nav(query, 
            "📅 Select decades — tap to toggle:",
            markup=InlineKeyboardMarkup(_decade_selector_buttons(chat_id))
        )

    # ── decade_toggle: toggle a decade on/off ────────────────────────────────
    elif action == "decade_toggle":
        decade   = value
        selected = _pending_decades.setdefault(chat_id, set())
        if decade in selected:
            selected.discard(decade)
        else:
            selected.add(decade)
        _nav(query, 
            "📅 Select decades — tap to toggle:",
            markup=InlineKeyboardMarkup(_decade_selector_buttons(chat_id))
        )

    # ── decade_confirm: execute pending generation ────────────────────────────
    elif action == "decade_confirm":
        pending = _pending_gen.pop(chat_id, {})
        gen_action = pending.get("action", "")
        decades    = _pending_decades.pop(chat_id, set()) or None

        if not gen_action:
            message.reply_text("Session expired. Please try again.")
            return

        if gen_action == "playlist":
            _nav(query, "🎵 Selecting tracks…")
            seeds       = extract_seed_artists()
            sent, timer = _working_message(message, "Still selecting…")
            result      = select_tracks_with_decades(expand_artist_graph(seeds), decades=decades, message=message)
            _cancel_working(sent, timer)
            era_tag = f" — {_decade_label_from_set(decades)}" if decades else ""
            send_playlist(message, result, title=f"✦ Kurator's Playlist{era_tag}", branded=True, chat_id=chat_id)

        elif gen_action == "dig":
            _nav(query, "⛏️ Digging deeper…")
            seeds       = extract_seed_artists()
            sent, timer = _working_message(message, "Going deeper…")
            result      = select_tracks_with_decades(expand_artist_graph_deep(seeds), decades=decades, message=message)
            _cancel_working(sent, timer)
            era_tag = f" — {_decade_label_from_set(decades)}" if decades else ""
            send_playlist(message, result, title=f"✦ Kurator's Dig{era_tag}", branded=True, chat_id=chat_id)

        elif gen_action == "rare":
            _nav(query, "💎 Searching for hidden gems…")
            sent, timer = _working_message(message, "Hunting for gems…")
            result      = select_tracks_with_decades(expand_artist_graph_rare(extract_seed_artists()), decades=decades, message=message)
            _cancel_working(sent, timer)
            era_tag = f" — {_decade_label_from_set(decades)}" if decades else ""
            send_playlist(message, result, title=f"✦ Kurator's Rare{era_tag}", branded=True, chat_id=chat_id)

        elif gen_action.startswith("trail|"):
            sub    = gen_action.split("|", 2)
            hops   = int(sub[1]) if len(sub) > 1 else 1
            artist = sub[2] if len(sub) > 2 else ""
            hop_labels = {1: "1 hop", 2: "2 hops", 3: "deep"}
            _nav(query, f"🧬 {artist} — Following the trail ({hop_labels.get(hops, '')})…")
            sent, timer = _working_message(message, "Still mapping…")
            # For 1 hop, use stored similar list if available (same as shown to user)
            if hops == 1:
                stored = map_memory.get(chat_id, {}).get("similar")
                names  = stored if stored else _expand_trail(artist, hops)
            else:
                names = _expand_trail(artist, hops)
            result = select_tracks_with_decades(names, decades=decades, message=message)
            _cancel_working(sent, timer)
            era_tag = f" — {_decade_label_from_set(decades)}" if decades else ""
            send_playlist(message, result,
                          title=f"🧬 {artist} — {hop_labels.get(hops, '')}{era_tag}",
                          branded=False, chat_id=chat_id, map_chat_id=chat_id)

        elif gen_action.startswith("build|"):
            style = gen_action.split("|", 1)[1]
            _nav(query, f"🎵🏷️ Building {style.upper()} playlist…")
            data  = lastfm("tag.gettoptracks", tag=style, limit=100)
            items = data.get("tracks", {}).get("track", [])
            names = list({t["artist"]["name"] for t in items if t.get("artist")})
            random.shuffle(names)
            sent, timer = _working_message(message, "Still building…")
            result = select_tracks_with_decades(names, size=GENRE_PLAYLIST_SIZE, decades=decades, message=message)
            _cancel_working(sent, timer)
            era_tag     = f" — {_decade_label_from_set(decades)}" if decades else ""
            from_map    = chat_id in map_memory
            send_playlist(message, result, title=f"🎵🏷️ {style.upper()}{era_tag}",
                          branded=False, chat_id=chat_id, size=GENRE_PLAYLIST_SIZE,
                          map_chat_id=chat_id if from_map else None)

    # ── decade_back: restore era choice screen ────────────────────────────────
    elif action == "decade_back":
        pending = _pending_gen.get(chat_id, {})
        back_cb = pending.get("back", "cmd|menu")
        _pending_decades.pop(chat_id, None)
        # Re-show era choice from pending
        gen_action = pending.get("action", "")
        parts = gen_action.split("|", 1)
        title_map = {"playlist": "🎵 Kurator's Playlist", "dig": "⛏️ Kurator's Dig", "rare": "💎 Kurator's Rare"}
        title = title_map.get(parts[0], "Generate playlist")
        _show_era_choice(query, chat_id, title, gen_action, back_cb)

    # ── onboarding ────────────────────────────────────────────────────────────
    elif action == "onboard":
        step = int(value)
        if step == 1:
            _nav(query, 
                "Kurator 📀\n\n"
                "A music discovery engine built around taste, not algorithms.\n\n"
                "Curated selections drawn from a real listening history — not what's trending.",
                markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("→ Next", callback_data="onboard|2")
                ]])
            )
        elif step == 2:
            _nav(query, 
                "2 / 3  —  WHAT CAN KURATOR DO?\n\n"
                "🍌 KURATOR'S PICKS\n"
                "Playlist, Dig and Rare pull from Kurator's own listening history.\n"
                "Curated, not generated.\n\n"
                "🔍 FREE EXPLORE\n"
                "Map explores any artist's world — styles, label, albums, similar artists.\n"
                "Tags builds your own genre library.",
                markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("← Back", callback_data="onboard|1"),
                    InlineKeyboardButton("→ Next", callback_data="onboard|3"),
                ]])
            )
        elif step == 3:
            _nav(query, 
                "3 / 3  —  HOW DOES IT WORK?\n\n"
                "Kurator connects to various music services to build playlists from real data — no AI, no guessing.\n\n"
                "Export your playlists to any platform via Soundiiz — Spotify, Qobuz, Apple Music and more.",
                markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("← Back",   callback_data="onboard|2"),
                    InlineKeyboardButton("Let's go", callback_data="onboard|0"),
                ]])
            )
        elif step == 0:
            mark_onboarded(chat_id)
            _nav(query, f"{BOT_VERSION}\n\nTap a command to begin.", markup=main_menu_markup())

    # ── label_menu: expand label into Hidden/All ──────────────────────────────
    elif action == "label_menu":
        mem          = map_memory.get(chat_id, {})
        info         = mem.get("info", {})
        display_name = mem.get("display_name", "")
        label        = info.get("label", "")
        _nav(query, 
            f"🎙️ {label}\n\nGenerate a playlist from this label?",
            markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💎 Hidden artists", callback_data=safe_callback(f"label_play|rare|{label}"))],
                [InlineKeyboardButton("🎵 All artists",    callback_data=safe_callback(f"label_play|all|{label}"))],
                [InlineKeyboardButton(f"← Back to {display_name[:20]}", callback_data=f"card_back|{chat_id}")],
            ])
        )

    # ── map_bio ───────────────────────────────────────────────────────────────
    elif action == "map_bio":
        mem          = map_memory.get(chat_id, {})
        info         = mem.get("info", {})
        display_name = mem.get("display_name", "")
        bio          = info.get("bio", "No bio available.")
        lfm          = info.get("lastfm_url", "")
        link         = f"\n\n→ Full profile\n{lfm}" if lfm else ""
        _nav(query, 
            f"{display_name}\n\n{bio}{link}",
            markup=InlineKeyboardMarkup([[
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
            ep_tag = " (EP)" if album.get("is_ep") else ""
            year   = f" ({album['year']})" if album.get("year") else ""
            label  = f"  {album['title'][:32]}{ep_tag}{year}"
            buttons.append([InlineKeyboardButton(
                label,
                callback_data=safe_callback(f"album_select|{chat_id}|{album['title'][:20]}")
            )])
        buttons.append([InlineKeyboardButton(f"← Back to {display_name[:20]}", callback_data=f"card_back|{chat_id}")])
        _nav(query, 
            f"{display_name} — Studio Albums",
            markup=InlineKeyboardMarkup(buttons)
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
        _nav(query, 
            f"{display_name} — {full_title}",
            markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🍌 Generate playlist", callback_data=safe_callback(f"album_playlist|{display_name}|{full_title}"))],
                [InlineKeyboardButton("🟢 Open in Spotify",   url=spotify_album_url(display_name, full_title))],
                [InlineKeyboardButton(f"← Back",             callback_data=f"map_albums|{album_chat}")],
            ])
        )

    # ── album_playlist ────────────────────────────────────────────────────────
    elif action == "album_playlist":
        sub    = value.split("|", 2)
        artist = sub[0] if len(sub) > 0 else ""
        album  = sub[1] if len(sub) > 1 else ""
        _nav(query, f"🍌 Building {album.upper()} playlist…")
        data   = lastfm("album.getinfo", artist=artist, album=album)
        tracks = [t.get("name") for t in data.get("album", {}).get("tracks", {}).get("track", []) if t.get("name")]
        track_strings = [f"{artist} - {t}" for t in tracks]
        if not track_strings:
            _nav(query, 
                f"No tracks found for {album}.",
                markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Back", callback_data=f"card_back|{chat_id}")]])
            )
            return
        send_playlist(message, track_strings, title=f"💿 {album.upper()}", branded=False, chat_id=chat_id, map_chat_id=chat_id, suppress_warning=True)

    # ── map_similar ───────────────────────────────────────────────────────────
    elif action == "map_similar":
        artist = value
        _nav(query, f"🧬 Fetching similar artists for {artist}…")
        similar = [s["name"] for s in
                   lastfm("artist.getsimilar", artist=artist, limit=30)
                   .get("similarartists", {}).get("artist", [])]

        # Store similar list so trail_go uses same artists shown to user
        if chat_id in map_memory:
            map_memory[chat_id]["similar"] = similar
            save_map_memory()

        buttons = [
            [InlineKeyboardButton("── 🍌 Generate playlist ──────────", callback_data="noop")],
            [InlineKeyboardButton("1 hop — Direct neighbours",  callback_data=safe_callback(f"trail_go|1|{artist}"))],
            [InlineKeyboardButton("2 hops — Wider connections", callback_data=safe_callback(f"trail_go|2|{artist}"))],
            [InlineKeyboardButton("3 hops — Deep exploration",  callback_data=safe_callback(f"trail_go|3|{artist}"))],
            [InlineKeyboardButton("── 🔍 Also explore ──────────────", callback_data="noop")],
        ]
        for s in similar[:20]:
            buttons.append([InlineKeyboardButton(
                s.upper(),
                callback_data=safe_callback(f"explore_artist|{s}|{artist}")
            )])
        buttons.append([InlineKeyboardButton(f"← Back to {artist[:20]}", callback_data=f"map_back|{chat_id}")])
        _nav(query, 
            f"🧬 Similar Artists — {artist}",
            markup=InlineKeyboardMarkup(buttons)
        )

    # ── explore_artist: navigate to a similar artist's map card ──────────────
    elif action == "explore_artist":
        sub        = value.split("|", 1)
        new_artist = sub[0]
        from_artist = sub[1] if len(sub) > 1 else ""

        # Save navigation history
        if chat_id not in _nav_history:
            _nav_history[chat_id] = []
        if from_artist:
            _nav_history[chat_id].append(from_artist)
        # Cap history at 10
        _nav_history[chat_id] = _nav_history[chat_id][-10:]

        _nav(query, f"🔭 Mapping {new_artist.upper()}…")
        _render_map(message, new_artist, chat_id, query=query)

    # ── trail_go ──────────────────────────────────────────────────────────────
    elif action == "trail_go":
        sub    = value.split("|", 1)
        hops   = int(sub[0])
        artist = sub[1] if len(sub) > 1 else ""
        labels = {1: "1 hop — Direct neighbours", 2: "2 hops — Wider connections", 3: "3 hops — Deep exploration"}
        _nav(query, 
            f"🧬 {artist}\n{labels.get(hops, '')}\n\nGenerate playlist?",
            markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🍌 Generate", callback_data=safe_callback(f"trail_confirm|{hops}|{artist}"))],
                [InlineKeyboardButton(f"← Back to {artist[:20]}", callback_data=f"map_back|{chat_id}")],
            ])
        )

    # ── trail_confirm ─────────────────────────────────────────────────────────
    elif action == "trail_confirm":
        sub    = value.split("|", 1)
        hops   = int(sub[0])
        artist = sub[1] if len(sub) > 1 else ""
        _pending_decades.pop(chat_id, None)
        hop_labels = {1: "1 hop", 2: "2 hops", 3: "deep"}
        _show_era_choice(query, chat_id,
                         f"🧬 {artist} — {hop_labels.get(hops, '')}",
                         f"trail|{hops}|{artist}",
                         f"map_back|{chat_id}")

    # ── map_more ──────────────────────────────────────────────────────────────
    elif action == "map_more":
        mem          = map_memory.get(chat_id, {})
        display_name = mem.get("display_name", "")
        styles       = mem.get("styles", [])
        remaining    = styles[4:]
        buttons = [
            [InlineKeyboardButton(f"{s}  ({c})", callback_data=safe_callback(f"map_style|{s}"))]
            for s, c in remaining
        ]
        buttons.append([InlineKeyboardButton(f"← Back to {display_name[:20]}", callback_data=f"card_back|{chat_id}")])
        _nav(query, 
            f"{display_name} — All styles:",
            markup=InlineKeyboardMarkup(buttons)
        )

    # ── map_style ─────────────────────────────────────────────────────────────
    elif action == "map_style":
        mem          = map_memory.get(chat_id, {})
        display_name = mem.get("display_name", "")
        back         = f"← Back to {display_name[:20]}" if display_name else "← Back"
        _nav(query, 
            f"🏷️ {value}\n\nGenerate a playlist for this style?",
            markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🍌 Generate playlist", callback_data=f"build|{value}")],
                [InlineKeyboardButton(back,                   callback_data=f"card_back|{chat_id}")],
                [InlineKeyboardButton("📀 Main menu",          callback_data="cmd|menu")],
            ])
        )

    # ── card_back: return to current artist card without nav_history ──────────
    elif action == "card_back":
        mem          = map_memory.get(chat_id, {})
        artist       = mem.get("artist", "")
        display_name = mem.get("display_name", "")
        styles       = mem.get("styles", [])
        info         = mem.get("info", {})
        if not artist or not styles:
            _nav(query, f"{BOT_VERSION}\n\nTap a command to begin.", markup=main_menu_markup())
            return
        card_text = _format_artist_card(artist, info)
        buttons   = _build_map_buttons(display_name, styles, info, chat_id)
        _nav(query, f"{card_text}\n\nExplore:", markup=InlineKeyboardMarkup(buttons))

    # ── map_back: navigate back through history ───────────────────────────────
    elif action == "map_back":
        # Check navigation history first
        history_stack = _nav_history.get(chat_id, [])
        if history_stack:
            prev_artist = history_stack.pop()
            _nav_history[chat_id] = history_stack
            _nav(query, f"🔭 Mapping {prev_artist.upper()}…")
            _render_map(message, prev_artist, chat_id, query=query)
            return

        mem          = map_memory.get(chat_id, {})
        artist       = mem.get("artist", "")
        display_name = mem.get("display_name", "")
        styles       = mem.get("styles", [])
        info         = mem.get("info", {})
        if not artist or not styles:
            _nav(query, f"{BOT_VERSION}\n\nTap a command to begin.", markup=main_menu_markup())
            return
        card_text = _format_artist_card(artist, info)
        buttons   = _build_map_buttons(display_name, styles, info, chat_id)
        _nav(query, f"{card_text}\n\nExplore:", markup=InlineKeyboardMarkup(buttons))

    # ── label_play ────────────────────────────────────────────────────────────
    elif action == "label_play":
        sub   = value.split("|", 1)
        mode  = sub[0]
        label = sub[1] if len(sub) > 1 else ""
        mem   = map_memory.get(chat_id, {})
        display_name = mem.get("display_name", label)
        _nav(query, f"🏷️ Building {label} playlist…")
        names = _mb_label_artists(label)
        # Fallback to Discogs if MusicBrainz returns nothing
        if not names:
            log.info(f"MusicBrainz returned no artists for label '{label}', trying Discogs...")
            try:
                r = requests.get(
                    "https://api.discogs.com/database/search",
                    params={"label": label, "type": "release",
                            "per_page": 100, "token": DISCOGS_TOKEN},
                    timeout=15
                ).json()
                seen = set()
                for rel in r.get("results", []):
                    title = rel.get("title", "")
                    if " - " in title:
                        artist = title.split(" - ")[0].strip()
                        if artist and artist not in seen:
                            seen.add(artist)
                            names.append(artist)
            except Exception as e:
                log.error(f"Discogs label fallback error: {e}")
        if not names:
            _nav(query, 
                f"No artists found for label \"{label}\".",
                markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Back", callback_data=f"card_back|{chat_id}")]])
            )
            return
        if mode == "rare":
            filtered = []
            with ThreadPoolExecutor(max_workers=10) as ex:
                for f in as_completed([ex.submit(_fetch_listeners, a) for a in names[:80]]):
                    try:
                        artist, listeners = f.result()
                        if 0 < listeners < RARE_MAX_LISTENERS:
                            filtered.append(artist)
                    except Exception: pass
            names = filtered or names
        send_playlist(message, select_tracks(names, size=GENRE_PLAYLIST_SIZE),
                      title=f"🎙️ {label}{' — Hidden' if mode == 'rare' else ''}",
                      branded=False, chat_id=chat_id, size=GENRE_PLAYLIST_SIZE, map_chat_id=chat_id)

    # ── tags ──────────────────────────────────────────────────────────────────
    elif action == "tags_page":
        page        = int(value)
        sorted_tags = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
        total_pages = max(1, (len(sorted_tags)-1) // TAGS_PAGE_SIZE + 1)
        start       = page * TAGS_PAGE_SIZE
        tag_list    = "\n".join(f"• {t}  ×{c}" for t, c in sorted_tags[start:start+TAGS_PAGE_SIZE])
        _nav(query, 
            f"🏷️ Tag Library — {len(sorted_tags)} genres (page {page+1}/{total_pages})\n\n{tag_list}",
            markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page))
        )

    elif action == "tags_edit":
        page        = int(value)
        sorted_tags = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
        total_pages = max(1, (len(sorted_tags)-1) // TAGS_PAGE_SIZE + 1)
        start       = page * TAGS_PAGE_SIZE
        tag_list    = "\n".join(f"• {t}  ×{c}" for t, c in sorted_tags[start:start+TAGS_PAGE_SIZE])
        _nav(query, 
            f"🏷️ Tag Library — {len(sorted_tags)} genres (page {page+1}/{total_pages})  ✏️ Edit mode\n\n{tag_list}",
            markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, page, edit_mode=True))
        )

    elif action == "tag_del":
        if value in tag_index:
            del tag_index[value]
            save_tag_index()
        sorted_tags = sorted(tag_index.items(), key=lambda x: x[1], reverse=True)
        if not sorted_tags:
            _nav(query, "Tag library is empty.\n\nUse /map <artist> to build it up.",
                markup=InlineKeyboardMarkup([[InlineKeyboardButton("📀 Main menu", callback_data="cmd|menu")]]))
            return
        total_pages = max(1, (len(sorted_tags)-1) // TAGS_PAGE_SIZE + 1)
        tag_list    = "\n".join(f"• {t}  ×{c}" for t, c in sorted_tags[:TAGS_PAGE_SIZE])
        _nav(query, 
            f"🏷️ Tag Library — {len(sorted_tags)} genres (page 1/{total_pages})  ✏️ Edit mode\n\n{tag_list}",
            markup=InlineKeyboardMarkup(_build_tags_buttons(sorted_tags, 0, edit_mode=True))
        )

    # ── soundiiz_help ─────────────────────────────────────────────────────────
    elif action == "soundiiz_help":
        key = value
        message.reply_text(
            "📡 Export your playlist\n\n"
            "1. Go to Soundiiz\n"
            "   Log in or create a free account\n\n"
            "2. Tap ··· top right\n"
            "   → Select \"Import playlist\"\n\n"
            "3. Select \"From plain text\"\n\n"
            "4. Paste your playlist\n"
            "   → Tap \"Send text\"\n\n"
            "5. Choose your platform —\n"
            "   Spotify, Qobuz, Apple Music and more\n\n"
            "→ soundiiz.com",
            disable_web_page_preview=True,
            markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("← Back", callback_data=f"export_back|{key}")],
            ])
        )

    # ── export_back ───────────────────────────────────────────────────────────
    elif action == "export_back":
        _nav(query, "Export options:",
            markup=InlineKeyboardMarkup(_export_buttons(value)))

    # ── sp_expand ─────────────────────────────────────────────────────────────
    elif action == "sp_expand":
        sub    = value.split("|", 1)
        key    = sub[0]
        page   = int(sub[1]) if len(sub) > 1 else 0
        tracks = _track_store.get(key, [])
        if not tracks:
            message.reply_text("Links expired. Generate a new playlist.")
            return
        total_pages = max(1, (len(tracks)-1) // TRACK_LINKS_PAGE + 1)
        start       = page * TRACK_LINKS_PAGE
        page_tracks = tracks[start:start+TRACK_LINKS_PAGE]
        buttons = []
        for t in page_tracks:
            parts = t.split(" - ", 1)
            label = f"{parts[0][:28]} – {parts[1][:18]}" if len(parts) == 2 else t[:48]
            buttons.append([InlineKeyboardButton(f"▶ {label}", url=spotify_url(t))])
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("← Prev", callback_data=f"sp_expand|{key}|{page-1}"))
        if (page+1) < total_pages:
            nav.append(InlineKeyboardButton("Next →", callback_data=f"sp_expand|{key}|{page+1}"))
        if nav:
            buttons.append(nav)
        buttons.append([InlineKeyboardButton("← Back",      callback_data=f"export_back|{key}")])
        buttons.append([InlineKeyboardButton("📀 Main menu", callback_data="cmd|menu")])
        _nav(query, 
            f"Track links — page {page+1}/{total_pages}",
            markup=InlineKeyboardMarkup(buttons)
        )

    # ── build: playlist from style — era choice first ─────────────────────────
    elif action == "build":
        style = value
        _pending_decades.pop(chat_id, None)
        # Only show back to artist if we have a valid map session
        back_cb = f"card_back|{chat_id}" if chat_id in map_memory else "cmd|menu"
        _show_era_choice(query, chat_id, f"🏷️ {style.upper()}", f"build|{style}", back_cb)

# ─── Boot ─────────────────────────────────────────────────────────────────────

updater  = Updater(TELEGRAM_TOKEN)
dp       = updater.dispatcher

dp.add_handler(CommandHandler("start",    start))
dp.add_handler(CommandHandler("help",     help_command))
dp.add_handler(CommandHandler("playlist", playlist))
dp.add_handler(CommandHandler("dig",      dig))
dp.add_handler(CommandHandler("rare",     rare))
dp.add_handler(CommandHandler("map",      map_command))
dp.add_handler(CommandHandler("tags",     tags))
dp.add_handler(CommandHandler("status",   status))
dp.add_handler(CommandHandler("reset",    reset))
dp.add_handler(CallbackQueryHandler(handle_buttons))

log.info(BOT_VERSION)
print(BOT_VERSION)
updater.start_polling()
updater.idle()
