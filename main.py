import os
import requests
import random
import time
from collections import Counter

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler
)

# =========================
# CONFIG
# =========================

VERSION = "Kurator v2.1.2"

LASTFM_USER = "burbq"
LASTFM_API = os.environ["LASTFM_API_KEY"]
TOKEN = os.environ["BOT_TOKEN"]

# =========================
# HELPERS
# =========================

def lastfm(method, **params):
    base = "http://ws.audioscrobbler.com/2.0/"
    p = {"method": method, "api_key": LASTFM_API, "format": "json", **params}
    return requests.get(base, params=p).json()


def get_recent_tracks(limit=1000):
    data = lastfm("user.getrecenttracks", user=LASTFM_USER, limit=limit)
    return data.get("recenttracks", {}).get("track", [])


def get_recent_artists(limit=600):
    tracks = get_recent_tracks(limit)
    return [t["artist"]["#text"] for t in tracks if t.get("artist")]


def get_artist_tags(artist):
    data = lastfm("artist.gettoptags", artist=artist)
    return [t["name"] for t in data.get("toptags", {}).get("tag", [])[:5]]


def build_playlist(artists):
    playlist = []
    used_tracks = set()

    for artist in artists:
        data = lastfm("artist.gettoptracks", artist=artist, limit=10)
        tracks = data.get("toptracks", {}).get("track", [])

        if not tracks:
            continue

        candidates = tracks[3:] if len(tracks) > 3 else tracks
        random.shuffle(candidates)

        for t in candidates:
            key = f"{artist}-{t['name']}"
            if key not in used_tracks:
                playlist.append(f"{artist} - {t['name']}")
                used_tracks.add(key)
                break

        if len(playlist) >= 30:
            break

    return playlist


# =========================
# SPOTIFY / YOUTUBE
# =========================

def spotify_link(tracks):
    queries = []

    for t in tracks[:10]:
        try:
            artist, song = t.split(" - ", 1)
            q = f'track:"{song}" artist:"{artist}"'
            queries.append(q)
        except:
            continue

    full_query = " OR ".join(queries)
    return f"https://open.spotify.com/search/{full_query.replace(' ', '%20')}"


def youtube_link(tracks):
    query = " ".join(tracks[:5])
    return f"https://www.youtube.com/results?search_query={query.replace(' ', '+')}"

# =========================
# BOTONES
# =========================

def build_buttons(tracks):
    keyboard = [
        [
            InlineKeyboardButton("Spotify", url=spotify_link(tracks)),
            InlineKeyboardButton("YouTube", url=youtube_link(tracks))
        ],
        [
            InlineKeyboardButton("Copy ready", callback_data="copy_playlist")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


async def copy_playlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    playlist_text = context.user_data.get("last_playlist", "No playlist available")

    await query.message.reply_text(
        "Copy below 👇\n\n" + f"```\n{playlist_text}\n```",
        parse_mode="Markdown"
    )

# =========================
# TIME FILTER
# =========================

def filter_by_days(tracks, days):
    now = int(time.time())
    cutoff = now - days * 86400

    filtered = []

    for t in tracks:
        uts = t.get("date", {}).get("uts")
        if uts and int(uts) >= cutoff:
            filtered.append(t)

    return filtered

# =========================
# COMMANDS
# =========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"{VERSION}\nMusical Discovery Engine\n\nType /help"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):

    msg = (
        f"{VERSION}\n\n"
        "DISCOVER\n"
        "/playlist\n"
        "/playlist <genre>\n"
        "/trail <artist>\n"
        "/scene <genre>\n"
        "/rare\n\n"
        "STATS\n"
        "/obsession\n"
        "/toptracks <days>\n"
        "/topartists <days>\n\n"
        "INFO\n"
        "/help"
    )

    await update.message.reply_text(msg)

# =========================
# PLAYLIST
# =========================

async def playlist(update: Update, context: ContextTypes.DEFAULT_TYPE):

    await update.message.reply_text("Building discovery playlist…")

    genre = " ".join(context.args)

    if genre:
        tag_data = lastfm("tag.gettopartists", tag=genre, limit=100)
        artists = [a["name"] for a in tag_data.get("topartists", {}).get("artist", [])]
        artists = artists[15:80]
    else:
        artists = get_recent_artists()

    random.shuffle(artists)

    tracks = build_playlist(artists[:40])
    text = "\n".join(tracks)

    context.user_data["last_playlist"] = text

    await update.message.reply_text(
        text,
        reply_markup=build_buttons(tracks)
    )

# =========================
# TRAIL
# =========================

async def trail(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not context.args:
        await update.message.reply_text("Usage: /trail <artist>")
        return

    artist = " ".join(context.args)

    await update.message.reply_text(f"Following trail from {artist}…")

    sim1 = lastfm("artist.getsimilar", artist=artist, limit=20)
    artists1 = [a["name"] for a in sim1.get("similarartists", {}).get("artist", [])]

    second = []

    for a in artists1[:5]:
        sim2 = lastfm("artist.getsimilar", artist=a, limit=10)
        second += [x["name"] for x in sim2.get("similarartists", {}).get("artist", [])]

    all_artists = list(set(artists1 + second))

    random.shuffle(all_artists)
    tracks = build_playlist(all_artists[:40])

    await update.message.reply_text("\n".join(tracks))

# =========================
# RARE
# =========================

async def rare(update: Update, context: ContextTypes.DEFAULT_TYPE):

    await update.message.reply_text("Searching rare artists…")

    artists = get_recent_artists()
    counts = Counter(artists)

    rare = [a for a, c in counts.items() if c <= 2]

    random.shuffle(rare)
    tracks = build_playlist(rare[:40])

    await update.message.reply_text("\n".join(tracks))

# =========================
# SCENE
# =========================

async def scene(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not context.args:
        await update.message.reply_text("Usage: /scene <genre>")
        return

    genre = " ".join(context.args)

    await update.message.reply_text("Mapping scene…")

    tag_data = lastfm("tag.gettopartists", tag=genre, limit=50)
    artists = [a["name"] for a in tag_data.get("topartists", {}).get("artist", [])]

    tag_counter = Counter()

    for artist in artists[:20]:
        tag_counter.update(get_artist_tags(artist))

    tag_counter.pop(genre, None)

    lines = [f"{genre}\n"]

    for t, _ in tag_counter.most_common(15):
        lines.append(f"├ {t}")

    await update.message.reply_text("\n".join(lines))

# =========================
# STATS
# =========================

async def obsession(update: Update, context: ContextTypes.DEFAULT_TYPE):

    tracks = get_recent_tracks(1000)
    tracks = filter_by_days(tracks, 30)

    artists = [t["artist"]["#text"] for t in tracks if t.get("artist")]

    counts = Counter(artists)

    if not counts:
        await update.message.reply_text("No data")
        return

    artist, plays = counts.most_common(1)[0]

    await update.message.reply_text(f"{artist} — {plays} plays (last 30 days)")


async def toptracks(update: Update, context: ContextTypes.DEFAULT_TYPE):

    days = int(context.args[0]) if context.args else 30

    tracks = get_recent_tracks(1000)
    tracks = filter_by_days(tracks, days)

    names = [f"{t['artist']['#text']} - {t['name']}" for t in tracks]

    counts = Counter(names)

    lines = [f"{n} ({c})" for n, c in counts.most_common(20)]

    await update.message.reply_text("\n".join(lines))


async def topartists(update: Update, context: ContextTypes.DEFAULT_TYPE):

    days = int(context.args[0]) if context.args else 30

    tracks = get_recent_tracks(1000)
    tracks = filter_by_days(tracks, days)

    artists = [t["artist"]["#text"] for t in tracks if t.get("artist")]

    counts = Counter(artists)

    lines = [f"{a} ({c})" for a, c in counts.most_common(15)]

    await update.message.reply_text("\n".join(lines))

# =========================
# APP
# =========================

app = ApplicationBuilder().token(TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("help", help_command))

app.add_handler(CommandHandler("playlist", playlist))
app.add_handler(CommandHandler("trail", trail))
app.add_handler(CommandHandler("scene", scene))
app.add_handler(CommandHandler("rare", rare))

app.add_handler(CommandHandler("obsession", obsession))
app.add_handler(CommandHandler("toptracks", toptracks))
app.add_handler(CommandHandler("topartists", topartists))

app.add_handler(CallbackQueryHandler(copy_playlist, pattern="copy_playlist"))

print(f"{VERSION} running…")

app.run_polling()
