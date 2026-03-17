import os
import requests
import random
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

VERSION = "Kurator v2.1.1"

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


def get_recent_artists(limit=600):
    data = lastfm("user.getrecenttracks", user=LASTFM_USER, limit=limit)
    tracks = data.get("recenttracks", {}).get("track", [])
    return [t["artist"]["#text"] for t in tracks if t.get("artist")]


def get_artist_tags(artist):
    data = lastfm("artist.gettoptags", artist=artist)
    tags = data.get("toptags", {}).get("tag", [])
    return [t["name"] for t in tags[:5]]


def build_playlist(artists):
    playlist = []
    used_tracks = set()

    for artist in artists:
        data = lastfm("artist.gettoptracks", artist=artist, limit=10)
        tracks = data.get("toptracks", {}).get("track", [])

        if not tracks:
            continue

        # evitar hits
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


def spotify_link(query):
    return f"https://open.spotify.com/search/{query.replace(' ', '%20')}"


def youtube_link(query):
    return f"https://www.youtube.com/results?search_query={query.replace(' ', '+')}"

# =========================
# UI BOTONES
# =========================

def build_buttons(tracks):
    query = " ".join(tracks[:5])

    keyboard = [
        [
            InlineKeyboardButton("Spotify", url=spotify_link(query)),
            InlineKeyboardButton("YouTube", url=youtube_link(query))
        ],
        [
            InlineKeyboardButton("Copy Playlist", callback_data="copy_playlist")
        ]
    ]

    return InlineKeyboardMarkup(keyboard)


async def copy_playlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    playlist_text = context.user_data.get("last_playlist", "No playlist available")
    await query.message.reply_text(playlist_text)

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
        "/dig\n"
        "/trail <artist>\n"
        "/scene <genre>\n"
        "/rare\n\n"
        "STATS\n"
        "/obsession <artist>\n"
        "/toptracks\n"
        "/topartists\n\n"
        "INFO\n"
        "/help"
    )

    await update.message.reply_text(msg)

# =========================
# PLAYLIST (FIXED)
# =========================

async def playlist(update: Update, context: ContextTypes.DEFAULT_TYPE):

    await update.message.reply_text("Building discovery playlist…")

    genre = " ".join(context.args)

    if genre:
        tag_data = lastfm("tag.gettopartists", tag=genre, limit=100)
        artists = [a["name"] for a in tag_data.get("topartists", {}).get("artist", [])]

        # 🔥 CLAVE: evitar mainstream
        artists = artists[15:80]

    else:
        artists = get_recent_artists()

    random.shuffle(artists)
    selected = artists[:40]

    tracks = build_playlist(selected)
    text = "\n".join(tracks)

    context.user_data["last_playlist"] = text

    await update.message.reply_text(
        text,
        reply_markup=build_buttons(tracks)
    )

# =========================
# TRAIL (OK)
# =========================

async def trail(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not context.args:
        await update.message.reply_text("Usage: /trail <artist>")
        return

    artist = " ".join(context.args)

    await update.message.reply_text(f"Following trail from {artist}…")

    sim1 = lastfm("artist.getsimilar", artist=artist, limit=20)
    artists1 = [a["name"] for a in sim1.get("similarartists", {}).get("artist", [])]

    second_layer = []

    for a in artists1[:5]:
        sim2 = lastfm("artist.getsimilar", artist=a, limit=10)
        second_layer += [x["name"] for x in sim2.get("similarartists", {}).get("artist", [])]

    all_artists = list(set(artists1 + second_layer))

    random.shuffle(all_artists)
    tracks = build_playlist(all_artists[:40])

    await update.message.reply_text("\n".join(tracks))

# =========================
# RARE (MEJORADO)
# =========================

async def rare(update: Update, context: ContextTypes.DEFAULT_TYPE):

    await update.message.reply_text("Searching rare artists…")

    artists = get_recent_artists()

    # frecuencia → detectar sobreescuchados
    counts = Counter(artists)

    # quedarnos con los menos repetidos
    rare_artists = [a for a, c in counts.items() if c <= 2]

    random.shuffle(rare_artists)

    tracks = build_playlist(rare_artists[:40])

    await update.message.reply_text("\n".join(tracks))

# =========================
# SCENE (REHECHO)
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
        tags = get_artist_tags(artist)
        tag_counter.update(tags)

    # eliminar el propio género
    if genre in tag_counter:
        del tag_counter[genre]

    common_tags = [t for t, _ in tag_counter.most_common(15)]

    lines = [f"{genre}\n"]

    for t in common_tags:
        lines.append(f"├ {t}")

    await update.message.reply_text("\n".join(lines))

# =========================
# STATS
# =========================

async def obsession(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not context.args:
        await update.message.reply_text("Usage: /obsession <artist>")
        return

    artist = " ".join(context.args)

    data = lastfm("user.gettopartists", user=LASTFM_USER, limit=200)
    artists = data.get("topartists", {}).get("artist", [])

    for a in artists:
        if a["name"].lower() == artist.lower():
            await update.message.reply_text(f"{artist}: {a['playcount']} plays")
            return

    await update.message.reply_text("Artist not found")


async def toptracks(update: Update, context: ContextTypes.DEFAULT_TYPE):

    data = lastfm("user.gettoptracks", user=LASTFM_USER, limit=20)
    tracks = data.get("toptracks", {}).get("track", [])

    lines = [f"{t['artist']['name']} - {t['name']}" for t in tracks]

    await update.message.reply_text("\n".join(lines))


async def topartists(update: Update, context: ContextTypes.DEFAULT_TYPE):

    data = lastfm("user.gettopartists", user=LASTFM_USER, limit=15)
    artists = data.get("topartists", {}).get("artist", [])

    lines = [f"{a['name']} ({a['playcount']})" for a in artists]

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
