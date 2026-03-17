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

VERSION = "Kurator v2.0.2"

TOKEN = os.environ["BOT_TOKEN"]
LASTFM_API = os.environ["LASTFM_API_KEY"]
LASTFM_USER = "burbq"

user_data_store = {}

# =========================
# LASTFM
# =========================

def lastfm(method, **params):
    base = "http://ws.audioscrobbler.com/2.0/"
    p = {"method": method, "api_key": LASTFM_API, "format": "json", **params}
    return requests.get(base, params=p).json()


def get_recent_tracks(limit=300):
    data = lastfm("user.getrecenttracks", user=LASTFM_USER, limit=limit)
    return data.get("recenttracks", {}).get("track", [])


def get_recent_artists(limit=300):
    tracks = get_recent_tracks(limit)
    return [t["artist"]["#text"] for t in tracks if t.get("artist")]

# =========================
# PLAYLIST CORE (NO TOCAR)
# =========================

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
# BOTONES (v2.0.2)
# =========================

def build_buttons():
    keyboard = [
        [InlineKeyboardButton("Copy ready", callback_data="copy_playlist")]
    ]
    return InlineKeyboardMarkup(keyboard)


async def copy_playlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    playlist_text = user_data_store.get(user_id, "")

    if not playlist_text:
        await query.message.reply_text("No playlist to copy.")
        return

    await query.message.reply_text(
        "Copy below 👇\n\n" + playlist_text
    )

# =========================
# COMMANDS
# =========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"{VERSION}\nMusical Discovery Engine"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        f"{VERSION}\n\n"
        "/playlist\n"
        "/trail <artist>\n"
        "/rare\n"
        "/help"
    )
    await update.message.reply_text(msg)

# =========================
# PLAYLIST (v2.0.1 aplicado)
# =========================

async def playlist(update: Update, context: ContextTypes.DEFAULT_TYPE):

    # v2.0.1 → mensaje de estado
    await update.message.reply_text("Building discovery playlist…")

    artists = get_recent_artists()
    random.shuffle(artists)

    tracks = build_playlist(artists[:40])
    text = "\n".join(tracks)

    user_id = update.message.from_user.id
    user_data_store[user_id] = text

    await update.message.reply_text(
        text,
        reply_markup=build_buttons()
    )

# =========================
# TRAIL (solo mensaje añadido)
# =========================

async def trail(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not context.args:
        await update.message.reply_text("Usage: /trail <artist>")
        return

    artist = " ".join(context.args)

    # v2.0.1
    await update.message.reply_text(f"Following trail from {artist}…")

    sim = lastfm("artist.getsimilar", artist=artist, limit=20)
    artists = [a["name"] for a in sim.get("similarartists", {}).get("artist", [])]

    random.shuffle(artists)
    tracks = build_playlist(artists[:40])

    await update.message.reply_text("\n".join(tracks))

# =========================
# RARE (solo mensaje añadido)
# =========================

async def rare(update: Update, context: ContextTypes.DEFAULT_TYPE):

    # v2.0.1
    await update.message.reply_text("Searching rare artists…")

    artists = get_recent_artists()
    counts = Counter(artists)

    rare_artists = [a for a, c in counts.items() if c <= 2]

    random.shuffle(rare_artists)
    tracks = build_playlist(rare_artists[:40])

    await update.message.reply_text("\n".join(tracks))

# =========================
# APP
# =========================

app = ApplicationBuilder().token(TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("help", help_command))

app.add_handler(CommandHandler("playlist", playlist))
app.add_handler(CommandHandler("trail", trail))
app.add_handler(CommandHandler("rare", rare))

app.add_handler(CallbackQueryHandler(copy_playlist, pattern="copy_playlist"))

print(f"{VERSION} running…")

app.run_polling()
