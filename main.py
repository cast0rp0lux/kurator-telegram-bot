# ================================
# Kurator v2.0.2
# Change: Improved /dig (true digging mode)
# ================================

import os
import random
import requests
from telegram import Update
from telegram.ext import Updater, CommandHandler, CallbackContext

LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
LASTFM_USER = os.getenv("LASTFM_USER")
TELEGRAM_TOKEN = os.getenv("BOT_TOKEN")

BASE_URL = "http://ws.audioscrobbler.com/2.0/"

# ================================
# HELP
# ================================

def help_command(update: Update, context: CallbackContext):
    text = (
        "Kurator | Music Discovery Engine (v2.0.2)\n\n"
        "DISCOVER\n\n"
        "/playlist — discovery playlist\n"
        "/dig — deep digging\n"
        "/trail <artist> — explore similar artists\n"
        "/scene <genre> — explore scene\n"
        "/rare — rare artists\n"
        "/help — commands"
    )
    update.message.reply_text(text)

# ================================
# LASTFM HELPERS
# ================================

def get_top_artists(limit=10):
    params = {
        "method": "user.gettopartists",
        "user": LASTFM_USER,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        "limit": limit
    }
    r = requests.get(BASE_URL, params=params).json()
    return [a["name"] for a in r.get("topartists", {}).get("artist", [])]

def get_similar(artist):
    params = {
        "method": "artist.getsimilar",
        "artist": artist,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        "limit": 30
    }
    r = requests.get(BASE_URL, params=params).json()
    return r.get("similarartists", {}).get("artist", [])

def get_top_tracks(artist):
    params = {
        "method": "artist.gettoptracks",
        "artist": artist,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        "limit": 10
    }
    r = requests.get(BASE_URL, params=params).json()
    return [t["name"] for t in r.get("toptracks", {}).get("track", [])]

# ================================
# DIG (UPDATED)
# ================================

def dig(update: Update, context: CallbackContext):
    update.message.reply_text("Digging deep…")

    seeds = get_top_artists(5)
    candidates = []

    for artist in seeds:
        similar = get_similar(artist)

        for sim in similar:
            listeners = int(sim.get("listeners", 0))

            # 🔥 NUEVO FILTRO (agresivo)
            if listeners > 150000:
                continue

            candidates.append((sim["name"], listeners))

    # 🔥 ordenar por menos popular
    candidates = sorted(candidates, key=lambda x: x[1])

    playlist = []
    used = set()

    for artist, _ in candidates:
        if artist in used:
            continue

        tracks = get_top_tracks(artist)

        if not tracks:
            continue

        # 🔥 evitar hits (no track 0-2)
        deep_tracks = tracks[3:10]

        if not deep_tracks:
            continue

        track = random.choice(deep_tracks)

        playlist.append(f"{artist} - {track}")
        used.add(artist)

        if len(playlist) >= 20:
            break

    update.message.reply_text("\n".join(playlist))

# ================================
# BASIC PLAYLIST (sin tocar)
# ================================

def playlist(update: Update, context: CallbackContext):
    update.message.reply_text("Building discovery playlist…")

    seeds = get_top_artists(5)
    results = []

    for artist in seeds:
        similar = get_similar(artist)

        for sim in similar[:5]:
            tracks = get_top_tracks(sim["name"])
            if tracks:
                results.append(f"{sim['name']} - {tracks[0]}")

    update.message.reply_text("\n".join(results[:20]))

# ================================
# MAIN
# ================================

def main():
    updater = Updater(TELEGRAM_TOKEN)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("help", help_command))
    dp.add_handler(CommandHandler("playlist", playlist))
    dp.add_handler(CommandHandler("dig", dig))

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
