import os
import re
import requests
from collections import Counter
from telegram.ext import Updater, CommandHandler

LASTFM_USER = "burbq"
LASTFM_API = os.environ["LASTFM_API_KEY"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]

playlist_history = {
    "artists": set(),
    "tracks": set(),
}


def lastfm(method, **params):
    base = "http://ws.audioscrobbler.com/2.0/"
    p = {"method": method, "api_key": LASTFM_API, "format": "json", **params}
    return requests.get(base, params=p).json()


def get_familiar_artists(min_plays=10):
    data = lastfm("user.gettopartists", user=LASTFM_USER, period="overall", limit=500)
    artists = data.get("topartists", {}).get("artist", [])
    familiar = set()
    for a in artists:
        try:
            if int(a.get("playcount", 0)) >= min_plays:
                familiar.add(a["name"].lower())
        except (ValueError, TypeError):
            pass
    return familiar


def parse_decade(text):
    t = text.strip().lower().rstrip("s")
    if not t.isdigit():
        return None
    n = int(t)
    if n < 100:
        n = (1900 + n) if n >= 20 else (2000 + n)
    return (n // 10) * 10


def fmt_decade(decade_start):
    return f"{decade_start}s"


def decade_to_tag(decade_start):
    return {
        1950: "50s",
        1960: "60s",
        1970: "70s",
        1980: "80s",
        1990: "90s",
        2000: "00s",
        2010: "2010s",
        2020: "2020s",
    }.get(decade_start)


def start(update, context):
    update.message.reply_text(
        "¡Hola! Soy tu bot de Last.fm 🎵\n\n"
        "/now — Canción que estás escuchando ahora\n"
        "/recent — Últimas 5 canciones\n"
        "/topartists — Tus artistas más escuchados\n"
        "/toptracks — Tus canciones más escuchadas\n"
        "/obsession — Tu artista más repetido esta semana\n"
        "/similar <artista> — Artistas similares\n"
        "/playlist — 30 canciones nuevas basadas en tu historial\n"
        "/playlist southern soul — Playlist de un género\n"
        "/dig post-punk 1980s — Crate digging\n"
        "/tags — Lista de tags útiles"
    )


def now(update, context):
    data = lastfm("user.getrecenttracks", user=LASTFM_USER, limit=1)
    tracks = data.get("recenttracks", {}).get("track", [])
    if not tracks:
        update.message.reply_text("No se encontró ninguna canción reciente.")
        return

    t = tracks[0]
    artist = t["artist"]["#text"]
    name = t["name"]
    album = t.get("album", {}).get("#text", "")
    is_now = "@attr" in t and t["@attr"].get("nowplaying") == "true"

    prefix = "▶️ Escuchando ahora" if is_now else "⏮ Última canción"
    msg = f"{prefix}:\n{artist} - {name}"
    if album:
        msg += f"\n💿 {album}"
    update.message.reply_text(msg)


def recent(update, context):
    data = lastfm("user.getrecenttracks", user=LASTFM_USER, limit=5)
    tracks = data.get("recenttracks", {}).get("track", [])

    lines = []
    for t in tracks:
        artist = t["artist"]["#text"]
        name = t["name"]
        lines.append(f"• {artist} - {name}")

    update.message.reply_text("🎵 Últimas canciones:\n" + "\n".join(lines))


def topartists(update, context):
    period = "7day"
    args = context.args
    if args and args[0] in ("week", "month", "year", "overall"):
        mapping = {"week": "7day", "month": "1month", "year": "12month", "overall": "overall"}
        period = mapping[args[0]]

    data = lastfm("user.gettopartists", user=LASTFM_USER, period=period, limit=10)
    artists = data.get("topartists", {}).get("artist", [])

    lines = [f"{i+1}. {a['name']} ({a['playcount']} plays)" for i, a in enumerate(artists)]
    update.message.reply_text("🎤 Top artistas:\n" + "\n".join(lines))


def toptracks(update, context):
    data = lastfm("user.gettoptracks", user=LASTFM_USER, period="7day", limit=10)
    tracks = data.get("toptracks", {}).get("track", [])

    lines = [f"{i+1}. {t['artist']['name']} - {t['name']} ({t['playcount']} plays)" for i, t in enumerate(tracks)]
    update.message.reply_text("🎵 Top canciones:\n" + "\n".join(lines))


def obsession(update, context):
    data = lastfm("user.gettopartists", user=LASTFM_USER, period="7day", limit=1)
    artists = data.get("topartists", {}).get("artist", [])
    if not artists:
        update.message.reply_text("No hay datos de esta semana aún.")
        return

    a = artists[0]
    update.message.reply_text(
        f"🔁 Tu obsesión de esta semana:\n{a['name']} — {a['playcount']} reproducciones"
    )


def similar(update, context):
    if not context.args:
        update.message.reply_text("Usa el comando así: /similar Radiohead")
        return

    artist_name = " ".join(context.args)
    data = lastfm("artist.getsimilar", artist=artist_name, limit=8)
    artists = data.get("similarartists", {}).get("artist", [])

    lines = [f"• {a['name']}" for a in artists]
    update.message.reply_text(
        f"🔍 Artistas similares a {artist_name}:\n" + "\n".join(lines)
    )


def build_tracks_from_artists(artist_pool, excluded_artists):
    playlist_tracks = []

    for artist in artist_pool:
        if len(playlist_tracks) >= 30:
            break
        if artist.lower() in excluded_artists:
            continue
        top_data = lastfm("artist.gettoptracks", artist=artist, limit=8)
        tracks = top_data.get("toptracks", {}).get("track", [])

        candidates = tracks[3:] if len(tracks) > 3 else tracks
        for t in candidates:
            track_key = f"{artist.lower()} - {t['name'].lower()}"
            if track_key not in playlist_history["tracks"]:
                playlist_tracks.append(f"{artist} - {t['name']}")
                playlist_history["artists"].add(artist.lower())
                playlist_history["tracks"].add(track_key)
                break

    return playlist_tracks


def playlist(update, context):
    genre = " ".join(context.args).strip() if context.args else ""

    if genre:
        playlist_by_genre(update, genre)
    else:
        playlist_by_history(update)


def playlist_by_history(update):
    update.message.reply_text("⏳ Generando playlist para Soundiiz, espera un momento...")

    data = lastfm("user.getrecenttracks", user=LASTFM_USER, limit=600)
    raw_tracks = data.get("recenttracks", {}).get("track", [])

    familiar = get_familiar_artists(min_plays=10)
    excluded_artists = familiar | playlist_history["artists"]

    artist_counts = Counter(t["artist"]["#text"] for t in raw_tracks if t["artist"]["#text"])
    top_artists = [artist for artist, _ in artist_counts.most_common(20)]

    similar_artists = []
    seen_similar = set()

    for artist in top_artists:
        if len(similar_artists) >= 30:
            break
        sim_data = lastfm("artist.getsimilar", artist=artist, limit=30)
        candidates = sim_data.get("similarartists", {}).get("artist", [])
        for s in candidates[5:]:
            name = s["name"]
            name_lower = name.lower()
            if name_lower not in excluded_artists and name_lower not in seen_similar:
                similar_artists.append(name)
                seen_similar.add(name_lower)

    playlist_tracks = build_tracks_from_artists(similar_artists, excluded_artists)

    soundiiz_text = "\n".join(playlist_tracks[:30])

    update.message.reply_text(
        f"🎶 Playlist para Soundiiz ({len(playlist_tracks[:30])} canciones)"
    )
    update.message.reply_text(soundiiz_text)


updater = Updater(TELEGRAM_TOKEN)
dp = updater.dispatcher

dp.add_handler(CommandHandler("start", start))
dp.add_handler(CommandHandler("now", now))
dp.add_handler(CommandHandler("recent", recent))
dp.add_handler(CommandHandler("topartists", topartists, pass_args=True))
dp.add_handler(CommandHandler("toptracks", toptracks, pass_args=True))
dp.add_handler(CommandHandler("obsession", obsession))
dp.add_handler(CommandHandler("similar", similar, pass_args=True))
dp.add_handler(CommandHandler("playlist", playlist, pass_args=True))

print("Bot iniciado...")
updater.start_polling()
updater.idle()
