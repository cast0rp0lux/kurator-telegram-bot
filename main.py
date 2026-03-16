import os
import requests
import random
from collections import Counter
from telegram.ext import Updater, CommandHandler

BOT_VERSION = "Kurator v1.2 / Musical Discovery"

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
        except:
            pass

    return familiar


def start(update, context):

    message = (
        f"{BOT_VERSION}\n\n"
        "Commands:\n"
        "/playlist — discovery playlist from history\n"
        "/playlist <genre> — discovery from genre\n"
        "/now — current track\n"
        "/recent — recent tracks\n"
    )

    update.message.reply_text(message)


def now(update, context):

    data = lastfm("user.getrecenttracks", user=LASTFM_USER, limit=1)

    tracks = data.get("recenttracks", {}).get("track", [])

    if not tracks:
        update.message.reply_text("No recent track found.")
        return

    t = tracks[0]

    artist = t["artist"]["#text"]
    name = t["name"]

    update.message.reply_text(f"{artist} - {name}")


def recent(update, context):

    data = lastfm("user.getrecenttracks", user=LASTFM_USER, limit=5)

    tracks = data.get("recenttracks", {}).get("track", [])

    lines = []

    for t in tracks:
        artist = t["artist"]["#text"]
        name = t["name"]
        lines.append(f"{artist} - {name}")

    update.message.reply_text("\n".join(lines))


def build_tracks_from_artists(artist_pool, excluded_artists):

    playlist_tracks = []

    for artist in artist_pool:

        if len(playlist_tracks) >= 30:
            break

        if artist.lower() in excluded_artists:
            continue

        top_data = lastfm("artist.gettoptracks", artist=artist, limit=10)

        tracks = top_data.get("toptracks", {}).get("track", [])

        if not tracks:
            continue

        candidates = tracks[3:] if len(tracks) > 3 else tracks

        random.shuffle(candidates)

        for t in candidates:

            track_key = f"{artist.lower()} - {t['name'].lower()}"

            if track_key not in playlist_history["tracks"]:

                playlist_tracks.append(f"{artist} - {t['name']}")

                playlist_history["artists"].add(artist.lower())

                playlist_history["tracks"].add(track_key)

                break

    return playlist_tracks


def playlist(update, context):

    if context.args:
        genre = " ".join(context.args)
        playlist_by_genre(update, genre)
    else:
        playlist_by_history(update)


def playlist_by_history(update):

    update.message.reply_text("Generating discovery playlist...")

    data = lastfm("user.getrecenttracks", user=LASTFM_USER, limit=600)

    raw_tracks = data.get("recenttracks", {}).get("track", [])

    familiar = get_familiar_artists(min_plays=10)

    excluded_artists = familiar | playlist_history["artists"]

    artist_counts = Counter(
        t["artist"]["#text"] for t in raw_tracks if t["artist"]["#text"]
    )

    top_artists = [artist for artist, _ in artist_counts.most_common(20)]

    similar_artists = []
    seen_similar = set()

    for artist in top_artists:

        if len(similar_artists) >= 60:
            break

        sim_data = lastfm("artist.getsimilar", artist=artist, limit=50)

        candidates = sim_data.get("similarartists", {}).get("artist", [])

        random.shuffle(candidates)

        for s in candidates:

            name = s["name"]
            name_lower = name.lower()

            listeners = int(s.get("listeners", 0))

            if listeners > 2000000:
                continue

            if name_lower not in excluded_artists and name_lower not in seen_similar:

                similar_artists.append(name)
                seen_similar.add(name_lower)

    playlist_tracks = build_tracks_from_artists(similar_artists, excluded_artists)

    if not playlist_tracks:

        update.message.reply_text("No new tracks found.")
        return

    soundiiz_text = "\n".join(playlist_tracks[:30])

    update.message.reply_text(
        f"{BOT_VERSION}\n\nDiscovery playlist ({len(playlist_tracks[:30])} tracks)\n"
    )

    update.message.reply_text(soundiiz_text)


def playlist_by_genre(update, genre):

    update.message.reply_text(f"Generating {genre} discovery playlist...")

    tag_data = lastfm("tag.gettopartists", tag=genre, limit=50)

    artists_raw = tag_data.get("topartists", {}).get("artist", [])

    if not artists_raw:
        update.message.reply_text("No artists found for that genre.")
        return

    candidate_artists = [a["name"] for a in artists_raw]

    familiar = get_familiar_artists(min_plays=10)

    excluded = familiar | playlist_history["artists"]

    playlist_tracks = build_tracks_from_artists(candidate_artists, excluded)

    if not playlist_tracks:
        update.message.reply_text("No tracks found.")
        return

    soundiiz_text = "\n".join(playlist_tracks[:30])

    update.message.reply_text(
        f"{BOT_VERSION}\n\n{genre} discovery playlist ({len(playlist_tracks[:30])} tracks)\n"
    )

    update.message.reply_text(soundiiz_text)


updater = Updater(TELEGRAM_TOKEN)
dp = updater.dispatcher

dp.add_handler(CommandHandler("start", start))
dp.add_handler(CommandHandler("playlist", playlist, pass_args=True))
dp.add_handler(CommandHandler("now", now))
dp.add_handler(CommandHandler("recent", recent))

print("Kurator running...")

updater.start_polling()
updater.idle()
