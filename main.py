import os
import re
import requests
from collections import Counter
from telegram.ext import Updater, CommandHandler

LASTFM_USER = "burbq"
LASTFM_API = os.environ["LASTFM_API_KEY"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]

# Persists across /playlist calls for the lifetime of the bot process.
# Tracks every artist and song already sent so they are never repeated.
playlist_history = {
    "artists": set(),
    "tracks": set(),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def lastfm(method, **params):
    base = "http://ws.audioscrobbler.com/2.0/"
    p = {"method": method, "api_key": LASTFM_API, "format": "json", **params}
    return requests.get(base, params=p).json()


def get_familiar_artists(min_plays=10):
    """
    Return a set of artist names (lowercase) the user knows well,
    defined as having at least `min_plays` all-time scrobbles.
    Uses user.gettopartists with overall period to get real play counts.
    """
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
    """
    Parse a decade string into the start year (int).
    Accepts: 1980s, 1980, 80s, 80  →  1980
             2000s, 00s             →  2000
             2010s, 10s             →  2010
    """
    t = text.strip().lower().rstrip("s")   # strip trailing 's' if present
    if not t.isdigit():
        return None
    n = int(t)
    if n < 100:
        # 2-digit: 60 → 1960, 70 → 1970, 00 → 2000, 10 → 2010
        n = (1900 + n) if n >= 20 else (2000 + n)
    return (n // 10) * 10


def fmt_decade(decade_start):
    """Format a decade start year as a user-friendly string: 1980 → '1980s'."""
    return f"{decade_start}s"


def decade_to_tag(decade_start):
    """Map decade start year to Last.fm crowd-sourced decade tag."""
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


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def start(update, context):
    update.message.reply_text(
        "¡Hola! Soy tu bot de Last.fm 🎵\n\n"
        "/now — Canción que estás escuchando ahora\n"
        "/recent — Últimas 5 canciones\n"
        "/topartists — Tus artistas más escuchados\n"
        "/toptracks — Tus canciones más escuchadas\n"
        "/obsession — Tu artista más repetido esta semana\n"
        "/similar <artista> — Artistas similares a uno que elijas\n"
        "/playlist — 30 canciones nuevas basadas en tu historial\n"
        "/playlist southern soul — Playlist de un género específico\n"
        "/dig post-punk 1980s — Crate digging por tag y década\n"
        "/tags — Lista de tags útiles agrupados por escena"
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

    period_label = {"7day": "esta semana", "1month": "este mes", "12month": "este año", "overall": "de siempre"}
    lines = [f"{i+1}. {a['name']} ({a['playcount']} plays)" for i, a in enumerate(artists)]
    update.message.reply_text(
        f"🎤 Top artistas {period_label.get(period, '')}:\n" + "\n".join(lines)
    )


def toptracks(update, context):
    period = "7day"
    args = context.args
    if args and args[0] in ("week", "month", "year", "overall"):
        mapping = {"week": "7day", "month": "1month", "year": "12month", "overall": "overall"}
        period = mapping[args[0]]

    data = lastfm("user.gettoptracks", user=LASTFM_USER, period=period, limit=10)
    tracks = data.get("toptracks", {}).get("track", [])

    period_label = {"7day": "esta semana", "1month": "este mes", "12month": "este año", "overall": "de siempre"}
    lines = [f"{i+1}. {t['artist']['name']} - {t['name']} ({t['playcount']} plays)" for i, t in enumerate(tracks)]
    update.message.reply_text(
        f"🎵 Top canciones {period_label.get(period, '')}:\n" + "\n".join(lines)
    )


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

    if not artists:
        update.message.reply_text(f"No se encontraron artistas similares a {artist_name}.")
        return

    lines = [f"• {a['name']}" for a in artists]
    update.message.reply_text(
        f"🔍 Artistas similares a {artist_name}:\n" + "\n".join(lines)
    )


def build_tracks_from_artists(artist_pool, excluded_artists):
    """Pick one deeper-cut track per artist, avoiding history. Returns list of 'Artist - Track' strings."""
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


def resolve_tag(description):
    """Try the description as a tag directly. If no artists found, search for the closest tag."""
    tag_data = lastfm("tag.gettopartists", tag=description, limit=35)
    artists_raw = tag_data.get("topartists", {}).get("artist", [])
    if artists_raw:
        return description, artists_raw

    # Fall back to tag search to find the closest matching tag
    search_data = lastfm("tag.search", tag=description, limit=5)
    matches = search_data.get("results", {}).get("tagmatches", {}).get("tag", [])
    if not matches:
        return None, []

    best_tag = matches[0]["name"]
    tag_data = lastfm("tag.gettopartists", tag=best_tag, limit=35)
    artists_raw = tag_data.get("topartists", {}).get("artist", [])
    return best_tag, artists_raw


def playlist_by_genre(update, description):
    update.message.reply_text(f"⏳ Buscando el mejor tag para '{description}', espera un momento...")

    resolved_tag, artists_raw = resolve_tag(description)

    if not resolved_tag or not artists_raw:
        update.message.reply_text(
            f"No se encontró ningún tag relacionado con '{description}'. "
            f"Prueba con una descripción más corta o en inglés (ej: 'southern soul', 'dark jazz')."
        )
        return

    # Let user know what tag was matched if it differs from their input
    if resolved_tag.lower() != description.lower():
        update.message.reply_text(f"🏷 Tag encontrado: '{resolved_tag}'")

    # Skip the 5 most famous names, take the rest as candidates
    candidate_artists = [a["name"] for a in artists_raw[5:]]

    # Exclude artists the user already knows well + previous playlist history
    familiar = get_familiar_artists(min_plays=10)
    excluded = familiar | playlist_history["artists"]
    playlist_tracks = build_tracks_from_artists(candidate_artists, excluded)

    if not playlist_tracks:
        update.message.reply_text("No se pudieron encontrar canciones. Prueba con otra descripción.")
        return

    soundiiz_text = "\n".join(playlist_tracks[:30])
    update.message.reply_text(
        f"🎶 Playlist '{resolved_tag}' para Soundiiz ({len(playlist_tracks[:30])} canciones)\n"
        f"Artistas que no conoces aún — copia y pega el siguiente mensaje en Soundiiz → Import → Text:"
    )
    update.message.reply_text(soundiiz_text)


def playlist_by_history(update):
    update.message.reply_text("⏳ Generando playlist para Soundiiz, espera un momento...")

    # Fetch last 300 scrobbled tracks to identify seed artists (recent listening mood)
    data = lastfm("user.getrecenttracks", user=LASTFM_USER, limit=300)
    raw_tracks = data.get("recenttracks", {}).get("track", [])

    # Exclude artists the user genuinely knows well (10+ all-time plays)
    # plus anything already sent in a previous /playlist call
    familiar = get_familiar_artists(min_plays=10)
    excluded_artists = familiar | playlist_history["artists"]

    # Use most-played artists from the last 300 tracks as seeds
    artist_counts = Counter(t["artist"]["#text"] for t in raw_tracks if t["artist"]["#text"])
    top_artists = [artist for artist, _ in artist_counts.most_common(10)]

    # Find similar artists not in excluded set — skip the 5 most obvious matches
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

    if not playlist_tracks:
        update.message.reply_text("No se pudieron encontrar canciones nuevas. El historial puede estar muy lleno — reinicia el bot para empezar de cero.")
        return

    soundiiz_text = "\n".join(playlist_tracks[:30])

    update.message.reply_text(
        f"🎶 Playlist para Soundiiz ({len(playlist_tracks[:30])} canciones)\n"
        f"Artistas que nunca has escuchado de verdad — copia y pega el siguiente mensaje en Soundiiz → Import → Text:"
    )
    update.message.reply_text(soundiiz_text)


def tags(update, context):
    part1 = (
        "🏷 Tags útiles para /dig y /playlist (1/2)\n\n"

        "━━ Punk / Post-Punk ━━\n"
        "punk, post-punk, hardcore, oi, anarcho-punk,\n"
        "uk punk, post-hardcore, melodic hardcore\n\n"

        "━━ Garage / 1960s Rock ━━\n"
        "garage rock, 60s, surf, british invasion,\n"
        "freakbeat, mod, nuggets, fuzz\n\n"

        "━━ Psychedelic ━━\n"
        "psychedelic, psychedelic rock, acid rock,\n"
        "space rock, krautrock, psych folk, stoner rock\n\n"

        "━━ Alternative / Indie ━━\n"
        "alternative, indie rock, college rock,\n"
        "shoegaze, dream pop, lo-fi, jangle pop, slowcore\n\n"

        "━━ 1990s / Grunge / Britpop ━━\n"
        "grunge, britpop, 90s, alternative rock,\n"
        "emo, post-grunge, noise pop, slacker rock\n\n"

        "━━ Noise / Experimental ━━\n"
        "noise rock, experimental, no wave, art rock,\n"
        "avant-garde, post-rock, math rock, drone\n\n"

        "━━ Gothic / Darkwave ━━\n"
        "gothic rock, darkwave, coldwave, death rock,\n"
        "batcave, ethereal, dark ambient, goth\n\n"

        "━━ Metal / Heavy ━━\n"
        "metal, heavy metal, doom metal, sludge metal,\n"
        "black metal, death metal, thrash metal, stoner metal"
    )

    part2 = (
        "🏷 Tags útiles para /dig y /playlist (2/2)\n\n"

        "━━ Soul / Funk ━━\n"
        "soul, funk, southern soul, rhythm and blues,\n"
        "northern soul, deep funk, afrobeat, neo soul\n\n"

        "━━ Jazz / Blues ━━\n"
        "jazz, blues, bebop, hard bop, free jazz,\n"
        "cool jazz, delta blues, chicago blues, soul jazz\n\n"

        "━━ Electronic / Industrial ━━\n"
        "electronic, synthpop, new wave, ebm,\n"
        "industrial, darkwave, coldwave, minimal synth\n\n"

        "━━ Hip-Hop / Beats ━━\n"
        "hip-hop, rap, boom bap, underground hip-hop,\n"
        "instrumental hip-hop, lo-fi hip-hop, trap\n\n"

        "━━ Reggae / Ska / Dub ━━\n"
        "reggae, ska, dub, rocksteady, dancehall,\n"
        "roots reggae, 2 tone, third wave ska\n\n"

        "━━ Country / Americana / Folk ━━\n"
        "country, americana, folk, outlaw country,\n"
        "bluegrass, alt-country, country blues, roots\n\n"

        "━━ Latin / World ━━\n"
        "latin, cumbia, salsa, bossa nova, samba,\n"
        "afrobeat, tropicalia, tango, fela kuti\n\n"

        "━━ 1970s Rock / Classic Rock ━━\n"
        "classic rock, 70s, hard rock, prog rock,\n"
        "glam rock, pub rock, southern rock, arena rock\n\n"

        "━━ 1980s / New Wave / Synth ━━\n"
        "80s, new wave, synth, post-punk, new romantic,\n"
        "sophisti-pop, jangle, college rock, mtv\n\n"

        "Ejemplo de uso:\n"
        "/dig delta blues 1960s\n"
        "/dig doom metal 1990s\n"
        "/playlist cumbia"
    )

    update.message.reply_text(part1)
    update.message.reply_text(part2)


def dig(update, context):
    if not context.args or len(context.args) < 2:
        update.message.reply_text(
            "Uso: /dig <tag> <década>\n"
            "Ejemplos:\n  /dig post-punk 1980s\n  /dig garage rock 1960s\n  /dig jazz 1970s"
        )
        return

    # Last arg that looks like a decade; everything before is the tag
    decade_raw = context.args[-1]
    decade_start = parse_decade(decade_raw)

    if decade_start is None:
        update.message.reply_text(
            f"No entendí la década '{decade_raw}'. Prueba con formato como: 1980s o 1980"
        )
        return

    decade_tag = decade_to_tag(decade_start)
    if not decade_tag:
        update.message.reply_text(
            f"Década {fmt_decade(decade_start)} no soportada. Prueba entre 1950s y 2020s."
        )
        return

    tag = " ".join(context.args[:-1])
    update.message.reply_text(f"⏳ Buscando '{tag}' de los {fmt_decade(decade_start)}...")

    # Resolve genre tag
    resolved_tag, genre_artists_raw = resolve_tag(tag)
    if not resolved_tag or not genre_artists_raw:
        update.message.reply_text(
            f"No se encontró el tag '{tag}'. Prueba en inglés (ej: 'post-punk', 'garage rock')."
        )
        return
    if resolved_tag.lower() != tag.lower():
        update.message.reply_text(f"🏷 Tag encontrado: '{resolved_tag}'")

    # Fetch artists for the decade tag — Last.fm's own crowd-sourced decade tagging
    decade_data = lastfm("tag.gettopartists", tag=decade_tag, limit=200)
    decade_artists = {
        a["name"].lower()
        for a in decade_data.get("topartists", {}).get("artist", [])
    }

    # Exclude artists the user knows well + previous playlists
    familiar = get_familiar_artists(min_plays=10)
    excluded = familiar | playlist_history["artists"]

    # Intersect: genre artists that also appear in the decade tag, skip top 5 obvious ones
    candidate_artists = [
        a["name"] for a in genre_artists_raw[5:]
        if a["name"].lower() in decade_artists
        and a["name"].lower() not in excluded
    ]

    # If intersection is thin, relax and allow all genre artists tagged in the decade
    if len(candidate_artists) < 5:
        candidate_artists = [
            a["name"] for a in genre_artists_raw
            if a["name"].lower() in decade_artists
            and a["name"].lower() not in excluded
        ]

    if not candidate_artists:
        update.message.reply_text(
            f"No encontré artistas de '{resolved_tag}' de los {fmt_decade(decade_start)}. "
            f"Prueba otro género o usa /tags para ver qué combina bien."
        )
        return

    # One deeper-cut track per artist
    playlist_tracks = build_tracks_from_artists(candidate_artists, excluded)

    if not playlist_tracks:
        update.message.reply_text(
            f"No se pudieron obtener canciones. Prueba otro tag o década."
        )
        return

    soundiiz_text = "\n".join(playlist_tracks[:30])
    update.message.reply_text(
        f"🕳 /dig '{resolved_tag}' — {fmt_decade(decade_start)} ({len(playlist_tracks[:30])} canciones)\n"
        f"Copia y pega el siguiente mensaje en Soundiiz → Import → Text:"
    )
    update.message.reply_text(soundiiz_text)


# ---------------------------------------------------------------------------
# Bot setup
# ---------------------------------------------------------------------------

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
dp.add_handler(CommandHandler("dig", dig, pass_args=True))
dp.add_handler(CommandHandler("tags", tags))

print("Bot iniciado...")
updater.start_polling()
updater.idle()
