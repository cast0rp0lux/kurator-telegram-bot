import os
import requests
import random
from collections import Counter, defaultdict
from telegram.ext import Updater, CommandHandler

BOT_VERSION = "Kurator | Music Discovery Engine (v2.0.2)"

LASTFM_USER = "burbq"
LASTFM_API = os.environ["LASTFM_API_KEY"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]

SCRIBBLE_LIMIT = 600
SEED_ARTISTS = 25
SIMILAR_EXPANSION = 40
PLAYLIST_SIZE = 30

RARE_LISTENER_THRESHOLD = 50000

TAG_BLACKLIST = {
"seen live","favorites","favorite","female vocalists","male vocalists",
"british","american","my favorites","love","awesome","good","bad"
}

history = {
"artists":set(),
"tracks":set()
}

def lastfm(method, **params):
    base="http://ws.audioscrobbler.com/2.0/"
    payload={
        "method":method,
        "api_key":LASTFM_API,
        "format":"json",
        **params
    }
    r=requests.get(base,params=payload)
    try:
        return r.json()
    except:
        return {}

def normalize(name):
    return name.lower().strip()

def get_recent_tracks():
    data=lastfm("user.getrecenttracks",
        user=LASTFM_USER,
        limit=SCRIBBLE_LIMIT
    )
    return data.get("recenttracks",{}).get("track",[])

def extract_seed_artists():
    tracks=get_recent_tracks()
    counter=Counter()
    for t in tracks:
        artist=t["artist"]["#text"]
        if artist:
            counter[artist]+=1
    seeds=[a for a,_ in counter.most_common(SEED_ARTISTS)]
    return seeds

def expand_artist_graph(seed_artists):
    pool=set()
    for artist in seed_artists:
        data=lastfm("artist.getsimilar",
            artist=artist,
            limit=SIMILAR_EXPANSION
        )
        sims=data.get("similarartists",{}).get("artist",[])
        for s in sims:
            name=s["name"]
            listeners=int(s.get("listeners",0))
            if listeners>2000000:
                continue
            pool.add(name)
    return list(pool)

def collect_scene_tags(artists):
    tag_counter=Counter()
    for a in artists[:20]:
        data=lastfm("artist.gettoptags",artist=a)
        tags=data.get("toptags",{}).get("tag",[])
        for t in tags:
            tag=t["name"].lower()
            if tag in TAG_BLACKLIST:
                continue
            if len(tag)<3:
                continue
            tag_counter[tag]+=1
    return tag_counter

def select_tracks(artists):
    tracks=[]
    random.shuffle(artists)
    for a in artists:
        if len(tracks)>=PLAYLIST_SIZE:
            break
        if normalize(a) in history["artists"]:
            continue
        data=lastfm("artist.gettoptracks",
            artist=a,
            limit=10
        )
        top=data.get("toptracks",{}).get("track",[])
        if not top:
            continue
        random.shuffle(top)
        for t in top:
            key=f"{normalize(a)}-{normalize(t['name'])}"
            if key in history["tracks"]:
                continue
            tracks.append(f"{a} - {t['name']}")
            history["artists"].add(normalize(a))
            history["tracks"].add(key)
            break
    return tracks

# -------- COMMANDS --------

def start(update,context):
    msg=f"""{BOT_VERSION}

DISCOVER

/playlist — discovery playlist
/dig — deep digging
/trail <artist> — explore similar artists
/scene <genre> — explore scene
/rare — rare artists
/help — commands
"""
    update.message.reply_text(msg)

def help_command(update, context):
    start(update,context)

# -------- DIG (NUEVO) --------

def dig(update,context):
    update.message.reply_text("Digging deep…")

    seeds=extract_seed_artists()

    candidates=[]

    for artist in seeds:
        data=lastfm("artist.getsimilar",
            artist=artist,
            limit=SIMILAR_EXPANSION
        )

        sims=data.get("similarartists",{}).get("artist",[])

        for s in sims:
            name=s["name"]
            listeners=int(s.get("listeners",0))

            if listeners > 150000:
                continue

            candidates.append((name,listeners))

    # ordenar por rareza
    candidates=sorted(candidates,key=lambda x:x[1])

    results=[]
    used=set()

    for artist,_ in candidates:

        if artist in used:
            continue

        data=lastfm("artist.gettoptracks",
            artist=artist,
            limit=10
        )

        tracks=data.get("toptracks",{}).get("track",[])

        if len(tracks)<5:
            continue

        deep_tracks=tracks[3:10]

        if not deep_tracks:
            continue

        t=random.choice(deep_tracks)

        results.append(f"{artist} - {t['name']}")
        used.add(artist)

        if len(results)>=PLAYLIST_SIZE:
            break

    if not results:
        update.message.reply_text("Nothing found.")
        return

    update.message.reply_text("\n".join(results))

# -------- SCENE --------

def scene(update,context):
    if not context.args:
        update.message.reply_text("Usage: /scene <genre>")
        return

    update.message.reply_text("Mapping scene…")

    genre=" ".join(context.args)

    data=lastfm("tag.gettopartists",
        tag=genre,
        limit=30
    )

    artists=data.get("topartists",{}).get("artist",[])
    names=[a["name"] for a in artists]

    tags=collect_scene_tags(names)
    results=[t for t,_ in tags.most_common(20)]

    msg=f"{BOT_VERSION}\n\nScene around '{genre}':\n\n"
    msg+="\n".join(results)

    update.message.reply_text(msg)

# -------- TRAIL --------

def trail(update,context):
    if not context.args:
        update.message.reply_text("Usage: /trail <artist>")
        return

    artist=" ".join(context.args)

    update.message.reply_text(f"Following trail from {artist}…")

    data=lastfm("artist.getsimilar",
        artist=artist,
        limit=60
    )

    sims=data.get("similarartists",{}).get("artist",[])
    artists=[a["name"] for a in sims]

    tracks=select_tracks(artists)

    update.message.reply_text("\n".join(tracks))

# -------- RARE --------

def rare(update,context):
    update.message.reply_text("Searching rare artists…")

    seeds=extract_seed_artists()
    graph=expand_artist_graph(seeds)

    rare=[]

    for a in graph:
        data=lastfm("artist.getinfo",artist=a)
        listeners=int(
            data.get("artist",{})
            .get("stats",{})
            .get("listeners",0)
        )
        if listeners<RARE_LISTENER_THRESHOLD:
            rare.append(a)

    tracks=select_tracks(rare)

    update.message.reply_text("\n".join(tracks))

# -------- PLAYLIST --------

def playlist(update,context):
    update.message.reply_text("Building discovery playlist…")

    seeds=extract_seed_artists()
    graph=expand_artist_graph(seeds)
    tracks=select_tracks(graph)

    update.message.reply_text("\n".join(tracks))

# -------- TELEGRAM --------

updater=Updater(TELEGRAM_TOKEN)
dp=updater.dispatcher

dp.add_handler(CommandHandler("start",start))
dp.add_handler(CommandHandler("help",help_command))
dp.add_handler(CommandHandler("dig",dig))
dp.add_handler(CommandHandler("scene",scene))
dp.add_handler(CommandHandler("trail",trail))
dp.add_handler(CommandHandler("rare",rare))
dp.add_handler(CommandHandler("playlist",playlist))

print("Kurator v2.0.2 running")

updater.start_polling()
updater.idle()
