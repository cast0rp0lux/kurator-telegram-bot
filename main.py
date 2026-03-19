import os
import requests
import random
from collections import Counter
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

BOT_VERSION = "Kurator | Music Discovery Engine (v2.1.2)"

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

BAD_TAGS = {"indie","rock","alternative","electronic","pop"}

GOOD_PATTERNS = [
"jazz","soul","funk","dub","kraut","psychedelic","ambient",
"minimal","boogie","disco","library","spiritual","cosmic",
"afro","latin","groove","motorik","kosmische"
]

DECADE_TAGS = ["60s","70s","80s","90s"]

history = {"artists":set(),"tracks":set()}

# -------- LASTFM --------

def lastfm(method, **params):
    base="http://ws.audioscrobbler.com/2.0/"
    payload={"method":method,"api_key":LASTFM_API,"format":"json",**params}
    r=requests.get(base,params=payload)
    try: return r.json()
    except: return {}

def normalize(name):
    return name.lower().strip()

# -------- SCORING --------

def score_tag(tag):

    score = 0

    if tag in BAD_TAGS:
        score -= 10

    if any(p in tag for p in GOOD_PATTERNS):
        score += 5

    if len(tag.split()) >= 2:
        score += 3

    if any(d in tag for d in DECADE_TAGS):
        score += 4

    return score

# -------- CORE --------

def get_recent_tracks():
    data=lastfm("user.getrecenttracks",user=LASTFM_USER,limit=SCRIBBLE_LIMIT)
    return data.get("recenttracks",{}).get("track",[])

def extract_seed_artists():
    counter=Counter()
    for t in get_recent_tracks():
        artist=t["artist"]["#text"]
        if artist: counter[artist]+=1
    return [a for a,_ in counter.most_common(SEED_ARTISTS)]

def expand_artist_graph(seed_artists):
    pool=set()
    for artist in seed_artists:
        data=lastfm("artist.getsimilar",artist=artist,limit=SIMILAR_EXPANSION)
        for s in data.get("similarartists",{}).get("artist",[]):
            if int(s.get("listeners",0))>2000000: continue
            pool.add(s["name"])
    return list(pool)

def collect_scene_tags(artists):
    counter=Counter()
    for a in artists[:20]:
        tags=lastfm("artist.gettoptags",artist=a).get("toptags",{}).get("tag",[])
        for t in tags:
            tag=t["name"].lower()
            if tag in TAG_BLACKLIST or len(tag)<3:
                continue
            counter[tag]+=1
    return counter

def select_tracks(artists):
    tracks=[]
    random.shuffle(artists)
    for a in artists:
        if len(tracks)>=PLAYLIST_SIZE: break
        if normalize(a) in history["artists"]: continue

        data=lastfm("artist.gettoptracks",artist=a,limit=10)
        top=data.get("toptracks",{}).get("track",[])
        random.shuffle(top)

        for t in top:
            key=f"{normalize(a)}-{normalize(t['name'])}"
            if key in history["tracks"]: continue

            tracks.append(f"{a} - {t['name']}")
            history["artists"].add(normalize(a))
            history["tracks"].add(key)
            break

    return tracks

# -------- START --------

def start(update,context):
    msg=f"""{BOT_VERSION}

DISCOVER

/playlist — discovery playlist
/playlist <genre>
/dig — deep digging
/trail <artist>
/scene <genre>
/rare
/help
"""
    update.message.reply_text(msg)

def help_command(update,context):
    start(update,context)

# -------- PLAYLIST --------

def playlist(update,context):
    update.message.reply_text("Building discovery playlist…")

    if context.args:
        tag=" ".join(context.args)
        data=lastfm("tag.gettopartists",tag=tag,limit=50)
        names=[a["name"] for a in data.get("topartists",{}).get("artist",[])]

        if not names:
            update.message.reply_text("No results.")
            return

        update.message.reply_text("\n".join(select_tracks(names)))
        return

    seeds=extract_seed_artists()
    graph=expand_artist_graph(seeds)
    update.message.reply_text("\n".join(select_tracks(graph)))

# -------- SCENE LIMPIO --------

def build_scene_message(genre, tags):

    sorted_tags = sorted(
        tags.items(),
        key=lambda x: (score_tag(x[0]), x[1]),
        reverse=True
    )

    top=[t for t,_ in sorted_tags[:12]]

    msg=f"{BOT_VERSION}\n\nScene: {genre}\n\n"
    msg += "\n".join(top)

    return msg, top

def scene(update,context):
    if not context.args:
        update.message.reply_text("Usage: /scene <genre>")
        return

    genre=" ".join(context.args)
    update.message.reply_text("Mapping scene…")

    data=lastfm("tag.gettopartists",tag=genre,limit=30)
    names=[a["name"] for a in data.get("topartists",{}).get("artist",[])]

    tags=collect_scene_tags(names)
    msg, top_tags = build_scene_message(genre, tags)

    buttons=[]
    for t in top_tags[:8]:
        buttons.append([InlineKeyboardButton(t, callback_data=f"scene|{t}")])

    buttons.append([InlineKeyboardButton("🎧 Build playlist", callback_data=f"playlist|{genre}")])

    update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(buttons))

# -------- CALLBACK --------

def handle_buttons(update,context):
    query=update.callback_query
    query.answer()

    action,value=query.data.split("|")

    if action=="scene":
        genre=value

        data=lastfm("tag.gettopartists",tag=genre,limit=30)
        names=[a["name"] for a in data.get("topartists",{}).get("artist",[])]

        tags=collect_scene_tags(names)
        msg, top_tags = build_scene_message(genre, tags)

        buttons=[]
        for t in top_tags[:8]:
            buttons.append([InlineKeyboardButton(t, callback_data=f"scene|{t}")])

        buttons.append([InlineKeyboardButton("🎧 Build playlist", callback_data=f"playlist|{genre}")])

        query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))

    elif action=="playlist":
        genre=value
        data=lastfm("tag.gettopartists",tag=genre,limit=50)
        names=[a["name"] for a in data.get("topartists",{}).get("artist",[])]

        tracks=select_tracks(names)
        query.message.reply_text("\n".join(tracks))

# -------- DIG / TRAIL / RARE --------

def dig(update,context):
    update.message.reply_text("Digging deep…")
    seeds=extract_seed_artists()
    graph=expand_artist_graph(seeds)
    update.message.reply_text("\n".join(select_tracks(graph)))

def trail(update,context):
    if not context.args:
        update.message.reply_text("Usage: /trail <artist>")
        return
    artist=" ".join(context.args)
    update.message.reply_text(f"Following trail from {artist}…")
    data=lastfm("artist.getsimilar",artist=artist,limit=60)
    names=[a["name"] for a in data.get("similarartists",{}).get("artist",[])]
    update.message.reply_text("\n".join(select_tracks(names)))

def rare(update,context):
    update.message.reply_text("Searching rare artists…")
    seeds=extract_seed_artists()
    graph=expand_artist_graph(seeds)
    update.message.reply_text("\n".join(select_tracks(graph)))

# -------- TELEGRAM --------

updater=Updater(TELEGRAM_TOKEN)
dp=updater.dispatcher

dp.add_handler(CommandHandler("start",start))
dp.add_handler(CommandHandler("help",help_command))
dp.add_handler(CommandHandler("playlist",playlist))
dp.add_handler(CommandHandler("scene",scene))
dp.add_handler(CommandHandler("dig",dig))
dp.add_handler(CommandHandler("trail",trail))
dp.add_handler(CommandHandler("rare",rare))
dp.add_handler(CallbackQueryHandler(handle_buttons))

print("Kurator v2.1.2 running")

updater.start_polling()
updater.idle()
