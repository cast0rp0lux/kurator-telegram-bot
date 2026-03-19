import os
import requests
import random
from collections import Counter
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler
from telegram import InlineKeyboardMarkup, InlineKeyboardButton

BOT_VERSION = "Kurator | Music Discovery Engine (v2.1)"

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

# -------- API --------

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

# -------- CORE --------

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
    return [a for a,_ in counter.most_common(SEED_ARTISTS)]

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
    return [t for t,_ in tag_counter.most_common(12)]

def select_tracks(artists):
    tracks=[]
    random.shuffle(artists)
    for a in artists:
        if len(tracks)>=PLAYLIST_SIZE:
            break
        if normalize(a) in history["artists"]:
            continue
        data=lastfm("artist.gettoptracks",artist=a,limit=10)
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

# -------- SCENE VIEW --------

def build_scene_view(tag):
    data=lastfm("tag.gettopartists",tag=tag,limit=30)
    artists=data.get("topartists",{}).get("artist",[])
    names=[a["name"] for a in artists]

    tags=collect_scene_tags(names)

    text=f"{BOT_VERSION}\n\nScene: {tag}\n\n"
    text += "Explore:\n\n"

    for i in range(0,len(tags),3):
        text += " || ".join(tags[i:i+3]) + "\n"

    keyboard=[]

    for t in tags:
        keyboard.append([InlineKeyboardButton(t,callback_data=f"scene:{t}")])

    keyboard.append([
        InlineKeyboardButton("🎧 Build playlist",callback_data=f"playlist:{tag}")
    ])

    keyboard.append([
        InlineKeyboardButton("⬅ Back",callback_data="back")
    ])

    return text, InlineKeyboardMarkup(keyboard)

# -------- COMMANDS --------

def start(update,context):
    update.message.reply_text(f"{BOT_VERSION}\n\nType /scene <genre>")

def scene(update,context):
    if not context.args:
        update.message.reply_text("Usage: /scene <genre>")
        return

    tag=" ".join(context.args)

    context.user_data["scene_stack"]=[tag]

    text,keyboard=build_scene_view(tag)

    update.message.reply_text(text,reply_markup=keyboard)

# -------- CALLBACKS --------

def handle_callback(update,context):
    query=update.callback_query
    query.answer()

    data=query.data

    stack=context.user_data.get("scene_stack",[])

    if data.startswith("scene:"):
        tag=data.split(":")[1]
        stack.append(tag)
        context.user_data["scene_stack"]=stack

        text,keyboard=build_scene_view(tag)
        query.edit_message_text(text,reply_markup=keyboard)

    elif data.startswith("playlist:"):
        tag=data.split(":")[1]

        data=lastfm("tag.gettopartists",tag=tag,limit=50)
        artists=data.get("topartists",{}).get("artist",[])
        names=[a["name"] for a in artists]

        tracks=select_tracks(names)

        query.message.reply_text("\n".join(tracks))

    elif data=="back":
        if len(stack)>1:
            stack.pop()
            tag=stack[-1]
            context.user_data["scene_stack"]=stack

            text,keyboard=build_scene_view(tag)
            query.edit_message_text(text,reply_markup=keyboard)

# -------- TELEGRAM --------

updater=Updater(TELEGRAM_TOKEN)
dp=updater.dispatcher

dp.add_handler(CommandHandler("start",start))
dp.add_handler(CommandHandler("scene",scene))
dp.add_handler(CallbackQueryHandler(handle_callback))

print("Kurator v2.1 running")

updater.start_polling()
updater.idle()
