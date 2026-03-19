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
DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]

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

# -------- CORE (TODO IGUAL) --------

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

# -------- NUEVO SCENE (DISCOGS) --------

def scene(update,context):

    if not context.args:
        update.message.reply_text("Usage: /scene <artist>")
        return

    artist_query=" ".join(context.args)

    update.message.reply_text("Mapping scene (Discogs)…")

    url="https://api.discogs.com/database/search"

    params={
        "artist":artist_query,
        "type":"release",
        "per_page":50,
        "token":DISCOGS_TOKEN
    }

    try:
        r=requests.get(url,params=params,timeout=8)
        data=r.json()
    except:
        update.message.reply_text("Discogs request failed.")
        return

    releases=data.get("results",[])

    if not releases:
        update.message.reply_text("No Discogs data found.")
        return

    # -------- styles --------
    style_count={}
    for r in releases:
        for s in r.get("style",[]):
            style_count[s]=style_count.get(s,0)+1

    if not style_count:
        update.message.reply_text("No styles found.")
        return

    sorted_styles=sorted(style_count.items(),key=lambda x:x[1],reverse=True)
    top_styles=[s[0] for s in sorted_styles[:6]]

    # -------- botones --------
    buttons=[]
    for s in top_styles[:6]:
        buttons.append([InlineKeyboardButton(s, callback_data=f"scene_style|{s}")])

    update.message.reply_text(
        f"{BOT_VERSION}\n\nScene: {artist_query}\n\n" + "\n".join(top_styles),
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# -------- CALLBACK MODIFICADO SOLO PARA SCENE --------

def handle_buttons(update,context):
    query=update.callback_query
    query.answer()

    action,value=query.data.split("|")

    if action=="scene_style":

        style=value

        url="https://api.discogs.com/database/search"

        params={
            "style":style,
            "type":"artist",
            "per_page":40,
            "token":DISCOGS_TOKEN
        }

        try:
            r=requests.get(url,params=params,timeout=8)
            data=r.json()
        except:
            query.edit_message_text("Discogs request failed.")
            return

        artists=[a["title"] for a in data.get("results",[])]

        tracks=select_tracks(artists)

        query.edit_message_text(
            f"{BOT_VERSION}\n\nStyle: {style}\n\n" + "\n".join(tracks)
        )

    # TODO lo demás igual
    elif action=="playlist":
        genre=value
        data=lastfm("tag.gettopartists",tag=genre,limit=50)
        names=[a["name"] for a in data.get("topartists",{}).get("artist",[])]
        tracks=select_tracks(names)
        query.message.reply_text("\n".join(tracks))

# -------- RESTO EXACTAMENTE IGUAL --------

def start(update,context):
    msg=f"""{BOT_VERSION}

DISCOVER

/playlist — discovery playlist
/playlist <genre>
/dig — deep digging
/trail <artist>
/scene <artist>
/rare
/help
"""
    update.message.reply_text(msg)

def help_command(update,context):
    start(update,context)

def playlist(update,context):
    update.message.reply_text("Building discovery playlist…")
    if context.args:
        tag=" ".join(context.args)
        data=lastfm("tag.gettopartists",tag=tag,limit=50)
        names=[a["name"] for a in data.get("topartists",{}).get("artist",[])]
        update.message.reply_text("\n".join(select_tracks(names)))
        return
    seeds=extract_seed_artists()
    graph=expand_artist_graph(seeds)
    update.message.reply_text("\n".join(select_tracks(graph)))

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
    data=lastfm("artist.getsimilar",artist=artist,limit=60)
    names=[a["name"] for a in data.get("similarartists",{}).get("artist",[])]
    update.message.reply_text("\n".join(select_tracks(names)))

def rare(update,context):
    update.message.reply_text("Searching rare artists…")
    seeds=extract_seed_artists()
    graph=expand_artist_graph(seeds)
    update.message.reply_text("\n".join(select_tracks(graph)))

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
