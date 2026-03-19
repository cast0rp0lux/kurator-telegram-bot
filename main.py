import os
import requests
import random
from collections import Counter
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

BOT_VERSION = "Kurator 📀 Music Discovery Engine (v2.2.1)"

LASTFM_USER = "burbq"
LASTFM_API = os.environ["LASTFM_API_KEY"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]

SCRIBBLE_LIMIT = 600
SEED_ARTISTS = 25
SIMILAR_EXPANSION = 40
PLAYLIST_SIZE = 30

history = {"artists":set(),"tracks":set()}
tag_cache = {}

# -------- LASTFM --------

def lastfm(method, **params):
    base="http://ws.audioscrobbler.com/2.0/"
    payload={"method":method,"api_key":LASTFM_API,"format":"json",**params}
    r=requests.get(base,params=payload)
    try: return r.json()
    except: return {}

def normalize(name):
    return name.lower().strip()

def is_valid_tag(tag):
    if tag in tag_cache:
        return tag_cache[tag]

    data = lastfm("tag.gettopartists", tag=tag, limit=10)
    artists = data.get("topartists", {}).get("artist", [])

    valid = len(artists) >= 10
    tag_cache[tag] = valid

    return valid

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

📀 /playlist — discovery playlist
📀 /playlist <genre>

🕳️ /dig — deep digging
🔗 /trail <artist>
🧠 /scene <artist>

🧪 /rare
/help
"""
    update.message.reply_text(msg)

def help_command(update,context):
    start(update,context)

# -------- PLAYLIST --------

def playlist(update,context):
    update.message.reply_text("📀 Building playlist…")

    if context.args:
        tag=" ".join(context.args)
        data=lastfm("tag.gettopartists",tag=tag,limit=50)
        names=[a["name"] for a in data.get("topartists",{}).get("artist",[])]

        tracks=select_tracks(names)

        update.message.reply_text(
            f"📀 Playlist ({len(tracks)} tracks)\n\n" + "\n".join(tracks)
        )
        return

    seeds=extract_seed_artists()
    graph=expand_artist_graph(seeds)
    tracks=select_tracks(graph)

    update.message.reply_text(
        f"📀 Discovery playlist ({len(tracks)} tracks)\n\n" + "\n".join(tracks)
    )

# -------- SCENE (FILTRADO) --------

def scene(update,context):

    if not context.args:
        update.message.reply_text("Usage: /scene <artist>")
        return

    artist_query=" ".join(context.args)

    update.message.reply_text("🧠 Mapping scene (Discogs)…")

    url="https://api.discogs.com/database/search"

    params={
        "artist":artist_query,
        "type":"release",
        "per_page":100,
        "token":DISCOGS_TOKEN
    }

    r=requests.get(url,params=params)
    data=r.json()

    releases=data.get("results",[])

    if not releases:
        update.message.reply_text("No Discogs data found.")
        return

    counter={}

    for rel in releases:
        for s in rel.get("style",[]):
            counter[s]=counter.get(s,0)+1
        for g in rel.get("genre",[]):
            counter[g]=counter.get(g,0)+1

    sorted_items=sorted(counter.items(),key=lambda x:x[1],reverse=True)

    # 🔥 FILTRO INTELIGENTE
    top=[]
    for name,_ in sorted_items:
        if is_valid_tag(name):
            top.append(name)
        if len(top)>=15:
            break

    if not top:
        update.message.reply_text("No usable genres found.")
        return

    buttons=[[InlineKeyboardButton(s, callback_data=f"confirm|{s}")] for s in top]

    update.message.reply_text(
        f"{BOT_VERSION}\n\n🧠 {artist_query}\n\nChoose a style:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# -------- CALLBACK --------

def handle_buttons(update,context):
    query=update.callback_query
    query.answer()

    action,value=query.data.split("|")

    if action=="confirm":

        buttons=[
            [InlineKeyboardButton("✅ Generate playlist", callback_data=f"build|{value}")]
        ]

        query.edit_message_text(
            f"🎧 {value}\n\nGenerate playlist?",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif action=="build":

        query.edit_message_text(f"📀 Building {value} playlist…")

        data=lastfm("tag.gettopartists",tag=value,limit=50)
        names=[a["name"] for a in data.get("topartists",{}).get("artist",[])]

        tracks=select_tracks(names)

        query.edit_message_text(
            f"📀 {value} ({len(tracks)} tracks)\n\n" + "\n".join(tracks)
        )

# -------- OTROS --------

def dig(update,context):
    update.message.reply_text("🕳️ Digging deep…")
    seeds=extract_seed_artists()
    graph=expand_artist_graph(seeds)
    tracks=select_tracks(graph)
    update.message.reply_text("\n".join(tracks))

def trail(update,context):
    artist=" ".join(context.args)
    data=lastfm("artist.getsimilar",artist=artist,limit=60)
    names=[a["name"] for a in data.get("similarartists",{}).get("artist",[])]
    tracks=select_tracks(names)
    update.message.reply_text("\n".join(tracks))

def rare(update,context):
    seeds=extract_seed_artists()
    graph=expand_artist_graph(seeds)
    tracks=select_tracks(graph)
    update.message.reply_text("\n".join(tracks))

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

print(BOT_VERSION)

updater.start_polling()
updater.idle()
