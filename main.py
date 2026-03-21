import os  
import requests  
import random  
from collections import Counter  
from urllib.parse import quote  
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler  
from telegram import InlineKeyboardButton, InlineKeyboardMarkup  

BOT_VERSION = "Kurator 📀 Music Discovery Engine (v2.4.4)"  

LASTFM_USER = "burbq"  
LASTFM_API = os.environ["LASTFM_API_KEY"]  
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]  
DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]  

SCRIBBLE_LIMIT = 600  
SEED_ARTISTS = 25  
SIMILAR_EXPANSION = 40  
PLAYLIST_SIZE = 30  

history = {"artists":set(),"tracks":set()}  
scene_memory = {}  

# 🔥 SOLO TAGS PINCHADAS
tag_index = set()

def spotify_search_url(track):  
    return f"https://open.spotify.com/search/{quote(track)}"  

def send_playlist_with_export(update, tracks, title="📀 Playlist"):  
    update.message.reply_text(
        f"{title} ({len(tracks)} tracks)\n\n" + "\n".join(tracks)
    )

    text = """
━━━━━━━━━━━━━━━━━━━
🎧 Export options
━━━━━━━━━━━━━━━━━━━
"""

    buttons = [[InlineKeyboardButton(t[:50], url=spotify_search_url(t))] for t in tracks]
    update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))

def lastfm(method, **params):
    base="http://ws.audioscrobbler.com/2.0/"
    payload={"method":method,"api_key":LASTFM_API,"format":"json",**params}
    r=requests.get(base,params=payload)
    try: return r.json()
    except: return {}

def normalize(name):
    return name.lower().strip()

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

Tap a command to begin:
"""

    buttons = [
        [InlineKeyboardButton("📀 Playlist", callback_data="cmd|playlist")],
        [InlineKeyboardButton("🕳️ Dig", callback_data="cmd|dig")],
        [InlineKeyboardButton("🔗 Trail", callback_data="cmd|trail")],
        [InlineKeyboardButton("🧠 Scene", callback_data="cmd|scene")],
        [InlineKeyboardButton("🧠 Tags", callback_data="cmd|tags")],
        [InlineKeyboardButton("🧪 Rare", callback_data="cmd|rare")]
    ]

    update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(buttons))

# -------- TAGS --------

def build_tags_keyboard(edit_mode=False):
    sorted_tags = sorted(list(tag_index))
    buttons = []
    row = []

    for i, t in enumerate(sorted_tags):

        if edit_mode:
            row.append(InlineKeyboardButton(t, callback_data=f"scene|{t}"))
            row.append(InlineKeyboardButton("❌", callback_data=f"confirm_delete|{t}"))
        else:
            row.append(InlineKeyboardButton(t, callback_data=f"scene|{t}"))

        if len(row) >= 2:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    # botón editar / salir
    if edit_mode:
        buttons.append([InlineKeyboardButton("✔ Done", callback_data="edit_off")])
    else:
        buttons.append([InlineKeyboardButton("✏ Edit", callback_data="edit_on")])

    return InlineKeyboardMarkup(buttons)

def tags(update, context):

    if not tag_index:
        update.message.reply_text("No tags yet.")
        return

    update.message.reply_text(
        f"{BOT_VERSION}\n\n🧠 Tag Library",
        reply_markup=build_tags_keyboard(edit_mode=False)
    )

# -------- SCENE --------

def scene(update,context):

    if not context.args:
        update.message.reply_text("/scene <artist>")
        return

    update.message.reply_text("🧠 Mapping scene…")

    artist_query=" ".join(context.args)
    scene_memory[update.effective_chat.id] = artist_query

    data=requests.get(
        "https://api.discogs.com/database/search",
        params={"artist":artist_query,"type":"release","per_page":100,"token":DISCOGS_TOKEN}
    ).json()

    releases=data.get("results",[])

    counter={}
    for rel in releases:
        for s in rel.get("style", []):
            counter[s]=counter.get(s,0)+1

    sorted_items=sorted(counter.items(),key=lambda x:x[1],reverse=True)
    top=[x[0] for x in sorted_items[:15]]

    buttons=[[InlineKeyboardButton(s, callback_data=f"scene|{s}")] for s in top]

    update.message.reply_text(
        f"{BOT_VERSION}\n\n🧠 {artist_query}",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# -------- CALLBACK --------

def handle_buttons(update,context):
    query=update.callback_query
    query.answer()

    action,value=query.data.split("|") if "|" in query.data else (query.data,None)

    if action=="scene":
        tag_index.add(value)

        buttons=[
            [InlineKeyboardButton("✅ Generate playlist", callback_data=f"build|{value}")],
            [InlineKeyboardButton("⬅ Back", callback_data="cmd|tags")]
        ]

        query.edit_message_text(
            f"🎧 {value}",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif action=="edit_on":
        query.edit_message_reply_markup(reply_markup=build_tags_keyboard(True))

    elif action=="edit_off":
        query.edit_message_reply_markup(reply_markup=build_tags_keyboard(False))

    elif action=="confirm_delete":

        buttons=[
            [InlineKeyboardButton("✅ Yes", callback_data=f"delete|{value}")],
            [InlineKeyboardButton("❌ Cancel", callback_data="edit_on")]
        ]

        query.edit_message_text(
            f"Delete {value}?",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif action=="delete":

        if value in tag_index:
            tag_index.remove(value)

        query.edit_message_text(
            f"{BOT_VERSION}\n\n🧠 Tag Library",
            reply_markup=build_tags_keyboard(True)
        )

# -------- TELEGRAM --------

updater=Updater(TELEGRAM_TOKEN)
dp=updater.dispatcher

dp.add_handler(CommandHandler("start",start))
dp.add_handler(CommandHandler("tags",tags))
dp.add_handler(CommandHandler("scene",scene))
dp.add_handler(CallbackQueryHandler(handle_buttons))

print(BOT_VERSION)

updater.start_polling()
updater.idle()
