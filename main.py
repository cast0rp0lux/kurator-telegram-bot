import os
import requests
import random
from collections import Counter
from urllib.parse import quote
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

BOT_VERSION = "Kurator 📀 Music Discovery Engine (v2.4.3)"

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
        [InlineKeyboardButton("📀 Playlist (by Kurator)", callback_data="cmd|playlist")],
        [InlineKeyboardButton("🕳️ Dig (deep discovery)", callback_data="cmd|dig")],
        [InlineKeyboardButton("🔗 Trail (artist)", callback_data="cmd|trail")],
        [InlineKeyboardButton("🧠 Scene (artist)", callback_data="cmd|scene")],
        [InlineKeyboardButton("🧠 Tags (explore)", callback_data="cmd|tags")],
        [InlineKeyboardButton("🧪 Rare (hidden artists)", callback_data="cmd|rare")],
        [InlineKeyboardButton("❓ Help", callback_data="cmd|help")]
    ]

    update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(buttons))

# -------- HELP --------

def help_command(update,context):
    update.message.reply_text("Use commands.")

# -------- TAGS (3 COLUMNAS) --------

def tags(update, context):

    if not tag_index:
        update.message.reply_text("No tags collected yet. Use /scene first.")
        return

    sorted_tags = sorted(list(tag_index))

    buttons = []
    row = []

    for t in sorted_tags:
        row.append(InlineKeyboardButton(t, callback_data=f"scene|{t}"))

        if len(row) == 3:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    buttons.append([InlineKeyboardButton("✏ Edit", callback_data="cmd|tags_edit")])

    update.message.reply_text(
        f"{BOT_VERSION}\n\n🧠 Tag Library",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# -------- PLAYLIST --------

def playlist(update,context):
    update.message.reply_text("📀 Building playlist…")
    tracks=select_tracks(expand_artist_graph(extract_seed_artists()))
    send_playlist_with_export(update, tracks)

# -------- DIG --------

def dig(update,context):
    update.message.reply_text("🕳️ Digging deep…")
    tracks=select_tracks(expand_artist_graph(extract_seed_artists()))
    send_playlist_with_export(update, tracks)

# -------- TRAIL --------

def trail(update,context):
    update.message.reply_text("🔗 Trail")

# -------- RARE --------

def rare(update,context):
    update.message.reply_text("🧪 Rare")

# -------- SCENE --------

def scene(update,context):
    update.message.reply_text("🧠 Scene")

# -------- CALLBACK --------

def handle_buttons(update,context):
    query=update.callback_query
    query.answer()

    action,value=query.data.split("|")

    if action=="cmd":
        if value=="tags":
            tags(query, context)

        elif value=="tags_edit":

            sorted_tags = sorted(list(tag_index))
            buttons = []

            for t in sorted_tags:
                buttons.append([
                    InlineKeyboardButton(t, callback_data=f"scene|{t}"),
                    InlineKeyboardButton("❌", callback_data=f"tags_delete|{t}")
                ])

            buttons.append([InlineKeyboardButton("✔ Done", callback_data="cmd|tags")])

            query.edit_message_text(
                f"{BOT_VERSION}\n\n🧠 Tag Library (edit mode)",
                reply_markup=InlineKeyboardMarkup(buttons)
            )

    elif action=="tags_delete":

        if value in tag_index:
            tag_index.remove(value)

        sorted_tags = sorted(list(tag_index))
        buttons = []

        for t in sorted_tags:
            buttons.append([
                InlineKeyboardButton(t, callback_data=f"scene|{t}"),
                InlineKeyboardButton("❌", callback_data=f"tags_delete|{t}")
            ])

        buttons.append([InlineKeyboardButton("✔ Done", callback_data="cmd|tags")])

        query.edit_message_text(
            f"{BOT_VERSION}\n\n🧠 Tag Library (edit mode)",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

# -------- TELEGRAM --------

updater=Updater(TELEGRAM_TOKEN)
dp=updater.dispatcher

dp.add_handler(CommandHandler("start",start))
dp.add_handler(CommandHandler("tags",tags))
dp.add_handler(CallbackQueryHandler(handle_buttons))

print(BOT_VERSION)

updater.start_polling()
updater.idle()
