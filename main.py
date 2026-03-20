import os
import requests
import random
from collections import Counter
from urllib.parse import quote
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

BOT_VERSION = "Kurator 📀 Music Discovery Engine (v3.0)"

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
spotify_memory = {}

def spotify_search_url(track):
    return f"https://open.spotify.com/search/{quote(track)}"

def send_playlist_with_export(update, tracks, title="📀 Playlist"):
    chat_id = update.effective_chat.id
    spotify_memory[chat_id] = tracks

    update.message.reply_text(
        f"{title} ({len(tracks)} tracks)\n\n" + "\n".join(tracks)
    )

    text = """
━━━━━━━━━━━━━━━━━━━
🎧 Export options
━━━━━━━━━━━━━━━━━━━

🔹 Soundiiz (recommended)
1. Go to https://soundiiz.com
2. Import → Text
3. Paste this list
4. Export to your preferred platform
"""

    buttons = [
        [InlineKeyboardButton("🎧 Show Spotify links", callback_data="spotify|show")]
    ]

    update.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup(buttons),
        disable_web_page_preview=True
    )

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

def select_tracks(artists):
    tracks=[]
    random.shuffle(artists)

    for a in artists:
        if len(tracks)>=PLAYLIST_SIZE: break
        if normalize(a) in history["artists"]: continue

        data=lastfm("artist.gettoptracks",artist=a,limit=50)
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
        [InlineKeyboardButton("🧪 Rare (hidden artists)", callback_data="cmd|rare")],
        [InlineKeyboardButton("❓ Help", callback_data="cmd|help")]
    ]

    update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(buttons))

# -------- PLAYLIST (NO TOCADO) --------

def playlist(update,context):

    history["artists"].clear()
    history["tracks"].clear()

    if context.args:
        tag=" ".join(context.args)
        update.message.reply_text(f"📀 Building {tag} playlist…")
        data=lastfm("tag.gettopartists",tag=tag,limit=50)
        names=[a["name"] for a in data.get("topartists",{}).get("artist",[])]
        tracks=select_tracks(names)
        send_playlist_with_export(update, tracks, f"📀 {tag}")

    else:
        update.message.reply_text("📀 Building playlist…")
        seeds=extract_seed_artists()
        tracks=select_tracks(seeds)
        send_playlist_with_export(update, tracks)

# -------- DIG (MEDIUM DEPTH) --------

def dig(update,context):

    history["artists"].clear()
    history["tracks"].clear()

    update.message.reply_text("🕳️ Digging deep…")

    seeds = extract_seed_artists()[:10]

    pool=set()
    for artist in seeds:
        data=lastfm("artist.getsimilar",artist=artist,limit=100)
        for s in data.get("similarartists",{}).get("artist",[]):
            if int(s.get("listeners",0)) > 500000:
                continue
            pool.add(s["name"])

    tracks=select_tracks(list(pool))
    send_playlist_with_export(update, tracks, "🕳️ Dig")

# -------- TRAIL (MULTI-HOP) --------

def trail(update,context):

    history["artists"].clear()
    history["tracks"].clear()

    if not context.args:
        update.message.reply_text("🔗 Trail\n\nType:\n/trail <artist>")
        return

    artist=" ".join(context.args)
    update.message.reply_text(f"🔗 Following {artist} trail…")

    level1 = lastfm("artist.getsimilar",artist=artist,limit=50).get("similarartists",{}).get("artist",[])

    level2=[]
    for a in level1:
        data=lastfm("artist.getsimilar",artist=a["name"],limit=30)
        level2 += data.get("similarartists",{}).get("artist",[])

    pool=set()
    for a in level1 + level2:
        if int(a.get("listeners",0)) < 1000000:
            pool.add(a["name"])

    tracks=select_tracks(list(pool))
    send_playlist_with_export(update, tracks, f"🔗 {artist}")

# -------- RARE (DEEP) --------

def rare(update,context):

    history["artists"].clear()
    history["tracks"].clear()

    update.message.reply_text("🧪 Searching rare artists…")

    seeds = extract_seed_artists()[:8]

    pool=set()
    for artist in seeds:
        data=lastfm("artist.getsimilar",artist=artist,limit=150)
        for s in data.get("similarartists",{}).get("artist",[]):
            if int(s.get("listeners",0)) < 100000:
                pool.add(s["name"])

    tracks=select_tracks(list(pool))
    send_playlist_with_export(update, tracks, "🧪 Rare")

# -------- CALLBACK --------

def handle_buttons(update,context):
    query=update.callback_query
    query.answer()

    action,value=query.data.split("|")

    if action=="cmd":
        if value=="playlist":
            playlist(query, context)
        elif value=="dig":
            dig(query, context)
        elif value=="trail":
            trail(query, context)
        elif value=="rare":
            rare(query, context)

    elif action=="spotify":

        tracks = spotify_memory.get(query.message.chat.id)

        if not tracks:
            query.message.reply_text("No playlist found.")
            return

        buttons = [
            [InlineKeyboardButton(t[:50], url=spotify_search_url(t))]
            for t in tracks
        ]

        query.message.reply_text(
            "🎧 Spotify links",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

# -------- TELEGRAM --------

updater=Updater(TELEGRAM_TOKEN)
dp=updater.dispatcher

dp.add_handler(CommandHandler("start",start))
dp.add_handler(CommandHandler("playlist",playlist))
dp.add_handler(CommandHandler("dig",dig))
dp.add_handler(CommandHandler("trail",trail))
dp.add_handler(CommandHandler("rare",rare))
dp.add_handler(CallbackQueryHandler(handle_buttons))

print(BOT_VERSION)

updater.start_polling()
updater.idle()
