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

🔹 Soundiiz (recommended)
1. Go to https://soundiiz.com
2. Import → Text
3. Paste this list
4. Export to your preferred platform

🔹 Spotify quick access
Tap any track below
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
        [InlineKeyboardButton("🧪 Rare (hidden artists)", callback_data="cmd|rare")],
        [InlineKeyboardButton("❓ Help", callback_data="cmd|help")]
    ]

    update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(buttons))

# -------- HELP --------

def help_command(update,context):
    msg = """❓ Help

📀 /playlist  
Playlist by Kurator

🕳️ /dig  
Deep discovery

🔗 /trail <artist>  
Explore similar artists

🧠 /scene <artist>  
Navigate styles and subgenres

🧪 /rare  
Hidden artists


Kurator is built around taste, not algorithms.

Some responses may take a few seconds — multiple sources are working to build something actually worth listening to.

Just be patient.
"""
    update.message.reply_text(msg)

# -------- PLAYLIST --------

def playlist(update,context):

    if context.args:
        tag=" ".join(context.args)
        update.message.reply_text(f"📀 Building {tag} playlist…")
        data=lastfm("tag.gettopartists",tag=tag,limit=50)
        names=[a["name"] for a in data.get("topartists",{}).get("artist",[])]
        tracks=select_tracks(names)
        send_playlist_with_export(update, tracks, f"📀 {tag}")

    else:
        update.message.reply_text("📀 Building playlist…")
        tracks=select_tracks(expand_artist_graph(extract_seed_artists()))
        send_playlist_with_export(update, tracks)

# -------- DIG --------

def dig(update,context):
    update.message.reply_text("🕳️ Digging deep…")
    tracks=select_tracks(expand_artist_graph(extract_seed_artists()))
    send_playlist_with_export(update, tracks, "🕳️ Dig")

# -------- TRAIL --------

def trail(update,context):

    if not context.args:
        update.message.reply_text("🔗 Trail\n\nType:\n/trail <artist>")
        return

    update.message.reply_text("🔗 Following trail…")

    artist=" ".join(context.args)
    data=lastfm("artist.getsimilar",artist=artist,limit=60)
    names=[a["name"] for a in data.get("similarartists",{}).get("artist",[])]
    tracks=select_tracks(names)

    send_playlist_with_export(update, tracks, f"🔗 {artist}")

# -------- RARE --------

def rare(update,context):
    update.message.reply_text("🧪 Searching rare artists…")
    tracks=select_tracks(expand_artist_graph(extract_seed_artists()))
    send_playlist_with_export(update, tracks, "🧪 Rare")

# -------- SCENE --------

def scene(update,context):

    if not context.args:
        update.message.reply_text("🧠 Scene\n\nType:\n/scene <artist>")
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
        f"{BOT_VERSION}\n\n🧠 {artist_query}\n\nChoose a style:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

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
        elif value=="scene":
            scene(query, context)
        elif value=="rare":
            rare(query, context)
        elif value=="help":
            help_command(query, context)

    elif action=="scene":
        buttons=[
            [InlineKeyboardButton("✅ Generate playlist", callback_data=f"build|{value}")],
            [InlineKeyboardButton("⬅ Back", callback_data="back|scene")]
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

        query.message.reply_text(
            f"📀 {value} ({len(tracks)} tracks)\n\n" + "\n".join(tracks)
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

🔹 Spotify quick access
Tap any track below
"""

        buttons = [[InlineKeyboardButton(t[:50], url=spotify_search_url(t))] for t in tracks]

        query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))

    elif action=="back":
        artist_query = scene_memory.get(query.message.chat.id)
        if artist_query:
            context.args = [artist_query]
            fake_update = type('', (), {})()
            fake_update.message = query.message
            fake_update.effective_chat = query.message.chat
            scene(fake_update, context)
