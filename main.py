# SOLO TE MARCO EL BLOQUE DIG CAMBIADO
# TODO LO DEMÁS ES EXACTAMENTE TUYO

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

    # 🔥 NUEVO: mezclar top candidatos (anti-loop)
    top_slice=candidates[:80]
    random.shuffle(top_slice)

    results=[]

    for artist,_ in top_slice:

        # 🔥 NUEVO: evitar repetir artistas globalmente
        if normalize(artist) in history["artists"]:
            continue

        data=lastfm("artist.gettoptracks",
            artist=artist,
            limit=10
        )

        tracks=data.get("toptracks",{}).get("track",[])

        if len(tracks)<5:
            continue

        # evitar hits
        deep_tracks=tracks[3:10]

        if not deep_tracks:
            continue

        t=random.choice(deep_tracks)

        key=f"{normalize(artist)}-{normalize(t['name'])}"

        if key in history["tracks"]:
            continue

        results.append(f"{artist} - {t['name']}")

        # 🔥 usar history real
        history["artists"].add(normalize(artist))
        history["tracks"].add(key)

        if len(results)>=PLAYLIST_SIZE:
            break

    if not results:
        update.message.reply_text("Nothing found.")
        return

    update.message.reply_text("\n".join(results))
