from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from typing import Dict, List, Tuple

import cityhash
from spotify_scraper import SpotifyClient

from artist_enrichment import enrich_artist, enrich_song
from config import settings
from db.client import ensure_collection, get_db
from graph import schema
from utils import normalization

logger = logging.getLogger(__name__)

# Optional manual overrides for artist name normalization.
# Keys should be normalized (lowercase, trimmed); values are canonical names.
ARTIST_ALIAS_OVERRIDES: Dict[str, str] = {}


def _farmhash_key(prefix: str, raw: str) -> str:
    seed = f"{prefix}|{raw or 'unknown'}"
    return str(cityhash.CityHash64(seed))


def _resolve_playlist_artist(name: str, lookup: Dict[str, str]) -> str | None:
    for variant in normalization.artist_variants(name):
        match = lookup.get(variant)
        if match:
            return match
    return None


def _load_playlist(playlist_url: str) -> Tuple[str, List[Dict]]:
    client = SpotifyClient()
    try:
        playlist = client.get_playlist_info(playlist_url)
        playlist_name = playlist.get("name") or settings.PLAYLIST_NAME_FALLBACK
        tracks_container = playlist.get("tracks", {})
        if isinstance(tracks_container, dict):
            tracks = tracks_container.get("items", [])
        else:
            tracks = tracks_container or []
        return playlist_name, tracks
    finally:
        client.close()


def _truncate_collection(db, name: str) -> None:
    if db.has_collection(name):
        db.collection(name).truncate()


def _batch_insert(db, collection: str, items: List[Dict], batch_size: int = 1000) -> None:
    if not items:
        return
    for start in range(0, len(items), batch_size):
        db.collection(collection).insert_many(items[start : start + batch_size], overwrite=True)


def _reset_graph(
    db,
    graph_name: str,
    node_collections: List[str],
    edge_collections: List[str],
) -> None:
    if db.has_graph(graph_name):
        db.delete_graph(graph_name, drop_collections=True, ignore_missing=True)
    for collection in edge_collections:
        db.delete_collection(collection, ignore_missing=True)
    for collection in node_collections:
        db.delete_collection(collection, ignore_missing=True)


def _ensure_graph(db, graph_name: str, nodes: Dict[str, str], edges: Dict[str, str]) -> None:
    if db.has_graph(graph_name):
        return
    edge_definitions = [
        {
            "edge_collection": edges["artists_songs"],
            "from_vertex_collections": [nodes["artists"]],
            "to_vertex_collections": [nodes["songs"]],
        },
        {
            "edge_collection": edges["artists_albums"],
            "from_vertex_collections": [nodes["artists"]],
            "to_vertex_collections": [nodes["albums"]],
        },
        {
            "edge_collection": edges["songs_albums"],
            "from_vertex_collections": [nodes["songs"]],
            "to_vertex_collections": [nodes["albums"]],
        },
        {
            "edge_collection": edges["albums_record_labels"],
            "from_vertex_collections": [nodes["albums"]],
            "to_vertex_collections": [nodes["record_labels"]],
        },
        {
            "edge_collection": edges["artists_genres"],
            "from_vertex_collections": [nodes["artists"]],
            "to_vertex_collections": [nodes["genres"]],
        },
        {
            "edge_collection": edges["artists_locations"],
            "from_vertex_collections": [nodes["artists"]],
            "to_vertex_collections": [nodes["locations"]],
        },
        {
            "edge_collection": edges["artists_record_labels"],
            "from_vertex_collections": [nodes["artists"]],
            "to_vertex_collections": [nodes["record_labels"]],
        },
        {
            "edge_collection": edges["artists_associated_acts"],
            "from_vertex_collections": [nodes["artists"]],
            "to_vertex_collections": [nodes["artists"]],
        },
        {
            "edge_collection": edges["songs_songwriters"],
            "from_vertex_collections": [nodes["songs"]],
            "to_vertex_collections": [nodes["songwriters"]],
        },
        {
            "edge_collection": edges["songs_producers"],
            "from_vertex_collections": [nodes["songs"]],
            "to_vertex_collections": [nodes["producers"]],
        },
        {
            "edge_collection": edges["songs_features"],
            "from_vertex_collections": [nodes["songs"]],
            "to_vertex_collections": [nodes["artists"]],
        },
        {
            "edge_collection": edges["songs_moods"],
            "from_vertex_collections": [nodes["songs"]],
            "to_vertex_collections": [nodes["moods"]],
        },
        {
            "edge_collection": edges["songs_instruments"],
            "from_vertex_collections": [nodes["songs"]],
            "to_vertex_collections": [nodes["instruments"]],
        },
        {
            "edge_collection": edges["songs_languages"],
            "from_vertex_collections": [nodes["songs"]],
            "to_vertex_collections": [nodes["languages"]],
        },
        {
            "edge_collection": edges["songs_contributors"],
            "from_vertex_collections": [nodes["songs"]],
            "to_vertex_collections": [nodes["contributors"]],
        },
    ]
    db.create_graph(
        graph_name,
        edge_definitions=edge_definitions,
        orphan_collections=[],
    )


def build_and_upload_graph(
    playlist_url: str,
    graph_id: str,
    reset: bool = True,
) -> Tuple[int, int, Dict[str, Dict[str, str]]]:
    db = get_db()
    graph_name = schema.graph_name(graph_id)
    collection_map = schema.collection_map(graph_id)
    nodes_map = collection_map["nodes"]
    edges_map = collection_map["edges"]
    node_collections = list(nodes_map.values())
    edge_collections = list(edges_map.values())

    if reset:
        _reset_graph(db, graph_name, node_collections, edge_collections)

    for collection in node_collections:
        ensure_collection(db, collection)
    for collection in edge_collections:
        ensure_collection(db, collection, edge=True)
    _ensure_graph(db, graph_name, nodes_map, edges_map)

    playlist_name, tracks = _load_playlist(playlist_url)
    if len(tracks) > 50:
        raise ValueError(f"Playlist has {len(tracks)} tracks; expected at most 50.")

    playlist_artist_lookup: Dict[str, str] = {}
    for item in tracks:
        track = item.get("track", item) or {}
        for artist in track.get("artists", []) or []:
            artist_name = artist.get("name", "")
            if not artist_name:
                continue
            for split_name in normalization.split_artist_names(artist_name):
                for variant in normalization.artist_variants(split_name):
                    if variant and variant not in playlist_artist_lookup:
                        playlist_artist_lookup[variant] = split_name

    for alias, canonical in ARTIST_ALIAS_OVERRIDES.items():
        normalized_alias = normalization.normalize_name_lower(alias)
        if normalized_alias:
            playlist_artist_lookup[normalized_alias] = canonical

    nodes_by_collection: Dict[str, Dict[str, Dict]] = {
        nodes_map["artists"]: {},
        nodes_map["songs"]: {},
        nodes_map["albums"]: {},
        nodes_map["record_labels"]: {},
        nodes_map["playlists"]: {},
    }
    edges_by_collection: Dict[str, List[Dict]] = {name: [] for name in edge_collections}
    edge_counters: Dict[str, int] = {name: 0 for name in edge_collections}

    def upsert_node(collection: str, key: str, payload: Dict) -> str:
        node_id = f"{collection}/{key}"
        if key in nodes_by_collection[collection]:
            return node_id
        payload["_key"] = key
        payload["_id"] = node_id
        nodes_by_collection[collection][key] = payload
        return node_id

    def add_edge(
        collection: str,
        from_id: str,
        to_id: str,
        label: str,
        source: str | None = None,
    ) -> None:
        edge_counters[collection] += 1
        payload = {
            "_key": f"e{edge_counters[collection]}",
            "_from": from_id,
            "_to": to_id,
            "label": label,
        }
        if source:
            payload["source"] = source
        edges_by_collection[collection].append(payload)

    playlist_key = _farmhash_key("playlist", playlist_name)
    upsert_node(nodes_map["playlists"], playlist_key, {"name": playlist_name})

    for item in tracks:
        track = item.get("track", item) or {}
        track_name = track.get("name", "") or "Unknown Song"
        track_uri = track.get("uri", "")
        duration_ms = track.get("duration_ms", "")

        song_key = _farmhash_key("song", track_uri or track_name)
        song_id = upsert_node(
            nodes_map["songs"],
            song_key,
            {"name": track_name, "track_uri": track_uri, "duration_ms": duration_ms},
        )

        album = track.get("album", {}) or {}
        album_name = album.get("name", "")
        album_id = ""
        if album_name:
            album_key = _farmhash_key("album", album_name)
            album_id = upsert_node(
                nodes_map["albums"],
                album_key,
                {"name": album_name, "release_date": album.get("release_date", "")},
            )
            add_edge(edges_map["songs_albums"], song_id, album_id, "on_album")

        for artist in track.get("artists", []) or []:
            artist_name = artist.get("name", "")
            if not artist_name:
                continue
            for split_name in normalization.split_artist_names(artist_name):
                artist_key = _farmhash_key("artist", split_name)
                artist_id = upsert_node(nodes_map["artists"], artist_key, {"name": split_name})
                add_edge(edges_map["artists_songs"], artist_id, song_id, "performed")
                if album_id:
                    add_edge(edges_map["artists_albums"], artist_id, album_id, "contributed_to")

        label_name = album.get("label", "")
        if label_name and album_id:
            label_key = _farmhash_key("label", label_name)
            label_id = upsert_node(nodes_map["record_labels"], label_key, {"name": label_name})
            add_edge(edges_map["albums_record_labels"], album_id, label_id, "released_by")

    for collection, nodes in nodes_by_collection.items():
        _batch_insert(db, collection, list(nodes.values()))
    for collection, edges in edges_by_collection.items():
        _batch_insert(db, collection, edges)

    node_count = sum(db.collection(name).count() for name in node_collections)
    edge_count = sum(db.collection(name).count() for name in edge_collections)
    collection_map["playlist_name"] = playlist_name
    return node_count, edge_count, collection_map


def enrich_graph(
    playlist_url: str,
    graph_id: str,
) -> Tuple[int, int, Dict[str, Dict[str, str]]]:
    db = get_db()
    graph_name = schema.graph_name(graph_id)
    collection_map = schema.collection_map(graph_id)
    nodes_map = collection_map["nodes"]
    edges_map = collection_map["edges"]
    node_collections = list(nodes_map.values())
    edge_collections = list(edges_map.values())

    for collection in node_collections:
        ensure_collection(db, collection)
    for collection in edge_collections:
        ensure_collection(db, collection, edge=True)
    _ensure_graph(db, graph_name, nodes_map, edges_map)

    playlist_name, tracks = _load_playlist(playlist_url)
    if len(tracks) > 50:
        raise ValueError(f"Playlist has {len(tracks)} tracks; expected at most 50.")

    playlist_artist_lookup: Dict[str, str] = {}
    for item in tracks:
        track = item.get("track", item) or {}
        for artist in track.get("artists", []) or []:
            artist_name = artist.get("name", "")
            if not artist_name:
                continue
                for split_name in normalization.split_artist_names(artist_name):
                    normalized = normalization.normalize_name_lower(split_name)
                if normalized and normalized not in playlist_artist_lookup:
                    playlist_artist_lookup[normalized] = split_name

    nodes_by_collection: Dict[str, Dict[str, Dict]] = {
        nodes_map["artists"]: {},
        nodes_map["songs"]: {},
        nodes_map["record_labels"]: {},
        nodes_map["genres"]: {},
        nodes_map["locations"]: {},
        nodes_map["songwriters"]: {},
        nodes_map["producers"]: {},
        nodes_map["moods"]: {},
        nodes_map["instruments"]: {},
        nodes_map["languages"]: {},
        nodes_map["contributors"]: {},
    }
    enrichment_edges = [
        edges_map["artists_genres"],
        edges_map["artists_locations"],
        edges_map["artists_record_labels"],
        edges_map["artists_associated_acts"],
        edges_map["songs_songwriters"],
        edges_map["songs_producers"],
        edges_map["songs_features"],
        edges_map["songs_moods"],
        edges_map["songs_instruments"],
        edges_map["songs_languages"],
        edges_map["songs_contributors"],
    ]
    edges_by_collection: Dict[str, List[Dict]] = {name: [] for name in enrichment_edges}

    def upsert_node(collection: str, key: str, payload: Dict) -> str:
        node_id = f"{collection}/{key}"
        if key in nodes_by_collection[collection]:
            return node_id
        payload["_key"] = key
        payload["_id"] = node_id
        nodes_by_collection[collection][key] = payload
        return node_id

    def add_edge(
        collection: str,
        from_id: str,
        to_id: str,
        label: str,
        source: str | None = None,
        extra: Dict | None = None,
    ) -> None:
        edge_key = _farmhash_key("edge", f"{from_id}|{to_id}|{label}")
        payload = {
            "_key": edge_key,
            "_from": from_id,
            "_to": to_id,
            "label": label,
        }
        if source:
            payload["source"] = source
        if extra:
            payload.update(extra)
        edges_by_collection[collection].append(payload)

    artist_enrichment_cache: Dict[str, Dict] = {}
    artist_to_genres: Dict[str, set[str]] = {}
    artist_to_locations: Dict[str, set[str]] = {}
    artist_to_labels: Dict[str, set[str]] = {}
    song_enrichment_cache: Dict[str, Dict] = {}

    unique_artists: set[str] = set()
    unique_song_keys: set[str] = set()
    song_key_to_payload: Dict[str, Tuple[str, str]] = {}
    for item in tracks:
        track = item.get("track", item) or {}
        track_name = track.get("name", "") or "Unknown Song"
        track_artists = track.get("artists", []) or []
        primary_artist = track_artists[0].get("name", "") if track_artists else ""
        for artist in track_artists:
            artist_name = artist.get("name", "")
            if artist_name:
                unique_artists.add(artist_name)
        if primary_artist:
            song_key = f"{track_name}|{primary_artist}"
            unique_song_keys.add(song_key)
            song_key_to_payload[song_key] = (track_name, primary_artist)

    def _safe_enrich_artist(artist_name: str) -> Dict:
        try:
            return enrich_artist(artist_name)
        except Exception:
            logger.warning("Artist enrichment failed", extra={"artist": artist_name})
            return {
                "genres": [],
                "labels": [],
                "locations": [],
                "associated_acts": [],
                "genres_source": None,
                "labels_source": None,
                "locations_source": None,
                "associated_acts_source": None,
            }

    def _safe_enrich_song(track_name: str, primary_artist: str) -> Dict:
        try:
            return enrich_song(track_name, primary_artist)
        except Exception:
            logger.warning(
                "Song enrichment failed",
                extra={"track": track_name, "artist": primary_artist},
            )
            return {
                "writers": [],
                "producers": [],
                "featured_artists": [],
                "moods": [],
                "instruments": [],
                "languages": [],
                "contributors": [],
                "songdna_relations": [],
                "stories": [],
                "writers_source": None,
                "producers_source": None,
                "featured_artists_source": None,
                "moods_source": None,
                "instruments_source": None,
                "languages_source": None,
                "contributors_source": None,
                "songdna_relations_source": None,
                "stories_source": None,
            }

    if unique_artists or unique_song_keys:
        max_workers = max(1, settings.ENRICH_MAX_WORKERS)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            artist_futures = {
                executor.submit(_safe_enrich_artist, name): name for name in unique_artists
            }
            song_futures = {
                executor.submit(_safe_enrich_song, *song_key_to_payload[key]): key
                for key in unique_song_keys
            }
            for future in as_completed({**artist_futures, **song_futures}):
                if future in artist_futures:
                    name = artist_futures[future]
                    artist_enrichment_cache[name] = future.result()
                else:
                    key = song_futures[future]
                    song_enrichment_cache[key] = future.result()

    for item in tracks:
        track = item.get("track", item) or {}
        track_name = track.get("name", "") or "Unknown Song"
        track_uri = track.get("uri", "")
        duration_ms = track.get("duration_ms", "")
        song_key = _farmhash_key("song", track_uri or track_name)
        song_id = upsert_node(
            nodes_map["songs"],
            song_key,
            {"name": track_name, "track_uri": track_uri, "duration_ms": duration_ms},
        )

        track_artists = track.get("artists", []) or []
        primary_artist = track_artists[0].get("name", "") if track_artists else ""

        for artist in track.get("artists", []) or []:
            artist_name = artist.get("name", "")
            if not artist_name:
                continue
            artist_key = _farmhash_key("artist", artist_name)
            artist_id = upsert_node(nodes_map["artists"], artist_key, {"name": artist_name})

            enrichment = artist_enrichment_cache.get(artist_name)
            if enrichment is None:
                enrichment = enrich_artist(artist_name)
                artist_enrichment_cache[artist_name] = enrichment

            for genre in enrichment.get("genres", []):
                genre_key = _farmhash_key("genre", genre)
                genre_id = upsert_node(nodes_map["genres"], genre_key, {"name": genre})
                add_edge(
                    edges_map["artists_genres"],
                    artist_id,
                    genre_id,
                    "has_genre",
                    enrichment.get("genres_source"),
                )
                artist_to_genres.setdefault(artist_id, set()).add(genre)

            for location in enrichment.get("locations", []):
                location_key = _farmhash_key("location", location)
                location_id = upsert_node(nodes_map["locations"], location_key, {"name": location})
                add_edge(
                    edges_map["artists_locations"],
                    artist_id,
                    location_id,
                    "from_location",
                    enrichment.get("locations_source"),
                )
                artist_to_locations.setdefault(artist_id, set()).add(location)

            for label in enrichment.get("labels", []):
                label_key = _farmhash_key("label", label)
                label_id = upsert_node(nodes_map["record_labels"], label_key, {"name": label})
                add_edge(
                    edges_map["artists_record_labels"],
                    artist_id,
                    label_id,
                    "signed_to",
                    enrichment.get("labels_source"),
                )
                artist_to_labels.setdefault(artist_id, set()).add(label)

            for act in enrichment.get("associated_acts", []):
                playlist_act = _resolve_playlist_artist(act, playlist_artist_lookup)
                if not playlist_act:
                    continue
                act_key = _farmhash_key("artist", playlist_act)
                act_id = upsert_node(nodes_map["artists"], act_key, {"name": playlist_act})
                add_edge(
                    edges_map["artists_associated_acts"],
                    artist_id,
                    act_id,
                    "associated_with",
                    enrichment.get("associated_acts_source"),
                )

        if primary_artist:
            song_cache_key = f"{track_name}|{primary_artist}"
            song_enrichment = song_enrichment_cache.get(song_cache_key)
            if song_enrichment is None:
                song_enrichment = enrich_song(track_name, primary_artist)
                song_enrichment_cache[song_cache_key] = song_enrichment

            for writer in song_enrichment.get("writers", []):
                writer_key = _farmhash_key("songwriter", writer)
                writer_id = upsert_node(nodes_map["songwriters"], writer_key, {"name": writer})
                add_edge(
                    edges_map["songs_songwriters"],
                    song_id,
                    writer_id,
                    "written_by",
                    song_enrichment.get("writers_source"),
                )

            for producer in song_enrichment.get("producers", []):
                producer_key = _farmhash_key("producer", producer)
                producer_id = upsert_node(nodes_map["producers"], producer_key, {"name": producer})
                add_edge(
                    edges_map["songs_producers"],
                    song_id,
                    producer_id,
                    "produced_by",
                    song_enrichment.get("producers_source"),
                )

            for featured in song_enrichment.get("featured_artists", []):
                playlist_feature = _resolve_playlist_artist(featured, playlist_artist_lookup)
                if not playlist_feature:
                    continue
                feature_key = _farmhash_key("artist", playlist_feature)
                feature_id = upsert_node(nodes_map["artists"], feature_key, {"name": playlist_feature})
                add_edge(
                    edges_map["songs_features"],
                    song_id,
                    feature_id,
                    "featured",
                    song_enrichment.get("featured_artists_source"),
                )

            # Moods are temporarily disabled.
            # for mood in song_enrichment.get("moods", []):
            #     mood_key = _farmhash_key("mood", mood)
            #     mood_id = upsert_node(nodes_map["moods"], mood_key, {"name": mood})
            #     add_edge(
            #         edges_map["songs_moods"],
            #         song_id,
            #         mood_id,
            #         "has_mood",
            #         song_enrichment.get("moods_source"),
            #     )

            for instrument in song_enrichment.get("instruments", []):
                instrument_key = _farmhash_key("instrument", instrument)
                instrument_id = upsert_node(nodes_map["instruments"], instrument_key, {"name": instrument})
                add_edge(
                    edges_map["songs_instruments"],
                    song_id,
                    instrument_id,
                    "has_instrument",
                    song_enrichment.get("instruments_source"),
                )

            for language in song_enrichment.get("languages", []):
                language_key = _farmhash_key("language", language)
                language_id = upsert_node(nodes_map["languages"], language_key, {"name": language})
                add_edge(
                    edges_map["songs_languages"],
                    song_id,
                    language_id,
                    "in_language",
                    song_enrichment.get("languages_source"),
                )

            for contributor in song_enrichment.get("contributors", []):
                contributor_name = contributor.get("name", "")
                contributor_role = contributor.get("role", "contributor")
                if not contributor_name or not contributor_role:
                    continue
                contributor_key = _farmhash_key("contributor", contributor_name)
                contributor_id = upsert_node(
                    nodes_map["contributors"],
                    contributor_key,
                    {"name": contributor_name},
                )
                add_edge(
                    edges_map["songs_contributors"],
                    song_id,
                    contributor_id,
                    contributor_role,
                    contributor.get("source") or song_enrichment.get("contributors_source"),
                    {"detail": contributor.get("detail")} if contributor.get("detail") else None,
                )

            song_payload = nodes_by_collection[nodes_map["songs"]][song_key]
            stories_payload = []
            for story in song_enrichment.get("stories", []):
                body = (story.get("body") or "").strip()
                if not body:
                    continue
                title = (story.get("title") or f"About {track_name}").strip()
                stories_payload.append(
                    {
                        "title": title,
                        "body": body,
                        "source": story.get("source"),
                        "source_url": story.get("source_url"),
                        "tags": story.get("tags") or [],
                    }
                )
            if stories_payload:
                song_payload["stories"] = stories_payload

            songdna_payload = []
            for relation in song_enrichment.get("songdna_relations", []):
                title = relation.get("title", "")
                if not title:
                    continue
                songdna_payload.append(
                    {
                        "relation": relation.get("relation") or "related_to",
                        "title": title,
                        "artist": relation.get("artist", ""),
                        "target_type": relation.get("target_type") or "recording",
                        "source": relation.get("source") or song_enrichment.get("songdna_relations_source"),
                    }
                )
            if songdna_payload:
                song_payload["songdna_relations"] = songdna_payload

    for collection, nodes in nodes_by_collection.items():
        _batch_insert(db, collection, list(nodes.values()))
    for collection, edges in edges_by_collection.items():
        _batch_insert(db, collection, edges)

    node_count = sum(db.collection(name).count() for name in node_collections)
    edge_count = sum(db.collection(name).count() for name in edge_collections)
    collection_map["playlist_name"] = playlist_name
    return node_count, edge_count, collection_map


if __name__ == "__main__":
    nodes, edges, _ = build_and_upload_graph(settings.PLAYLIST_URL, "default")
    print(f"Uploaded {nodes} nodes and {edges} edges to ArangoDB.")