from typing import Dict, List, Tuple

import cityhash
from arango import ArangoClient
from spotify_scraper import SpotifyClient

from artist_enrichment import enrich_artist


DB_NAME = "spotify"
DB_PASSWORD = "test"
PLAYLIST_NAME_FALLBACK = "playlist"
GRAPH_NAME_PREFIX = "spotify_kg"
GRAPH_JOBS_COLLECTION = "graph_jobs"
PLAYLIST_URL = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"

# Node collections (snake_case)
ARTISTS_COLLECTION = "artists"
SONGS_COLLECTION = "songs"
ALBUMS_COLLECTION = "albums"
LABELS_COLLECTION = "record_labels"
PLAYLISTS_COLLECTION = "playlists"
GENRES_COLLECTION = "genres"
LOCATIONS_COLLECTION = "locations"

# Edge collections (snake_case)
ARTISTS_SONGS = "artists_songs"
ARTISTS_ALBUMS = "artists_albums"
SONGS_ALBUMS = "songs_albums"
ALBUMS_LABELS = "albums_record_labels"
ARTISTS_GENRES = "artists_genres"
ARTISTS_LOCATIONS = "artists_locations"
ARTISTS_LABELS = "artists_record_labels"
ARTISTS_ACTS = "artists_associated_acts"
ARTISTS_RELATED = "artists_related"


def _farmhash_key(prefix: str, raw: str) -> str:
    seed = f"{prefix}|{raw or 'unknown'}"
    return str(cityhash.CityHash64(seed))


def _collection_prefix(graph_id: str) -> str:
    return f"g_{graph_id}"


def _graph_name(graph_id: str) -> str:
    return f"{GRAPH_NAME_PREFIX}_{_collection_prefix(graph_id)}"


def _collection_map(graph_id: str) -> Dict[str, Dict[str, str]]:
    prefix = _collection_prefix(graph_id)
    nodes = {
        "artists": f"{prefix}_{ARTISTS_COLLECTION}",
        "songs": f"{prefix}_{SONGS_COLLECTION}",
        "albums": f"{prefix}_{ALBUMS_COLLECTION}",
        "record_labels": f"{prefix}_{LABELS_COLLECTION}",
        "playlists": f"{prefix}_{PLAYLISTS_COLLECTION}",
        "genres": f"{prefix}_{GENRES_COLLECTION}",
        "locations": f"{prefix}_{LOCATIONS_COLLECTION}",
    }
    edges = {
        "artists_songs": f"{prefix}_{ARTISTS_SONGS}",
        "artists_albums": f"{prefix}_{ARTISTS_ALBUMS}",
        "songs_albums": f"{prefix}_{SONGS_ALBUMS}",
        "albums_record_labels": f"{prefix}_{ALBUMS_LABELS}",
        "artists_genres": f"{prefix}_{ARTISTS_GENRES}",
        "artists_locations": f"{prefix}_{ARTISTS_LOCATIONS}",
        "artists_record_labels": f"{prefix}_{ARTISTS_LABELS}",
        "artists_associated_acts": f"{prefix}_{ARTISTS_ACTS}",
        "artists_related": f"{prefix}_{ARTISTS_RELATED}",
    }
    return {"nodes": nodes, "edges": edges}


def _load_playlist(playlist_url: str) -> Tuple[str, List[Dict]]:
    client = SpotifyClient()
    try:
        playlist = client.get_playlist_info(playlist_url)
        playlist_name = playlist.get("name") or PLAYLIST_NAME_FALLBACK
        tracks_container = playlist.get("tracks", {})
        if isinstance(tracks_container, dict):
            tracks = tracks_container.get("items", [])
        else:
            tracks = tracks_container or []
        return playlist_name, tracks
    finally:
        client.close()


def _ensure_collection(db, name: str, edge: bool = False) -> None:
    if not db.has_collection(name):
        db.create_collection(name, edge=edge)


def _truncate_collection(db, name: str) -> None:
    if db.has_collection(name):
        db.collection(name).truncate()


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
            "edge_collection": edges["artists_related"],
            "from_vertex_collections": [nodes["artists"]],
            "to_vertex_collections": [nodes["artists"]],
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
    db = ArangoClient().db(DB_NAME, password=DB_PASSWORD)
    graph_name = _graph_name(graph_id)
    collection_map = _collection_map(graph_id)
    nodes_map = collection_map["nodes"]
    edges_map = collection_map["edges"]
    node_collections = list(nodes_map.values())
    edge_collections = list(edges_map.values())

    if reset:
        _reset_graph(db, graph_name, node_collections, edge_collections)

    for collection in node_collections:
        _ensure_collection(db, collection)
    for collection in edge_collections:
        _ensure_collection(db, collection, edge=True)
    _ensure_graph(db, graph_name, nodes_map, edges_map)

    playlist_name, tracks = _load_playlist(playlist_url)
    if len(tracks) > 200:
        raise ValueError(f"Playlist has {len(tracks)} tracks; expected at most 200.")

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
            artist_key = _farmhash_key("artist", artist_name)
            artist_id = upsert_node(nodes_map["artists"], artist_key, {"name": artist_name})
            add_edge(edges_map["artists_songs"], artist_id, song_id, "performed")
            if album_id:
                add_edge(edges_map["artists_albums"], artist_id, album_id, "contributed_to")

        label_name = album.get("label", "")
        if label_name and album_id:
            label_key = _farmhash_key("label", label_name)
            label_id = upsert_node(nodes_map["record_labels"], label_key, {"name": label_name})
            add_edge(edges_map["albums_record_labels"], album_id, label_id, "released_by")

    for collection, nodes in nodes_by_collection.items():
        if nodes:
            db.collection(collection).insert_many(list(nodes.values()), overwrite=True)
    for collection, edges in edges_by_collection.items():
        if edges:
            db.collection(collection).insert_many(edges, overwrite=True)

    node_count = sum(db.collection(name).count() for name in node_collections)
    edge_count = sum(db.collection(name).count() for name in edge_collections)
    collection_map["playlist_name"] = playlist_name
    return node_count, edge_count, collection_map


def enrich_graph(
    playlist_url: str,
    graph_id: str,
) -> Tuple[int, int, Dict[str, Dict[str, str]]]:
    db = ArangoClient().db(DB_NAME, password=DB_PASSWORD)
    graph_name = _graph_name(graph_id)
    collection_map = _collection_map(graph_id)
    nodes_map = collection_map["nodes"]
    edges_map = collection_map["edges"]
    node_collections = list(nodes_map.values())
    edge_collections = list(edges_map.values())

    for collection in node_collections:
        _ensure_collection(db, collection)
    for collection in edge_collections:
        _ensure_collection(db, collection, edge=True)
    _ensure_graph(db, graph_name, nodes_map, edges_map)

    playlist_name, tracks = _load_playlist(playlist_url)
    if len(tracks) > 200:
        raise ValueError(f"Playlist has {len(tracks)} tracks; expected at most 200.")

    nodes_by_collection: Dict[str, Dict[str, Dict]] = {
        nodes_map["artists"]: {},
        nodes_map["record_labels"]: {},
        nodes_map["genres"]: {},
        nodes_map["locations"]: {},
    }
    enrichment_edges = [
        edges_map["artists_genres"],
        edges_map["artists_locations"],
        edges_map["artists_record_labels"],
        edges_map["artists_associated_acts"],
        edges_map["artists_related"],
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
        edges_by_collection[collection].append(payload)

    artist_enrichment_cache: Dict[str, Dict] = {}
    artist_to_genres: Dict[str, set[str]] = {}
    artist_to_locations: Dict[str, set[str]] = {}
    artist_to_labels: Dict[str, set[str]] = {}

    for item in tracks:
        track = item.get("track", item) or {}
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
                act_key = _farmhash_key("artist", act)
                act_id = upsert_node(nodes_map["artists"], act_key, {"name": act})
                add_edge(
                    edges_map["artists_associated_acts"],
                    artist_id,
                    act_id,
                    "associated_with",
                    enrichment.get("associated_acts_source"),
                )

    def add_related_edges(
        mapping: Dict[str, set[str]],
        relation: str,
    ) -> None:
        inverted: Dict[str, List[str]] = {}
        for artist_id, values in mapping.items():
            for value in values:
                inverted.setdefault(value, []).append(artist_id)

        for value, artists in inverted.items():
            if len(artists) < 2:
                continue
            for i in range(len(artists)):
                for j in range(i + 1, len(artists)):
                    from_id = artists[i]
                    to_id = artists[j]
                    edge_key = _farmhash_key("artist_related", f"{from_id}|{to_id}|{relation}|{value}")
                    edges_by_collection[edges_map["artists_related"]].append(
                        {
                            "_key": edge_key,
                            "_from": from_id,
                            "_to": to_id,
                            "label": relation,
                            "value": value,
                        }
                    )

    add_related_edges(artist_to_genres, "shares_genre")
    add_related_edges(artist_to_locations, "shares_location")
    add_related_edges(artist_to_labels, "shares_label")

    for collection, nodes in nodes_by_collection.items():
        if nodes:
            db.collection(collection).insert_many(list(nodes.values()), overwrite=True)
    for collection, edges in edges_by_collection.items():
        if edges:
            db.collection(collection).insert_many(edges, overwrite=True)

    node_count = sum(db.collection(name).count() for name in node_collections)
    edge_count = sum(db.collection(name).count() for name in edge_collections)
    collection_map["playlist_name"] = playlist_name
    return node_count, edge_count, collection_map


if __name__ == "__main__":
    nodes, edges, _ = build_and_upload_graph(PLAYLIST_URL, "default")
    print(f"Uploaded {nodes} nodes and {edges} edges to ArangoDB.")