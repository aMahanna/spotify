from typing import Dict, List, Tuple

import cityhash
from arango import ArangoClient
from spotify_scraper import SpotifyClient


DB_NAME = "spotify"
DB_PASSWORD = "test"
PLAYLIST_NAME_FALLBACK = "playlist"
GRAPH_NAME = "SpotifyKnowledgeGraph"
PLAYLIST_URL = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"

# Node collections (snake_case)
ARTISTS_COLLECTION = "artists"
SONGS_COLLECTION = "songs"
ALBUMS_COLLECTION = "albums"
LABELS_COLLECTION = "record_labels"
PLAYLISTS_COLLECTION = "playlists"

# Edge collections (snake_case)
ARTISTS_SONGS = "artists_songs"
ARTISTS_ALBUMS = "artists_albums"
SONGS_ALBUMS = "songs_albums"
ALBUMS_LABELS = "albums_record_labels"


def _farmhash_key(prefix: str, raw: str) -> str:
    seed = f"{prefix}|{raw or 'unknown'}"
    return str(cityhash.CityHash64(seed))


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


def _reset_graph(db, node_collections: List[str], edge_collections: List[str]) -> None:
    if db.has_graph(GRAPH_NAME):
        db.delete_graph(GRAPH_NAME, drop_collections=True, ignore_missing=True)
    for collection in edge_collections:
        db.delete_collection(collection, ignore_missing=True)
    for collection in node_collections:
        db.delete_collection(collection, ignore_missing=True)


def _ensure_graph(db) -> None:
    if db.has_graph(GRAPH_NAME):
        return
    edge_definitions = [
        {
            "edge_collection": ARTISTS_SONGS,
            "from_vertex_collections": [ARTISTS_COLLECTION],
            "to_vertex_collections": [SONGS_COLLECTION],
        },
        {
            "edge_collection": ARTISTS_ALBUMS,
            "from_vertex_collections": [ARTISTS_COLLECTION],
            "to_vertex_collections": [ALBUMS_COLLECTION],
        },
        {
            "edge_collection": SONGS_ALBUMS,
            "from_vertex_collections": [SONGS_COLLECTION],
            "to_vertex_collections": [ALBUMS_COLLECTION],
        },
        {
            "edge_collection": ALBUMS_LABELS,
            "from_vertex_collections": [ALBUMS_COLLECTION],
            "to_vertex_collections": [LABELS_COLLECTION],
        },
    ]
    db.create_graph(
        GRAPH_NAME,
        edge_definitions=edge_definitions,
        orphan_collections=[],
    )


def build_and_upload_graph(
    reset: bool = True,
) -> Tuple[int, int]:
    db = ArangoClient().db(DB_NAME, password=DB_PASSWORD)

    node_collections = [
        ARTISTS_COLLECTION,
        SONGS_COLLECTION,
        ALBUMS_COLLECTION,
        LABELS_COLLECTION,
        PLAYLISTS_COLLECTION,
    ]
    edge_collections = [
        ARTISTS_SONGS,
        ARTISTS_ALBUMS,
        SONGS_ALBUMS,
        ALBUMS_LABELS,
    ]

    if reset:
        _reset_graph(db, node_collections, edge_collections)

    for collection in node_collections:
        _ensure_collection(db, collection)
    for collection in edge_collections:
        _ensure_collection(db, collection, edge=True)
    _ensure_graph(db)

    playlist_name, tracks = _load_playlist(PLAYLIST_URL)
    if len(tracks) > 50:
        raise ValueError(f"Playlist has {len(tracks)} tracks; expected at most 50.")

    nodes_by_collection: Dict[str, Dict[str, Dict]] = {
        ARTISTS_COLLECTION: {},
        SONGS_COLLECTION: {},
        ALBUMS_COLLECTION: {},
        LABELS_COLLECTION: {},
        PLAYLISTS_COLLECTION: {},
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

    def add_edge(collection: str, from_id: str, to_id: str, label: str) -> None:
        edge_counters[collection] += 1
        edges_by_collection[collection].append(
            {
                "_key": f"e{edge_counters[collection]}",
                "_from": from_id,
                "_to": to_id,
                "label": label,
            }
        )

    playlist_key = _farmhash_key("playlist", playlist_name)
    upsert_node(PLAYLISTS_COLLECTION, playlist_key, {"name": playlist_name})

    for item in tracks:
        track = item.get("track", item) or {}
        track_name = track.get("name", "") or "Unknown Song"
        track_uri = track.get("uri", "")
        duration_ms = track.get("duration_ms", "")

        song_key = _farmhash_key("song", track_uri or track_name)
        song_id = upsert_node(
            SONGS_COLLECTION,
            song_key,
            {"name": track_name, "track_uri": track_uri, "duration_ms": duration_ms},
        )

        album = track.get("album", {}) or {}
        album_name = album.get("name", "")
        album_id = ""
        if album_name:
            album_key = _farmhash_key("album", album_name)
            album_id = upsert_node(
                ALBUMS_COLLECTION,
                album_key,
                {"name": album_name, "release_date": album.get("release_date", "")},
            )
            add_edge(SONGS_ALBUMS, song_id, album_id, "on_album")

        for artist in track.get("artists", []) or []:
            artist_name = artist.get("name", "")
            if not artist_name:
                continue
            artist_key = _farmhash_key("artist", artist_name)
            artist_id = upsert_node(ARTISTS_COLLECTION, artist_key, {"name": artist_name})
            add_edge(ARTISTS_SONGS, artist_id, song_id, "performed")
            if album_id:
                add_edge(ARTISTS_ALBUMS, artist_id, album_id, "contributed_to")

        label_name = album.get("label", "")
        if label_name and album_id:
            label_key = _farmhash_key("label", label_name)
            label_id = upsert_node(LABELS_COLLECTION, label_key, {"name": label_name})
            add_edge(ALBUMS_LABELS, album_id, label_id, "released_by")

    for collection, nodes in nodes_by_collection.items():
        if nodes:
            db.collection(collection).insert_many(list(nodes.values()), overwrite=True)
    for collection, edges in edges_by_collection.items():
        if edges:
            db.collection(collection).insert_many(edges, overwrite=True)

    node_count = sum(db.collection(name).count() for name in nodes_by_collection)
    edge_count = sum(db.collection(name).count() for name in edge_collections)
    return node_count, edge_count


if __name__ == "__main__":
    nodes, edges = build_and_upload_graph()
    print(f"Uploaded {nodes} nodes and {edges} edges to ArangoDB.")