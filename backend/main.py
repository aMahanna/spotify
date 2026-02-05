"""
Minimal Python backend that serves a knowledge graph from ArangoDB.
"""

from flask import Flask, jsonify
from flask_cors import CORS

from arango import ArangoClient
from playlist import (
    ALBUMS_COLLECTION,
    ALBUMS_LABELS,
    ARTISTS_ALBUMS,
    ARTISTS_COLLECTION,
    ARTISTS_SONGS,
    DB_NAME,
    DB_PASSWORD,
    LABELS_COLLECTION,
    PLAYLISTS_COLLECTION,
    SONGS_ALBUMS,
    SONGS_COLLECTION,
)

app = Flask(__name__)
CORS(app)

NODE_COLLECTIONS = [
    ARTISTS_COLLECTION,
    SONGS_COLLECTION,
    ALBUMS_COLLECTION,
    LABELS_COLLECTION,
    PLAYLISTS_COLLECTION,
]

EDGE_COLLECTIONS = [
    ARTISTS_SONGS,
    ARTISTS_ALBUMS,
    SONGS_ALBUMS,
    ALBUMS_LABELS,
]


def build_graph_documents():
    db = ArangoClient().db(DB_NAME, password=DB_PASSWORD)
    nodes = []
    edges = []

    for collection in NODE_COLLECTIONS:
        if db.has_collection(collection):
            nodes.extend(list(db.collection(collection).all()))

    for collection in EDGE_COLLECTIONS:
        if db.has_collection(collection):
            edges.extend(list(db.collection(collection).all()))

    return nodes, edges


@app.route("/api/graph", methods=["GET"])
def get_graph():
    """Return knowledge graph as node/edge documents."""
    nodes, edges = build_graph_documents()
    return jsonify({"nodes": nodes, "edges": edges})


@app.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy"})


if __name__ == "__main__":
    print("Starting Knowledge Graph Backend on http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
