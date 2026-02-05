"""
Minimal Python backend that serves a knowledge graph from ArangoDB.
"""

import threading
import uuid
from datetime import datetime, timezone

from arango import ArangoClient
from flask import Flask, jsonify, request
from flask_cors import CORS

from playlist import DB_NAME, DB_PASSWORD, GRAPH_JOBS_COLLECTION, build_and_upload_graph, enrich_graph

app = Flask(__name__)
CORS(app)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_db():
    return ArangoClient().db(DB_NAME, password=DB_PASSWORD)


def _ensure_jobs_collection(db) -> None:
    if not db.has_collection(GRAPH_JOBS_COLLECTION):
        db.create_collection(GRAPH_JOBS_COLLECTION)


def _build_job(db, playlist_url: str) -> dict:
    job_id = uuid.uuid4().hex
    graph_id = uuid.uuid4().hex
    job_doc = {
        "_key": job_id,
        "job_id": job_id,
        "graph_id": graph_id,
        "playlist_url": playlist_url,
        "status": "queued",
        "created_at": _utc_now(),
        "updated_at": _utc_now(),
    }
    db.collection(GRAPH_JOBS_COLLECTION).insert(job_doc, overwrite=True)
    return job_doc


def _update_job(db, job_id: str, fields: dict) -> None:
    fields["updated_at"] = _utc_now()
    db.collection(GRAPH_JOBS_COLLECTION).update({"_key": job_id, **fields})


def _get_job(db, job_id: str) -> dict | None:
    if not db.has_collection(GRAPH_JOBS_COLLECTION):
        return None
    return db.collection(GRAPH_JOBS_COLLECTION).get(job_id)


def _find_job_by_playlist_url(db, playlist_url: str) -> dict | None:
    if not db.has_collection(GRAPH_JOBS_COLLECTION):
        return None
    jobs = db.collection(GRAPH_JOBS_COLLECTION).find({"playlist_url": playlist_url}, limit=1)
    return next(jobs, None)


def _find_job_by_graph_id(db, graph_id: str) -> dict | None:
    if not db.has_collection(GRAPH_JOBS_COLLECTION):
        return None
    jobs = db.collection(GRAPH_JOBS_COLLECTION).find({"graph_id": graph_id}, limit=1)
    return next(jobs, None)


def _resolve_collection_map(db, graph_id: str | None) -> dict | None:
    collection = db.collection(GRAPH_JOBS_COLLECTION)
    if graph_id:
        job = collection.find({"graph_id": graph_id}, limit=1)
        return next(job, None)

    jobs = list(collection.find({"status": "ready"}))
    if not jobs:
        return None
    jobs.sort(key=lambda job: job.get("updated_at") or "", reverse=True)
    return jobs[0]


def build_graph_documents(graph_id: str | None = None):
    db = _get_db()
    _ensure_jobs_collection(db)

    job_doc = _resolve_collection_map(db, graph_id)
    if not job_doc:
        return [], []

    collection_map = job_doc.get("collection_map") or {}
    nodes_map = collection_map.get("nodes") or {}
    edges_map = collection_map.get("edges") or {}
    node_collections = list(nodes_map.values())
    edge_collections = list(edges_map.values())

    nodes = []
    edges = []
    for collection in node_collections:
        if db.has_collection(collection):
            nodes.extend(list(db.collection(collection).all()))
    for collection in edge_collections:
        if db.has_collection(collection):
            edges.extend(list(db.collection(collection).all()))

    return nodes, edges


@app.route("/api/graph", methods=["GET"])
def get_graph():
    """Return knowledge graph as node/edge documents."""
    graph_id = request.args.get("graph_id")
    nodes, edges = build_graph_documents(graph_id)
    return jsonify({"nodes": nodes, "edges": edges})


@app.route("/api/playlist/build", methods=["POST"])
def build_playlist_graph():
    payload = request.get_json(silent=True) or {}
    playlist_url = payload.get("playlist_url")
    if not playlist_url:
        return jsonify({"error": "playlist_url is required"}), 400

    db = _get_db()
    _ensure_jobs_collection(db)
    existing_job = _find_job_by_playlist_url(db, playlist_url)
    if existing_job:
        job_doc = {
            "job_id": existing_job["job_id"],
            "graph_id": existing_job["graph_id"],
        }
        _update_job(
            db,
            existing_job["job_id"],
            {
                "status": "queued",
                "error": None,
                "node_count": None,
                "edge_count": None,
                "collection_map": None,
                "playlist_name": None,
            },
        )
    else:
        job_doc = _build_job(db, playlist_url)

    def _run_job(job_id: str, graph_id: str, url: str) -> None:
        job_db = _get_db()
        try:
            _update_job(job_db, job_id, {"status": "running"})
            node_count, edge_count, collection_map = build_and_upload_graph(url, graph_id, reset=True)
            _update_job(
                job_db,
                job_id,
                {
                    "status": "ready",
                    "node_count": node_count,
                    "edge_count": edge_count,
                    "collection_map": collection_map,
                    "playlist_name": collection_map.get("playlist_name"),
                },
            )
        except Exception as exc:  # noqa: BLE001
            _update_job(job_db, job_id, {"status": "failed", "error": str(exc)})

    threading.Thread(
        target=_run_job,
        args=(job_doc["job_id"], job_doc["graph_id"], playlist_url),
        daemon=True,
    ).start()

    return jsonify({"job_id": job_doc["job_id"], "graph_id": job_doc["graph_id"]})


@app.route("/api/playlist/enrich", methods=["POST"])
def enrich_playlist_graph():
    payload = request.get_json(silent=True) or {}
    graph_id = payload.get("graph_id")
    if not graph_id:
        return jsonify({"error": "graph_id is required"}), 400

    db = _get_db()
    _ensure_jobs_collection(db)
    job_doc = _find_job_by_graph_id(db, graph_id)
    if not job_doc:
        return jsonify({"error": "graph_id not found"}), 404

    playlist_url = payload.get("playlist_url") or job_doc.get("playlist_url")
    if not playlist_url:
        return jsonify({"error": "playlist_url is required"}), 400

    _update_job(
        db,
        job_doc["job_id"],
        {
            "status": "queued",
            "error": None,
        },
    )

    def _run_enrichment(job_id: str, url: str, graph_id_value: str) -> None:
        job_db = _get_db()
        try:
            _update_job(job_db, job_id, {"status": "running"})
            node_count, edge_count, collection_map = enrich_graph(url, graph_id_value)
            _update_job(
                job_db,
                job_id,
                {
                    "status": "ready",
                    "node_count": node_count,
                    "edge_count": edge_count,
                    "collection_map": collection_map,
                    "playlist_name": collection_map.get("playlist_name"),
                    "enriched_at": _utc_now(),
                },
            )
        except Exception as exc:  # noqa: BLE001
            _update_job(job_db, job_id, {"status": "failed", "error": str(exc)})

    threading.Thread(
        target=_run_enrichment,
        args=(job_doc["job_id"], playlist_url, graph_id),
        daemon=True,
    ).start()

    return jsonify({"job_id": job_doc["job_id"], "graph_id": graph_id})


@app.route("/api/playlist/status/<job_id>", methods=["GET"])
def playlist_job_status(job_id: str):
    db = _get_db()
    _ensure_jobs_collection(db)
    job = _get_job(db, job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404
    return jsonify(job)


@app.route("/api/playlists", methods=["GET"])
def list_playlists():
    db = _get_db()
    _ensure_jobs_collection(db)
    collection = db.collection(GRAPH_JOBS_COLLECTION)
    jobs = list(collection.find({"status": "ready"}))
    jobs.sort(key=lambda job: job.get("updated_at") or "", reverse=True)
    playlists = [
        {
            "graph_id": job.get("graph_id"),
            "playlist_url": job.get("playlist_url"),
            "playlist_name": (job.get("playlist_name") or ""),
            "node_count": job.get("node_count"),
            "edge_count": job.get("edge_count"),
            "updated_at": job.get("updated_at"),
        }
        for job in jobs
    ]
    return jsonify({"playlists": playlists})


@app.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy"})


if __name__ == "__main__":
    print("Starting Knowledge Graph Backend on http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
