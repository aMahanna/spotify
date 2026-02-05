"""Flask API routes for the knowledge graph."""

from __future__ import annotations

import threading

from flask import Blueprint, jsonify, request

from db.client import get_db
from services import graph_service, jobs

bp = Blueprint("api", __name__)


@bp.route("/api/graph", methods=["GET"])
def get_graph():
    """Return knowledge graph as node/edge documents."""
    graph_id = request.args.get("graph_id")
    nodes, edges = graph_service.build_graph_documents(graph_id)
    return jsonify({"nodes": nodes, "edges": edges})


@bp.route("/api/playlist/build", methods=["POST"])
def build_playlist_graph():
    payload = request.get_json(silent=True) or {}
    playlist_url = payload.get("playlist_url")
    if not playlist_url:
        return jsonify({"error": "playlist_url is required"}), 400

    db = get_db()
    jobs.ensure_jobs_collection(db)
    existing_job = jobs.find_job_by_playlist_url(db, playlist_url)
    if existing_job:
        job_doc = {
            "job_id": existing_job["job_id"],
            "graph_id": existing_job["graph_id"],
        }
        jobs.update_job(
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
        job_doc = jobs.build_job(db, playlist_url)

    threading.Thread(
        target=graph_service.run_build_job,
        args=(job_doc["job_id"], job_doc["graph_id"], playlist_url),
        daemon=True,
    ).start()

    return jsonify({"job_id": job_doc["job_id"], "graph_id": job_doc["graph_id"]})


@bp.route("/api/playlist/enrich", methods=["POST"])
def enrich_playlist_graph():
    payload = request.get_json(silent=True) or {}
    graph_id = payload.get("graph_id")
    if not graph_id:
        return jsonify({"error": "graph_id is required"}), 400

    db = get_db()
    jobs.ensure_jobs_collection(db)
    job_doc = jobs.find_job_by_graph_id(db, graph_id)
    if not job_doc:
        return jsonify({"error": "graph_id not found"}), 404

    playlist_url = payload.get("playlist_url") or job_doc.get("playlist_url")
    if not playlist_url:
        return jsonify({"error": "playlist_url is required"}), 400

    jobs.update_job(
        db,
        job_doc["job_id"],
        {
            "status": "queued",
            "error": None,
        },
    )

    threading.Thread(
        target=graph_service.run_enrich_job,
        args=(job_doc["job_id"], graph_id, playlist_url),
        daemon=True,
    ).start()

    return jsonify({"job_id": job_doc["job_id"], "graph_id": graph_id})


@bp.route("/api/playlist/status/<job_id>", methods=["GET"])
def playlist_job_status(job_id: str):
    db = get_db()
    jobs.ensure_jobs_collection(db)
    job = jobs.get_job(db, job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404
    return jsonify(job)


@bp.route("/api/playlists", methods=["GET"])
def list_playlists():
    db = get_db()
    jobs.ensure_jobs_collection(db)
    jobs_list = jobs.list_ready_jobs(db)
    playlists = [
        {
            "graph_id": job.get("graph_id"),
            "playlist_url": job.get("playlist_url"),
            "playlist_name": (job.get("playlist_name") or ""),
            "node_count": job.get("node_count"),
            "edge_count": job.get("edge_count"),
            "updated_at": job.get("updated_at"),
        }
        for job in jobs_list
    ]
    return jsonify({"playlists": playlists})


@bp.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy"})
