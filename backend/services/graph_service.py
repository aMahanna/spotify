"""Graph service orchestration."""

from __future__ import annotations

import logging

from db.client import get_db
from playlist import build_and_upload_graph, enrich_graph
from services import jobs

logger = logging.getLogger(__name__)


def build_graph_documents(graph_id: str | None = None):
    db = get_db()
    jobs.ensure_jobs_collection(db)

    job_doc = jobs.resolve_collection_map(db, graph_id)
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


def run_build_job(job_id: str, graph_id: str, playlist_url: str) -> None:
    job_db = get_db()
    try:
        jobs.update_job(job_db, job_id, {"status": "running"})
        node_count, edge_count, collection_map = build_and_upload_graph(
            playlist_url, graph_id, reset=True
        )
        jobs.update_job(
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
        logger.exception("Build job failed", extra={"job_id": job_id, "graph_id": graph_id})
        jobs.update_job(job_db, job_id, {"status": "failed", "error": str(exc)})


def run_enrich_job(job_id: str, graph_id: str, playlist_url: str) -> None:
    job_db = get_db()
    try:
        jobs.update_job(job_db, job_id, {"status": "running"})
        node_count, edge_count, collection_map = enrich_graph(playlist_url, graph_id)
        jobs.update_job(
            job_db,
            job_id,
            {
                "status": "ready",
                "node_count": node_count,
                "edge_count": edge_count,
                "collection_map": collection_map,
                "playlist_name": collection_map.get("playlist_name"),
                "enriched_at": jobs.utc_now(),
            },
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Enrichment job failed", extra={"job_id": job_id, "graph_id": graph_id})
        jobs.update_job(job_db, job_id, {"status": "failed", "error": str(exc)})
