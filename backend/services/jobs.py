"""Job service for playlist graph builds."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from config import settings


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_now() -> str:
    return _utc_now()


def ensure_jobs_collection(db) -> None:
    if not db.has_collection(settings.GRAPH_JOBS_COLLECTION):
        db.create_collection(settings.GRAPH_JOBS_COLLECTION)


def build_job(db, playlist_url: str) -> dict:
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
    db.collection(settings.GRAPH_JOBS_COLLECTION).insert(job_doc, overwrite=True)
    return job_doc


def update_job(db, job_id: str, fields: dict) -> None:
    fields["updated_at"] = _utc_now()
    db.collection(settings.GRAPH_JOBS_COLLECTION).update({"_key": job_id, **fields})


def get_job(db, job_id: str) -> dict | None:
    if not db.has_collection(settings.GRAPH_JOBS_COLLECTION):
        return None
    return db.collection(settings.GRAPH_JOBS_COLLECTION).get(job_id)


def find_job_by_playlist_url(db, playlist_url: str) -> dict | None:
    if not db.has_collection(settings.GRAPH_JOBS_COLLECTION):
        return None
    jobs = db.collection(settings.GRAPH_JOBS_COLLECTION).find({"playlist_url": playlist_url}, limit=1)
    return next(jobs, None)


def find_job_by_graph_id(db, graph_id: str) -> dict | None:
    if not db.has_collection(settings.GRAPH_JOBS_COLLECTION):
        return None
    jobs = db.collection(settings.GRAPH_JOBS_COLLECTION).find({"graph_id": graph_id}, limit=1)
    return next(jobs, None)


def resolve_collection_map(db, graph_id: str | None) -> dict | None:
    collection = db.collection(settings.GRAPH_JOBS_COLLECTION)
    if graph_id:
        job = collection.find({"graph_id": graph_id}, limit=1)
        return next(job, None)

    jobs = list(collection.find({"status": "ready"}))
    if not jobs:
        return None
    jobs.sort(key=lambda job: job.get("updated_at") or "", reverse=True)
    return jobs[0]


def list_ready_jobs(db) -> list[dict]:
    collection = db.collection(settings.GRAPH_JOBS_COLLECTION)
    jobs = list(collection.find({"status": "ready"}))
    jobs.sort(key=lambda job: job.get("updated_at") or "", reverse=True)
    return jobs
