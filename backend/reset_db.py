from typing import Iterable, List

from arango import ArangoClient

from playlist import DB_NAME, DB_PASSWORD, GRAPH_JOBS_COLLECTION, GRAPH_NAME_PREFIX


def _get_db():
    return ArangoClient().db(DB_NAME, password=DB_PASSWORD)


def _graph_names(db) -> List[str]:
    return [graph.get("name", "") for graph in db.graphs()]


def _delete_collections(db, names: Iterable[str]) -> None:
    for name in names:
        if name:
            db.delete_collection(name, ignore_missing=True)


def reset_db() -> None:
    db = _get_db()

    # Drop graphs created by this app (and their collections).
    for graph_name in _graph_names(db):
        if graph_name.startswith(f"{GRAPH_NAME_PREFIX}_"):
            db.delete_graph(graph_name, drop_collections=True, ignore_missing=True)

    graph_ids: List[str] = []
    collections_to_drop: List[str] = []

    if db.has_collection(GRAPH_JOBS_COLLECTION):
        jobs = list(db.collection(GRAPH_JOBS_COLLECTION).all())
        for job in jobs:
            graph_id = job.get("graph_id")
            if graph_id:
                graph_ids.append(graph_id)
            collection_map = job.get("collection_map") or {}
            for group in ("nodes", "edges"):
                for collection in (collection_map.get(group) or {}).values():
                    collections_to_drop.append(collection)

        _delete_collections(db, collections_to_drop)
        db.delete_collection(GRAPH_JOBS_COLLECTION, ignore_missing=True)

    if graph_ids:
        for collection in db.collections():
            name = collection.get("name", "")
            if any(name.startswith(f"{graph_id}_") for graph_id in graph_ids):
                db.delete_collection(name, ignore_missing=True)

    print("Reset complete: removed graphs, collections, and job metadata.")


if __name__ == "__main__":
    reset_db()
