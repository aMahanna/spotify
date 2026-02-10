"""ArangoDB client helpers."""

from __future__ import annotations

from arango import ArangoClient

from config import settings


def get_db():
    return ArangoClient(hosts=settings.ARANGO_DB_URL).db(
        settings.ARANGO_DB_NAME,
        username=settings.ARANGO_USER,
        password=settings.ARANGO_PASS,
    )


def ensure_collection(db, name: str, *, edge: bool = False) -> None:
    if not db.has_collection(name):
        db.create_collection(name, edge=edge)
