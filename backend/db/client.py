"""ArangoDB client helpers."""

from __future__ import annotations

from arango import ArangoClient

from config import settings


def get_db():
    return ArangoClient(hosts=settings.DB_URL).db(
        settings.DB_NAME,
        username=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )


def ensure_collection(db, name: str, *, edge: bool = False) -> None:
    if not db.has_collection(name):
        db.create_collection(name, edge=edge)
