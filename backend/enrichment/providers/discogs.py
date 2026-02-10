"""Discogs enrichment provider."""

from __future__ import annotations

from typing import Optional

import requests

from config import settings
from utils import normalization

DISCOGS_API = "https://api.discogs.com"


def _discogs_headers() -> dict:
    return settings.DEFAULT_HEADERS


def _discogs_search_artist(name: str, token: str, session: requests.Session) -> Optional[str]:
    params = {
        "q": name,
        "type": "artist",
        "per_page": 1,
        "page": 1,
        "token": token,
    }
    response = session.get(
        f"{DISCOGS_API}/database/search",
        params=params,
        headers=_discogs_headers(),
        timeout=settings.DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()
    results = data.get("results") or []
    if not results:
        return None
    return results[0].get("resource_url")


def _discogs_artist(resource_url: str, token: str, session: requests.Session) -> dict:
    response = session.get(
        resource_url,
        params={"token": token},
        headers=_discogs_headers(),
        timeout=settings.DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def enrich_discogs(name: str, token: str, session: requests.Session) -> dict:
    resource_url = _discogs_search_artist(name, token, session)
    if not resource_url:
        return {"associated_acts": []}
    payload = _discogs_artist(resource_url, token, session)
    groups = [item.get("name", "") for item in (payload.get("groups") or [])]
    members = [item.get("name", "") for item in (payload.get("members") or [])]
    associated = normalization.unique(groups + members)
    normalized = normalization.normalize_whitespace(name).lower()
    associated = [act for act in associated if act.lower() != normalized]
    return {"associated_acts": associated}
