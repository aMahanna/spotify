"""Genius enrichment provider."""

from __future__ import annotations

from typing import List, Optional

import requests

from enrichment import http

GENIUS_API = "https://api.genius.com"


def search_song(query: str, token: str, session: requests.Session) -> Optional[int]:
    headers = {"Authorization": f"Bearer {token}"}
    data = http.safe_get_json(session, f"{GENIUS_API}/search", params={"q": query}, headers=headers) or {}
    hits = data.get("response", {}).get("hits", [])
    if not hits:
        return None
    return hits[0].get("result", {}).get("id")


def song(song_id: int, token: str, session: requests.Session) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    data = http.safe_get_json(session, f"{GENIUS_API}/songs/{song_id}", headers=headers) or {}
    return data.get("response", {}).get("song", {}) or {}


def referents(song_id: int, token: str, session: requests.Session) -> List[dict]:
    headers = {"Authorization": f"Bearer {token}"}
    params = {"song_id": song_id, "text_format": "plain"}
    data = http.safe_get_json(
        session,
        f"{GENIUS_API}/referents",
        params=params,
        headers=headers,
    ) or {}
    return data.get("response", {}).get("referents", []) or []
