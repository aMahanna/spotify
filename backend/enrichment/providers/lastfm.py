"""Last.fm enrichment provider."""

from __future__ import annotations

from typing import List

import requests

from enrichment import http
from utils import normalization

LASTFM_API = "https://ws.audioscrobbler.com/2.0/"


def _lastfm_call(method: str, api_key: str, session: requests.Session, **params) -> dict:
    payload = {
        "method": method,
        "api_key": api_key,
        "format": "json",
    }
    payload.update(params)
    return http.safe_get_json(session, LASTFM_API, params=payload) or {}


def artist_tags(name: str, api_key: str, session: requests.Session) -> List[str]:
    data = _lastfm_call("artist.getTopTags", api_key, session, artist=name, limit=10)
    tags = data.get("toptags", {}).get("tag", [])
    return normalization.unique([tag.get("name", "") for tag in tags])


def track_tags(track: str, artist: str, api_key: str, session: requests.Session) -> List[str]:
    data = _lastfm_call(
        "track.getTopTags",
        api_key,
        session,
        track=track,
        artist=artist,
        limit=10,
    )
    tags = data.get("toptags", {}).get("tag", [])
    return normalization.unique([tag.get("name", "") for tag in tags])
