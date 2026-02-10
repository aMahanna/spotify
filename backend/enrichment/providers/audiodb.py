"""TheAudioDB enrichment provider."""

from __future__ import annotations

import requests

from enrichment import http

AUDIODB_API = "https://www.theaudiodb.com/api/v1/json"


def artist(name: str, api_key: str, session: requests.Session) -> dict:
    data = http.safe_get_json(
        session,
        f"{AUDIODB_API}/{api_key}/search.php",
        params={"s": name},
    ) or {}
    artists = data.get("artists") or []
    return artists[0] if artists else {}


def track(artist_name: str, track_name: str, api_key: str, session: requests.Session) -> dict:
    data = http.safe_get_json(
        session,
        f"{AUDIODB_API}/{api_key}/searchtrack.php",
        params={"s": artist_name, "t": track_name},
    ) or {}
    tracks = data.get("track") or []
    return tracks[0] if tracks else {}
