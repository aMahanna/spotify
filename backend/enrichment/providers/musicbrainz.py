"""MusicBrainz and AcousticBrainz enrichment provider."""

from __future__ import annotations

from typing import Optional

import requests

from enrichment import http

MUSICBRAINZ_API = "https://musicbrainz.org/ws/2"
ACOUSTICBRAINZ_API = "https://acousticbrainz.org"


def search_artist(name: str, session: requests.Session) -> Optional[str]:
    params = {"query": f'artist:"{name}"', "fmt": "json", "limit": 1}
    data = http.safe_get_json(session, f"{MUSICBRAINZ_API}/artist", params=params) or {}
    artists = data.get("artists") or []
    if not artists:
        return None
    return artists[0].get("id")


def artist(mbid: str, session: requests.Session) -> dict:
    params = {"fmt": "json", "inc": "aliases+tags+genres+artist-rels"}
    return http.safe_get_json(session, f"{MUSICBRAINZ_API}/artist/{mbid}", params=params) or {}


def search_recording(track: str, artist_name: str, session: requests.Session) -> Optional[str]:
    query = f'recording:"{track}" AND artist:"{artist_name}"'
    params = {"query": query, "fmt": "json", "limit": 1}
    data = http.safe_get_json(session, f"{MUSICBRAINZ_API}/recording", params=params) or {}
    recordings = data.get("recordings") or []
    if not recordings:
        return None
    return recordings[0].get("id")


def recording(mbid: str, session: requests.Session) -> dict:
    params = {"fmt": "json", "inc": "artist-credits+artist-rels+work-rels+recording-rels"}
    return http.safe_get_json(session, f"{MUSICBRAINZ_API}/recording/{mbid}", params=params) or {}


def acousticbrainz_highlevel(mbid: str, session: requests.Session) -> dict:
    return http.safe_get_json(session, f"{ACOUSTICBRAINZ_API}/{mbid}/high-level") or {}
