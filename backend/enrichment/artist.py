"""Artist enrichment orchestration."""

from __future__ import annotations

import logging
import os
import time
from typing import List, Optional

from config import settings
from enrichment import http
from enrichment.providers import discogs, lastfm, musicbrainz, wikidata, audiodb
from utils import normalization
from utils import timing

logger = logging.getLogger(__name__)

WIKIPEDIA_SUMMARY_API = "https://en.wikipedia.org/api/rest_v1/page/summary/{}"


def _wikipedia_summary(name: str, session) -> dict:
    title = name.replace(" ", "_")
    response = session.get(WIKIPEDIA_SUMMARY_API.format(title), timeout=settings.DEFAULT_TIMEOUT)
    if response.status_code == 404:
        return {}
    response.raise_for_status()
    data = response.json() or {}
    summary = normalization.normalize_whitespace(data.get("extract", ""))
    if not summary:
        return {}
    return {
        "title": data.get("title", name),
        "summary": summary,
        "url": (data.get("content_urls") or {}).get("desktop", {}).get("page", ""),
    }


def enrich_artist(name: str, discogs_token: Optional[str] = None) -> dict:
    normalized = normalization.normalize_whitespace(name)
    if not normalized:
        return {
            "genres": [],
            "labels": [],
            "locations": [],
            "associated_acts": [],
            "stories": [],
            "genres_source": None,
            "labels_source": None,
            "locations_source": None,
            "associated_acts_source": None,
            "stories_source": None,
        }

    session = http.build_session()
    discogs_token = discogs_token or os.getenv(settings.DISCOGS_TOKEN)
    lastfm_key = os.getenv(settings.LASTFM_API_KEY)
    audiodb_key = os.getenv(settings.AUDIODB_API_KEY)

    genres: List[str] = []
    labels: List[str] = []
    locations: List[str] = []
    associated_acts: List[str] = []
    stories: List[dict] = []
    genre_sources: set[str] = set()
    label_sources: set[str] = set()
    location_sources: set[str] = set()
    acts_sources: set[str] = set()
    story_sources: set[str] = set()

    try:
        started = time.perf_counter()
        wikidata_payload = wikidata.enrich_wikidata(normalized, session)
        timing.record_timing("wikidata", time.perf_counter() - started)
        genres = normalization.unique(genres + wikidata_payload.get("genres", []))
        labels = normalization.unique(labels + wikidata_payload.get("labels", []))
        locations = normalization.unique(locations + wikidata_payload.get("locations", []))
        if wikidata_payload.get("genres"):
            genre_sources.add("wikidata")
        if wikidata_payload.get("labels"):
            label_sources.add("wikidata")
        if wikidata_payload.get("locations"):
            location_sources.add("wikidata")
    except Exception:
        logger.warning("Wikidata enrichment failed", extra={"artist": normalized})

    if discogs_token:
        try:
            started = time.perf_counter()
            discogs_payload = discogs.enrich_discogs(normalized, discogs_token, session)
            timing.record_timing("discogs", time.perf_counter() - started)
            associated_acts = normalization.unique(associated_acts + discogs_payload.get("associated_acts", []))
            if discogs_payload.get("associated_acts"):
                acts_sources.add("discogs")
        except Exception:
            logger.warning("Discogs enrichment failed", extra={"artist": normalized})

    if lastfm_key:
        try:
            started = time.perf_counter()
            tags = lastfm.artist_tags(normalized, lastfm_key, session)
            timing.record_timing("lastfm_artist_tags", time.perf_counter() - started)
            genres = normalization.unique(genres + tags)
            if tags:
                genre_sources.add("lastfm")
        except Exception:
            logger.warning("Last.fm enrichment failed", extra={"artist": normalized})

    if audiodb_key:
        try:
            started = time.perf_counter()
            audiodb_artist = audiodb.artist(normalized, audiodb_key, session)
            timing.record_timing("theaudiodb_artist", time.perf_counter() - started)
            genre = audiodb_artist.get("strGenre", "")
            style = audiodb_artist.get("strStyle", "")
            country = audiodb_artist.get("strCountry", "")
            genres = normalization.unique(genres + [genre, style])
            locations = normalization.unique(locations + [country])
            if genre or style:
                genre_sources.add("theaudiodb")
            if country:
                location_sources.add("theaudiodb")
        except Exception:
            logger.warning("AudioDB enrichment failed", extra={"artist": normalized})

    try:
        started = time.perf_counter()
        wiki_summary = _wikipedia_summary(normalized, session)
        timing.record_timing("wikipedia", time.perf_counter() - started)
        if wiki_summary:
            stories.append(
                {
                    "title": f"About {wiki_summary.get('title') or normalized}",
                    "body": wiki_summary.get("summary", ""),
                    "source": "wikipedia",
                    "source_url": wiki_summary.get("url", ""),
                    "tags": [],
                }
            )
            story_sources.add("wikipedia")
    except Exception:
        logger.warning("Wikipedia enrichment failed", extra={"artist": normalized})

    try:
        started = time.perf_counter()
        mbid = musicbrainz.search_artist(normalized, session)
        if mbid:
            mb_artist = musicbrainz.artist(mbid, session)
            timing.record_timing("musicbrainz_artist", time.perf_counter() - started)
            mb_genres = [item.get("name", "") for item in (mb_artist.get("genres") or [])]
            if not mb_genres:
                mb_genres = [item.get("name", "") for item in (mb_artist.get("tags") or [])]
            mb_location = (mb_artist.get("area") or {}).get("name", "")
            relations = mb_artist.get("relations") or []
            mb_acts = [
                rel.get("artist", {}).get("name", "")
                for rel in relations
                if rel.get("target-type") == "artist"
            ]
            genres = normalization.unique(genres + mb_genres)
            locations = normalization.unique(locations + [mb_location])
            associated_acts = normalization.unique(associated_acts + mb_acts)
            if mb_genres:
                genre_sources.add("musicbrainz")
            if mb_location:
                location_sources.add("musicbrainz")
            if mb_acts:
                acts_sources.add("musicbrainz")
        else:
            timing.record_timing("musicbrainz_artist_search", time.perf_counter() - started)
    except Exception:
        logger.warning("MusicBrainz enrichment failed", extra={"artist": normalized})

    return {
        "genres": genres,
        "labels": labels,
        "locations": locations,
        "associated_acts": associated_acts,
        "stories": stories,
        "genres_source": normalization.source_label(genre_sources),
        "labels_source": normalization.source_label(label_sources),
        "locations_source": normalization.source_label(location_sources),
        "associated_acts_source": normalization.source_label(acts_sources),
        "stories_source": normalization.source_label(story_sources),
    }
