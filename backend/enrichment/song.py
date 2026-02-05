"""Song enrichment orchestration."""

from __future__ import annotations

import logging
import os
import time
from typing import List, Optional

from config import settings
from enrichment import http
from enrichment.providers import audiodb, genius, lastfm, musicbrainz
from utils import normalization
from utils import timing

logger = logging.getLogger(__name__)


def _clean_annotation_text(text: str) -> str:
    cleaned = normalization.normalize_whitespace(text)
    if not cleaned:
        return ""
    lowered = cleaned.lower()
    if lowered.startswith("proposed suggestion"):
        return ""
    # Filter out very low-signal annotations.
    if len(cleaned) < 80:
        return ""
    if len(cleaned.split()) < 12:
        return ""
    return cleaned


def enrich_song(
    track: str,
    artist: str,
    *,
    album: Optional[str] = None,
) -> dict:
    normalized_track = normalization.normalize_whitespace(track)
    normalized_artist = normalization.normalize_whitespace(artist)
    if not normalized_track or not normalized_artist:
        return {
            "writers": [],
            "producers": [],
            "featured_artists": [],
            # "moods": [],
            "instruments": [],
            "languages": [],
            "contributors": [],
            "songdna_relations": [],
            "stories": [],
            "writers_source": None,
            "producers_source": None,
            "featured_artists_source": None,
            # "moods_source": None,
            "instruments_source": None,
            "languages_source": None,
            "contributors_source": None,
            "songdna_relations_source": None,
            "stories_source": None,
        }

    session = http.build_session()
    genius_token = os.getenv(settings.GENIUS_ACCESS_TOKEN_ENV)
    lastfm_key = os.getenv(settings.LASTFM_API_KEY_ENV)
    audiodb_key = os.getenv(settings.AUDIODB_API_KEY_ENV)

    writers: List[str] = []
    producers: List[str] = []
    featured_artists: List[str] = []
    # moods: List[str] = []
    instruments: List[str] = []
    languages: List[str] = []
    contributors: List[dict] = []
    songdna_relations: List[dict] = []
    stories: List[dict] = []
    writer_sources: set[str] = set()
    producer_sources: set[str] = set()
    feature_sources: set[str] = set()
    # mood_sources: set[str] = set()
    instrument_sources: set[str] = set()
    language_sources: set[str] = set()
    contributor_sources: set[str] = set()
    songdna_sources: set[str] = set()
    story_sources: set[str] = set()

    mbid: Optional[str] = None
    try:
        started = time.perf_counter()
        mbid = musicbrainz.search_recording(normalized_track, normalized_artist, session)
        if mbid:
            recording = musicbrainz.recording(mbid, session)
            timing.record_timing("musicbrainz_recording", time.perf_counter() - started)
            relations = recording.get("relations") or []
            for rel in relations:
                rel_type = normalization.normalize_relation(rel.get("type") or "")
                target_type = normalization.normalize_relation(rel.get("target-type") or "")
                target_artist = rel.get("artist", {}).get("name", "")
                if rel_type in {"writer", "composer", "lyricist"}:
                    if target_artist:
                        contributors.append(
                            {"name": target_artist, "role": rel_type, "source": "musicbrainz"}
                        )
                        contributor_sources.add("musicbrainz")
                        writers.append(target_artist)
                    writer_sources.add("musicbrainz")
                if rel_type in {"producer", "recording producer"}:
                    if target_artist:
                        contributors.append(
                            {"name": target_artist, "role": "producer", "source": "musicbrainz"}
                        )
                        contributor_sources.add("musicbrainz")
                        producers.append(target_artist)
                    producer_sources.add("musicbrainz")
                if rel_type == "instrument":
                    instruments_list = rel.get("attributes") or []
                    instruments.extend(instruments_list)
                    instrument_sources.add("musicbrainz")
                    if target_artist:
                        if instruments_list:
                            for instrument in instruments_list:
                                contributors.append(
                                    {
                                        "name": target_artist,
                                        "role": "instrument",
                                        "detail": instrument,
                                        "source": "musicbrainz",
                                    }
                                )
                        else:
                            contributors.append(
                                {"name": target_artist, "role": "instrument", "source": "musicbrainz"}
                            )
                        contributor_sources.add("musicbrainz")
                if target_type == "artist" and target_artist:
                    if rel_type in {
                        "engineer",
                        "mix",
                        "mastering",
                        "remixer",
                        "arranger",
                        "conductor",
                        "vocal",
                        "vocals",
                    }:
                        role_label = "mixer" if rel_type == "mix" else rel_type
                        if rel_type in {"vocal", "vocals"}:
                            role_label = "vocalist"
                        contributors.append(
                            {"name": target_artist, "role": role_label, "source": "musicbrainz"}
                        )
                        contributor_sources.add("musicbrainz")
                if target_type in {"recording", "work"}:
                    if rel_type in {
                        "dj-mix",
                        "samples",
                        "sampled by",
                        "cover",
                        "covered by",
                        "performance",
                        "remix of",
                        "remixed by",
                        "based on",
                        "medley",
                        "quotes",
                        "mash-up",
                        "performance of",
                    }:
                        target = rel.get(target_type) or {}
                        title = target.get("title") or target.get("name") or ""
                        relation_artist = (target.get("artist") or {}).get("name", "")
                        songdna_relations.append(
                            {
                                "relation": rel_type,
                                "title": title,
                                "artist": relation_artist,
                                "target_type": target_type,
                                "source": "musicbrainz",
                            }
                        )
                        songdna_sources.add("musicbrainz")
            credits = recording.get("artist-credit") or []
            for credit in credits:
                joinphrase = (credit.get("joinphrase") or "").lower()
                if "feat" in joinphrase:
                    artist_name = (credit.get("artist") or {}).get("name", "")
                    if artist_name:
                        featured_artists.append(artist_name)
                        feature_sources.add("musicbrainz")
                        contributors.append(
                            {"name": artist_name, "role": "featured", "source": "musicbrainz"}
                        )
                        contributor_sources.add("musicbrainz")
        else:
            timing.record_timing("musicbrainz_recording_search", time.perf_counter() - started)
    except Exception:
        logger.warning(
            "MusicBrainz recording enrichment failed",
            extra={"track": normalized_track, "artist": normalized_artist},
        )

    # Moods are temporarily disabled.
    # if mbid:
    #     try:
    #         started = time.perf_counter()
    #         acoustic = musicbrainz.acousticbrainz_highlevel(mbid, session)
    #         timing.record_timing("acousticbrainz_highlevel", time.perf_counter() - started)
    #         highlevel = acoustic.get("highlevel") or {}
    #         for key, value in highlevel.items():
    #             if not key.startswith("mood_"):
    #                 continue
    #             mood_value = value.get("value")
    #             if mood_value:
    #                 moods.append(mood_value)
    #         if moods:
    #             mood_sources.add("acousticbrainz")
    #     except Exception:
    #         pass
    #
    # if lastfm_key:
    #     try:
    #         started = time.perf_counter()
    #         tags = lastfm.track_tags(normalized_track, normalized_artist, lastfm_key, session)
    #         timing.record_timing("lastfm_track_tags", time.perf_counter() - started)
    #         moods = normalization.unique(moods + tags)
    #         if tags:
    #             mood_sources.add("lastfm")
    #     except Exception:
    #         pass

    if genius_token:
        try:
            started = time.perf_counter()
            query = f"{normalized_track} {normalized_artist}"
            song_id = genius.search_song(query, genius_token, session)
            song_payload = genius.song(song_id, genius_token, session) if song_id else {}
            timing.record_timing("genius", time.perf_counter() - started)
            if song_payload:
                writers = normalization.unique(
                    writers + [artist_item.get("name", "") for artist_item in song_payload.get("writer_artists", [])]
                )
                producers = normalization.unique(
                    producers + [artist_item.get("name", "") for artist_item in song_payload.get("producer_artists", [])]
                )
                featured_artists = normalization.unique(
                    featured_artists
                    + [artist_item.get("name", "") for artist_item in song_payload.get("featured_artists", [])]
                )
                for artist_item in song_payload.get("writer_artists", []) or []:
                    name = artist_item.get("name", "")
                    if name:
                        contributors.append({"name": name, "role": "writer", "source": "genius"})
                        contributor_sources.add("genius")
                for artist_item in song_payload.get("producer_artists", []) or []:
                    name = artist_item.get("name", "")
                    if name:
                        contributors.append({"name": name, "role": "producer", "source": "genius"})
                        contributor_sources.add("genius")
                for artist_item in song_payload.get("featured_artists", []) or []:
                    name = artist_item.get("name", "")
                    if name:
                        contributors.append({"name": name, "role": "featured", "source": "genius"})
                        contributor_sources.add("genius")
                if song_payload.get("writer_artists"):
                    writer_sources.add("genius")
                if song_payload.get("producer_artists"):
                    producer_sources.add("genius")
                if song_payload.get("featured_artists"):
                    feature_sources.add("genius")
                description = (song_payload.get("description") or {}).get("plain", "").strip()
                if not description:
                    full_title = (
                        song_payload.get("full_title") or song_payload.get("title_with_featured") or ""
                    ).strip()
                    release_date = (song_payload.get("release_date_for_display") or "").strip()
                    album_name = ((song_payload.get("album") or {}).get("name") or "").strip()
                    primary_artist = ((song_payload.get("primary_artist") or {}).get("name") or "").strip()
                    annotation_count = song_payload.get("annotation_count")
                    summary_bits = []
                    if full_title:
                        summary_bits.append(full_title)
                    elif song_payload.get("title"):
                        summary_bits.append(str(song_payload.get("title")))
                    if primary_artist and primary_artist not in (summary_bits[0] if summary_bits else ""):
                        summary_bits.append(f"by {primary_artist}")
                    if release_date:
                        summary_bits.append(f"released {release_date}")
                    if album_name:
                        summary_bits.append(f"appears on {album_name}")
                    if isinstance(annotation_count, int) and annotation_count > 0:
                        summary_bits.append(f"Genius annotations: {annotation_count}")
                    if summary_bits:
                        description = ". ".join(summary_bits).strip() + "."

                if description:
                    tags = []
                    for tag in song_payload.get("tags", []) or []:
                        if isinstance(tag, dict):
                            tags.append(tag.get("name", ""))
                        else:
                            tags.append(str(tag))
                    story = {
                        "title": f"About {track}",
                        "body": description.strip(),
                        "source": "genius",
                        "source_url": song_payload.get("url"),
                        "tags": tags,
                    }
                    stories.append(story)
                    story_sources.add("genius")

                referents = genius.referents(song_id, genius_token, session) if song_id else []
                for ref in referents:
                    fragment = (ref.get("fragment") or "").strip()
                    annotations = ref.get("annotations") or []
                    for annotation in annotations:
                        body = (annotation.get("body") or {}).get("plain", "")
                        cleaned = _clean_annotation_text(body)
                        if not cleaned:
                            continue
                        title = f"Annotation: {fragment}" if fragment else f"Annotation: {track}"
                        story = {
                            "title": title,
                            "body": cleaned,
                            "source": "genius",
                            "source_url": annotation.get("url") or ref.get("url"),
                            "tags": ["genius_annotation"],
                        }
                        stories.append(story)
                        story_sources.add("genius")
                        break
        except Exception:
            logger.warning(
                "Genius enrichment failed",
                extra={"track": normalized_track, "artist": normalized_artist},
            )

    if audiodb_key:
        try:
            started = time.perf_counter()
            audiodb_track = audiodb.track(normalized_artist, normalized_track, audiodb_key, session)
            timing.record_timing("theaudiodb_track", time.perf_counter() - started)
            # mood = audiodb_track.get("strMood", "")
            language = audiodb_track.get("strLanguage", "")
            # if mood:
            #     moods = normalization.unique(moods + [mood])
            #     mood_sources.add("theaudiodb")
            if language:
                languages = normalization.unique(languages + [language])
                language_sources.add("theaudiodb")
        except Exception:
            logger.warning(
                "AudioDB track enrichment failed",
                extra={"track": normalized_track, "artist": normalized_artist},
            )

    writers = normalization.unique(writers)
    producers = normalization.unique(producers)
    featured_artists = normalization.unique(featured_artists)
    # moods = normalization.unique(moods)
    instruments = normalization.unique(instruments)
    languages = normalization.unique(languages)
    contributors = normalization.unique_contributors(contributors)

    return {
        "writers": writers,
        "producers": producers,
        "featured_artists": featured_artists,
        # "moods": moods,
        "instruments": instruments,
        "languages": languages,
        "contributors": contributors,
        "songdna_relations": songdna_relations,
        "stories": stories,
        "writers_source": normalization.source_label(writer_sources),
        "producers_source": normalization.source_label(producer_sources),
        "featured_artists_source": normalization.source_label(feature_sources),
        # "moods_source": normalization.source_label(mood_sources),
        "instruments_source": normalization.source_label(instrument_sources),
        "languages_source": normalization.source_label(language_sources),
        "contributors_source": normalization.source_label(contributor_sources),
        "songdna_relations_source": normalization.source_label(songdna_sources),
        "stories_source": normalization.source_label(story_sources),
    }
