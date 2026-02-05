from __future__ import annotations

import os
import threading
import time
from typing import Dict, List, Optional, Tuple

import requests

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY = "https://www.wikidata.org/wiki/Special:EntityData/{}.json"
DISCOGS_API = "https://api.discogs.com"
LASTFM_API = "https://ws.audioscrobbler.com/2.0/"
MUSICBRAINZ_API = "https://musicbrainz.org/ws/2"
GENIUS_API = "https://api.genius.com"
AUDIODB_API = "https://www.theaudiodb.com/api/v1/json"
ACOUSTICBRAINZ_API = "https://acousticbrainz.org"

DISCOGS_TOKEN_ENV = "DISCOGS_TOKEN"
LASTFM_API_KEY_ENV = "LASTFM_API_KEY"
LASTFM_SHARED_SECRET_ENV = "LASTFM_SHARED_SECRET"
GENIUS_ACCESS_TOKEN_ENV = "GENIUS_ACCESS_TOKEN"
AUDIODB_API_KEY_ENV = "THEAUDIODB_API_KEY"
DEFAULT_TIMEOUT = 10
DEFAULT_HEADERS = {
    "User-Agent": "txt2kg/1.0 (https://github.com)",
}

_timing_lock = threading.Lock()
_timings: Dict[str, float] = {}


def _record_timing(name: str, elapsed: float) -> None:
    with _timing_lock:
        _timings[name] = _timings.get(name, 0.0) + elapsed


def reset_timing_report() -> None:
    with _timing_lock:
        _timings.clear()


def get_timing_report() -> Dict[str, float]:
    with _timing_lock:
        return dict(_timings)


def _normalize_name(name: str) -> str:
    return " ".join((name or "").strip().split())


def _unique(values: List[str]) -> List[str]:
    seen = set()
    ordered = []
    for value in values:
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(value)
    return ordered


def _unique_contributors(contributors: List[dict]) -> List[dict]:
    seen = set()
    ordered = []
    for contributor in contributors:
        name = (contributor.get("name") or "").strip()
        role = (contributor.get("role") or "").strip()
        detail = (contributor.get("detail") or "").strip()
        if not name or not role:
            continue
        key = f"{name.lower()}|{role.lower()}|{detail.lower()}"
        if key in seen:
            continue
        seen.add(key)
        ordered.append(contributor)
    return ordered


def _normalize_role(role: str) -> str:
    return " ".join((role or "").strip().split()).lower()


def _normalize_relation(relation: str) -> str:
    return " ".join((relation or "").strip().split()).lower()


def _build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def _safe_get_json(
    session: requests.Session,
    url: str,
    *,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Optional[dict]:
    response = session.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _source_label(sources: set[str]) -> Optional[str]:
    if not sources:
        return None
    return "|".join(sorted(sources))


def _wikidata_search(name: str, session: requests.Session, limit: int = 5) -> List[str]:
    params = {
        "action": "wbsearchentities",
        "search": name,
        "language": "en",
        "format": "json",
        "type": "item",
        "limit": limit,
    }
    response = session.get(WIKIDATA_API, params=params, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    results = data.get("search") or []
    return [result.get("id") for result in results if result.get("id")]


def _wikidata_entity(entity_id: str, session: requests.Session) -> dict:
    response = session.get(WIKIDATA_ENTITY.format(entity_id), timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    return data.get("entities", {}).get(entity_id, {})


def _extract_ids(claims: dict, prop: str) -> List[str]:
    values = []
    for claim in claims.get(prop, []):
        mainsnak = claim.get("mainsnak", {})
        datavalue = mainsnak.get("datavalue", {})
        value = datavalue.get("value", {})
        if isinstance(value, dict):
            entity_id = value.get("id")
            if entity_id:
                values.append(entity_id)
    return values


def _has_instance_of(claims: dict, accepted_ids: set[str]) -> bool:
    instance_ids = set(_extract_ids(claims, "P31"))
    return bool(instance_ids & accepted_ids)


def _wikidata_labels(entity_ids: List[str], session: requests.Session) -> Dict[str, str]:
    if not entity_ids:
        return {}
    params = {
        "action": "wbgetentities",
        "ids": "|".join(entity_ids),
        "props": "labels",
        "languages": "en",
        "format": "json",
    }
    response = session.get(WIKIDATA_API, params=params, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    labels = {}
    for entity_id, entity in (data.get("entities") or {}).items():
        label = (entity.get("labels") or {}).get("en", {}).get("value")
        if label:
            labels[entity_id] = label
    return labels


def _enrich_wikidata(name: str, session: requests.Session) -> dict:
    candidate_queries = [
        name,
        f"{name} musician",
        f"{name} band",
        f"{name} singer",
    ]
    accepted_instance_ids = {
        "Q5",        # human
        "Q215380",   # musical group
        "Q639669",   # musical artist
        "Q177220",   # singer
        "Q488205",   # musician
    }

    entity = None
    for query in candidate_queries:
        candidate_ids = _wikidata_search(query, session)
        for candidate_id in candidate_ids:
            candidate = _wikidata_entity(candidate_id, session)
            claims = candidate.get("claims") or {}
            if _has_instance_of(claims, accepted_instance_ids):
                entity = candidate
                break
        if entity:
            break

    if not entity:
        return {"genres": [], "locations": [], "labels": []}

    claims = entity.get("claims") or {}

    genre_ids = _extract_ids(claims, "P136")
    label_ids = _extract_ids(claims, "P264")
    location_ids = _extract_ids(claims, "P19") + _extract_ids(claims, "P740")

    labels_map = _wikidata_labels(_unique(genre_ids + label_ids + location_ids), session)

    genres = [labels_map.get(entity_id, "") for entity_id in genre_ids]
    labels = [labels_map.get(entity_id, "") for entity_id in label_ids]
    locations = [labels_map.get(entity_id, "") for entity_id in location_ids]

    return {
        "genres": _unique(genres),
        "labels": _unique(labels),
        "locations": _unique(locations),
    }


def _lastfm_call(method: str, api_key: str, session: requests.Session, **params) -> dict:
    payload = {
        "method": method,
        "api_key": api_key,
        "format": "json",
    }
    payload.update(params)
    return _safe_get_json(session, LASTFM_API, params=payload) or {}


def _lastfm_artist_tags(name: str, api_key: str, session: requests.Session) -> List[str]:
    data = _lastfm_call("artist.getTopTags", api_key, session, artist=name, limit=10)
    tags = data.get("toptags", {}).get("tag", [])
    return _unique([tag.get("name", "") for tag in tags])


def _lastfm_track_tags(
    track: str,
    artist: str,
    api_key: str,
    session: requests.Session,
) -> List[str]:
    data = _lastfm_call(
        "track.getTopTags",
        api_key,
        session,
        track=track,
        artist=artist,
        limit=10,
    )
    tags = data.get("toptags", {}).get("tag", [])
    return _unique([tag.get("name", "") for tag in tags])


def _musicbrainz_search_artist(name: str, session: requests.Session) -> Optional[str]:
    params = {"query": f'artist:"{name}"', "fmt": "json", "limit": 1}
    data = _safe_get_json(session, f"{MUSICBRAINZ_API}/artist", params=params) or {}
    artists = data.get("artists") or []
    if not artists:
        return None
    return artists[0].get("id")


def _musicbrainz_artist(mbid: str, session: requests.Session) -> dict:
    params = {"fmt": "json", "inc": "aliases+tags+genres+artist-rels"}
    return _safe_get_json(session, f"{MUSICBRAINZ_API}/artist/{mbid}", params=params) or {}


def _musicbrainz_search_recording(
    track: str,
    artist: str,
    session: requests.Session,
) -> Optional[str]:
    query = f'recording:"{track}" AND artist:"{artist}"'
    params = {"query": query, "fmt": "json", "limit": 1}
    data = _safe_get_json(session, f"{MUSICBRAINZ_API}/recording", params=params) or {}
    recordings = data.get("recordings") or []
    if not recordings:
        return None
    return recordings[0].get("id")


def _musicbrainz_recording(mbid: str, session: requests.Session) -> dict:
    params = {"fmt": "json", "inc": "artist-credits+artist-rels+work-rels+recording-rels"}
    return _safe_get_json(session, f"{MUSICBRAINZ_API}/recording/{mbid}", params=params) or {}


def _genius_search_song(query: str, token: str, session: requests.Session) -> Optional[int]:
    headers = {"Authorization": f"Bearer {token}"}
    data = _safe_get_json(session, f"{GENIUS_API}/search", params={"q": query}, headers=headers) or {}
    hits = data.get("response", {}).get("hits", [])
    if not hits:
        return None
    return hits[0].get("result", {}).get("id")


def _genius_song(song_id: int, token: str, session: requests.Session) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    data = _safe_get_json(session, f"{GENIUS_API}/songs/{song_id}", headers=headers) or {}
    return data.get("response", {}).get("song", {}) or {}


def _genius_referents(song_id: int, token: str, session: requests.Session) -> List[dict]:
    headers = {"Authorization": f"Bearer {token}"}
    params = {"song_id": song_id, "text_format": "plain"}
    data = _safe_get_json(
        session,
        f"{GENIUS_API}/referents",
        params=params,
        headers=headers,
    ) or {}
    return data.get("response", {}).get("referents", []) or []


def _clean_annotation_text(text: str) -> str:
    cleaned = " ".join((text or "").strip().split())
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


def _audiodb_artist(name: str, api_key: str, session: requests.Session) -> dict:
    data = _safe_get_json(
        session,
        f"{AUDIODB_API}/{api_key}/search.php",
        params={"s": name},
    ) or {}
    artists = data.get("artists") or []
    return artists[0] if artists else {}


def _audiodb_track(artist: str, track: str, api_key: str, session: requests.Session) -> dict:
    data = _safe_get_json(
        session,
        f"{AUDIODB_API}/{api_key}/searchtrack.php",
        params={"s": artist, "t": track},
    ) or {}
    tracks = data.get("track") or []
    return tracks[0] if tracks else {}


def _acousticbrainz_highlevel(mbid: str, session: requests.Session) -> dict:
    return _safe_get_json(session, f"{ACOUSTICBRAINZ_API}/{mbid}/high-level") or {}


def _discogs_headers() -> dict:
    return DEFAULT_HEADERS


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
        timeout=DEFAULT_TIMEOUT,
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
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def _enrich_discogs(name: str, token: str, session: requests.Session) -> dict:
    resource_url = _discogs_search_artist(name, token, session)
    if not resource_url:
        return {"associated_acts": []}
    payload = _discogs_artist(resource_url, token, session)
    groups = [item.get("name", "") for item in (payload.get("groups") or [])]
    members = [item.get("name", "") for item in (payload.get("members") or [])]
    associated = _unique(groups + members)
    normalized = _normalize_name(name).lower()
    associated = [act for act in associated if act.lower() != normalized]
    return {"associated_acts": associated}


def enrich_artist(name: str, discogs_token: Optional[str] = None) -> dict:
    normalized = _normalize_name(name)
    if not normalized:
        return {
            "genres": [],
            "labels": [],
            "locations": [],
            "associated_acts": [],
            "genres_source": None,
            "labels_source": None,
            "locations_source": None,
            "associated_acts_source": None,
        }

    session = _build_session()
    discogs_token = discogs_token or os.getenv(DISCOGS_TOKEN_ENV)
    lastfm_key = os.getenv(LASTFM_API_KEY_ENV)
    audiodb_key = os.getenv(AUDIODB_API_KEY_ENV)

    genres: List[str] = []
    labels: List[str] = []
    locations: List[str] = []
    associated_acts: List[str] = []
    genre_sources: set[str] = set()
    label_sources: set[str] = set()
    location_sources: set[str] = set()
    acts_sources: set[str] = set()

    try:
        started = time.perf_counter()
        wikidata = _enrich_wikidata(normalized, session)
        _record_timing("wikidata", time.perf_counter() - started)
        genres = _unique(genres + wikidata.get("genres", []))
        labels = _unique(labels + wikidata.get("labels", []))
        locations = _unique(locations + wikidata.get("locations", []))
        if wikidata.get("genres"):
            genre_sources.add("wikidata")
        if wikidata.get("labels"):
            label_sources.add("wikidata")
        if wikidata.get("locations"):
            location_sources.add("wikidata")
    except Exception:
        pass

    if discogs_token:
        try:
            started = time.perf_counter()
            discogs = _enrich_discogs(normalized, discogs_token, session)
            _record_timing("discogs", time.perf_counter() - started)
            associated_acts = _unique(associated_acts + discogs.get("associated_acts", []))
            if discogs.get("associated_acts"):
                acts_sources.add("discogs")
        except Exception:
            pass

    if lastfm_key:
        try:
            started = time.perf_counter()
            tags = _lastfm_artist_tags(normalized, lastfm_key, session)
            _record_timing("lastfm_artist_tags", time.perf_counter() - started)
            genres = _unique(genres + tags)
            if tags:
                genre_sources.add("lastfm")
        except Exception:
            pass

    if audiodb_key:
        try:
            started = time.perf_counter()
            audiodb_artist = _audiodb_artist(normalized, audiodb_key, session)
            _record_timing("theaudiodb_artist", time.perf_counter() - started)
            genre = audiodb_artist.get("strGenre", "")
            style = audiodb_artist.get("strStyle", "")
            country = audiodb_artist.get("strCountry", "")
            genres = _unique(genres + [genre, style])
            locations = _unique(locations + [country])
            if genre or style:
                genre_sources.add("theaudiodb")
            if country:
                location_sources.add("theaudiodb")
        except Exception:
            pass

    try:
        started = time.perf_counter()
        mbid = _musicbrainz_search_artist(normalized, session)
        if mbid:
            mb_artist = _musicbrainz_artist(mbid, session)
            _record_timing("musicbrainz_artist", time.perf_counter() - started)
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
            genres = _unique(genres + mb_genres)
            locations = _unique(locations + [mb_location])
            associated_acts = _unique(associated_acts + mb_acts)
            if mb_genres:
                genre_sources.add("musicbrainz")
            if mb_location:
                location_sources.add("musicbrainz")
            if mb_acts:
                acts_sources.add("musicbrainz")
        else:
            _record_timing("musicbrainz_artist_search", time.perf_counter() - started)
    except Exception:
        pass

    return {
        "genres": genres,
        "labels": labels,
        "locations": locations,
        "associated_acts": associated_acts,
        "genres_source": _source_label(genre_sources),
        "labels_source": _source_label(label_sources),
        "locations_source": _source_label(location_sources),
        "associated_acts_source": _source_label(acts_sources),
    }


def enrich_song(
    track: str,
    artist: str,
    *,
    album: Optional[str] = None,
) -> dict:
    normalized_track = _normalize_name(track)
    normalized_artist = _normalize_name(artist)
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

    session = _build_session()
    genius_token = os.getenv(GENIUS_ACCESS_TOKEN_ENV)
    lastfm_key = os.getenv(LASTFM_API_KEY_ENV)
    audiodb_key = os.getenv(AUDIODB_API_KEY_ENV)

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
        mbid = _musicbrainz_search_recording(normalized_track, normalized_artist, session)
        if mbid:
            recording = _musicbrainz_recording(mbid, session)
            _record_timing("musicbrainz_recording", time.perf_counter() - started)
            relations = recording.get("relations") or []
            for rel in relations:
                rel_type = _normalize_relation(rel.get("type") or "")
                target_type = _normalize_relation(rel.get("target-type") or "")
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
            _record_timing("musicbrainz_recording_search", time.perf_counter() - started)
    except Exception:
        pass

    # Moods are temporarily disabled.
    # if mbid:
    #     try:
    #         started = time.perf_counter()
    #         acoustic = _acousticbrainz_highlevel(mbid, session)
    #         _record_timing("acousticbrainz_highlevel", time.perf_counter() - started)
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
    #         tags = _lastfm_track_tags(normalized_track, normalized_artist, lastfm_key, session)
    #         _record_timing("lastfm_track_tags", time.perf_counter() - started)
    #         moods = _unique(moods + tags)
    #         if tags:
    #             mood_sources.add("lastfm")
    #     except Exception:
    #         pass

    if genius_token:
        try:
            started = time.perf_counter()
            query = f"{normalized_track} {normalized_artist}"
            song_id = _genius_search_song(query, genius_token, session)
            song = _genius_song(song_id, genius_token, session) if song_id else {}
            _record_timing("genius", time.perf_counter() - started)
            if song:
                writers = _unique(
                    writers + [artist.get("name", "") for artist in song.get("writer_artists", [])]
                )
                producers = _unique(
                    producers + [artist.get("name", "") for artist in song.get("producer_artists", [])]
                )
                featured_artists = _unique(
                    featured_artists
                    + [artist.get("name", "") for artist in song.get("featured_artists", [])]
                )
                for artist in song.get("writer_artists", []) or []:
                    name = artist.get("name", "")
                    if name:
                        contributors.append({"name": name, "role": "writer", "source": "genius"})
                        contributor_sources.add("genius")
                for artist in song.get("producer_artists", []) or []:
                    name = artist.get("name", "")
                    if name:
                        contributors.append({"name": name, "role": "producer", "source": "genius"})
                        contributor_sources.add("genius")
                for artist in song.get("featured_artists", []) or []:
                    name = artist.get("name", "")
                    if name:
                        contributors.append({"name": name, "role": "featured", "source": "genius"})
                        contributor_sources.add("genius")
                if song.get("writer_artists"):
                    writer_sources.add("genius")
                if song.get("producer_artists"):
                    producer_sources.add("genius")
                if song.get("featured_artists"):
                    feature_sources.add("genius")
                description = (song.get("description") or {}).get("plain", "").strip()
                if not description:
                    full_title = (song.get("full_title") or song.get("title_with_featured") or "").strip()
                    release_date = (song.get("release_date_for_display") or "").strip()
                    album_name = ((song.get("album") or {}).get("name") or "").strip()
                    primary_artist = ((song.get("primary_artist") or {}).get("name") or "").strip()
                    annotation_count = song.get("annotation_count")
                    summary_bits = []
                    if full_title:
                        summary_bits.append(full_title)
                    elif song.get("title"):
                        summary_bits.append(str(song.get("title")))
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
                    for tag in song.get("tags", []) or []:
                        if isinstance(tag, dict):
                            tags.append(tag.get("name", ""))
                        else:
                            tags.append(str(tag))
                    story = {
                        "title": f"About {track}",
                        "body": description.strip(),
                        "source": "genius",
                        "source_url": song.get("url"),
                        "tags": tags,
                    }
                    stories.append(story)
                    story_sources.add("genius")

                referents = _genius_referents(song_id, genius_token, session) if song_id else []
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
            pass

    if audiodb_key:
        try:
            started = time.perf_counter()
            audiodb_track = _audiodb_track(normalized_artist, normalized_track, audiodb_key, session)
            _record_timing("theaudiodb_track", time.perf_counter() - started)
            # mood = audiodb_track.get("strMood", "")
            language = audiodb_track.get("strLanguage", "")
            # if mood:
            #     moods = _unique(moods + [mood])
            #     mood_sources.add("theaudiodb")
            if language:
                languages = _unique(languages + [language])
                language_sources.add("theaudiodb")
        except Exception:
            pass

    writers = _unique(writers)
    producers = _unique(producers)
    featured_artists = _unique(featured_artists)
    # moods = _unique(moods)
    instruments = _unique(instruments)
    languages = _unique(languages)
    contributors = _unique_contributors(contributors)

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
        "writers_source": _source_label(writer_sources),
        "producers_source": _source_label(producer_sources),
        "featured_artists_source": _source_label(feature_sources),
        # "moods_source": _source_label(mood_sources),
        "instruments_source": _source_label(instrument_sources),
        "languages_source": _source_label(language_sources),
        "contributors_source": _source_label(contributor_sources),
        "songdna_relations_source": _source_label(songdna_sources),
        "stories_source": _source_label(story_sources),
    }
