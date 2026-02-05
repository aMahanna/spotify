from __future__ import annotations

import os
from typing import Dict, List, Optional

import requests

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY = "https://www.wikidata.org/wiki/Special:EntityData/{}.json"
DISCOGS_API = "https://api.discogs.com"

DISCOGS_TOKEN_ENV = "DISCOGS_TOKEN"
DEFAULT_TIMEOUT = 10
DEFAULT_HEADERS = {
    "User-Agent": "txt2kg/1.0 (https://github.com)",
}


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


def _build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


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

    wikidata = {"genres": [], "labels": [], "locations": []}
    discogs = {"associated_acts": []}

    try:
        wikidata = _enrich_wikidata(normalized, session)
    except Exception:
        pass

    if discogs_token:
        try:
            discogs = _enrich_discogs(normalized, discogs_token, session)
        except Exception:
            pass

    return {
        "genres": wikidata.get("genres", []),
        "labels": wikidata.get("labels", []),
        "locations": wikidata.get("locations", []),
        "associated_acts": discogs.get("associated_acts", []),
        "genres_source": "wikidata" if wikidata.get("genres") else None,
        "labels_source": "wikidata" if wikidata.get("labels") else None,
        "locations_source": "wikidata" if wikidata.get("locations") else None,
        "associated_acts_source": "discogs" if discogs.get("associated_acts") else None,
    }
