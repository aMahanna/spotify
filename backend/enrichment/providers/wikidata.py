"""Wikidata enrichment provider."""

from __future__ import annotations

from typing import Dict, List

import requests

from config import settings
from utils import normalization

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY = "https://www.wikidata.org/wiki/Special:EntityData/{}.json"


def _wikidata_search(name: str, session: requests.Session, limit: int = 5) -> List[str]:
    params = {
        "action": "wbsearchentities",
        "search": name,
        "language": "en",
        "format": "json",
        "type": "item",
        "limit": limit,
    }
    response = session.get(WIKIDATA_API, params=params, timeout=settings.DEFAULT_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    results = data.get("search") or []
    return [result.get("id") for result in results if result.get("id")]


def _wikidata_entity(entity_id: str, session: requests.Session) -> dict:
    response = session.get(WIKIDATA_ENTITY.format(entity_id), timeout=settings.DEFAULT_TIMEOUT)
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
    response = session.get(WIKIDATA_API, params=params, timeout=settings.DEFAULT_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    labels = {}
    for entity_id, entity in (data.get("entities") or {}).items():
        label = (entity.get("labels") or {}).get("en", {}).get("value")
        if label:
            labels[entity_id] = label
    return labels


def enrich_wikidata(name: str, session: requests.Session) -> dict:
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

    labels_map = _wikidata_labels(normalization.unique(genre_ids + label_ids + location_ids), session)

    genres = [labels_map.get(entity_id, "") for entity_id in genre_ids]
    labels = [labels_map.get(entity_id, "") for entity_id in label_ids]
    locations = [labels_map.get(entity_id, "") for entity_id in location_ids]

    return {
        "genres": normalization.unique(genres),
        "labels": normalization.unique(labels),
        "locations": normalization.unique(locations),
    }
