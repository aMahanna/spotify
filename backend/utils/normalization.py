"""String normalization helpers."""

from __future__ import annotations

import re
from typing import List, Optional


def normalize_whitespace(value: str) -> str:
    return " ".join((value or "").strip().split())


def normalize_name_lower(value: str) -> str:
    return normalize_whitespace(value).lower()


def strip_feat(value: str) -> str:
    return re.split(r"\s+(feat\.?|featuring|ft\.?)\s+", value, maxsplit=1, flags=re.IGNORECASE)[0]


def artist_variants(value: str) -> set[str]:
    base = normalize_name_lower(value)
    if not base:
        return set()
    variants = {base}
    stripped_feat = normalize_name_lower(strip_feat(base))
    if stripped_feat:
        variants.add(stripped_feat)
    no_punct = normalize_name_lower(re.sub(r"[^\w\s]", "", base))
    if no_punct:
        variants.add(no_punct)
    no_the = normalize_name_lower(re.sub(r"^the\s+", "", base))
    if no_the:
        variants.add(no_the)
    if "&" in base:
        variants.add(normalize_name_lower(base.replace("&", "and")))
    if " and " in base:
        variants.add(normalize_name_lower(base.replace(" and ", " & ")))
    return variants


def split_artist_names(value: str) -> List[str]:
    if not value:
        return []
    parts = [part.strip() for part in value.split(",")]
    return [part for part in parts if part]


def unique(values: List[str]) -> List[str]:
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


def unique_contributors(contributors: List[dict]) -> List[dict]:
    seen = set()
    ordered = []
    for contributor in contributors:
        name = normalize_whitespace(contributor.get("name") or "")
        role = normalize_whitespace(contributor.get("role") or "")
        detail = normalize_whitespace(contributor.get("detail") or "")
        if not name or not role:
            continue
        key = f"{name.lower()}|{role.lower()}|{detail.lower()}"
        if key in seen:
            continue
        seen.add(key)
        ordered.append(contributor)
    return ordered


def normalize_role(role: str) -> str:
    return normalize_whitespace(role).lower()


def normalize_relation(relation: str) -> str:
    return normalize_whitespace(relation).lower()


def source_label(sources: set[str]) -> Optional[str]:
    if not sources:
        return None
    return "|".join(sorted(sources))
