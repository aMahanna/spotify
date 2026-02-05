"""HTTP helpers for enrichment providers."""

from __future__ import annotations

from typing import Optional

import requests

from config import settings


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(settings.DEFAULT_HEADERS)
    return session


def safe_get_json(
    session: requests.Session,
    url: str,
    *,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: int = settings.DEFAULT_TIMEOUT,
) -> Optional[dict]:
    response = session.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()
