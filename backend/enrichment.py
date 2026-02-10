"""Compatibility wrapper for enrichment modules."""

from __future__ import annotations

from enrichment.artist import enrich_artist
from enrichment.song import enrich_song
from utils.timing import get_timing_report, reset_timing_report

__all__ = ["enrich_artist", "enrich_song", "get_timing_report", "reset_timing_report"]
