"""Minimal Python backend that serves a knowledge graph from ArangoDB."""

from __future__ import annotations

from app import create_app


if __name__ == "__main__":
    app = create_app()
    print("Starting Knowledge Graph Backend on http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
