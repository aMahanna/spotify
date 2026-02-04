"""
Minimal Python backend that generates a sample knowledge graph.
"""

from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Sample knowledge graph triples
SAMPLE_TRIPLES = [
    {"subject": "Albert Einstein", "predicate": "developed", "object": "Theory of Relativity"},
    {"subject": "Albert Einstein", "predicate": "won", "object": "Nobel Prize in Physics"},
    {"subject": "Albert Einstein", "predicate": "born in", "object": "Germany"},
    {"subject": "Albert Einstein", "predicate": "worked at", "object": "Princeton University"},
    {"subject": "Theory of Relativity", "predicate": "describes", "object": "Spacetime"},
    {"subject": "Theory of Relativity", "predicate": "includes", "object": "E=mc²"},
    {"subject": "Nobel Prize in Physics", "predicate": "awarded for", "object": "Photoelectric Effect"},
    {"subject": "Photoelectric Effect", "predicate": "is a", "object": "Quantum Phenomenon"},
    {"subject": "Princeton University", "predicate": "located in", "object": "New Jersey"},
    {"subject": "Princeton University", "predicate": "is a", "object": "Ivy League University"},
    {"subject": "Germany", "predicate": "is in", "object": "Europe"},
    {"subject": "Europe", "predicate": "is a", "object": "Continent"},
    {"subject": "Spacetime", "predicate": "has", "object": "Four Dimensions"},
    {"subject": "E=mc²", "predicate": "relates", "object": "Mass and Energy"},
    {"subject": "Quantum Phenomenon", "predicate": "part of", "object": "Quantum Mechanics"},
    {"subject": "Quantum Mechanics", "predicate": "developed by", "object": "Max Planck"},
    {"subject": "Max Planck", "predicate": "won", "object": "Nobel Prize in Physics"},
    {"subject": "Max Planck", "predicate": "born in", "object": "Germany"},
    {"subject": "Ivy League University", "predicate": "located in", "object": "United States"},
    {"subject": "United States", "predicate": "is in", "object": "North America"},
    {"subject": "North America", "predicate": "is a", "object": "Continent"},
    {"subject": "New Jersey", "predicate": "is in", "object": "United States"},
    {"subject": "Mass and Energy", "predicate": "related to", "object": "Physics"},
    {"subject": "Physics", "predicate": "is a", "object": "Natural Science"},
    {"subject": "Natural Science", "predicate": "studies", "object": "Nature"},
]


@app.route("/api/graph", methods=["GET"])
def get_graph():
    """Return sample knowledge graph triples."""
    return jsonify({"triples": SAMPLE_TRIPLES})


@app.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy"})


if __name__ == "__main__":
    print("Starting Knowledge Graph Backend on http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
