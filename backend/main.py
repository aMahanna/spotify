"""
Minimal Python backend that generates a sample knowledge graph.
"""

from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

NODE_COLLECTION = "nodes"
EDGE_COLLECTION = "edges"

SAMPLE_RELATIONSHIPS = [
    {"source": "Albert Einstein", "label": "developed", "target": "Theory of Relativity"},
    {"source": "Albert Einstein", "label": "won", "target": "Nobel Prize in Physics"},
    {"source": "Albert Einstein", "label": "born in", "target": "Germany"},
    {"source": "Albert Einstein", "label": "worked at", "target": "Princeton University"},
    {"source": "Theory of Relativity", "label": "describes", "target": "Spacetime"},
    {"source": "Theory of Relativity", "label": "includes", "target": "E=mc²"},
    {"source": "Nobel Prize in Physics", "label": "awarded for", "target": "Photoelectric Effect"},
    {"source": "Photoelectric Effect", "label": "is a", "target": "Quantum Phenomenon"},
    {"source": "Princeton University", "label": "located in", "target": "New Jersey"},
    {"source": "Princeton University", "label": "is a", "target": "Ivy League University"},
    {"source": "Germany", "label": "is in", "target": "Europe"},
    {"source": "Europe", "label": "is a", "target": "Continent"},
    {"source": "Spacetime", "label": "has", "target": "Four Dimensions"},
    {"source": "E=mc²", "label": "relates", "target": "Mass and Energy"},
    {"source": "Quantum Phenomenon", "label": "part of", "target": "Quantum Mechanics"},
    {"source": "Quantum Mechanics", "label": "developed by", "target": "Max Planck"},
    {"source": "Max Planck", "label": "won", "target": "Nobel Prize in Physics"},
    {"source": "Max Planck", "label": "born in", "target": "Germany"},
    {"source": "Ivy League University", "label": "located in", "target": "United States"},
    {"source": "United States", "label": "is in", "target": "North America"},
    {"source": "North America", "label": "is a", "target": "Continent"},
    {"source": "New Jersey", "label": "is in", "target": "United States"},
    {"source": "Mass and Energy", "label": "related to", "target": "Physics"},
    {"source": "Physics", "label": "is a", "target": "Natural Science"},
    {"source": "Natural Science", "label": "studies", "target": "Nature"},
]


def _normalize_key(value: str) -> str:
    value = "".join(ch if ch.isalnum() or ch in "_-:" else "_" for ch in value.strip().lower())
    value = value.strip("_")
    return value or "node"


def build_graph_documents(relationships):
    nodes_by_name = {}
    key_counts = {}

    def get_node_key(name: str) -> str:
        if name in nodes_by_name:
            return nodes_by_name[name]["_key"]

        base_key = _normalize_key(name)
        count = key_counts.get(base_key, 0)
        unique_key = base_key if count == 0 else f"{base_key}_{count}"
        key_counts[base_key] = count + 1

        nodes_by_name[name] = {
            "_key": unique_key,
            "_id": f"{NODE_COLLECTION}/{unique_key}",
            "name": name,
            "type": "entity",
        }

        return unique_key

    edges = []
    for idx, rel in enumerate(relationships):
        source_key = get_node_key(rel["source"])
        target_key = get_node_key(rel["target"])
        edge_key = f"e{idx + 1}"
        edges.append(
            {
                "_key": edge_key,
                "_id": f"{EDGE_COLLECTION}/{edge_key}",
                "_from": f"{NODE_COLLECTION}/{source_key}",
                "_to": f"{NODE_COLLECTION}/{target_key}",
                "label": rel["label"],
                "type": "relationship",
            }
        )

    return list(nodes_by_name.values()), edges


@app.route("/api/graph", methods=["GET"])
def get_graph():
    """Return sample knowledge graph as node/edge documents."""
    nodes, edges = build_graph_documents(SAMPLE_RELATIONSHIPS)
    return jsonify({"nodes": nodes, "edges": edges})


@app.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy"})


if __name__ == "__main__":
    print("Starting Knowledge Graph Backend on http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
