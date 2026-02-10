"""Flask API routes for the knowledge graph."""

from __future__ import annotations

import json
import os
import random
import threading

from flask import Blueprint, jsonify, request, Response, stream_with_context
from openai import OpenAI

from db.client import get_db
from graph import schema as graph_schema
from services import graph_service, jobs

bp = Blueprint("api", __name__)
CHAT_GRAPH_CAP_BYTES = 100 * 1024
CHAT_SNIPPET_CHAR_LIMIT = 280
CHAT_SAMPLE_LIMIT = 24
QUESTION_DEFS = {
    "themes": {
        "label": "What are the themes around this playlist?",
        "focus": "Summarize recurring genres, moods, topics, and artistic themes.",
    },
    "collabs": {
        "label": "Which artists have worked together?",
        "focus": "Highlight artist collaborations, shared tracks, or direct relationships.",
    },
    "fun_facts": {
        "label": "What is a fun fact about this playlist?",
        "focus": "Share exactly one interesting, short fact from the graph (stat, rare item, or notable node).",
    },
    "tour": {
        "label": "Give me a tour",
        "focus": "Provide a brief guided tour of the most central nodes in the graph.",
    },
    "selection_summary": {
        "label": "Summarize this selected cluster of nodes and edges.",
        "focus": "Summarize the selected subgraph, noting dominant entities, relationships, and notable patterns.",
    },
}

KNOWN_NODE_TYPES = {
    graph_schema.ARTISTS_COLLECTION,
    graph_schema.SONGS_COLLECTION,
    graph_schema.ALBUMS_COLLECTION,
    graph_schema.LABELS_COLLECTION,
    graph_schema.PLAYLISTS_COLLECTION,
    graph_schema.GENRES_COLLECTION,
    graph_schema.LOCATIONS_COLLECTION,
    graph_schema.MOODS_COLLECTION,
    graph_schema.INSTRUMENTS_COLLECTION,
    graph_schema.LANGUAGES_COLLECTION,
}
KNOWN_EDGE_TYPES = {
    graph_schema.ARTISTS_SONGS,
    graph_schema.ARTISTS_ALBUMS,
    graph_schema.SONGS_ALBUMS,
    graph_schema.ALBUMS_LABELS,
    graph_schema.ARTISTS_GENRES,
    graph_schema.ARTISTS_LOCATIONS,
    graph_schema.ARTISTS_LABELS,
    graph_schema.ARTISTS_ACTS,
    graph_schema.SONGS_WRITERS,
    graph_schema.SONGS_PRODUCERS,
    graph_schema.SONGS_FEATURES,
    graph_schema.SONGS_MOODS,
    graph_schema.SONGS_INSTRUMENTS,
    graph_schema.SONGS_LANGUAGES,
    graph_schema.SONGS_CONTRIBUTORS,
}


def _truncate_text(text: str | None, limit: int) -> str:
    if not text:
        return ""
    trimmed = text.strip()
    if len(trimmed) <= limit:
        return trimmed
    return f"{trimmed[: max(0, limit - 3)].rstrip()}..."


def _infer_collection_name(value: str | None) -> str:
    if not value:
        return ""
    base = str(value).split("/", 1)[0]
    candidates = sorted(KNOWN_NODE_TYPES | KNOWN_EDGE_TYPES, key=len, reverse=True)
    for name in candidates:
        if base == name or base.endswith(f"_{name}"):
            return name
    return ""


def _is_artist_type(node_type: str) -> bool:
    return node_type in {"artist", "artists"}


def _node_type(node: dict) -> str:
    explicit = str(node.get("type") or node.get("group") or "").lower()
    if explicit:
        return explicit
    inferred = _infer_collection_name(_node_id(node))
    return inferred.lower() if inferred else ""


def _node_id(node: dict) -> str:
    return str(
        node.get("id")
        or node.get("_id")
        or node.get("_key")
        or node.get("name")
        or node.get("label")
        or ""
    )


def _node_name(node: dict) -> str:
    return str(node.get("name") or node.get("label") or _node_id(node))


def _edge_label(edge: dict) -> str:
    label = edge.get("label") or edge.get("name") or edge.get("predicate")
    if label:
        return str(label)
    inferred = _infer_collection_name(str(edge.get("_id") or edge.get("id") or ""))
    return inferred or "related_to"


def _extract_story_snippets(nodes: list[dict], limit: int) -> list[dict]:
    snippets = []
    for node in nodes:
        stories = node.get("stories")
        if not isinstance(stories, list):
            continue
        for story in stories:
            if not isinstance(story, dict):
                continue
            title = _truncate_text(str(story.get("title") or ""), 120)
            body = _truncate_text(str(story.get("body") or ""), CHAT_SNIPPET_CHAR_LIMIT)
            if not title and not body:
                continue
            snippets.append(
                {
                    "node": _node_name(node),
                    "title": title,
                    "body": body,
                    "source": story.get("source"),
                }
            )
            if len(snippets) >= limit:
                return snippets
    return snippets


def _build_themes_context(nodes: list[dict], edges: list[dict]) -> dict:
    type_counts: dict[str, int] = {}
    for node in nodes:
        node_type = _node_type(node) or "unknown"
        type_counts[node_type] = type_counts.get(node_type, 0) + 1
    top_types = sorted(type_counts.items(), key=lambda item: item[1], reverse=True)[:10]

    genres = [_node_name(node) for node in nodes if "genre" in _node_type(node)]
    moods = [_node_name(node) for node in nodes if "mood" in _node_type(node)]
    songs = [_node_name(node) for node in nodes if "song" in _node_type(node)]
    artists = [_node_name(node) for node in nodes if "artist" in _node_type(node)]

    return {
        "payload_mode": "themes",
        "counts": {
            "nodes": len(nodes),
            "edges": len(edges),
        },
        "top_node_types": [{"type": node_type, "count": count} for node_type, count in top_types],
        "sample_genres": genres[:CHAT_SAMPLE_LIMIT],
        "sample_moods": moods[:CHAT_SAMPLE_LIMIT],
        "sample_songs": songs[:CHAT_SAMPLE_LIMIT],
        "sample_artists": artists[:CHAT_SAMPLE_LIMIT],
        "annotation_snippets": _extract_story_snippets(nodes, limit=CHAT_SAMPLE_LIMIT),
    }


def _build_collabs_context(nodes: list[dict], edges: list[dict]) -> dict:
    artist_ids = {_node_id(node) for node in nodes if _is_artist_type(_node_type(node))}
    artist_lookup = {_node_id(node): _node_name(node) for node in nodes if _is_artist_type(_node_type(node))}

    pair_counts: dict[tuple[str, str], int] = {}
    label_counts: dict[str, int] = {}

    song_to_artists: dict[str, set[str]] = {}
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        source = str(edge.get("_from") or edge.get("source") or "")
        target = str(edge.get("_to") or edge.get("target") or "")
        label = _edge_label(edge).lower()
        if not artist_ids:
            if _infer_collection_name(source) == graph_schema.ARTISTS_COLLECTION:
                artist_ids.add(source)
            if _infer_collection_name(target) == graph_schema.ARTISTS_COLLECTION:
                artist_ids.add(target)
        if source in artist_ids and target in artist_ids and source != target:
            pair = tuple(sorted([source, target]))
            pair_counts[pair] = pair_counts.get(pair, 0) + 1
            label_counts[label or "related_to"] = label_counts.get(label or "related_to", 0) + 1
            continue
        if "song" in label or "track" in label or "perform" in label or "artist" in label or "feat" in label:
            if source in artist_ids and target:
                song_to_artists.setdefault(target, set()).add(source)
            if target in artist_ids and source:
                song_to_artists.setdefault(source, set()).add(target)

    for artists in song_to_artists.values():
        artists_list = sorted(artists)
        if len(artists_list) < 2:
            continue
        for i, first in enumerate(artists_list):
            for second in artists_list[i + 1 :]:
                pair = (first, second)
                pair_counts[pair] = pair_counts.get(pair, 0) + 1

    top_pairs = sorted(pair_counts.items(), key=lambda item: item[1], reverse=True)[:CHAT_SAMPLE_LIMIT]
    pairs_payload = [
        {
            "artist_a": artist_lookup.get(pair[0], pair[0]),
            "artist_b": artist_lookup.get(pair[1], pair[1]),
            "count": count,
        }
        for pair, count in top_pairs
    ]

    top_labels = sorted(label_counts.items(), key=lambda item: item[1], reverse=True)[:10]

    return {
        "payload_mode": "collabs",
        "artist_pairs": pairs_payload,
        "top_edge_labels": [{"label": label or "related_to", "count": count} for label, count in top_labels],
    }


def _parse_year(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value if 1000 <= value <= 2100 else None
    text = str(value)
    for token in text.replace("-", " ").split():
        if token.isdigit() and 1000 <= int(token) <= 2100:
            return int(token)
    return None


def _build_fun_facts_context(nodes: list[dict], edges: list[dict]) -> dict:
    node_lookup = {_node_id(node): _node_name(node) for node in nodes}
    degree: dict[str, int] = {}
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        source = str(edge.get("_from") or edge.get("source") or "")
        target = str(edge.get("_to") or edge.get("target") or "")
        if source:
            degree[source] = degree.get(source, 0) + 1
        if target:
            degree[target] = degree.get(target, 0) + 1

    most_connected = None
    if degree:
        most_connected = max(degree.items(), key=lambda item: item[1])

    genres = [_node_name(node) for node in nodes if "genre" in _node_type(node)]
    genre_counts: dict[str, int] = {}
    for genre in genres:
        genre_counts[genre] = genre_counts.get(genre, 0) + 1
    rare_genre = None
    if genre_counts:
        rare_genre = min(genre_counts.items(), key=lambda item: item[1])

    year_nodes = []
    for node in nodes:
        year = _parse_year(node.get("year") or node.get("release_year") or node.get("release_date"))
        if year:
            year_nodes.append((year, _node_name(node)))
    oldest = min(year_nodes, key=lambda item: item[0]) if year_nodes else None
    newest = max(year_nodes, key=lambda item: item[0]) if year_nodes else None

    sample_nodes = random.sample(nodes, min(CHAT_SAMPLE_LIMIT, len(nodes))) if nodes else []
    sample_edges = random.sample(edges, min(CHAT_SAMPLE_LIMIT, len(edges))) if edges else []

    facts = []
    if most_connected:
        facts.append(
            {
                "fact": f"Most connected node: {node_lookup.get(most_connected[0], most_connected[0])}",
                "count": most_connected[1],
            }
        )
    if rare_genre:
        facts.append({"fact": f"Rare genre appears: {rare_genre[0]}", "count": rare_genre[1]})
    if oldest:
        facts.append({"fact": f"Oldest release year spotted: {oldest[0]}", "node": oldest[1]})
    if newest:
        facts.append({"fact": f"Newest release year spotted: {newest[0]}", "node": newest[1]})

    return {
        "payload_mode": "fun_facts",
        "facts": facts,
        "sample_nodes": _compact_nodes(sample_nodes),
        "sample_edges": _compact_edges(sample_edges),
    }


def _build_tour_context(
    nodes: list[dict],
    edges: list[dict],
    tour_order: list[str] | None = None,
    max_nodes: int = 12,
) -> dict:
    node_lookup = {_node_id(node): node for node in nodes}
    name_lookup = {_node_id(node): _node_name(node) for node in nodes}

    adjacency: dict[str, set[str]] = {node_id: set() for node_id in node_lookup.keys()}
    degree: dict[str, int] = {node_id: 0 for node_id in node_lookup.keys()}

    for edge in edges:
        if not isinstance(edge, dict):
            continue
        source = str(edge.get("_from") or edge.get("source") or "")
        target = str(edge.get("_to") or edge.get("target") or "")
        if not source or not target:
            continue
        if source not in adjacency:
            adjacency[source] = set()
            name_lookup.setdefault(source, source)
        if target not in adjacency:
            adjacency[target] = set()
            name_lookup.setdefault(target, target)
        adjacency[source].add(target)
        adjacency[target].add(source)
        degree[source] = degree.get(source, 0) + 1
        degree[target] = degree.get(target, 0) + 1

    ranked_nodes = sorted(degree.items(), key=lambda item: item[1], reverse=True)
    selected_nodes = [node_id for node_id, _count in ranked_nodes][:max_nodes]

    if tour_order:
        ordered_nodes = [node_id for node_id in tour_order if node_id in adjacency]
        if ordered_nodes:
            selected_nodes = ordered_nodes

    tour_nodes = []
    for node_id in selected_nodes:
        neighbors = sorted(adjacency.get(node_id, set()))
        neighbor_names = [name_lookup.get(neighbor, neighbor) for neighbor in neighbors[:4]]
        node = node_lookup.get(node_id, {})
        tour_nodes.append(
            {
                "id": node_id,
                "name": name_lookup.get(node_id, node_id),
                "type": _node_type(node),
                "degree": degree.get(node_id, 0),
                "neighbors": neighbor_names,
            }
        )

    return {
        "payload_mode": "tour",
        "counts": {"nodes": len(nodes), "edges": len(edges)},
        "tour_nodes": tour_nodes,
    }


def _compact_nodes(nodes: list[dict]) -> list[dict]:
    compacted = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = node.get("id") or node.get("_id") or node.get("_key") or node.get("name") or node.get("label")
        node_name = node.get("name") or node.get("label") or node_id
        node_type = node.get("type") or node.get("group") or _infer_collection_name(node_id)
        compacted.append(
            {
                "id": str(node_id) if node_id is not None else "",
                "name": str(node_name) if node_name is not None else "",
                "type": str(node_type) if node_type is not None else "",
            }
        )
    return compacted


def _compact_edges(edges: list[dict]) -> list[dict]:
    compacted = []
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        source = edge.get("_from") or edge.get("source")
        target = edge.get("_to") or edge.get("target")
        label = _edge_label(edge)
        compacted.append(
            {
                "source": str(source) if source is not None else "",
                "target": str(target) if target is not None else "",
                "label": str(label) if label is not None else "",
            }
        )
    return compacted


def _compact_triples(triples: list[dict]) -> list[dict]:
    compacted = []
    for triple in triples:
        if not isinstance(triple, dict):
            continue
        compacted.append(
            {
                "subject": str(triple.get("subject") or ""),
                "predicate": str(triple.get("predicate") or ""),
                "object": str(triple.get("object") or ""),
            }
        )
    return compacted


def _summarize_graph(nodes: list[dict], edges: list[dict], triples: list[dict]) -> dict:
    type_counts: dict[str, int] = {}
    for node in nodes:
        node_type = str(node.get("type") or node.get("group") or "unknown")
        type_counts[node_type] = type_counts.get(node_type, 0) + 1

    label_counts: dict[str, int] = {}
    for edge in edges:
        label = _edge_label(edge)
        label_counts[label] = label_counts.get(label, 0) + 1

    top_types = sorted(type_counts.items(), key=lambda item: item[1], reverse=True)[:12]
    top_labels = sorted(label_counts.items(), key=lambda item: item[1], reverse=True)[:12]

    return {
        "payload_mode": "summary",
        "counts": {
            "nodes": len(nodes),
            "edges": len(edges),
            "triples": len(triples),
        },
        "top_node_types": [{"type": node_type, "count": count} for node_type, count in top_types],
        "top_edge_labels": [{"label": label, "count": count} for label, count in top_labels],
        "sample_nodes": nodes[:20],
        "sample_edges": edges[:20],
        "sample_triples": triples[:20],
    }


def _build_selection_summary_context(nodes: list[dict], edges: list[dict]) -> dict:
    compact_nodes = _compact_nodes(nodes)
    compact_edges = _compact_edges(edges)
    summary = _summarize_graph(compact_nodes, compact_edges, [])
    return {
        **summary,
        "payload_mode": "selection_summary",
        "sample_node_names": [_node_name(node) for node in nodes][:CHAT_SAMPLE_LIMIT],
        "sample_edge_labels": [_edge_label(edge) for edge in edges][:CHAT_SAMPLE_LIMIT],
    }


def _inflate_from_triples(triples: list[dict]) -> tuple[list[dict], list[dict]]:
    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    for triple in triples:
        if not isinstance(triple, dict):
            continue
        subject = str(triple.get("subject") or "")
        predicate = str(triple.get("predicate") or "")
        obj = str(triple.get("object") or "")
        if subject and subject not in nodes:
            nodes[subject] = {"id": subject, "name": subject}
        if obj and obj not in nodes:
            nodes[obj] = {"id": obj, "name": obj}
        if subject and obj:
            edges.append({"source": subject, "target": obj, "label": predicate})
    return list(nodes.values()), edges


@bp.route("/api/graph", methods=["GET"])
def get_graph():
    """Return knowledge graph as node/edge documents."""
    graph_id = request.args.get("graph_id")
    nodes, edges = graph_service.build_graph_documents(graph_id)
    return jsonify({"nodes": nodes, "edges": edges})


@bp.route("/api/playlist/build", methods=["POST"])
def build_playlist_graph():
    payload = request.get_json(silent=True) or {}
    playlist_url = payload.get("playlist_url")
    if not playlist_url:
        return jsonify({"error": "playlist_url is required"}), 400

    db = get_db()
    jobs.ensure_jobs_collection(db)
    existing_job = jobs.find_job_by_playlist_url(db, playlist_url)
    if existing_job:
        job_doc = {
            "job_id": existing_job["job_id"],
            "graph_id": existing_job["graph_id"],
        }
        jobs.update_job(
            db,
            existing_job["job_id"],
            {
                "status": "queued",
                "error": None,
                "node_count": None,
                "edge_count": None,
                "collection_map": None,
                "playlist_name": None,
            },
        )
    else:
        job_doc = jobs.build_job(db, playlist_url)

    threading.Thread(
        target=graph_service.run_build_job,
        args=(job_doc["job_id"], job_doc["graph_id"], playlist_url),
        daemon=True,
    ).start()

    return jsonify({"job_id": job_doc["job_id"], "graph_id": job_doc["graph_id"]})


@bp.route("/api/playlist/enrich", methods=["POST"])
def enrich_playlist_graph():
    payload = request.get_json(silent=True) or {}
    graph_id = payload.get("graph_id")
    if not graph_id:
        return jsonify({"error": "graph_id is required"}), 400

    db = get_db()
    jobs.ensure_jobs_collection(db)
    job_doc = jobs.find_job_by_graph_id(db, graph_id)
    if not job_doc:
        return jsonify({"error": "graph_id not found"}), 404

    playlist_url = payload.get("playlist_url") or job_doc.get("playlist_url")
    if not playlist_url:
        return jsonify({"error": "playlist_url is required"}), 400

    jobs.update_job(
        db,
        job_doc["job_id"],
        {
            "status": "queued",
            "error": None,
        },
    )

    threading.Thread(
        target=graph_service.run_enrich_job,
        args=(job_doc["job_id"], graph_id, playlist_url),
        daemon=True,
    ).start()

    return jsonify({"job_id": job_doc["job_id"], "graph_id": graph_id})


@bp.route("/api/playlist/status/<job_id>", methods=["GET"])
def playlist_job_status(job_id: str):
    db = get_db()
    jobs.ensure_jobs_collection(db)
    job = jobs.get_job(db, job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404
    return jsonify(job)


@bp.route("/api/playlists", methods=["GET"])
def list_playlists():
    db = get_db()
    jobs.ensure_jobs_collection(db)
    jobs_list = jobs.list_ready_jobs(db)
    playlists = [
        {
            "graph_id": job.get("graph_id"),
            "playlist_url": job.get("playlist_url"),
            "playlist_name": (job.get("playlist_name") or ""),
            "node_count": job.get("node_count"),
            "edge_count": job.get("edge_count"),
            "updated_at": job.get("updated_at"),
        }
        for job in jobs_list
    ]
    return jsonify({"playlists": playlists})


@bp.route("/api/chat/stream", methods=["POST"])
def chat_stream():
    payload = request.get_json(silent=True) or {}
    question_id = str(payload.get("question_id") or "").strip()
    nodes = payload.get("nodes") or []
    edges = payload.get("edges") or []
    triples = payload.get("triples") or []
    tour_order = payload.get("tour_order") or []

    if question_id not in QUESTION_DEFS:
        return jsonify(
            {"error": "question_id must be one of: themes, collabs, fun_facts, tour, selection_summary"}
        ), 400
    if not isinstance(nodes, list) or not isinstance(edges, list) or not isinstance(triples, list):
        return jsonify({"error": "nodes, edges, and triples must be arrays"}), 400
    if not isinstance(tour_order, list):
        return jsonify({"error": "tour_order must be an array"}), 400
    if not os.getenv("OPENAI_API_KEY"):
        return jsonify({"error": "OPENAI_API_KEY is not configured"}), 500
    if not nodes and not edges and triples:
        nodes, edges = _inflate_from_triples(triples)

    def generate():
        question_def = QUESTION_DEFS[question_id]
        system_prompt = (
            "You are an expert music analyst helping users understand a playlist "
            "knowledge graph. Use the provided graph data to answer questions. "
            "If the graph data is incomplete, add a brief caveat at the end (one short sentence) "
            "and still answer. Keep the response to 2 to 3 short sentences max. "
            "Avoid bullet lists unless explicitly asked. Do not preface answers with filler phrases. "
            "The graph payload is tailored to the requested question."
        )
        if question_id == "tour":
            system_prompt = (
                "You are an expert music analyst guiding a user through a playlist knowledge graph. "
                "Use the provided tour nodes to narrate a guided tour. "
                "Keep each step to one short sentence."
            )
        if question_id == "selection_summary":
            system_prompt = (
                "You are an expert music analyst summarizing a selected cluster in a playlist knowledge graph. "
                "Be concise: 2 to 3 short sentences max. "
                "Avoid bullet lists and avoid prefacing with filler phrases."
            )

        if question_id == "themes":
            graph_context = _build_themes_context(nodes, edges)
        elif question_id == "collabs":
            graph_context = _build_collabs_context(nodes, edges)
        elif question_id == "fun_facts":
            graph_context = _build_fun_facts_context(nodes, edges)
        elif question_id == "selection_summary":
            graph_context = _build_selection_summary_context(nodes, edges)
        else:
            graph_context = _build_tour_context(nodes, edges, tour_order=tour_order)

        graph_payload = json.dumps(graph_context, ensure_ascii=True)
        payload_size = len(graph_payload.encode("utf-8"))

        if payload_size > CHAT_GRAPH_CAP_BYTES:
            if question_id == "tour":
                graph_context = _build_tour_context(nodes, edges, tour_order=tour_order, max_nodes=8)
                graph_payload = json.dumps(graph_context, ensure_ascii=True)
            else:
                compact_nodes = _compact_nodes(nodes)
                compact_edges = _compact_edges(edges)
                compact_triples = _compact_triples(triples) if not (nodes or edges) else []
                graph_context = _summarize_graph(compact_nodes, compact_edges, compact_triples)
                graph_payload = json.dumps(graph_context, ensure_ascii=True)

        if question_id == "tour":
            user_prompt = (
                f"Question: {question_def['label']}\n"
                f"Focus: {question_def['focus']}\n"
                "Response format: Numbered list, one short sentence per node. "
                "Start each line with the node name. "
                "No extra intro or outro.\n"
                f"Context:\n{graph_payload}"
            )
        else:
            user_prompt = (
                f"Question: {question_def['label']}\n"
                f"Focus: {question_def['focus']}\n"
                "Response format: 1 short paragraph, 2 to 3 short sentences, no bullet lists.\n"
                "Avoid prefacing with phrases like \"A fun fact from the graph\" or similar.\n"
                f"Context:\n{graph_payload}"
            )
        if question_id == "selection_summary":
            user_prompt = (
                f"Question: {question_def['label']}\n"
                f"Focus: {question_def['focus']}\n"
                "Response format: 2 to 3 short sentences, no bullet lists, no extra intro or outro.\n"
                "Keep it concise and specific to the selected cluster.\n"
                f"Context:\n{graph_payload}"
            )

        temperature = 0.4
        if question_id == "fun_facts":
            temperature = 0.7
        if question_id == "tour":
            temperature = 0.5

        try:
            client = OpenAI()
            with client.responses.stream(
                model="gpt-5.2",
                input=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=temperature,
            ) as stream:
                for event in stream:
                    if event.type == "response.output_text.delta":
                        delta = event.delta
                        if delta:
                            yield f"data: {json.dumps({'delta': delta})}\n\n"
            yield f"data: {json.dumps({'done': True})}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps({'error': str(exc)})}\n\n"
            yield f"data: {json.dumps({'done': True})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


@bp.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy"})
