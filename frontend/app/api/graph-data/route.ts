//
// SPDX-FileCopyrightText: Copyright (c) 1993-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
import { type NextRequest, NextResponse } from "next/server"

const buildGraphDataFromTriples = (triples: any[]) => {
  const nodesMap = new Map<string, { id: string; name: string; group: string }>()
  const links: { source: string; target: string; name: string }[] = []

  triples.forEach((triple) => {
    const subject = String(triple.subject || '').trim()
    const object = String(triple.object || '').trim()
    const predicate = String(triple.predicate || '').trim()

    if (!subject || !object || !predicate) return

    if (!nodesMap.has(subject)) {
      nodesMap.set(subject, { id: subject, name: subject, group: "entity" })
    }
    if (!nodesMap.has(object)) {
      nodesMap.set(object, { id: object, name: object, group: "entity" })
    }

    links.push({ source: subject, target: object, name: predicate })
  })

  return { nodes: Array.from(nodesMap.values()), links }
}

const buildLinksFromEdges = (edges: any[]) => {
  return edges
    .map((edge) => {
      const source = String(edge._from || '').trim()
      const target = String(edge._to || '').trim()
      const name = String(edge.label || '').trim()

      if (!source || !target || !name) return null

      return { source, target, name }
    })
    .filter(Boolean) as { source: string; target: string; name: string }[]
}

export async function POST(request: NextRequest) {
  try {
    const { nodes, edges, documentName } = await request.json()

    if (!nodes || !Array.isArray(nodes) || !edges || !Array.isArray(edges)) {
      return NextResponse.json({ error: "Invalid graph data" }, { status: 400 })
    }

    console.log(`Accepted graph data with ${nodes.length} nodes and ${edges.length} edges`)
    return NextResponse.json({ graphId: "backend", documentName: documentName || "Backend Graph" })
  } catch (error) {
    console.error("Error storing graph data:", error)
    return NextResponse.json({ error: "Failed to store graph data" }, { status: 500 })
  }
}

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const graphId = searchParams.get("graph_id") || searchParams.get("id")
    const normalizedGraphId = graphId === "backend" ? null : graphId
    const backendUrl = normalizedGraphId
      ? `http://localhost:5000/api/graph?graph_id=${encodeURIComponent(normalizedGraphId)}`
      : "http://localhost:5000/api/graph"
    const response = await fetch(backendUrl)
    if (!response.ok) {
      const errorText = await response.text()
      throw new Error(`Backend responded with ${response.status}: ${errorText}`)
    }

    const data = await response.json()
    const nodes = Array.isArray(data?.nodes) ? data.nodes : []
    const edges = Array.isArray(data?.edges) ? data.edges : []

    if (nodes.length && edges.length) {
      const links = buildLinksFromEdges(edges)
      return NextResponse.json({
        nodes,
        edges,
        links,
        documentName: "Backend Graph"
      })
    }

    const triples = Array.isArray(data?.triples) ? data.triples : []
    const { nodes: fallbackNodes, links } = buildGraphDataFromTriples(triples)

    return NextResponse.json({
      triples,
      nodes: fallbackNodes,
      links,
      documentName: "Backend Graph"
    })
  } catch (error) {
    console.error("Error retrieving graph data:", error)
    return NextResponse.json({ error: "Failed to retrieve graph data" }, { status: 500 })
  }
}

