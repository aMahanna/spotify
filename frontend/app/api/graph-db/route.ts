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
import { NextRequest, NextResponse } from 'next/server';

const buildGraphData = (triples: any[]) => {
  const nodesMap = new Map<string, { id: string; name: string; label: string; val: number; color: string }>()
  const links: { source: string; target: string; label: string }[] = []

  triples.forEach((triple) => {
    const subject = String(triple.subject || '').trim()
    const object = String(triple.object || '').trim()
    const predicate = String(triple.predicate || '').trim()

    if (!subject || !object || !predicate) return

    if (!nodesMap.has(subject)) {
      nodesMap.set(subject, {
        id: subject,
        name: subject,
        label: 'Entity',
        val: 1,
        color: '#76b900'
      })
    }
    if (!nodesMap.has(object)) {
      nodesMap.set(object, {
        id: object,
        name: object,
        label: 'Entity',
        val: 1,
        color: '#4ecdc4'
      })
    }

    links.push({ source: subject, target: object, label: predicate })
  })

  return { nodes: Array.from(nodesMap.values()), links }
}

const buildGraphDataFromEdges = (nodes: any[], edges: any[]) => {
  const mappedNodes = nodes.map((node) => ({
    id: String(node?._id || ''),
    name: String(node?.name || ''),
    label: String(node?.type || 'Entity'),
    val: 1,
    color: '#76b900'
  })).filter((node) => node.id && node.name)

  const links = edges
    .map((edge) => {
      const source = String(edge?._from || '').trim()
      const target = String(edge?._to || '').trim()
      const label = String(edge?.label || '').trim()
      if (!source || !target || !label) return null
      return { source, target, label }
    })
    .filter(Boolean) as { source: string; target: string; label: string }[]

  return { nodes: mappedNodes, links }
}

/**
 * GET handler for retrieving graph data from the selected graph database
 */
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const graphId = searchParams.get("graph_id") || searchParams.get("id");
    const backendUrl = graphId
      ? `http://localhost:5000/api/graph?graph_id=${encodeURIComponent(graphId)}`
      : 'http://localhost:5000/api/graph';
    const response = await fetch(backendUrl);
    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Backend responded with ${response.status}: ${errorText}`);
    }

    const data = await response.json();
    const nodes = Array.isArray(data?.nodes) ? data.nodes : [];
    const edges = Array.isArray(data?.edges) ? data.edges : [];

    if (nodes.length && edges.length) {
      const graphData = buildGraphDataFromEdges(nodes, edges)
      return NextResponse.json({ 
        nodes: graphData.nodes, 
        links: graphData.links, 
        connectionUrl: 'http://localhost:5000/api/graph',
        databaseType: 'backend'
      });
    }

    const triples = Array.isArray(data?.triples) ? data.triples : [];
    const graphData = buildGraphData(triples);

    return NextResponse.json({ 
      nodes: graphData.nodes, 
      links: graphData.links, 
      connectionUrl: 'http://localhost:5000/api/graph',
      databaseType: 'backend'
    });
  } catch (error) {
    console.error(`Error in graph database GET handler:`, error);
    return NextResponse.json(
      { error: `Failed to fetch graph data: ${error instanceof Error ? error.message : String(error)}` },
      { status: 500 }
    );
  }
}

/**
 * POST handler for importing triples into the selected graph database
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    
    // Validate request body
    if ((!body.triples || !Array.isArray(body.triples)) && (!body.nodes || !Array.isArray(body.nodes) || !body.edges || !Array.isArray(body.edges))) {
      return NextResponse.json(
        { error: 'Invalid request: graph data is required' },
        { status: 400 }
      );
    }

    return NextResponse.json({
      success: true,
      message: `Accepted ${body.triples?.length || body.edges?.length || 0} items for visualization`,
      databaseType: 'backend'
    });
  } catch (error) {
    console.error(`Error in graph database POST handler:`, error);
    return NextResponse.json(
      { error: `Failed to import triples: ${error instanceof Error ? error.message : String(error)}` },
      { status: 500 }
    );
  }
} 