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
import type { Triple } from '@/types/graph';

/**
 * API endpoint for fetching all triples from the selected graph database
 * GET /api/graph-db/triples
 */
export async function GET(req: NextRequest) {
  try {
    const { searchParams } = new URL(req.url);
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
    const triples = edges.length ? edgesToTriples(nodes, edges) : (Array.isArray(data?.triples) ? data.triples : []);
    const uniqueTriples = deduplicateTriples(triples);

    return NextResponse.json({
      success: true,
      triples: uniqueTriples,
      nodes,
      edges,
      count: uniqueTriples.length,
      databaseType: 'backend'
    });
  } catch (error) {
    console.error(`Error fetching triples from backend:`, error);
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    return NextResponse.json(
      { error: `Failed to fetch triples: ${errorMessage}` },
      { status: 500 }
    );
  }
}

/**
 * Helper function to deduplicate triples
 */
function deduplicateTriples(triples: Triple[]): Triple[] {
  const seen = new Set<string>();
  return triples.filter(triple => {
    // Create a string key for this triple
    const key = `${triple.subject.toLowerCase()}|${triple.predicate.toLowerCase()}|${triple.object.toLowerCase()}`;
    
    // Check if we've seen this triple before
    if (seen.has(key)) {
      return false;
    }
    
    // Mark this triple as seen
    seen.add(key);
    return true;
  });
}

function edgesToTriples(nodes: any[], edges: any[]): Triple[] {
  const nodeIdToName = new Map<string, string>();
  nodes.forEach((node) => {
    if (node?._id && node?.name) {
      nodeIdToName.set(String(node._id), String(node.name));
    }
  });

  return edges
    .map((edge) => {
      const subject = nodeIdToName.get(String(edge?._from || ""));
      const object = nodeIdToName.get(String(edge?._to || ""));
      const predicate = edge?.label ? String(edge.label) : "";
      if (!subject || !object || !predicate) return null;
      return { subject, predicate, object };
    })
    .filter(Boolean) as Triple[];
}

/**
 * API endpoint for storing triples in the selected graph database
 * POST /api/graph-db/triples
 */
export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const { triples, nodes, edges, documentName } = body;

    if ((!triples || !Array.isArray(triples)) && (!nodes || !Array.isArray(nodes) || !edges || !Array.isArray(edges))) {
      return NextResponse.json({ error: 'Graph data is required' }, { status: 400 });
    }

    const validTriples = Array.isArray(triples)
      ? triples.filter((triple: any) => {
          return (
            triple &&
            typeof triple.subject === 'string' && triple.subject.trim() !== '' &&
            typeof triple.predicate === 'string' && triple.predicate.trim() !== '' &&
            typeof triple.object === 'string' && triple.object.trim() !== ''
          );
        }) as Triple[]
      : [];
    const nodeCount = Array.isArray(nodes) ? nodes.length : 0;
    const edgeCount = Array.isArray(edges) ? edges.length : 0;

    return NextResponse.json({
      success: true,
      message: 'Graph data accepted for visualization',
      count: validTriples.length || edgeCount,
      documentName,
      databaseType: 'backend'
    });

  } catch (error) {
    console.error('Error handling triples request:', error);
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    return NextResponse.json(
      { error: `Failed to store triples: ${errorMessage}` },
      { status: 500 }
    );
  }
} 