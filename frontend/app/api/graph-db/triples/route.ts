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
export async function GET() {
  try {
    const response = await fetch('http://localhost:5000/api/graph');

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Backend responded with ${response.status}: ${errorText}`);
    }

    const data = await response.json();
    const triples = Array.isArray(data?.triples) ? data.triples : [];
    const uniqueTriples = deduplicateTriples(triples);

    return NextResponse.json({
      success: true,
      triples: uniqueTriples,
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

/**
 * API endpoint for storing triples in the selected graph database
 * POST /api/graph-db/triples
 */
export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const { triples, documentName } = body;

    if (!triples || !Array.isArray(triples)) {
      return NextResponse.json({ error: 'Triples are required' }, { status: 400 });
    }

    const validTriples = triples.filter((triple: any) => {
      return (
        triple &&
        typeof triple.subject === 'string' && triple.subject.trim() !== '' &&
        typeof triple.predicate === 'string' && triple.predicate.trim() !== '' &&
        typeof triple.object === 'string' && triple.object.trim() !== ''
      );
    }) as Triple[];

    return NextResponse.json({
      success: true,
      message: 'Triples accepted for visualization',
      count: validTriples.length,
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