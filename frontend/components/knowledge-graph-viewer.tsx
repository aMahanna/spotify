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
"use client"

import { useState, useEffect, useRef, useMemo } from "react"
import { useDocuments } from "@/contexts/document-context"
import { useKeyboardShortcuts } from "@/hooks/use-keyboard-shortcuts"
import { Download, Maximize, Minimize, Search as SearchIcon, CuboidIcon } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Slider } from "@/components/ui/slider"

import { Switch } from "@/components/ui/switch"
import { Label } from "@/components/ui/label"
import { FallbackGraph } from "@/components/fallback-graph"
import { GraphVisualization } from "@/components/graph-visualization"
import { GraphLegend } from "@/components/graph-legend"
import { GraphToolbar } from "@/components/graph-toolbar"
import { Triple, NodeDocument, EdgeDocument } from "@/types/graph"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"

type Node = {
  id: string
  label: string
  color?: string
  size?: number
  group?: string
}

type Edge = {
  source: string
  target: string
  label: string
  id: string
}

type GraphData = {
  nodes: Node[]
  edges: Edge[]
}

type KnowledgeGraphViewerProps = {
  graphId?: string
  refreshToken?: number
}

export function KnowledgeGraphViewer({ graphId, refreshToken }: KnowledgeGraphViewerProps) {
  const { documents } = useDocuments()
  const [graphData, setGraphData] = useState<GraphData>({ nodes: [], edges: [] })
  const [isFullscreen, setIsFullscreen] = useState(false)
  const [searchTerm, setSearchTerm] = useState("")
  const [highlightedNodes, setHighlightedNodes] = useState<string[]>([])
  const [layoutType, setLayoutType] = useState<"force" | "hierarchical" | "radial">("force")
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [use3D, setUse3D] = useState(false)
  const [storedTriples, setStoredTriples] = useState<Triple[]>([])
  const [storedGraphDocuments, setStoredGraphDocuments] = useState<{
    nodes: NodeDocument[]
    edges: EdgeDocument[]
  } | null>(null)
  const [includeStoredTriples, setIncludeStoredTriples] = useState(true)
  const [loadingStoredTriples, setLoadingStoredTriples] = useState(false)
  const [enrichJobId, setEnrichJobId] = useState<string | null>(null)
  const [enrichStatus, setEnrichStatus] = useState<"idle" | "queued" | "running" | "ready" | "failed">("idle")
  const [enrichError, setEnrichError] = useState<string | null>(null)
  const [refreshKey, setRefreshKey] = useState(0)
  const graphContainerRef = useRef<HTMLDivElement>(null)
  const searchInputRef = useRef<HTMLInputElement>(null)

  const normalizeKey = (value: string) => {
    const normalized = value
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9_:-]/g, "_")
      .replace(/^_+|_+$/g, "")
    return normalized || "node"
  }

  const documentGraph = useMemo(() => {
    const keyCounts = new Map<string, number>()
    const nodeKeyMap = new Map<string, string>()
    const nodes: NodeDocument[] = []
    const edges: EdgeDocument[] = []

    graphData.nodes.forEach((node) => {
      const baseKey = normalizeKey(node.id)
      const count = keyCounts.get(baseKey) || 0
      const key = count === 0 ? baseKey : `${baseKey}_${count}`
      keyCounts.set(baseKey, count + 1)

      nodeKeyMap.set(node.id, key)
      nodes.push({
        _key: key,
        _id: `nodes/${key}`,
        name: node.label,
        type: node.group
      })
    })

    graphData.edges.forEach((edge, index) => {
      const sourceKey = nodeKeyMap.get(edge.source)
      const targetKey = nodeKeyMap.get(edge.target)
      if (!sourceKey || !targetKey) return

      const edgeKey = `e${index + 1}`
      edges.push({
        _key: edgeKey,
        _id: `edges/${edgeKey}`,
        _from: `nodes/${sourceKey}`,
        _to: `nodes/${targetKey}`,
        label: edge.label,
        type: edge.id
      })
    })

    return { nodes, edges }
  }, [graphData.nodes, graphData.edges])

  // Monitor fullscreen state changes
  useEffect(() => {
    const handleFullscreenChange = () => {
      setIsFullscreen(!!document.fullscreenElement);
    };

    document.addEventListener('fullscreenchange', handleFullscreenChange);
    
    return () => {
      document.removeEventListener('fullscreenchange', handleFullscreenChange);
    };
  }, []);

  // Trigger rerender when fullscreen changes to ensure graph updates properly
  useEffect(() => {
    // A small timeout to ensure the graph has time to adjust
    if (isFullscreen) {
      const timer = setTimeout(() => {
        // Dispatch a resize event to make sure canvas and component sizes are updated
        window.dispatchEvent(new Event('resize'));
      }, 100);
      return () => clearTimeout(timer);
    }
  }, [isFullscreen]);

  // Fetch stored triples from ArangoDB
  useEffect(() => {
    const fetchStoredTriples = async () => {
      if (!includeStoredTriples) {
        setStoredTriples([])
        setStoredGraphDocuments(null)
        return
      }

      try {
        setLoadingStoredTriples(true)
        const endpoint = graphId ? `/api/graph-db/triples?graph_id=${encodeURIComponent(graphId)}` : '/api/graph-db/triples'
        const response = await fetch(endpoint)
        
        if (response.ok) {
          const data = await response.json()
          setStoredTriples(data.triples || [])
          if (Array.isArray(data.nodes) && Array.isArray(data.edges)) {
            setStoredGraphDocuments({ nodes: data.nodes, edges: data.edges })
          } else {
            setStoredGraphDocuments(null)
          }
          console.log(`Loaded ${data.triples?.length || 0} stored triples from ArangoDB`)
        } else {
          console.warn('Failed to fetch stored triples:', response.statusText)
          setStoredTriples([])
          setStoredGraphDocuments(null)
        }
      } catch (error) {
        console.error('Error fetching stored triples:', error)
        setStoredTriples([])
      } finally {
        setLoadingStoredTriples(false)
      }
    }

    fetchStoredTriples()
  }, [includeStoredTriples, graphId, refreshKey, refreshToken])

  const graphDocuments = useMemo(() => {
    if (includeStoredTriples && storedGraphDocuments?.nodes?.length && storedGraphDocuments?.edges?.length) {
      return storedGraphDocuments
    }
    return documentGraph
  }, [includeStoredTriples, storedGraphDocuments, documentGraph])

  useEffect(() => {
    if (!enrichJobId) return

    let isActive = true
    let interval: ReturnType<typeof setInterval> | null = null

    const pollStatus = async () => {
      try {
        const response = await fetch(`http://localhost:5000/api/playlist/status/${enrichJobId}`)
        if (!response.ok) {
          const text = await response.text()
          throw new Error(text || `Status check failed: ${response.status}`)
        }
        const data = await response.json()
        const status = data?.status as typeof enrichStatus
        if (!isActive) return
        setEnrichStatus(status || "running")

        if (status === "ready") {
          setRefreshKey((current) => current + 1)
          if (interval) clearInterval(interval)
          setEnrichJobId(null)
        } else if (status === "failed") {
          setEnrichError(data?.error || "Enrichment failed")
          if (interval) clearInterval(interval)
          setEnrichJobId(null)
        }
      } catch (err) {
        if (!isActive) return
        setEnrichError(err instanceof Error ? err.message : "Failed to check enrichment status")
        setEnrichStatus("failed")
        if (interval) clearInterval(interval)
        setEnrichJobId(null)
      }
    }

    interval = setInterval(pollStatus, 1500)
    pollStatus()
    return () => {
      isActive = false
      if (interval) clearInterval(interval)
    }
  }, [enrichJobId])

  // Generate combined graph data from all processed documents and stored triples
  useEffect(() => {
    try {
      setLoading(true)
      
      const allNodes: Node[] = []
      const allEdges: Edge[] = []
      const nodeMap = new Map<string, Node>()
      
      // Helper function to process triples and add to graph
      const processTriples = (triples: Triple[], source: "document" | "stored") => {
        triples.forEach(triple => {
          // Add subject node if doesn't exist
          if (!nodeMap.has(triple.subject)) {
            const subjectNode: Node = {
              id: triple.subject,
              label: triple.subject,
              group: source === "stored" ? "stored-subject" : "subject"
            }
            nodeMap.set(triple.subject, subjectNode)
            allNodes.push(subjectNode)
          }
          
          // Add object node if doesn't exist
          if (!nodeMap.has(triple.object)) {
            const objectNode: Node = {
              id: triple.object,
              label: triple.object,
              group: source === "stored" ? "stored-object" : "object"
            }
            nodeMap.set(triple.object, objectNode)
            allNodes.push(objectNode)
          }
          
          // Add edge
          const edgeId = `${source}-${triple.subject}-${triple.predicate}-${triple.object}`
          allEdges.push({
            id: edgeId,
            source: triple.subject,
            target: triple.object,
            label: triple.predicate
          })
        })
      }
      
      // Process all documents with triples
      documents
        .filter(doc => doc.status === "Processed" && doc.triples && doc.triples.length > 0)
        .forEach(doc => {
          if (!doc.triples) return
          processTriples(doc.triples, "document")
        })
      
      // Process stored triples if enabled
      if (includeStoredTriples && storedTriples.length > 0) {
        processTriples(storedTriples, "stored")
      }
      
      setGraphData({ nodes: allNodes, edges: allEdges })
      setError(null)
    } catch (err) {
      console.error("Error generating graph data:", err)
      setError("Failed to generate knowledge graph visualization.")
    } finally {
      setLoading(false)
    }
  }, [documents, storedTriples, includeStoredTriples])

  // Convert graph data to triples format for FallbackGraph
  const getTriples = (): Triple[] => {
    if (!graphData || !graphData.edges) {
      return [];
    }
    return graphData.edges.map(edge => ({
      subject: edge.source,
      predicate: edge.label,
      object: edge.target
    }))
  }

  const handleSearch = () => {
    if (!searchTerm) {
      setHighlightedNodes([])
      return
    }
    
    const lowerSearchTerm = searchTerm.toLowerCase()
    const matches = graphData.nodes.filter(node => 
      node.label.toLowerCase().includes(lowerSearchTerm)
    ).map(node => node.id)
    
    setHighlightedNodes(matches)
  }

  const toggleFullscreen = () => {
    if (!graphContainerRef.current) return;
    
    if (!document.fullscreenElement) {
      // Enter fullscreen
      graphContainerRef.current.requestFullscreen().catch(err => {
        console.error(`Error attempting to enter fullscreen: ${err.message}`);
      });
    } else {
      // Exit fullscreen
      document.exitFullscreen().catch(err => {
        console.error(`Error attempting to exit fullscreen: ${err.message}`);
      });
    }
    // No need to set state here as the fullscreenchange event will handle it
  }

  const toggleViewMode = () => {
    if (!use3D) {
      // Navigate to 3D view using stored triples from database
      // This avoids large request headers by using the database instead of localStorage/URL
      console.log('Switching to 3D view using stored triples from database');
      
      // Check if we have stored triples available
      if (includeStoredTriples && storedTriples.length > 0) {
        // Use stored triples from database - no need to pass data through browser
        window.location.href = `/graph3d?source=stored&layout=${layoutType}`;
      } else {
        // Fallback: use current document triples, but try to store them in DB first
        const currentTriples = getTriples();
        
        if (currentTriples.length > 0) {
          // Try to store current triples in database first, then use stored source
          fetch('/api/graph-db/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
              triples: currentTriples,
              documentName: 'Current Graph View'
            })
          }).then(response => {
            if (response.ok) {
              console.log('Successfully stored current triples in database');
              // Use stored triples source
              window.location.href = `/graph3d?source=stored&layout=${layoutType}`;
            } else {
              console.warn('Failed to store triples in database, using fallback');
              // Fallback to localStorage for small datasets only
              if (currentTriples.length <= 100) {
                const storageId = `graph_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
                try {
                  localStorage.setItem(storageId, JSON.stringify(currentTriples));
                  window.location.href = `/graph3d?source=local&storageId=${storageId}&layout=${layoutType}`;
                } catch (storageError) {
                  console.error("localStorage also failed:", storageError);
                  window.location.href = `/graph3d?source=stored&layout=${layoutType}`;
                }
              } else {
                // For large datasets, just use stored source (may be empty but won't cause header issues)
                console.warn('Large dataset detected, using stored source to avoid header size limits');
                window.location.href = `/graph3d?source=stored&layout=${layoutType}`;
              }
            }
          }).catch(error => {
            console.error('Error storing triples:', error);
            // Fallback to stored source
            window.location.href = `/graph3d?source=stored&layout=${layoutType}`;
          });
        } else {
          // No triples available, use stored source
          window.location.href = `/graph3d?source=stored&layout=${layoutType}`;
        }
      }
    } else {
      // Toggle back to 2D
      setUse3D(false);
    }
  }

  const handleEnrich = async () => {
    if (!graphId) return
    setEnrichError(null)
    setEnrichStatus("queued")
    setEnrichJobId(null)

    try {
      const response = await fetch("http://localhost:5000/api/playlist/enrich", {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ graph_id: graphId })
      })
      if (!response.ok) {
        const text = await response.text()
        throw new Error(text || `Enrich request failed: ${response.status}`)
      }
      const data = await response.json()
      setEnrichJobId(data?.job_id || null)
      setEnrichStatus("running")
    } catch (error) {
      setEnrichError(error instanceof Error ? error.message : "Failed to start enrichment")
      setEnrichStatus("failed")
    }
  }

  const exportGraph = (format: "json" | "csv" | "png") => {
    switch (format) {
      case "json":
        const jsonData = JSON.stringify(graphData, null, 2)
        const jsonBlob = new Blob([jsonData], { type: 'application/json' })
        const jsonUrl = URL.createObjectURL(jsonBlob)
        const jsonLink = document.createElement('a')
        jsonLink.href = jsonUrl
        jsonLink.download = 'knowledge-graph.json'
        jsonLink.click()
        break
      case "csv":
        // Create nodes CSV
        let nodesCSV = "id,label,group\n"
        graphData.nodes.forEach(node => {
          nodesCSV += `"${node.id}","${node.label}","${node.group || ''}"\n`
        })
        
        // Create edges CSV
        let edgesCSV = "id,source,target,label\n"
        graphData.edges.forEach(edge => {
          edgesCSV += `"${edge.id}","${edge.source}","${edge.target}","${edge.label}"\n`
        })
        
        // Download nodes CSV
        const nodesBlob = new Blob([nodesCSV], { type: 'text/csv' })
        const nodesUrl = URL.createObjectURL(nodesBlob)
        const nodesLink = document.createElement('a')
        nodesLink.href = nodesUrl
        nodesLink.download = 'knowledge-graph-nodes.csv'
        nodesLink.click()
        
        // Download edges CSV
        const edgesBlob = new Blob([edgesCSV], { type: 'text/csv' })
        const edgesUrl = URL.createObjectURL(edgesBlob)
        const edgesLink = document.createElement('a')
        edgesLink.href = edgesUrl
        edgesLink.download = 'knowledge-graph-edges.csv'
        edgesLink.click()
        break
      case "png":
        // Screenshot functionality would be implemented here
        alert("PNG export would capture the current graph view")
        break
    }
  }

  // Keyboard shortcuts
  useKeyboardShortcuts([
    {
      key: 'f',
      callback: toggleFullscreen,
      description: 'Toggle fullscreen'
    },
    {
      key: '3',
      callback: toggleViewMode,
      description: 'Toggle 3D view'
    },
    {
      key: 'k',
      ctrlKey: true,
      callback: () => searchInputRef.current?.focus(),
      description: 'Focus search'
    },
    {
      key: '1',
      callback: () => setLayoutType('force'),
      description: 'Force layout'
    },
    {
      key: '2',
      callback: () => setLayoutType('hierarchical'),
      description: 'Hierarchical layout'
    },
    {
      key: '3',
      shiftKey: true,
      callback: () => setLayoutType('radial'),
      description: 'Radial layout'
    }
  ], !isFullscreen) // Disable shortcuts in fullscreen to avoid conflicts

  return (
    <div className="space-y-4">
      {/* New Organized Toolbar */}
      <GraphToolbar
        use3D={use3D}
        onToggle3D={toggleViewMode}
        isFullscreen={isFullscreen}
        onToggleFullscreen={toggleFullscreen}
        layoutType={layoutType}
        onLayoutChange={setLayoutType}
        includeStoredTriples={includeStoredTriples}
        onToggleStoredTriples={setIncludeStoredTriples}
        storedTriplesCount={storedTriples.length}
        loadingStoredTriples={loadingStoredTriples}
        onExport={exportGraph}
        searchTerm={searchTerm}
        onSearchChange={setSearchTerm}
        onSearch={handleSearch}
        searchInputRef={searchInputRef}
        nodeCount={graphData.nodes.length}
        edgeCount={graphData.edges.length}
        onEnrich={handleEnrich}
        enriching={enrichStatus === "queued" || enrichStatus === "running"}
        enrichDisabled={!graphId}
      />
      {enrichError && (
        <div className="text-sm text-destructive">{enrichError}</div>
      )}
      
      <div className="space-y-6">
          
          <div 
            ref={graphContainerRef}
            className={`overflow-hidden border border-border rounded-lg transition-all ${isFullscreen ? 'fixed inset-0 z-50 bg-background' : 'relative'}`}
            style={{ height: isFullscreen ? '100vh' : '500px' }}
          >
            {loading ? (
              <div className="absolute inset-0 flex items-center justify-center">
                <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-primary"></div>
              </div>
            ) : error ? (
              <div className="absolute inset-0 flex items-center justify-center">
                <div className="text-destructive">{error}</div>
              </div>
            ) : graphData.nodes.length === 0 ? (
              <div className="absolute inset-0 flex items-center justify-center">
                <div className="text-center">
                  <p className="mb-2">No knowledge graph data available</p>
                  <p className="text-sm text-muted-foreground">Process documents to generate a knowledge graph</p>
                </div>
              </div>
            ) : use3D ? (
              <GraphVisualization 
                nodes={graphDocuments.nodes}
                edges={graphDocuments.edges}
                fullscreen={isFullscreen}
                highlightedNodes={highlightedNodes}
                layoutType={layoutType}
                initialMode='3d'
              />
            ) : (
              <FallbackGraph 
                nodes={graphDocuments.nodes} 
                edges={graphDocuments.edges}
                fullscreen={isFullscreen}
              />
            )}
            {!loading && (
              <GraphLegend
                nodes={graphDocuments.nodes}
                edges={graphDocuments.edges}
                className="absolute bottom-2 right-2 z-10 max-w-[220px]"
              />
            )}
          </div>
      </div>
    </div>
  )
} 