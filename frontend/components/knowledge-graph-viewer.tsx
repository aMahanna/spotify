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
import { useKeyboardShortcuts } from "@/hooks/use-keyboard-shortcuts"
import { GraphVisualization } from "@/components/graph-visualization"
import { GraphLegend } from "@/components/graph-legend"
import { GraphToolbar } from "@/components/graph-toolbar"
import { NodeDocument, EdgeDocument } from "@/types/graph"

type KnowledgeGraphViewerProps = {
  graphId?: string
  refreshToken?: number
}

export function KnowledgeGraphViewer({ graphId, refreshToken }: KnowledgeGraphViewerProps) {
  const [searchTerm, setSearchTerm] = useState("")
  const [highlightedNodes, setHighlightedNodes] = useState<string[]>([])
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)
  const [layoutType, setLayoutType] = useState<"force" | "hierarchical" | "radial">("force")
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [storedGraphDocuments, setStoredGraphDocuments] = useState<{
    nodes: NodeDocument[]
    edges: EdgeDocument[]
  } | null>(null)
  const [refreshKey, setRefreshKey] = useState(0)
  const searchInputRef = useRef<HTMLInputElement>(null)

  // Fetch graph data from ArangoDB
  useEffect(() => {
    const fetchGraphDocuments = async () => {
      try {
        setLoading(true)
        setError(null)
        const endpoint = graphId ? `/api/graph-db/triples?graph_id=${encodeURIComponent(graphId)}` : '/api/graph-db/triples'
        const response = await fetch(endpoint)
        
        if (response.ok) {
          const data = await response.json()
          if (Array.isArray(data.nodes) && Array.isArray(data.edges)) {
            setStoredGraphDocuments({ nodes: data.nodes, edges: data.edges })
          } else {
            setStoredGraphDocuments(null)
          }
          console.log(`Loaded ${data.triples?.length || 0} stored triples from ArangoDB`)
        } else {
          console.warn('Failed to fetch stored triples:', response.statusText)
          setStoredGraphDocuments(null)
          setError("Failed to load graph data.")
        }
      } catch (error) {
        console.error('Error fetching stored triples:', error)
        setStoredGraphDocuments(null)
        setError("Failed to load graph data.")
      } finally {
        setLoading(false)
      }
    }

    fetchGraphDocuments()
  }, [graphId, refreshKey, refreshToken])

  const graphDocuments = useMemo(() => {
    if (storedGraphDocuments?.nodes?.length && storedGraphDocuments?.edges?.length) {
      return storedGraphDocuments
    }
    return { nodes: [], edges: [] }
  }, [storedGraphDocuments])

  const graphData = useMemo(() => {
    const nodes = graphDocuments.nodes.map((node) => ({
      id: node._id,
      label: node.name,
      group: node.type,
    }))
    const edges = graphDocuments.edges.map((edge) => ({
      id: edge._id || edge._key,
      source: edge._from,
      target: edge._to,
      label: edge.label,
    }))
    return { nodes, edges }
  }, [graphDocuments])

  const handleSearch = () => {
    if (!searchTerm) {
      setHighlightedNodes([])
      setSelectedNodeId(null)
      return
    }
    
    const lowerSearchTerm = searchTerm.toLowerCase()
    const matches = graphDocuments.nodes.filter((node) => {
      const name = node.name?.toLowerCase() || ""
      const id = node._id?.toLowerCase() || ""
      return name.includes(lowerSearchTerm) || id.includes(lowerSearchTerm)
    })
    if (matches.length === 0) {
      setHighlightedNodes([])
      setSelectedNodeId(null)
      return
    }
    const selected = matches[0]
    setHighlightedNodes([selected._id])
    setSelectedNodeId(selected._id)
  }

  const openFullscreen3D = () => {
    const params = new URLSearchParams()
    params.set("layout", layoutType)
    if (highlightedNodes.length > 0) {
      params.set("highlightedNodes", JSON.stringify(highlightedNodes))
    }
    if (selectedNodeId) {
      params.set("selectedNodeId", selectedNodeId)
    }

    if (graphId) {
      params.set("id", graphId)
      window.location.href = `/graph3d?${params.toString()}`
      return
    }

    params.set("source", "stored")
    window.location.href = `/graph3d?${params.toString()}`
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
      callback: openFullscreen3D,
      description: 'Open 3D fullscreen'
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
  ]) 

  return (
    <div className="space-y-4">
      {/* New Organized Toolbar */}
      <GraphToolbar
        onToggleFullscreen={openFullscreen3D}
        layoutType={layoutType}
        onLayoutChange={setLayoutType}
        onExport={exportGraph}
        searchTerm={searchTerm}
        onSearchChange={setSearchTerm}
        onSearch={handleSearch}
        searchInputRef={searchInputRef}
      />
      
      <div className="space-y-6">
          
          <div 
            className="overflow-hidden border border-border rounded-lg transition-all relative"
            style={{ height: '500px' }}
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
                  <p className="text-sm text-muted-foreground">Build or select a playlist graph to visualize.</p>
                </div>
              </div>
            ) : (
              <GraphVisualization 
                nodes={graphDocuments.nodes}
                edges={graphDocuments.edges}
                highlightedNodes={highlightedNodes}
                selectedNodeId={selectedNodeId}
                layoutType={layoutType}
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