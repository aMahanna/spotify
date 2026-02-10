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

import { useEffect, useRef, useCallback, useState } from "react"
import type { NodeDocument, EdgeDocument } from "@/types/graph"

interface GraphVisualizationProps {
  nodes: NodeDocument[]
  edges: EdgeDocument[]
  fullscreen?: boolean
  highlightedNodes?: string[]
  selectedNodeId?: string | null
  layoutType?: "force" | "hierarchical" | "radial"
}

export function GraphVisualization({ 
  nodes,
  edges,
  fullscreen = false,
  highlightedNodes = [],
  selectedNodeId = null,
  layoutType = "force"
}: GraphVisualizationProps) {
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const iframeRef = useRef<HTMLIFrameElement>(null)
  const loadTimerRef = useRef<NodeJS.Timeout | null>(null)
  const pendingGraphPayloadRef = useRef<{ nodes: NodeDocument[]; edges: EdgeDocument[] } | null>(null)
  
  // Handle 3D view errors that come from the iframe
  const handleIframeError = useCallback((event: MessageEvent) => {
    if (event.data && event.data.type === '3d-graph-error') {
      setError(event.data.message || 'Error loading 3D graph');
      setIsLoading(false);
    }
  }, []);
  
  // Handle 3D view in an iframe to completely isolate it from the main DOM
  useEffect(() => {
    setIsLoading(true);
    setError(null);
    
    // Set a safety timeout in case the iframe never loads
    loadTimerRef.current = setTimeout(() => {
      setIsLoading(false);
    }, 10000); // 10 second timeout
    
    if (iframeRef.current) {
      // Create an event listener to know when the iframe is loaded
      const handleLoad = () => {
        if (loadTimerRef.current) {
          clearTimeout(loadTimerRef.current);
          loadTimerRef.current = null;
        }
        
        setTimeout(() => {
          setIsLoading(false);
        }, 2000);

        if (pendingGraphPayloadRef.current && iframeRef.current?.contentWindow) {
          iframeRef.current.contentWindow.postMessage(
            {
              type: "graph-data",
              payload: pendingGraphPayloadRef.current
            },
            window.location.origin
          );
          pendingGraphPayloadRef.current = null;
        }
      };
      
      // Add the event listener
      iframeRef.current.addEventListener('load', handleLoad);
      
      // Add message listener for error communication
      window.addEventListener('message', handleIframeError);
      
      const setupIframe = () => {
        try {
          // Get graph ID from URL if available
          const params = new URLSearchParams(window.location.search);
          const graphId = params.get("id") || params.get("graph_id");
          
          const timestamp = Date.now();
          const baseParams = `&fullscreen=${fullscreen}&layout=${layoutType}&t=${timestamp}`;
          
          let iframeSrc = '';
          
          if (graphId) {
            // If we have a graph ID, we can just pass that
            iframeSrc = `/graph3d?id=${graphId}${baseParams}`;
          } else {
            // For embedded graphs without a persisted graph ID, pass data directly
            // to the iframe via postMessage once the frame has loaded.
            pendingGraphPayloadRef.current = { nodes, edges };
            iframeSrc = `/graph3d?source=message${baseParams}`;
          }
          
          // Set the iframe source
          if (iframeRef.current) {
            iframeRef.current.src = iframeSrc;
          }
        } catch (err) {
          console.error("Error setting iframe source:", err);
          setError("Failed to prepare graph data for visualization");
          setIsLoading(false);
        }
      };
      
      setupIframe();
      
      // Clean up
      return () => {
        if (loadTimerRef.current) {
          clearTimeout(loadTimerRef.current);
        }
        if (iframeRef.current) {
          iframeRef.current.removeEventListener('load', handleLoad);
        }
        window.removeEventListener('message', handleIframeError);
      };
    }
  }, [nodes, edges, fullscreen, handleIframeError, layoutType]);

  useEffect(() => {
    const targetWindow = iframeRef.current?.contentWindow;
    if (!targetWindow) return;
    targetWindow.postMessage(
      {
        type: "graph-select",
        highlightedNodes,
        selectedNodeId
      },
      window.location.origin
    );
  }, [highlightedNodes, selectedNodeId]);
  
  return (
    <div className={`relative ${fullscreen ? "h-full" : "h-[500px]"}`}>
      <div className="relative h-full w-full">
        <iframe
          ref={iframeRef}
          className="w-full h-full border-0"
          title="3D Graph Visualization"
          sandbox="allow-scripts allow-same-origin"
        />
        
        {isLoading && (
          <div className="absolute inset-0 flex items-center justify-center bg-black/70 z-10">
            <div className="flex flex-col items-center gap-3">
              <div className="animate-spin w-12 h-12 rounded-full border-t-2 border-l-2 border-primary border-r-transparent border-b-transparent"></div>
              <div className="text-primary font-medium">Loading 3D graph visualization...</div>
              <div className="text-xs text-gray-400">This may take a moment</div>
            </div>
          </div>
        )}
        
        {error && (
          <div className="absolute inset-0 flex items-center justify-center bg-black/80 z-10">
            <div className="text-red-500 p-6 bg-black/90 rounded-lg max-w-md text-center">
              <p className="font-bold mb-3">Error loading 3D visualization</p>
              <p className="text-sm mb-4">{error}</p>
              <p className="text-xs mb-4 text-gray-400">Your browser may not support WebGL or 3D rendering.</p>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

