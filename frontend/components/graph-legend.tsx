// Legend for node and link collection colors.

"use client"

import { getEdgeLegendItems, getNodeLegendItems } from "@/lib/collection-colors"

interface GraphLegendProps {
  nodes?: Array<{ _id?: string; id?: string }>
  edges?: Array<{ _id?: string; id?: string; _from?: string }>
  className?: string
}

export function GraphLegend({ nodes = [], edges = [], className = "" }: GraphLegendProps) {
  const nodeItems = getNodeLegendItems(nodes)
  const edgeItems = getEdgeLegendItems(edges)

  if (nodeItems.length === 0 && edgeItems.length === 0) {
    return null
  }

  return (
    <div className={`rounded-md border border-border bg-background/90 text-foreground shadow-sm ${className}`}>
      <div className="px-3 py-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
        Legend
      </div>
      {nodeItems.length > 0 && (
        <div className="px-3 pb-2">
          <div className="text-xs font-medium mb-1">Nodes</div>
          <div className="flex flex-col gap-1 text-xs">
            {nodeItems.map((item) => (
              <div key={item.key} className="flex items-center gap-2">
                <span
                  className="inline-block h-3 w-3 rounded-sm"
                  style={{ backgroundColor: item.color }}
                />
                <span>{item.label}</span>
              </div>
            ))}
          </div>
        </div>
      )}
      {edgeItems.length > 0 && null}
    </div>
  )
}
