type GraphLike = {
  nodes?: any[]
  links?: any[]
  edges?: any[]
  triples?: any[]
}

type LinkLike = {
  source?: any
  target?: any
  _from?: any
  _to?: any
}

type TripleLike = {
  subject?: any
  object?: any
}

export const DEFAULT_TOUR_NODE_COUNT = 12

const toNodeId = (value: any): string => {
  if (!value) return ""
  if (typeof value === "string" || typeof value === "number") return String(value)
  return String(value.id || value._id || value._key || value.name || value.label || "")
}

const extractLinks = (graphData: GraphLike): Array<{ source: string; target: string }> => {
  const links: Array<{ source: string; target: string }> = []
  const rawLinks = Array.isArray(graphData?.links) ? graphData.links : []
  const rawEdges = Array.isArray(graphData?.edges) ? graphData.edges : []

  for (const link of rawLinks as LinkLike[]) {
    const source = toNodeId(link?.source)
    const target = toNodeId(link?.target)
    if (source && target) {
      links.push({ source, target })
    }
  }

  for (const edge of rawEdges as LinkLike[]) {
    const source = toNodeId(edge?._from || edge?.source)
    const target = toNodeId(edge?._to || edge?.target)
    if (source && target) {
      links.push({ source, target })
    }
  }

  return links
}

const extractTriples = (graphData: GraphLike): Array<{ subject: string; object: string }> => {
  const triples = Array.isArray(graphData?.triples) ? graphData.triples : []
  const normalized: Array<{ subject: string; object: string }> = []
  for (const triple of triples as TripleLike[]) {
    const subject = toNodeId(triple?.subject)
    const object = toNodeId(triple?.object)
    if (subject && object) {
      normalized.push({ subject, object })
    }
  }
  return normalized
}

const extractNodeIds = (graphData: GraphLike): string[] => {
  const nodes = Array.isArray(graphData?.nodes) ? graphData.nodes : []
  const ids = nodes.map(toNodeId).filter(Boolean)
  return Array.from(new Set(ids))
}

const buildAdjacency = (graphData: GraphLike): Map<string, Set<string>> => {
  const adjacency = new Map<string, Set<string>>()
  const nodeIds = extractNodeIds(graphData)
  for (const nodeId of nodeIds) {
    adjacency.set(nodeId, new Set())
  }

  for (const link of extractLinks(graphData)) {
    if (!adjacency.has(link.source)) adjacency.set(link.source, new Set())
    if (!adjacency.has(link.target)) adjacency.set(link.target, new Set())
    adjacency.get(link.source)?.add(link.target)
    adjacency.get(link.target)?.add(link.source)
  }

  for (const triple of extractTriples(graphData)) {
    if (!adjacency.has(triple.subject)) adjacency.set(triple.subject, new Set())
    if (!adjacency.has(triple.object)) adjacency.set(triple.object, new Set())
    adjacency.get(triple.subject)?.add(triple.object)
    adjacency.get(triple.object)?.add(triple.subject)
  }

  return adjacency
}

const rankNodesByDegree = (adjacency: Map<string, Set<string>>): string[] => {
  const entries = Array.from(adjacency.entries()).map(([id, neighbors]) => ({
    id,
    degree: neighbors.size,
  }))
  entries.sort((a, b) => {
    if (b.degree !== a.degree) return b.degree - a.degree
    return a.id.localeCompare(b.id)
  })
  return entries.map((entry) => entry.id)
}

const buildDfsOrder = (
  adjacency: Map<string, Set<string>>,
  rankedNodes: string[],
  count: number
): string[] => {
  const selected = new Set(rankedNodes.slice(0, count))
  const visited = new Set<string>()
  const order: string[] = []

  const dfs = (nodeId: string) => {
    visited.add(nodeId)
    order.push(nodeId)
    const neighbors = Array.from(adjacency.get(nodeId) || []).filter((neighbor) => selected.has(neighbor))
    neighbors.sort((a, b) => {
      const aDegree = adjacency.get(a)?.size ?? 0
      const bDegree = adjacency.get(b)?.size ?? 0
      if (bDegree !== aDegree) return bDegree - aDegree
      return a.localeCompare(b)
    })
    for (const neighbor of neighbors) {
      if (!visited.has(neighbor)) {
        dfs(neighbor)
      }
    }
  }

  for (const start of rankedNodes) {
    if (!selected.has(start) || visited.has(start)) continue
    dfs(start)
  }

  return order
}

export const buildTourOrder = (graphData: GraphLike, count: number = DEFAULT_TOUR_NODE_COUNT): string[] => {
  const adjacency = buildAdjacency(graphData)
  if (!adjacency.size) return []
  const rankedNodes = rankNodesByDegree(adjacency)
  return buildDfsOrder(adjacency, rankedNodes, count)
}
