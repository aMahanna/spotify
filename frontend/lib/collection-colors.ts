// Centralized collection color definitions and helpers.

type CollectionInfo = {
  label: string
  color: string
}

export const NODE_COLLECTIONS: Record<string, CollectionInfo> = {
  artists: { label: "Artists", color: "#8E44AD" },
  songs: { label: "Songs", color: "#3498DB" },
  albums: { label: "Albums", color: "#1ABC9C" },
  record_labels: { label: "Record Labels", color: "#F39C12" },
  playlists: { label: "Playlists", color: "#E74C3C" },
  genres: { label: "Genres", color: "#2ECC71" },
  locations: { label: "Locations", color: "#95A5A6" }
}

export const EDGE_COLLECTIONS: Record<string, CollectionInfo> = {
  artists_songs: { label: "Artists → Songs", color: "#F1C40F" },
  artists_albums: { label: "Artists → Albums", color: "#D35400" },
  songs_albums: { label: "Songs → Albums", color: "#16A085" },
  albums_record_labels: { label: "Albums → Record Labels", color: "#C0392B" },
  artists_genres: { label: "Artists → Genres", color: "#27AE60" },
  artists_locations: { label: "Artists → Locations", color: "#2980B9" },
  artists_record_labels: { label: "Artists → Record Labels", color: "#7F8C8D" },
  artists_associated_acts: { label: "Artists → Associated Acts", color: "#9B59B6" },
  artists_related: { label: "Artists → Related Artists", color: "#34495E" }
}

const normalizeCollectionName = (collection: string, candidates: string[]) => {
  if (!collection) return ""
  const directMatch = candidates.find((candidate) => candidate === collection)
  if (directMatch) return directMatch
  return (
    candidates.find((candidate) => collection.endsWith(`_${candidate}`)) || collection
  )
}

const getCollectionFromId = (id?: string) => {
  if (!id) return ""
  const [collection] = id.split("/")
  return collection || ""
}

export const getNodeCollectionKey = (node: { _id?: string; id?: string }) => {
  const collection = getCollectionFromId(node?._id || node?.id || "")
  return normalizeCollectionName(collection, Object.keys(NODE_COLLECTIONS))
}

export const getEdgeCollectionKey = (edge: { _id?: string; id?: string; _from?: string }) => {
  const collection = getCollectionFromId(edge?._id || edge?.id || edge?._from || "")
  return normalizeCollectionName(collection, Object.keys(EDGE_COLLECTIONS))
}

export const getNodeColor = (node: { _id?: string; id?: string; color?: string }) => {
  if (node?.color) return node.color
  const key = getNodeCollectionKey(node)
  return NODE_COLLECTIONS[key]?.color || "#76B900"
}

export const getEdgeColor = (edge: { _id?: string; id?: string; _from?: string; color?: string }) => {
  if (edge?.color) return edge.color
  const key = getEdgeCollectionKey(edge)
  return EDGE_COLLECTIONS[key]?.color || "#CCCCCC"
}

export const getNodeLegendItems = (nodes: Array<{ _id?: string; id?: string }>) => {
  const keys = new Set<string>()
  nodes.forEach((node) => {
    const key = getNodeCollectionKey(node)
    if (NODE_COLLECTIONS[key]) {
      keys.add(key)
    }
  })
  keys.delete("playlists")
  return Array.from(keys).map((key) => ({
    key,
    ...NODE_COLLECTIONS[key]
  }))
}

export const getEdgeLegendItems = (edges: Array<{ _id?: string; id?: string; _from?: string }>) => {
  const keys = new Set<string>()
  edges.forEach((edge) => {
    const key = getEdgeCollectionKey(edge)
    if (EDGE_COLLECTIONS[key]) {
      keys.add(key)
    }
  })
  return Array.from(keys).map((key) => ({
    key,
    ...EDGE_COLLECTIONS[key]
  }))
}
