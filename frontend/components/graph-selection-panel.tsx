"use client"

import { useEffect, useRef, useState } from "react"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { ScrollArea } from "@/components/ui/scroll-area"

type GraphSelectionPanelProps = {
  selectedNodes: any[]
  selectedEdges: any[]
}

export function GraphSelectionPanel({ selectedNodes, selectedEdges }: GraphSelectionPanelProps) {
  const [summary, setSummary] = useState("")
  const [isStreaming, setIsStreaming] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const scrollAnchorRef = useRef<HTMLDivElement | null>(null)

  const hasSelection = selectedNodes.length > 0 || selectedEdges.length > 0

  useEffect(() => {
    scrollAnchorRef.current?.scrollIntoView({ behavior: "smooth" })
  }, [summary, isStreaming])

  useEffect(() => {
    setError(null)
    setSummary("")
  }, [selectedNodes, selectedEdges])

  const handleSummarize = async () => {
    if (!hasSelection || isStreaming) return

    setError(null)
    setSummary("")
    setIsStreaming(true)

    try {
      const response = await fetch("http://localhost:5000/api/chat/stream", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question_id: "selection_summary",
          nodes: selectedNodes,
          edges: selectedEdges,
          triples: [],
        }),
      })

      if (!response.ok || !response.body) {
        const text = await response.text()
        throw new Error(text || `Request failed: ${response.status}`)
      }

      const reader = response.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ""
      let doneStreaming = false

      while (!doneStreaming) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })

        const parts = buffer.split("\n\n")
        buffer = parts.pop() || ""

        for (const part of parts) {
          const trimmed = part.trim()
          if (!trimmed.startsWith("data:")) continue
          const payload = trimmed.replace(/^data:\s*/, "")
          if (!payload) continue

          let parsed: { delta?: string; done?: boolean; error?: string } | null = null
          try {
            parsed = JSON.parse(payload)
          } catch {
            parsed = null
          }

          if (!parsed) continue
          if (parsed.error) {
            setError(parsed.error)
          }
          if (parsed.delta) {
            setSummary((prev) => prev + parsed.delta)
          }
          if (parsed.done) {
            doneStreaming = true
            break
          }
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to stream response")
    } finally {
      setIsStreaming(false)
    }
  }

  return (
    <div className="fixed right-4 top-1/2 z-40 w-[360px] -translate-y-1/2">
      <Card className="bg-black/90 border-gray-700 text-white shadow-lg">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">Selection Summary</CardTitle>
          <div className="text-xs text-gray-400">
            Nodes: <span className="text-gray-200">{selectedNodes.length}</span> &bull; Edges:{" "}
            <span className="text-gray-200">{selectedEdges.length}</span>
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
          <button
            type="button"
            disabled={!hasSelection || isStreaming}
            onClick={handleSummarize}
            className="w-full rounded-md border border-gray-700 bg-gray-900/70 px-3 py-2 text-xs text-gray-200 hover:bg-gray-800 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {isStreaming ? "Summarizing..." : "Summarize"}
          </button>
          <ScrollArea className="h-[220px] pr-3">
            {summary ? (
              <div className="prose prose-invert prose-p:my-1 prose-li:my-1 prose-ul:my-1 prose-ol:my-1 prose-strong:text-gray-100 max-w-none text-xs">
                <ReactMarkdown remarkPlugins={[remarkGfm]}>
                  {summary}
                </ReactMarkdown>
              </div>
            ) : (
              <div className="text-xs text-gray-400">
                {hasSelection ? "Click Summarize to analyze this cluster." : "Select nodes and edges to summarize."}
              </div>
            )}
            <div ref={scrollAnchorRef} />
          </ScrollArea>
          {error && <div className="text-xs text-red-400">{error}</div>}
        </CardContent>
      </Card>
    </div>
  )
}
