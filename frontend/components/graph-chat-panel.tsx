"use client"

import { useEffect, useRef, useState } from "react"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { ScrollArea } from "@/components/ui/scroll-area"
import { ChevronDown, ChevronUp, GripVertical } from "lucide-react"
import { buildTourOrder, DEFAULT_TOUR_NODE_COUNT } from "@/lib/graph-tour"

type ChatMessage = {
  role: "user" | "assistant"
  content: string
}

type GraphChatPanelProps = {
  graphData: any
  onTourStart?: (tourOrder?: string[]) => void
  tourStepMs?: number
  tourSignal?: {
    type: "step" | "done" | "stop"
    nodeId?: string
    index?: number
    total?: number
    nonce: number
  } | null
}

const FIXED_QUESTIONS = [
  { id: "themes", label: "What are the themes around this playlist?" },
  { id: "collabs", label: "Which artists have worked together that are part of this playlist?" },
  { id: "fun_facts", label: "What is a fun fact about this playlist?" },
  { id: "tour", label: "Give me a Tour" },
]

export function GraphChatPanel({ graphData, onTourStart, tourSignal, tourStepMs = 3200 }: GraphChatPanelProps) {
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [isStreaming, setIsStreaming] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [isMinimized, setIsMinimized] = useState(true)
  const [position, setPosition] = useState<{ x: number; y: number } | null>(null)
  const [isDragging, setIsDragging] = useState(false)
  const assistantIndexRef = useRef<number | null>(null)
  const scrollAnchorRef = useRef<HTMLDivElement | null>(null)
  const panelRef = useRef<HTMLDivElement | null>(null)
  const dragOffsetRef = useRef<{ x: number; y: number } | null>(null)
  const tourTextBufferRef = useRef("")
  const tourLinesRef = useRef<string[]>([])
  const pendingTourStepsRef = useRef(0)
  const tourResponseDoneRef = useRef(false)
  const tourTimeoutsRef = useRef<number[]>([])
  const tourTypingRef = useRef(false)
  const TOUR_END_BUFFER_MS = 600
  const TOUR_CHAR_MIN_MS = 4
  const TOUR_CHAR_MAX_MS = 18

  const LoadingDots = () => (
    <span className="inline-flex items-center gap-0.5">
      <span className="h-1 w-1 rounded-full bg-gray-300 animate-bounce [animation-delay:-0.2s]" />
      <span className="h-1 w-1 rounded-full bg-gray-300 animate-bounce [animation-delay:-0.1s]" />
      <span className="h-1 w-1 rounded-full bg-gray-300 animate-bounce" />
    </span>
  )

  useEffect(() => {
    scrollAnchorRef.current?.scrollIntoView({ behavior: "smooth" })
  }, [messages, isStreaming])

  useEffect(() => {
    if (position || !panelRef.current) return
    const rect = panelRef.current.getBoundingClientRect()
    const fallbackY = Math.max(16, window.innerHeight - rect.height - 16)
    setPosition({ x: 16, y: fallbackY })
  }, [position])

  useEffect(() => {
    if (!panelRef.current || isMinimized || !position) return

    const clampToViewport = () => {
      if (!panelRef.current) return
      const rect = panelRef.current.getBoundingClientRect()
      const maxX = Math.max(16, window.innerWidth - rect.width - 16)
      const maxY = Math.max(16, window.innerHeight - rect.height - 16)
      const nextX = Math.min(Math.max(16, position.x), maxX)
      const nextY = Math.min(Math.max(16, position.y), maxY)
      if (nextX !== position.x || nextY !== position.y) {
        setPosition({ x: nextX, y: nextY })
      }
    }

    const frame = window.requestAnimationFrame(clampToViewport)
    return () => window.cancelAnimationFrame(frame)
  }, [isMinimized, position])

  useEffect(() => {
    const handleMouseMove = (event: MouseEvent) => {
      if (!isDragging || !panelRef.current || !dragOffsetRef.current) return
      const rect = panelRef.current.getBoundingClientRect()
      const nextX = event.clientX - dragOffsetRef.current.x
      const nextY = event.clientY - dragOffsetRef.current.y
      const maxX = Math.max(16, window.innerWidth - rect.width - 16)
      const maxY = Math.max(16, window.innerHeight - rect.height - 16)
      setPosition({
        x: Math.min(Math.max(16, nextX), maxX),
        y: Math.min(Math.max(16, nextY), maxY),
      })
    }

    const handleMouseUp = () => {
      setIsDragging(false)
    }

    if (isDragging) {
      window.addEventListener("mousemove", handleMouseMove)
      window.addEventListener("mouseup", handleMouseUp)
    }

    return () => {
      window.removeEventListener("mousemove", handleMouseMove)
      window.removeEventListener("mouseup", handleMouseUp)
    }
  }, [isDragging])

  const handleDragStart = (event: React.MouseEvent<HTMLDivElement>) => {
    if (!panelRef.current) return
    const rect = panelRef.current.getBoundingClientRect()
    dragOffsetRef.current = {
      x: event.clientX - rect.left,
      y: event.clientY - rect.top,
    }
    setIsDragging(true)
  }

  const buildEdgesPayload = () => {
    if (Array.isArray(graphData?.edges) && graphData.edges.length > 0) {
      return graphData.edges
    }
    if (Array.isArray(graphData?.links) && graphData.links.length > 0) {
      return graphData.links.map((link: any) => ({
        source: link.source ?? link._from ?? "",
        target: link.target ?? link._to ?? "",
        label: link.name ?? link.label ?? link.predicate ?? "",
        id: link.id ?? link._id ?? undefined,
      }))
    }
    return []
  }

  const appendAssistantDelta = (delta: string) => {
    setMessages((prev) => {
      const next = [...prev]
      const idx = assistantIndexRef.current
      if (idx !== null && next[idx]) {
        next[idx] = { ...next[idx], content: next[idx].content + delta }
      }
      return next
    })
  }

  const processTourQueue = () => {
    if (tourTypingRef.current) return
    if (pendingTourStepsRef.current <= 0) return
    if (tourLinesRef.current.length === 0) return

    const nextLine = tourLinesRef.current.shift()
    if (!nextLine) return

    const availableMs = Math.max(600, tourStepMs - TOUR_END_BUFFER_MS)
    const perChar = Math.min(
      TOUR_CHAR_MAX_MS,
      Math.max(TOUR_CHAR_MIN_MS, Math.floor(availableMs / Math.max(nextLine.length, 1)))
    )

    tourTypingRef.current = true
    let index = 0

    const tick = () => {
      if (!tourTypingRef.current) return
      if (index < nextLine.length) {
        appendAssistantDelta(nextLine[index])
        index += 1
        const timeoutId = window.setTimeout(tick, perChar)
        tourTimeoutsRef.current.push(timeoutId)
        return
      }
      appendAssistantDelta("\n")
      tourTypingRef.current = false
      pendingTourStepsRef.current -= 1
      processTourQueue()
    }

    tick()
  }

  const flushPendingTourLines = () => {
    processTourQueue()
  }

  const enqueueTourLine = () => {
    pendingTourStepsRef.current += 1
    processTourQueue()
  }

  const ingestTourText = (delta: string) => {
    tourTextBufferRef.current += delta
    const parts = tourTextBufferRef.current.split("\n")
    tourTextBufferRef.current = parts.pop() || ""
    const cleaned = parts.map((line) => line.trim()).filter(Boolean)
    if (cleaned.length > 0) {
      tourLinesRef.current.push(...cleaned)
      processTourQueue()
    }
  }

  const handleSend = async (questionId: string, questionLabel: string) => {
    if (!questionId || isStreaming) return

    setError(null)
    setIsStreaming(true)
    const isTour = questionId === "tour"

    setMessages((prev) => {
      const next = [...prev, { role: "user", content: questionLabel }, { role: "assistant", content: "" }]
      assistantIndexRef.current = next.length - 1
      return next
    })

    try {
      const tourOrder =
        isTour ? buildTourOrder(graphData, DEFAULT_TOUR_NODE_COUNT) : undefined
      if (isTour) {
        tourTextBufferRef.current = ""
        tourLinesRef.current = []
        pendingTourStepsRef.current = 0
        tourResponseDoneRef.current = false
        tourTimeoutsRef.current.forEach((timeoutId) => window.clearTimeout(timeoutId))
        tourTimeoutsRef.current = []
        tourTypingRef.current = false
        if (onTourStart) {
          onTourStart(tourOrder)
        } else {
          window.postMessage(
            { type: "graph-tour-start", tourOrder },
            window.location.origin
          )
        }
      }
      const edgesPayload = buildEdgesPayload()
      const response = await fetch("http://localhost:5000/api/chat/stream", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question_id: questionId,
          nodes: graphData?.nodes || [],
          edges: edgesPayload,
          triples: graphData?.triples || [],
          tour_order: tourOrder,
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
            const delta = parsed.delta
            if (isTour) {
              ingestTourText(delta)
            } else {
              appendAssistantDelta(delta)
            }
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
      if (isTour) {
        tourResponseDoneRef.current = true
        flushPendingTourLines()
      } else {
        setIsStreaming(false)
      }
    }
  }

  useEffect(() => {
    if (!tourSignal) return
    if (tourSignal.type === "step") {
      enqueueTourLine()
      return
    }
    if (tourSignal.type === "done") {
      if (tourResponseDoneRef.current) {
        setIsStreaming(false)
      }
      return
    }
    if (tourSignal.type === "stop") {
      setIsStreaming(false)
    }
  }, [tourSignal])

  useEffect(() => {
    return () => {
      tourTimeoutsRef.current.forEach((timeoutId) => window.clearTimeout(timeoutId))
      tourTimeoutsRef.current = []
    }
  }, [])

  return (
    <div
      ref={panelRef}
      className="fixed z-50 w-[360px]"
      style={position ? { left: position.x, top: position.y } : undefined}
    >
      <Card className="bg-black/90 border-gray-700 text-white shadow-lg">
        <CardHeader
          className="pb-2 cursor-move select-none"
          onMouseDown={handleDragStart}
        >
          <div className="flex items-start justify-between gap-3">
            <div className="flex items-center gap-2">
              <GripVertical className="h-4 w-4 text-gray-400" />
              <div>
                <CardTitle className="text-sm">Playlist Chat</CardTitle>
                <CardDescription className="text-xs text-gray-400">
                  Ask questions about the current knowledge graph.
                </CardDescription>
              </div>
            </div>
            <button
              type="button"
              aria-label={isMinimized ? "Expand chat" : "Minimize chat"}
              onClick={() => setIsMinimized((prev) => !prev)}
              onMouseDown={(event) => event.stopPropagation()}
              className="rounded-md border border-gray-700 bg-gray-900/70 p-1 text-gray-200 hover:bg-gray-800"
            >
              {isMinimized ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
            </button>
          </div>
        </CardHeader>
        {!isMinimized && (
          <CardContent className="space-y-3">
          <ScrollArea className="h-[240px] pr-3">
            <div className="space-y-3 text-sm">
              {messages.length === 0 && (
                <div className="text-xs text-gray-400">
              Choose one of the questions below.
                </div>
              )}
              {messages.map((message, index) => (
                <div
                  key={`${message.role}-${index}`}
                  className={message.role === "user" ? "text-right" : "text-left"}
                >
                  <div
                    className={
                      message.role === "user"
                        ? "inline-block rounded-lg bg-blue-600/70 px-3 py-2 text-xs text-white"
                        : "inline-block rounded-lg bg-gray-800/80 px-3 py-2 text-xs text-gray-100"
                    }
                  >
                    {message.role === "assistant" ? (
                      message.content ? (
                        <div className="prose prose-invert prose-p:my-1 prose-li:my-1 prose-ul:my-1 prose-ol:my-1 prose-strong:text-gray-100 max-w-none text-xs">
                          <ReactMarkdown remarkPlugins={[remarkGfm]}>
                            {message.content}
                          </ReactMarkdown>
                        </div>
                      ) : isStreaming && index === messages.length - 1 ? (
                        <LoadingDots />
                      ) : (
                        ""
                      )
                    ) : (
                      message.content || (isStreaming && index === messages.length - 1 ? <LoadingDots /> : "")
                    )}
                  </div>
                </div>
              ))}
              <div ref={scrollAnchorRef} />
            </div>
          </ScrollArea>

          {error && <div className="text-xs text-red-400">{error}</div>}

          <div className="flex flex-wrap gap-2">
            {FIXED_QUESTIONS.map((question) => (
              <button
                key={question.id}
                type="button"
                disabled={isStreaming}
                onClick={() => handleSend(question.id, question.label)}
                className="rounded-md border border-gray-700 bg-gray-900/70 px-2 py-1 text-[11px] text-gray-200 hover:bg-gray-800"
              >
                {question.label}
              </button>
            ))}
          </div>
        </CardContent>
        )}
      </Card>
    </div>
  )
}
