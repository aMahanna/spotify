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

import Link from "next/link"
import { useEffect, useMemo, useState } from "react"
import { Network, CuboidIcon, Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { VisualizeTab } from "@/components/tabs/VisualizeTab"
import { useRouter, useSearchParams } from "next/navigation"

export default function Home() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const [playlistUrl, setPlaylistUrl] = useState("")
  const [jobId, setJobId] = useState<string | null>(null)
  const [jobGraphId, setJobGraphId] = useState<string | null>(null)
  const [jobStatus, setJobStatus] = useState<"idle" | "queued" | "running" | "ready" | "failed">("idle")
  const [jobError, setJobError] = useState<string | null>(null)
  const [enrichJobId, setEnrichJobId] = useState<string | null>(null)
  const [enrichStatus, setEnrichStatus] = useState<"idle" | "queued" | "running" | "ready" | "failed">("idle")
  const [enrichError, setEnrichError] = useState<string | null>(null)
  const [availablePlaylists, setAvailablePlaylists] = useState<
    { graph_id: string; playlist_url: string; playlist_name?: string }[]
  >([])
  const [selectedGraphId, setSelectedGraphId] = useState<string | null>(null)
  const [graphRefreshToken, setGraphRefreshToken] = useState<number>(0)

  const graphId = searchParams.get("graph_id")
  const effectiveGraphId = graphId || selectedGraphId

  const sortedPlaylists = useMemo(() => {
    return [...availablePlaylists].filter((item) => item.graph_id)
  }, [availablePlaylists])

  useEffect(() => {
    if (!jobId) return

    let isActive = true
    let interval: ReturnType<typeof setInterval> | null = null

    const pollStatus = async () => {
      try {
        const response = await fetch(`http://localhost:5000/api/playlist/status/${jobId}`)
        if (!response.ok) {
          const text = await response.text()
          throw new Error(text || `Status check failed: ${response.status}`)
        }
        const data = await response.json()
        const status = data?.status as typeof jobStatus
        if (!isActive) return
        setJobStatus(status || "running")

        if (status === "ready") {
          const resolvedGraphId = data?.graph_id || jobGraphId
          if (resolvedGraphId) {
            router.replace(`/?graph_id=${resolvedGraphId}`)
          }
          if (interval) clearInterval(interval)
          setJobId(null)
        } else if (status === "failed") {
          setJobError(data?.error || "Graph build failed")
          if (interval) clearInterval(interval)
          setJobId(null)
        }
      } catch (err) {
        if (!isActive) return
        setJobError(err instanceof Error ? err.message : "Failed to check job status")
        setJobStatus("failed")
        if (interval) clearInterval(interval)
        setJobId(null)
      }
    }

    interval = setInterval(pollStatus, 1500)
    pollStatus()
    return () => {
      isActive = false
      if (interval) clearInterval(interval)
    }
  }, [jobId, jobGraphId, router])

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
          if (effectiveGraphId) {
            router.replace(`/?graph_id=${effectiveGraphId}`)
          }
          setGraphRefreshToken(Date.now())
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
  }, [enrichJobId, effectiveGraphId, router])

  useEffect(() => {
    const loadPlaylists = async () => {
      try {
        const response = await fetch("http://localhost:5000/api/playlists")
        if (!response.ok) return
        const data = await response.json()
        const playlists = Array.isArray(data?.playlists) ? data.playlists : []
        setAvailablePlaylists(playlists)
      } catch {
        setAvailablePlaylists([])
      }
    }
    loadPlaylists()
  }, [jobId, jobStatus, enrichJobId, enrichStatus])

  const handlePlaylistSubmit = async (event: React.FormEvent) => {
    event.preventDefault()
    setJobError(null)
    setJobStatus("queued")
    setJobId(null)
    setJobGraphId(null)

    try {
      const response = await fetch("http://localhost:5000/api/playlist/build", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ playlist_url: playlistUrl })
      })
      if (!response.ok) {
        const text = await response.text()
        throw new Error(text || `Build request failed: ${response.status}`)
      }
      const data = await response.json()
      setJobId(data?.job_id || null)
      setJobGraphId(data?.graph_id || null)
      setJobStatus("running")
    } catch (err) {
      setJobError(err instanceof Error ? err.message : "Failed to start build")
      setJobStatus("failed")
    }
  }

  const isBuilding = jobStatus === "queued" || jobStatus === "running"
  const isEnriching = enrichStatus === "queued" || enrichStatus === "running"
  const handleSelectChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
    const value = event.target.value
    setSelectedGraphId(value || null)
    if (value) {
      router.replace(`/?graph_id=${value}`)
    } else {
      router.replace("/")
    }
  }

  const handleEnrich = async () => {
    if (!effectiveGraphId) return
    setEnrichError(null)
    setEnrichStatus("queued")
    setEnrichJobId(null)

    try {
      const response = await fetch("http://localhost:5000/api/playlist/enrich", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ graph_id: effectiveGraphId })
      })
      if (!response.ok) {
        const text = await response.text()
        throw new Error(text || `Enrich request failed: ${response.status}`)
      }
      const data = await response.json()
      setEnrichJobId(data?.job_id || null)
      setEnrichStatus("running")
    } catch (err) {
      setEnrichError(err instanceof Error ? err.message : "Failed to start enrichment")
      setEnrichStatus("failed")
    }
  }
  return (
    <div className="min-h-screen bg-background text-foreground">
      <main className="container mx-auto px-6 py-12 space-y-8">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold">txt2kg minimal</h1>
            <p className="text-sm text-muted-foreground">
              Visualization, clustering, and navigation powered by the backend graph.
            </p>
          </div>
          <div className="flex flex-wrap gap-3">
            <Button asChild variant="outline">
              <Link href="/graph" className="inline-flex items-center gap-2">
                <Network className="h-4 w-4" />
                2D Graph
              </Link>
            </Button>
            <Button asChild>
              <Link href="/graph3d?source=stored" className="inline-flex items-center gap-2">
                <CuboidIcon className="h-4 w-4" />
                3D + Clustering
              </Link>
            </Button>
          </div>
        </div>

        <div className="rounded-lg border border-border bg-card p-4">
          <h2 className="text-lg font-semibold mb-2">Generate Graph from Playlist</h2>
          <form onSubmit={handlePlaylistSubmit} className="flex flex-col gap-3 md:flex-row md:items-end">
            <div className="flex-1">
              <label htmlFor="playlist-url" className="block text-sm font-medium mb-1">
                Playlist URL
              </label>
              <input
                id="playlist-url"
                type="url"
                required
                value={playlistUrl}
                onChange={(event) => setPlaylistUrl(event.target.value)}
                className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm"
                placeholder="https://open.spotify.com/playlist/..."
              />
            </div>
            <Button type="submit" disabled={isBuilding}>
              {isBuilding ? (
                <span className="inline-flex items-center gap-2">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Building...
                </span>
              ) : (
                "Build Graph"
              )}
            </Button>
          </form>
          {isBuilding && (
            <p className="text-sm text-muted-foreground mt-2">
              Building knowledge graph… this may take a moment.
            </p>
          )}
          {jobError && (
            <p className="text-sm text-destructive mt-2">{jobError}</p>
          )}
        </div>

        <div className="rounded-lg border border-border bg-card p-4">
          <label htmlFor="graph-selector" className="block text-sm font-medium mb-2">
            Select a saved playlist
          </label>
          <select
            id="graph-selector"
            className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm"
            value={effectiveGraphId || ""}
            onChange={handleSelectChange}
          >
            <option value="">Most recent</option>
            {sortedPlaylists.map((item) => (
              <option key={item.graph_id} value={item.graph_id}>
                {item.playlist_name || item.playlist_url || item.graph_id}
              </option>
            ))}
          </select>
          <div className="mt-3 flex flex-wrap items-center gap-3">
            <Button
              type="button"
              variant="outline"
              onClick={handleEnrich}
              disabled={!effectiveGraphId || isEnriching}
            >
              {isEnriching ? (
                <span className="inline-flex items-center gap-2">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Enriching...
                </span>
              ) : (
                "Enrich Selected Graph"
              )}
            </Button>
            {enrichError && (
              <span className="text-sm text-destructive">{enrichError}</span>
            )}
          </div>
        </div>

        <VisualizeTab
          key={`${effectiveGraphId || "latest"}-${graphRefreshToken}`}
          graphId={effectiveGraphId || undefined}
          refreshToken={graphRefreshToken}
        />
      </main>
    </div>
  )
}

