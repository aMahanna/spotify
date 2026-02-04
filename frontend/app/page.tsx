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
import { Network, CuboidIcon } from "lucide-react"
import { Button } from "@/components/ui/button"
import { VisualizeTab } from "@/components/tabs/VisualizeTab"

export default function Home() {
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

        <VisualizeTab />
      </main>
    </div>
  )
}

