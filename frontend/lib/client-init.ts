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
/**
 * Client-side initialization utilities
 * This file contains functions for initializing the application on the client side
 */

/**
 * Initialize default database settings if not already set
 * Called before syncing with server to ensure defaults are available
 * NOTE: Don't set graph_db_type here - let server's GRAPH_DB_TYPE env var control it
 */
export function initializeDefaultSettings() {
  if (typeof window === 'undefined') {
    return; // Only run on client side
  }

  // Don't set graph_db_type default - let it be controlled by server's GRAPH_DB_TYPE env var
  // The server will use its environment variable if no client setting is provided
  
  // Set default connection settings only (not the database type selection)
  if (!localStorage.getItem('arango_url')) {
    localStorage.setItem('arango_url', 'http://localhost:8529');
  }

  if (!localStorage.getItem('arango_db')) {
    localStorage.setItem('arango_db', 'txt2kg');
  }
  
  // Set default Neo4j settings
  if (!localStorage.getItem('neo4j_url')) {
    localStorage.setItem('neo4j_url', 'bolt://localhost:7687');
  }
}

/**
 * Synchronize settings from localStorage with the server
 * Called on app initialization to ensure server has access to client settings
 */
export async function syncSettingsWithServer() {
  if (typeof window === 'undefined') {
    return; // Only run on client side
  }

  // Initialize default settings first
  initializeDefaultSettings();

  return;
} 