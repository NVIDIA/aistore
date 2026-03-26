// Package apc: API control messages and constants
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

// swagger:model
// Rechunk transforms object storage format (monolithic <-> chunked).
// By default, rechunk operates only on in-cluster (cached) objects - it does not
// fetch objects from remote backends. Use `SyncRemote=true` to also update remote storage.
type RechunkMsg struct {
	ObjSizeLimit int64  `json:"objsize-limit"` // 0: disable chunking (restore existing chunks to monolithic); > 0: chunk objects >= this size
	ChunkSize    int64  `json:"chunk-size"`    // size of each chunk
	Prefix       string `json:"prefix"`        // only rechunk objects with this prefix
	SyncRemote   bool   `json:"sync-remote"`   // if true, also write rechunked objects to remote backend (default: false, local-only)
}
