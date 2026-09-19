// Package apc: API control messages and constants
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

// RechunkMsg parameterizes an in-place transform of a bucket's objects
// between monolithic and chunked storage formats.
//
// Rechunk operates only on in-cluster (cached) objects and does not
// fetch from remote backends. Set `sync-remote` to also write the
// rechunked result back to the remote backend.
type RechunkMsg struct {
	// Object-size threshold (bytes):
	//   - `0`: Disable chunking; restore any existing chunked objects
	//     to monolithic form.
	//   - `>0`: Rechunk objects at or above this size.
	//
	// Deprecated: the bucket's `chunks.objsize_limit` is authoritative;
	// set it via bucket props, then run rechunk. Removal planned for v5.2
	// (see docs/relnotes/5.1.md, "Deprecated APIs").
	ObjSizeLimit int64 `json:"objsize-limit"` // +gen:optional
	// Target chunk size in bytes.
	//
	// Deprecated: the bucket's `chunks.chunk_size` is authoritative;
	// set it via bucket props, then run rechunk. Removal planned for v5.2
	// (see docs/relnotes/5.1.md, "Deprecated APIs").
	ChunkSize int64 `json:"chunk-size"` // +gen:optional
	// Rechunk only objects whose name starts with this prefix. Empty
	// applies to all objects in the bucket.
	Prefix string `json:"prefix"` // +gen:optional
	// Also write rechunked objects back to the remote backend.
	SyncRemote bool `json:"sync-remote"` // +gen:optional
}
