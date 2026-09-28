// Package apc: API control messages and constants
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

// RechunkMsg parameterizes an in-place transform of a bucket's objects
// between monolithic and chunked storage formats. The target layout comes
// from the bucket's `chunks` property.
//
// Rechunk operates only on in-cluster (cached) objects and does not
// fetch from remote backends. Set `sync-remote` to also write the
// rechunked result back to the remote backend.
type RechunkMsg struct {
	// Rechunk only objects whose name starts with this prefix. Empty
	// applies to all objects in the bucket.
	Prefix string `json:"prefix"` // +gen:optional
	// Also write rechunked objects back to the remote backend.
	SyncRemote bool `json:"sync-remote"` // +gen:optional
}
