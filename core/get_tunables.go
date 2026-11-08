// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import "github.com/NVIDIA/aistore/cmn/cos"

// In this source: fixed limits and defaults for reading remote objects:
// parallel cold GET, blob download, and the target-wide range-reader budget they share.
//
// - current hardcoded heuristics, not configuration
// - documented in docs/howto_large_cold_objects.md ("Limits and defaults"); keep both in sync
// - API bounds on the requested number of blob-download workers: apc.MinBlobWorkers, apc.MaxBlobWorkers

// parallel cold GET (ais/tgtcrange.go)
// - readers per GET: bucket-configurable chunks.cold_readers (see cmn.ColdReadersDflt)
const (
	ColdRangeMinSize = 16 * cos.MiB // minimum object size: amortize setup and additional backend requests
)

// media-type multipliers, per available mountpath
// - RRMult*: target-wide budget of extra range readers (see RR); SSD counts as NVMe
// - NwpMult*: default number of job workers (see xact.DefaultNW), incl. blob download
const (
	RRMultHDD  = 2  // rotational or slow (FUSE, overlay, AUFS)
	RRMultNVMe = 16 // NVMe and SSD

	NwpMultHDD  = 1 // ditto
	NwpMultSSD  = 4
	NwpMultNVMe = 8
)

// blob download (xact/xs)
const (
	MinBlobDlGetSize      = 128 * cos.KiB // GET: smaller objects use regular GET
	MinBlobDlPrefetchSize = cos.MiB       // prefetch: minimum blob threshold

	BlobDlChunkSize             = 4 * cos.MiB  // default, when neither the request nor (prefetch) the bucket sets it
	BlobDlMaxStreamingChunkSize = 64 * cos.MiB // streaming GET; ~625GiB maximum object size, up to 2GiB streaming SGLs

	BlobDlMinBytesPerWorker = 256 * cos.MiB // size-based worker cap, with a minimum of two
	BlobDlMaxWorkers        = 32            // absolute max workers for a single blob download
)
