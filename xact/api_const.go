// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact

// constant prefixes for assorted xaction kinds

const (
	PrefixDnlID = "dnl-" // http downloader (not to confuse with blob-downloader)
	PrefixEtlID = "etl-" // ETL
	PrefixTcoID = "tco-" // transform/copy objects
	PrefixInvID = "inv-" // create bucket inventory (NBI)
	PrefixGbtID = "gbt-" // get-batch (internally, x-moss)
	PrefixSrtID = "srt-" // conditional linkage (build tag "dsort")

	// evict
	PrefixEvictKeepID   = "kpmd-"
	PrefixEvictRemoveID = "rmmd-"
)
