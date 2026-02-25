// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact

// constant prefixes for assorted xaction kinds

const (
	PrefixDnlID = "dnl-"
	PrefixEtlID = "etl-"
	PrefixTcoID = "tco-"
	PrefixInvID = "inv-"
	PrefixSrtID = "srt-" // build tag "dsort"

	// evict
	PrefixEvictKeepID   = "kpmd-"
	PrefixEvictRemoveID = "rmmd-"
)
