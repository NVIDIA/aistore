// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "github.com/NVIDIA/aistore/cmn/cos"

func DiscardFirstEntries(entries []*BucketEntry, n int) []*BucketEntry {
	if n == 0 {
		return entries
	}
	if n >= len(entries) {
		return entries[:0]
	}

	toDiscard := cos.Min(len(entries), n)

	copy(entries, entries[toDiscard:])
	for i := len(entries) - toDiscard; i < len(entries); i++ {
		entries[i] = nil
	}

	return entries[:len(entries)-toDiscard]
}
