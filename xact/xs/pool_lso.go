// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
)

// TODO: consider a separate pool for ais

const maxEntries = apc.DefaultPageSizeCloud

var (
	lstPool sync.Pool
	entry0  cmn.LsoEntry
)

func allocLsoEntries() (entries cmn.LsoEntries) {
	if v := lstPool.Get(); v != nil {
		entries = *v.(*cmn.LsoEntries)
	}
	return
}

func freeLsoEntries(entries cmn.LsoEntries) {
	// gc
	l := min(len(entries), maxEntries)
	entries = entries[:cap(entries)]
	for i := l; i < cap(entries); i++ {
		entries[i] = nil
	}
	// truncate
	entries = entries[:l]
	// cleanup
	for _, e := range entries {
		*e = entry0
	}
	// recycle
	lstPool.Put(&entries)
}
