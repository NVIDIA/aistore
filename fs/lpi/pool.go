// Package lpi: local page iterator
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package lpi

import "sync"

var pool sync.Pool

func allocPage() Page {
	v := pool.Get()
	if v != nil {
		return v.(Page)
	}
	return make(Page, 1000)
}

func freePage(v Page) {
	clear(v)
	pool.Put(v)
}
