// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"net/url"
	"sync"
)

var qpool sync.Pool

func qalloc() url.Values {
	v := qpool.Get()
	if v != nil {
		return v.(url.Values)
	}
	return make(url.Values, 4) // NOTE: 4
}

func qfree(v url.Values) {
	clear(v)
	qpool.Put(v)
}
