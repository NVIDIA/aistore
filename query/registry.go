// Package query provides interface to iterate over objects with additional filtering
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package query

import (
	"sync"
)

type (
	QueryRegistry struct {
		m   map[string]*ObjectsListingXact
		mtx sync.RWMutex
	}
)

var Registry = newQueryRegistry()

func newQueryRegistry() *QueryRegistry {
	return &QueryRegistry{
		m: make(map[string]*ObjectsListingXact),
	}
}

func (r *QueryRegistry) Put(handle string, query *ObjectsListingXact) {
	if handle == "" {
		return
	}
	r.mtx.Lock()
	r.m[handle] = query
	r.mtx.Unlock()
}

func (r *QueryRegistry) Get(handle string) (x *ObjectsListingXact) {
	r.mtx.RLock()
	x = r.m[handle]
	r.mtx.RUnlock()
	return
}

func (r *QueryRegistry) Delete(handle string) {
	r.mtx.Lock()
	delete(r.m, handle)
	r.mtx.Unlock()
}
