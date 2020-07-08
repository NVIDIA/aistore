// Package query provides interface to iterate over objects with additional filtering
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

import (
	"sync"
)

type (
	TransformRegistry struct {
		m   map[string]string
		mtx sync.RWMutex
	}
)

var Registry = newTransformRegistry()

func newTransformRegistry() *TransformRegistry {
	return &TransformRegistry{
		m: make(map[string]string),
	}
}

func (r *TransformRegistry) Put(uuid, url string) {
	if uuid == "" {
		return
	}
	r.mtx.Lock()
	r.m[uuid] = url
	r.mtx.Unlock()
}

func (r *TransformRegistry) Get(uuid string) (url string) {
	r.mtx.RLock()
	url = r.m[url]
	r.mtx.RUnlock()
	return
}

func (r *TransformRegistry) Delete(uuid string) {
	r.mtx.Lock()
	delete(r.m, uuid)
	r.mtx.Unlock()
}
