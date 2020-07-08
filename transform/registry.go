// Package transform provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

import (
	"sync"
)

const (
	pushPullComm = "ppc"
	putComm      = "putc"
)

type (
	entry struct {
		url      string
		commType string
	}

	registry struct {
		m   map[string]entry
		mtx sync.RWMutex
	}
)

var reg = newRegistry()

func newRegistry() *registry {
	return &registry{
		m: make(map[string]entry),
	}
}

func (r *registry) put(uuid string, e entry) {
	if uuid == "" {
		return
	}
	r.mtx.Lock()
	r.m[uuid] = e
	r.mtx.Unlock()
}

func (r *registry) get(uuid string) (e entry) {
	r.mtx.RLock()
	e = r.m[uuid]
	r.mtx.RUnlock()
	return
}
