// Package transform provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

import (
	"sync"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	pushPullCommType = "ppc"
	putCommType      = "putc"
	redirectCommType = "redir"
)

type (
	registry struct {
		m   map[string]Communicator
		mtx sync.RWMutex
	}
)

var reg = newRegistry()

func newRegistry() *registry {
	return &registry{
		m: make(map[string]Communicator),
	}
}

func (r *registry) put(uuid string, c Communicator) {
	cmn.Assert(uuid != "")
	r.mtx.Lock()
	r.m[uuid] = c
	r.mtx.Unlock()
}

func (r *registry) get(uuid string) (c Communicator, exists bool) {
	r.mtx.RLock()
	c, exists = r.m[uuid]
	r.mtx.RUnlock()
	return
}
