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
	// Transformer receives POST request from target with the data. It must read
	// the data and return response to the target which then will be transferred
	// to the client.
	putCommType = "hpush://"
	// Target redirects the GET request to the transformer. Then transformer
	// contacts the target via `AIS_TARGET_URL` env variable to get the data.
	// The data is then transformed and returned to the client.
	redirectCommType = "hpull://"
	// Similar to redirection strategy but with usage of reverse proxy.
	revProxyCommType = "hrev://" // TODO: try to think on different naming, it is not much different than redirect
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
