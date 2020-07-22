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
	PushCommType = "hpush://"
	// Target redirects the GET request to the transformer. Then transformer
	// contacts the target via `AIS_TARGET_URL` env variable to get the data.
	// The data is then transformed and returned to the client.
	RedirectCommType = "hpull://"
	// Similar to redirection strategy but with usage of reverse proxy.
	RevProxyCommType = "hrev://" // TODO: try to think on different naming, it is not much different than redirect
)

type (
	registry struct {
		mtx    sync.RWMutex
		byUUID map[string]Communicator
		byName map[string]Communicator
	}
)

var reg = newRegistry()

func newRegistry() *registry {
	return &registry{
		byUUID: make(map[string]Communicator),
		byName: make(map[string]Communicator),
	}
}

func (r *registry) put(uuid string, c Communicator) {
	cmn.Assert(uuid != "")
	r.mtx.Lock()
	r.byUUID[uuid] = c
	r.byName[c.Name()] = c
	r.mtx.Unlock()
}

func (r *registry) getByUUID(uuid string) (c Communicator, exists bool) {
	r.mtx.RLock()
	c, exists = r.byUUID[uuid]
	r.mtx.RUnlock()
	return
}

func (r *registry) getByName(name string) (c Communicator, exists bool) {
	r.mtx.RLock()
	c, exists = r.byName[name]
	r.mtx.RUnlock()
	return
}

func (r *registry) removeByUUID(uuid string) {
	cmn.Assert(uuid != "")
	r.mtx.Lock()
	if c, ok := r.byUUID[uuid]; ok {
		delete(r.byUUID, uuid)
		delete(r.byName, c.Name())
	}
	r.mtx.Unlock()
}

func (r *registry) list() []TransformationInfo {
	r.mtx.RLock()
	transformations := make([]TransformationInfo, 0, len(r.byUUID))
	for uuid, c := range r.byUUID {
		if IsStaticTransformer(c.Name()) {
			uuid = "-" // Don't expose UUID of static transformers.
		}
		transformations = append(transformations, TransformationInfo{
			ID:   uuid,
			Name: c.Name(),
		})
	}
	r.mtx.RUnlock()
	return transformations
}
