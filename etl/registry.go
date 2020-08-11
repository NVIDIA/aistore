// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	// The ETL container receives POST request from target with the data. It must read
	// the data and return response to the target which then will be transferred
	// to the client.
	PushCommType = "hpush://"
	// Target redirects the GET request to the ETL container. Then ETL container
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
	}
)

var reg = newRegistry()

func newRegistry() *registry {
	return &registry{
		byUUID: make(map[string]Communicator),
	}
}

func (r *registry) put(uuid string, c Communicator) error {
	cmn.Assert(uuid != "")
	r.mtx.Lock()
	defer r.mtx.Unlock()
	if _, ok := r.byUUID[uuid]; ok {
		return fmt.Errorf("ETL with uuid %q already exists", uuid)
	}
	r.byUUID[uuid] = c
	return nil
}

func (r *registry) getByUUID(uuid string) (c Communicator, exists bool) {
	r.mtx.RLock()
	c, exists = r.byUUID[uuid]
	r.mtx.RUnlock()
	return
}

func (r *registry) removeByUUID(uuid string) (c Communicator) {
	var (
		ok bool
	)
	cmn.Assert(uuid != "")
	r.mtx.Lock()
	if c, ok = r.byUUID[uuid]; ok {
		delete(r.byUUID, uuid)
	}
	r.mtx.Unlock()
	return c
}

func (r *registry) list() []Info {
	r.mtx.RLock()
	etls := make([]Info, 0, len(r.byUUID))
	for uuid, c := range r.byUUID {
		etls = append(etls, Info{
			ID:   uuid,
			Name: c.Name(),
		})
	}
	r.mtx.RUnlock()
	return etls
}
