// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"errors"
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

type (
	entity struct {
		comm  Communicator
		stage Stage
	}
	registry struct {
		m   map[string]*entity
		mtx sync.RWMutex
	}
)

var (
	reg       *registry
	reqSecret string
)

func init() {
	reg = &registry{m: make(map[string]*entity)}
	reqSecret = cos.CryptoRandS(10)
}

func (r *registry) add(name string, c Communicator) (err error) {
	r.mtx.Lock()
	if _, ok := r.m[name]; ok {
		err = fmt.Errorf("etl[%s] already exists", name)
	} else {
		r.m[name] = &entity{c, Initializing}
	}
	r.mtx.Unlock()
	return
}

// transition return false if the entry does not exist or is already in the given stage
func (r *registry) transition(name string, stage Stage) (updated bool) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	entry, exists := r.m[name]
	if !exists || entry.stage == stage {
		return false
	}

	entry.stage = stage
	r.m[name] = entry
	return true
}

func (r *registry) get(name string) (c Communicator, stage Stage) {
	r.mtx.RLock()
	if en, exists := r.m[name]; exists {
		c = en.comm
		stage = en.stage
	}
	r.mtx.RUnlock()
	return c, stage
}

func (r *registry) getByXid(xid string) (c Communicator, stage Stage) {
	r.mtx.RLock()
	for _, en := range r.m {
		if en.comm.Xact().ID() == xid {
			c = en.comm
			stage = en.stage
		}
	}
	r.mtx.RUnlock()
	return c, stage
}

func (r *registry) del(name string) (exists bool) {
	debug.Assert(name != "")
	r.mtx.Lock()
	if _, exists = r.m[name]; exists {
		delete(r.m, name)
	}
	r.mtx.Unlock()
	return exists
}

func (r *registry) list() []Info {
	r.mtx.RLock()
	etls := make([]Info, 0, len(r.m))
	for name, en := range r.m {
		etls = append(etls, Info{
			Name:     name,
			Stage:    en.stage.String(),
			XactID:   en.comm.Xact().ID(),
			ObjCount: en.comm.ObjCount(),
			InBytes:  en.comm.InBytes(),
			OutBytes: en.comm.OutBytes(),
		})
	}
	r.mtx.RUnlock()
	return etls
}

func CheckSecret(secret string) error {
	if secret != reqSecret {
		return errors.New("unrecognized request source")
	}
	return nil
}
