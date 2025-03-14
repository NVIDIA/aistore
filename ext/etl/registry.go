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
	registry struct {
		m   map[string]Communicator // primary index
		mtx sync.RWMutex
	}
)

var (
	reg       *registry
	reqSecret string
)

func init() {
	reg = &registry{m: make(map[string]Communicator)}
	reqSecret = cos.CryptoRandS(10)
}

func (r *registry) add(name string, c Communicator) (err error) {
	r.mtx.Lock()
	if _, ok := r.m[name]; ok {
		err = fmt.Errorf("etl[%s] already exists", name)
	} else {
		r.m[name] = c
	}
	r.mtx.Unlock()
	return
}

func (r *registry) get(name string) (c Communicator, exists bool) {
	r.mtx.RLock()
	c, exists = r.m[name]
	r.mtx.RUnlock()
	return
}

func (r *registry) getByXid(xid string) (c Communicator, exists bool) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	for _, c := range r.m {
		if c.Xact().ID() == xid {
			return c, true
		}
	}
	return nil, false
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

func (r *registry) delByXid(xid string) (exists bool) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	for name, c := range r.m {
		if c.Xact().ID() == xid {
			delete(r.m, name)
			return true
		}
	}
	return false
}

func (r *registry) list() []Info {
	r.mtx.RLock()
	etls := make([]Info, 0, len(r.m))
	for name, comm := range r.m {
		etls = append(etls, Info{
			Name:     name,
			Stage:    Running.String(), // for now, targets should only manage and report "Running" ETL instances
			XactID:   comm.Xact().ID(),
			ObjCount: comm.ObjCount(),
			InBytes:  comm.InBytes(),
			OutBytes: comm.OutBytes(),
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
