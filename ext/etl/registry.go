// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

type (
	registry struct {
		m   map[string]Communicator
		mtx sync.RWMutex
	}
)

var (
	reg       *registry
	reqSecret string
)

func init() {
	reg = &registry{m: make(map[string]Communicator)}
	reqSecret = cos.RandStringStrong(10)
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

func (r *registry) del(name string) (c Communicator) {
	var ok bool
	debug.Assert(name != "")
	r.mtx.Lock()
	if c, ok = r.m[name]; ok {
		delete(r.m, name)
	}
	r.mtx.Unlock()
	return c
}

func (r *registry) list() []Info {
	r.mtx.RLock()
	etls := make([]Info, 0, len(r.m))
	for name, comm := range r.m {
		etls = append(etls, Info{
			Name:     name,
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
		return fmt.Errorf("unrecognized request source")
	}
	return nil
}
