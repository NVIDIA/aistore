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
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	// entity represents an ETL instance managed by an individual target.
	// - Created and added to the manager before entering the `Initializing` stage.
	// - Removed only after the user explicitly deletes it from the `Stopped` stage.
	//
	// Expected state transitions:
	// - `Initializing`: Set up resources in the following order:
	//     1. Create (or reuse) communicator and pod watcher
	//     2. Start (or renew) xaction
	//     3. Create Kubernetes resources (pod/service)
	// - `Running`: All resources are active, handling inline and offline transform requests via the communicator.
	// - `Stopped`: Kubernetes resources (pod/service) are cleaned up, but the communicator,
	//   pod watcher, and xaction remain available. This allows the ETL to be restarted by
	//   reusing these components during the initialization process.
	entity struct {
		comm  Communicator // TODO: decouple xaction and pod watcher from Communicator.
		stage Stage
	}
	manager struct {
		m   map[string]*entity
		mtx sync.RWMutex
	}
)

var mgr *manager

// called during target initialization
func Tinit() {
	mgr = &manager{m: make(map[string]*entity, 4)}
	xreg.RegNonBckXact(&factory{})
}

func (r *manager) add(name string, c Communicator) (err error) {
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
func (r *manager) transition(name string, stage Stage) (updated bool) {
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

func (r *manager) getByName(name string) (c Communicator, stage Stage) {
	r.mtx.RLock()
	if en, exists := r.m[name]; exists {
		c = en.comm
		stage = en.stage
	}
	r.mtx.RUnlock()
	return c, stage
}

func (r *manager) getByXid(xid string) (c Communicator, stage Stage) {
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

func (r *manager) del(name string) (exists bool) {
	debug.Assert(name != "")
	r.mtx.Lock()
	if _, exists = r.m[name]; exists {
		delete(r.m, name)
	}
	r.mtx.Unlock()
	return exists
}

func (r *manager) list() []Info {
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

func ValidateSecret(etlName, secret string) error {
	mgr.mtx.RLock()
	defer mgr.mtx.RUnlock()

	en, ok := mgr.m[etlName]
	if !ok {
		return cos.NewErrNotFound(core.T, "etl not found"+etlName)
	}
	if en.comm.GetSecret() == secret {
		return nil
	}

	return errors.New("unrecognized request source")
}
