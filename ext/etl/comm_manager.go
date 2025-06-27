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
	// - `Stopped`: Kubernetes resources (pod/service) are cleaned up.
	manager struct {
		m   map[string]Communicator
		mtx sync.RWMutex
	}
)

var mgr *manager

func Tinit() {
	mgr = &manager{m: make(map[string]Communicator, 4)}
	xreg.RegNonBckXact(&factory{})
}

func (r *manager) add(name string, c Communicator) (err error) {
	r.mtx.Lock()
	if _, ok := r.m[name]; ok {
		err = fmt.Errorf("etl[%s] already exists", name)
	} else {
		r.m[name] = c
	}
	r.mtx.Unlock()
	return err
}

func (r *manager) getByName(name string) Communicator {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if comm, exists := r.m[name]; exists {
		return comm
	}
	return r.m[name]
}

func (r *manager) getByXid(xid string) Communicator {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	for _, comm := range r.m {
		if comm.Xact().ID() == xid {
			return comm
		}
	}
	return nil
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
	for name, comm := range r.m {
		etls = append(etls, Info{
			Name:     name,
			Stage:    Running.String(), // must be in the running stage as long as the target's comm manager includes it in the list
			XactID:   comm.Xact().ID(),
			ObjCount: comm.ObjCount(),
			InBytes:  comm.InBytes(),
			OutBytes: comm.OutBytes(),
		})
	}
	r.mtx.RUnlock()
	return etls
}

func ValidateSecret(etlName, secret string) error {
	mgr.mtx.RLock()
	defer mgr.mtx.RUnlock()

	comm, ok := mgr.m[etlName]
	if !ok {
		return cos.NewErrNotFound(core.T, "etl not found"+etlName)
	}
	if comm.GetSecret() == secret {
		return nil
	}

	return errors.New("unrecognized request source")
}
