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
	etlInstance struct {
		comm Communicator
		boot *etlBootstrapper // TODO: move all bootstrapper logic to proxy
	}
	manager struct {
		m   map[string]etlInstance
		mtx sync.RWMutex
	}
)

var mgr *manager

func Tinit() {
	mgr = &manager{m: make(map[string]etlInstance, 4)}
	xreg.RegNonBckXact(&factory{})
}

func (r *manager) add(name string, c Communicator, boot *etlBootstrapper) (err error) {
	r.mtx.Lock()
	if _, ok := r.m[name]; ok {
		err = fmt.Errorf("etl[%s] already exists", name)
	} else {
		r.m[name] = etlInstance{comm: c, boot: boot}
	}
	r.mtx.Unlock()
	return err
}

func (r *manager) getByName(name string) (Communicator, *etlBootstrapper) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if ei, exists := r.m[name]; exists {
		return ei.comm, ei.boot
	}
	return nil, nil
}

func (r *manager) getByXid(xid string) Communicator {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	for _, ei := range r.m {
		if ei.comm.Xact().ID() == xid {
			return ei.comm
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
	for name, ei := range r.m {
		etls = append(etls, Info{
			Name:     name,
			Stage:    Running.String(), // must be in the running stage as long as the target's comm manager includes it in the list
			XactID:   ei.comm.Xact().ID(),
			ObjCount: ei.comm.ObjCount(),
			InBytes:  ei.comm.InBytes(),
			OutBytes: ei.comm.OutBytes(),
		})
	}
	r.mtx.RUnlock()
	return etls
}

func ValidateSecret(etlName, secret string) error {
	mgr.mtx.RLock()
	defer mgr.mtx.RUnlock()

	ei, ok := mgr.m[etlName]
	if !ok {
		return cos.NewErrNotFound(core.T, "etl not found"+etlName)
	}
	if ei.comm.GetSecret() == secret {
		return nil
	}

	return errors.New("unrecognized request source")
}
