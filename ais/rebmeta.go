// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

// Rebalance metadata is distributed to start rebalance. We must do it:
// - when new target(s) joins the cluster - classical case
// - at startup when cluster starts with unfinished rebalance (was aborted)
// - when we unregister a target and some bucket(s) use EC - we must redistribute
//   the slices
// - on bucket rename:
//    1. bucket is renamed (and the paths of the objects change)
//    2. rebalance must be started to redistribute the objects to the targets
//       depending on HRW
// - when requested by the user - `ais start rebalance` or via HTTP API

const (
	rmdFname = ".ais.rmd" // rmd persistent file basename
)

type (
	// rebMD is revs (see metasync) which is distributed by primary proxy to
	// the targets. It is distributed when some kind of rebalance is required.
	rebMD struct {
		cluster.RMD
	}

	// rmdOwner is used to keep the information about the rebalances. Currently
	// it keeps the Version of the latest rebalance.
	rmdOwner struct {
		sync.Mutex
		rmd atomic.Pointer
	}
)

// interface guard
var _ revs = &rebMD{}

func (r *rebMD) tag() string     { return revsRMDTag }
func (r *rebMD) version() int64  { return r.Version }
func (r *rebMD) marshal() []byte { return cmn.MustMarshal(r) }
func (r *rebMD) inc()            { r.Version++ }
func (r *rebMD) clone() *rebMD {
	dst := &rebMD{}
	cmn.CopyStruct(dst, r)
	return dst
}

func (r *rebMD) String() string {
	if r == nil {
		return "RMD <nil>"
	}
	return fmt.Sprintf("RMD v%d", r.Version)
}

func newRMDOwner() *rmdOwner {
	rmdo := &rmdOwner{}
	rmdo.put(&rebMD{})
	return rmdo
}

func (r *rmdOwner) persist(rmd *rebMD) {
	rmdPathName := filepath.Join(cmn.GCO.Get().Confdir, rmdFname)
	if err := jsp.Save(rmdPathName, rmd, jsp.CCSign()); err != nil {
		glog.Errorf("error writing rmd to %s: %v", rmdPathName, err)
	}
}

func (r *rmdOwner) load() {
	err := jsp.Load(filepath.Join(cmn.GCO.Get().Confdir, rmdFname), &r.rmd, jsp.CCSign())
	if err != nil && !os.IsNotExist(err) {
		glog.Errorf("failed to load rmd: %v", err)
	}
}

func (r *rmdOwner) put(rmd *rebMD) { r.rmd.Store(unsafe.Pointer(rmd)) }
func (r *rmdOwner) get() *rebMD    { return (*rebMD)(r.rmd.Load()) }
func (r *rmdOwner) modify(ctx *rmdModifier) *rebMD {
	r.Lock()
	clone := r.get().clone()
	clone.TargetIDs = nil
	clone.Resilver = false
	ctx.pre(ctx, clone)
	r.persist(clone)
	r.put(clone)
	r.Unlock()
	if ctx.final != nil {
		ctx.final(ctx, clone)
	}
	return clone
}
