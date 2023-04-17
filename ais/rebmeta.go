// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xact"
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
// - when requested by the user - `ais job start rebalance` or via HTTP API

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
		// global local atomic state
		interrupted atomic.Bool // when joining target reports interrupted rebalance
		starting    atomic.Bool // when starting up
	}
)

// interface guard
var _ revs = (*rebMD)(nil)

// as revs
func (*rebMD) tag() string       { return revsRMDTag }
func (r *rebMD) version() int64  { return r.Version }
func (r *rebMD) marshal() []byte { return cos.MustMarshal(r) }
func (*rebMD) jit(p *proxy) revs { return p.owner.rmd.get() }
func (*rebMD) sgl() *memsys.SGL  { return nil }

func (r *rebMD) inc() { r.Version++ }

func (r *rebMD) clone() *rebMD {
	dst := &rebMD{}
	cos.CopyStruct(dst, r)
	return dst
}

func (r *rebMD) String() string {
	if r == nil {
		return "RMD <nil>"
	}
	if len(r.TargetIDs) == 0 && r.Resilver == "" {
		return fmt.Sprintf("RMD v%d", r.Version)
	}
	if r.Resilver == "" {
		return fmt.Sprintf("RMD v%d(%v)", r.Version, r.TargetIDs)
	}
	return fmt.Sprintf("RMD v%d(%v, %s)", r.Version, r.TargetIDs, r.Resilver)
}

func newRMDOwner() *rmdOwner {
	rmdo := &rmdOwner{}
	rmdo.put(&rebMD{})
	return rmdo
}

func (*rmdOwner) persist(rmd *rebMD) error {
	rmdPathName := filepath.Join(cmn.GCO.Get().ConfigDir, fname.Rmd)
	return jsp.SaveMeta(rmdPathName, rmd, nil /*wto*/)
}

func (r *rmdOwner) load() {
	rmd := &rebMD{}
	_, err := jsp.LoadMeta(filepath.Join(cmn.GCO.Get().ConfigDir, fname.Rmd), rmd)
	if err == nil {
		r.put(rmd)
		return
	}
	if !os.IsNotExist(err) {
		glog.Errorf("failed to load rmd: %v", err)
	}
}

func (r *rmdOwner) put(rmd *rebMD) { r.rmd.Store(unsafe.Pointer(rmd)) }
func (r *rmdOwner) get() *rebMD    { return (*rebMD)(r.rmd.Load()) }

func (r *rmdOwner) _runPre(ctx *rmdModifier) (clone *rebMD, err error) {
	r.Lock()
	ctx.prev = r.get()
	clone = ctx.prev.clone()
	clone.TargetIDs = nil
	clone.Resilver = ""
	ctx.pre(ctx, clone)
	if err = r.persist(clone); err == nil {
		r.put(clone)
	}
	ctx.cur = clone
	ctx.rebID = xact.RebID2S(clone.Version)
	r.Unlock()
	return
}

func (r *rmdOwner) modify(ctx *rmdModifier) (clone *rebMD, err error) {
	clone, err = r._runPre(ctx)
	if err == nil && ctx.final != nil {
		ctx.final(ctx, clone)
	}
	return
}
