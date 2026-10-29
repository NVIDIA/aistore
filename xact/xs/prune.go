// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/cmn/prob"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
)

// When synchronizing source => destination:
// remove destination objects that are not present at the source (any longer)
// Limitations (TODO):
// - not supporting apc.ListRange
// - use probabilistic filtering to skip received and locally copied obj-s (see `reb.FilterAdd` et al)

type prune struct {
	r       core.Xact
	bckFrom *meta.Bck
	bckTo   *meta.Bck
	smap    *meta.Smap
	joggers *mpather.Jgroup
	filter  *prob.Filter
	hdb     *core.HdbBatcher
	prefix  string
	lsflags uint64
	pruned  atomic.Int64
	busy    atomic.Int64
	same    bool
}

func (rp *prune) init(config *cmn.Config) {
	debug.Func(func() { debug.Assert(rp.bckFrom.IsAIS() || rp.bckFrom.HasVersioningMD(), rp.bckFrom.String()) })
	rmopts := &mpather.JgroupOpts{
		Parent:   rp.r,
		CTs:      []string{fs.ObjCT},
		VisitObj: rp.do,
		Prefix:   rp.prefix,
		RW:       false,
	}
	rmopts.Bck.Copy(rp.bckTo.Bucket())
	rp.joggers = mpather.NewJgroup(rmopts, config, nil)
	rp.filter = prob.NewDefaultFilter()
	rp.same = rp.bckTo.Equal(rp.bckFrom, true /*same BID*/, true)
	rp.initHdb()
}

// check the sources on other targets in batches (see peerReply)
func (rp *prune) initHdb() {
	rp.hdb = core.NewHdbBatcher(&core.HdbArgs{
		Cb: rp.peerReply, Abort: rp.r, Smap: rp.smap, LogHdr: rp.r.Name(), Mod: cos.ModXs, Strict: true,
	})
}

// after the producers stop
func (rp *prune) endHdb() {
	var sb cos.SB
	sb.Init(128)
	st := rp.hdb.End()
	st.Str(&sb, '=')
	nlog.Infoln(rp.r.Name(), "prune peer checks:", sb.String())
	if n := st.Dropped + st.SmapChanged + rp.busy.Load(); n > 0 && !rp.r.IsAborted() {
		rp.r.AddErr(fmt.Errorf("%s: %d destination objects not checked (%s)", rp.r.Name(), n, apc.HdbTag), 0)
	}
}

func (rp *prune) run() {
	rp.joggers.Run()
}

func (rp *prune) wait() {
	defer rp.endHdb()
	if err := rp.r.AbortErr(); err != nil {
		rp.joggers.Stop()
		return
	}

	rp._wait()
	rp.filter.Reset()
}

func (rp *prune) _wait() {
	stopCh := rp.r.ChanAbort()
outer:
	for {
		select {
		case <-stopCh:
			break outer
		case <-rp.joggers.ListenFinished():
			break outer
		}
	}
	rp.joggers.Stop()
}

func (rp *prune) do(dst *core.LOM, _ []byte) error {
	debug.Func(func() {
		_, local, err := dst.HrwTarget(rp.smap)
		debug.Assertf(local, "local %t, err: %v", local, err)
	})
	// construct src lom
	var src *core.LOM
	if rp.same {
		src = dst
	} else {
		src = core.AllocLOM(dst.ObjName)
		defer core.FreeLOM(src)
		if src.InitBck(rp.bckFrom) != nil {
			return nil
		}
	}

	// skip objects already copied by rp.r (compare w/ reb)
	uname := src.UnamePtr()
	bname := cos.UnsafeBptr(uname)
	if rp.filter != nil && rp.filter.Lookup(*bname) { // TODO -- FIXME: rm filter nil check once x-tco supports prob. filtering
		rp.filter.Delete(*bname)
		return nil
	}

	// check whether src lom exists
	var (
		err   error
		ecode int
	)
	if src.Bck().IsAIS() {
		tsi, errV := rp.smap.HrwObjHash2T(src.Bck(), src.Digest())
		if errV != nil {
			return fmt.Errorf("prune %s: fatal err: %w", rp.r.Name(), errV)
		}
		if tsi.ID() != core.T.SID() {
			if err := dst.Load(false, false); err == nil {
				rp.hdb.AddAs(dst, src.Bucket(), tsi)
			} else if !cmn.IsErrObjNought(err) {
				rp.r.AddErr(err, 4, cos.ModXs)
			}
			return nil
		}
		err = src.Load(false, false)
	} else {
		_, ecode, err = core.T.HeadCold(src, nil /*origReq*/)
	}

	if (err == nil && ecode == 0) || !cos.IsNotExist(err, ecode) /*not complaining*/ {
		return nil
	}

	rp.remove(dst, nil)
	return nil
}

// head-batch callback
func (rp *prune) peerReply(ent *core.HdbEnt) {
	switch ent.Status {
	case apc.HdbMissing:
	case apc.HdbSame, apc.HdbDiverged: // the source exists
		return
	case apc.HdbBusy: // the peer could not lock it (the next sync checks it again)
		rp.busy.Inc()
		if cmn.Rom.V(4, cos.ModXs) {
			nlog.Infoln(rp.r.Name(), "peer busy, keep:", ent.LIF.Cname(), "peer:", ent.Tsi.StringEx())
		}
		return
	default: // existence unknown: keep the destination
		rp.r.AddErr(errors.New(ent.Cause), 4, cos.ModXs)
		return
	}
	dst, err := ent.LIF.LOM()
	if err != nil {
		return
	}
	rp.remove(dst, ent)
	core.FreeLOM(dst)
}

// source does not exist: try to remove the destination (NOTE best effort)
func (rp *prune) remove(dst *core.LOM, ent *core.HdbEnt) {
	if !dst.TryLock(true) {
		return
	}
	err := dst.Load(false, true)
	if err == nil && ent != nil && !ent.Unchanged(dst) {
		dst.Unlock(true)
		return
	}
	if err == nil {
		err = dst.RemoveObj()
	}
	dst.Unlock(true)

	if err == nil {
		rp.pruned.Inc()
		if cmn.Rom.V(5, cos.ModXs) {
			nlog.Infoln(rp.r.Name(), dst.Cname())
		}
	} else if !cmn.IsErrObjNought(err) && !cmn.IsErrBucketNought(err) {
		rp.r.AddErr(err, 4, cos.ModXs)
	}
}
