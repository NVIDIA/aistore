// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"net/http"

	"github.com/NVIDIA/aistore/cmn"
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
	prefix  string
	lsflags uint64
	same    bool
}

func (rp *prune) init(config *cmn.Config) {
	debug.Assert(rp.bckFrom.IsAIS() || rp.bckFrom.HasVersioningMD(), rp.bckFrom.String())
	rmopts := &mpather.JgroupOpts{
		CTs:      []string{fs.ObjCT},
		VisitObj: rp.do,
		Prefix:   rp.prefix,
		// DoLoad:  noLoad
	}
	rmopts.Bck.Copy(rp.bckTo.Bucket())
	rp.joggers = mpather.NewJoggerGroup(rmopts, config, nil)
	rp.filter = prob.NewDefaultFilter()
	rp.same = rp.bckTo.Equal(rp.bckFrom, true, true)
}

func (rp *prune) run() {
	rp.joggers.Run()
}

func (rp *prune) wait() {
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
		if src.InitBck(rp.bckFrom.Bucket()) != nil {
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
		tsi, errV := rp.smap.HrwHash2T(src.Digest())
		if errV != nil {
			return fmt.Errorf("prune %s: fatal err: %w", rp.r.Name(), errV)
		}
		if tsi.ID() == core.T.SID() {
			err = src.Load(false, false)
		} else {
			if present := core.T.HeadObjT2T(src, tsi); !present {
				ecode = http.StatusNotFound
			}
		}
	} else {
		_, ecode, err = core.T.HeadCold(src, nil /*origReq*/)
	}

	if (err == nil && ecode == 0) || !cos.IsNotExist(err, ecode) /*not complaining*/ {
		return nil
	}

	// source does not exist: try to remove the destination (NOTE best effort)
	if !dst.TryLock(true) {
		return nil
	}
	err = dst.Load(false, true)
	if err == nil {
		err = dst.RemoveObj()
	}
	dst.Unlock(true)

	if err == nil {
		if cmn.Rom.FastV(5, cos.SmoduleXs) {
			nlog.Infoln(rp.r.Name(), dst.Cname())
		}
	} else if !cmn.IsErrObjNought(err) && !cmn.IsErrBucketNought(err) {
		rp.r.AddErr(err, 4, cos.SmoduleXs)
	}
	return nil
}
