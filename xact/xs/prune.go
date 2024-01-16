// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"context"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
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
	parent         core.Xact
	bckFrom, bckTo *meta.Bck
	prefix         string
	// run
	joggers *mpather.Jgroup
	same    bool
}

func (rp *prune) init(config *cmn.Config) {
	debug.Assert(rp.bckFrom.HasVersioningMD(), rp.bckFrom.String())
	rmopts := &mpather.JgroupOpts{
		CTs:      []string{fs.ObjectType},
		VisitObj: rp.do,
		Prefix:   rp.prefix,
		Parallel: 1, // TODO: tune-up
		// DoLoad:  noLoad
	}
	rmopts.Bck.Copy(rp.bckTo.Bucket())
	rp.joggers = mpather.NewJoggerGroup(rmopts, config, "")
	rp.same = rp.bckTo.Equal(rp.bckFrom, true, true)
}

func (rp *prune) run() {
	rp.joggers.Run()
}

func (rp *prune) wait() {
	if err := rp.parent.AbortErr(); err != nil {
		rp.joggers.Stop()
		return
	}

	// wait for: joggers || parent-aborted
	ticker := time.NewTicker(cmn.Rom.MaxKeepalive())
	rp._wait(ticker)
	ticker.Stop()
}

func (rp *prune) _wait(ticker *time.Ticker) {
	for {
		select {
		case <-ticker.C:
			if rp.parent.IsAborted() {
				rp.joggers.Stop()
				return
			}
		case <-rp.joggers.ListenFinished():
			rp.joggers.Stop()
			return
		}
	}
}

func (rp *prune) do(dst *core.LOM, _ []byte) error {
	var src *core.LOM

	// construct src lom
	if rp.same {
		src = dst
	} else {
		src = core.AllocLOM(dst.ObjName)
		if src.InitBck(rp.bckFrom.Bucket()) != nil {
			core.FreeLOM(src)
			return nil
		}
	}
	_, errCode, err := core.T.Backend(src.Bck()).HeadObj(context.Background(), src)
	if !rp.same {
		core.FreeLOM(src)
	}
	if err == nil || !cos.IsNotExist(err, errCode) {
		return nil
	}
	// source does not exist: try to remove the destination (NOTE: best effort)
	if !dst.TryLock(true) {
		return nil
	}
	err = dst.Load(false, true)
	if err == nil {
		err = dst.Remove()
	}
	dst.Unlock(true)

	if err == nil {
		if cmn.Rom.FastV(5, cos.SmoduleXs) {
			nlog.Infoln(rp.parent.Name(), dst.Cname())
		}
	} else if !cmn.IsErrObjNought(err) && !cmn.IsErrBucketNought(err) {
		rp.parent.AddErr(err, 4, cos.SmoduleXs)
	}
	return nil
}
