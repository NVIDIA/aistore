// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"context"
	"errors"
	"sync"
	gatomic "sync/atomic"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/objwalk"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	"golang.org/x/sync/errgroup"
)

type (
	taskState struct {
		Result interface{} `json:"res"`
		Err    error       `json:"error"`
	}
	bsummFactory struct {
		xreg.RenewBase
		xctn *bsummXact
		msg  *apc.BckSummMsg
	}
	bsummXact struct {
		xact.Base
		t              cluster.Target
		msg            *apc.BckSummMsg
		res            atomic.Pointer
		summaries      cmn.BckSummaries
		totalDisksSize uint64
	}
)

// interface guard
var (
	_ xreg.Renewable = (*bsummFactory)(nil)
	_ cluster.Xact   = (*bsummXact)(nil)
)

//////////////////
// bsummFactory //
//////////////////

func (*bsummFactory) New(args xreg.Args, bck *cluster.Bck) xreg.Renewable {
	msg := args.Custom.(*apc.BckSummMsg)
	p := &bsummFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, msg: msg}
	return p
}

func (p *bsummFactory) Start() error {
	xctn := &bsummXact{t: p.T, msg: p.msg}
	xctn.InitBase(p.UUID(), apc.ActSummaryBck, p.Bck)
	p.xctn = xctn
	xact.GoRunW(xctn)
	return nil
}

func (*bsummFactory) Kind() string        { return apc.ActSummaryBck }
func (p *bsummFactory) Get() cluster.Xact { return p.xctn }

func (*bsummFactory) WhenPrevIsRunning(xreg.Renewable) (w xreg.WPR, e error) {
	return xreg.WprUse, nil
}

///////////////
// bsummXact //
///////////////

func (r *bsummXact) Run(rwg *sync.WaitGroup) {
	var (
		err error
		si  *cluster.Snode
	)
	rwg.Done()
	if r.totalDisksSize, err = fs.GetTotalDisksSize(); err != nil {
		r.updRes(err)
		return
	}
	if si, err = cluster.HrwTargetTask(r.msg.UUID, r.t.Sowner().Get()); err != nil {
		r.updRes(err)
		return
	}
	// (we only want a single target listing remote bucket)
	shouldListCB := r.msg.Cached || (si.ID() == r.t.SID() && !r.msg.Cached)
	if !r.Bck().IsQuery() {
		r.summaries = make(cmn.BckSummaries, 0, 1)
		err = r.runBck(r.Bck(), shouldListCB)
	} else {
		var (
			pq   *string
			qbck = (*cmn.QueryBcks)(r.Bck())
			bmd  = r.t.Bowner().Get()
		)
		if provider := qbck.Provider; provider != "" {
			pq = &provider
		}
		r.summaries = make(cmn.BckSummaries, 0, 8)
		bmd.Range(pq, nil, func(bck *cluster.Bck) bool {
			if err := r.runBck(bck, shouldListCB); err != nil {
				glog.Error(err)
			}
			return false // keep going
		})
	}
	r.updRes(err)
}

func (r *bsummXact) runBck(bck *cluster.Bck, shouldListCB bool) (err error) {
	summ := cmn.NewBckSumm(bck.Bucket(), r.totalDisksSize)
	if err = r._run(bck, summ, shouldListCB); err == nil {
		r.summaries = append(r.summaries, summ)
	}
	return
}

func (r *bsummXact) _run(bck *cluster.Bck, summ *cmn.BckSumm, shouldListCB bool) error {
	msg := apc.BckSummMsg{}
	summ.Bck.Copy(bck.Bucket())

	// Each bucket should have it's own copy of the msg (we may update it).
	cos.CopyStruct(&msg, r.msg)
	if bck.IsHTTP() {
		msg.Cached = true
	}

	// fast path, estimated sizes
	if msg.Fast && (bck.IsAIS() || msg.Cached) {
		objCount, size, err := r.fast(bck)
		summ.ObjCount = objCount
		summ.Size = size
		return err
	}

	// slow path, exact sizes
	var (
		list *cmn.BucketList
		err  error
	)
	if msg.Fast {
		glog.Warningf("%s: remote bucket w/ `--cached=false`: executing slow and precise version (`--fast=false`)", r)
	}
	if !shouldListCB {
		msg.Cached = true
	}
	lsmsg := &apc.ListObjsMsg{Props: apc.GetPropsSize}
	if msg.Cached {
		lsmsg.Flags = apc.LsPresent
	}
	for {
		walk := objwalk.NewWalk(context.Background(), r.t, bck, lsmsg)
		if bck.IsAIS() {
			list, err = walk.DefaultLocalObjPage()
		} else {
			list, err = walk.RemoteObjPage()
		}
		if err != nil {
			return err
		}
		for _, v := range list.Entries {
			summ.Size += uint64(v.Size)
			if v.Size < summ.ObjSize.Min {
				summ.ObjSize.Min = v.Size
			}
			if v.Size > summ.ObjSize.Max {
				summ.ObjSize.Max = v.Size
			}

			// for remote backends, not updating obj-count to avoid double counting
			// (by other targets).
			if bck.IsAIS() || shouldListCB {
				summ.ObjCount++
			}
			// generic x stats
			r.ObjsAdd(1, v.Size)
		}
		if list.ContinuationToken == "" {
			break
		}
		list.Entries = nil
		lsmsg.ContinuationToken = list.ContinuationToken
	}
	return nil
}

func (*bsummXact) fast(bck *cluster.Bck) (uint64, uint64, error) {
	var (
		objCount, size uint64
		err            error
		availablePaths = fs.GetAvail()
		group, _       = errgroup.WithContext(context.Background())
	)
	for _, mi := range availablePaths {
		group.Go(func(mi *fs.MountpathInfo) func() error {
			return func() error {
				siz, num, erm := mi.SizeBck(bck.Bucket())
				if erm != nil {
					return erm
				}
				gatomic.AddUint64(&objCount, uint64(num))
				gatomic.AddUint64(&size, siz)
				return nil
			}
		}(mi))
	}
	err = group.Wait()
	if copies := uint64(bck.Props.Mirror.Copies); copies > 1 && bck.Props.Mirror.Enabled {
		objCount = cos.DivRoundU64(objCount, copies)
	}
	return objCount, size, err
}

func (r *bsummXact) updRes(err error) {
	res := &taskState{Err: err}
	if err == nil {
		res.Result = r.summaries
	}
	r.res.Store(unsafe.Pointer(res))
	r.Finish(err)
}

func (r *bsummXact) Result() (interface{}, error) {
	ts := (*taskState)(r.res.Load())
	if ts == nil {
		return nil, errors.New("no result to load")
	}
	return ts.Result, ts.Err
}
