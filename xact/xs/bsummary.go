// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
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
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	taskState struct {
		Result any   `json:"res"`
		Err    error `json:"error"`
	}
	bsummFactory struct {
		xreg.RenewBase
		xctn *bsummXact
		msg  *cmn.BsummCtrlMsg
	}
	bsummXact struct {
		t         cluster.Target
		msg       *cmn.BsummCtrlMsg
		res       atomic.Pointer
		summaries cmn.AllBsummResults
		xact.Base
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
	msg := args.Custom.(*cmn.BsummCtrlMsg)
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
	if r.Bck() == nil || r.Bck().IsEmpty() {
		glog.Infof("%s - all buckets", r.Name())
	} else {
		glog.Infof("%s - bucket(s) %s", r.Name(), r.Bck().Bucket())
	}
	if r.totalDisksSize, err = fs.GetTotalDisksSize(); err != nil {
		r.updRes(err)
		return
	}
	if si, err = cluster.HrwTargetTask(r.msg.UUID, r.t.Sowner().Get()); err != nil {
		r.updRes(err)
		return
	}

	listRemote := si.ID() == r.t.SID() // we only want a single target listing remote bucket

	if !r.Bck().IsQuery() {
		r.summaries = make(cmn.AllBsummResults, 0, 1)
		err = r.runBck(r.Bck(), listRemote)
	} else {
		var (
			pq   *string
			qbck = (*cmn.QueryBcks)(r.Bck())
			bmd  = r.t.Bowner().Get()
		)
		if provider := qbck.Provider; provider != "" {
			pq = &provider
		}
		r.summaries = make(cmn.AllBsummResults, 0, 8)

		// TODO: currently, summarizing only the _present_ buckets
		// (see apc.QparamFltPresence and commentary)

		bmd.Range(pq, nil, func(bck *cluster.Bck) bool {
			if err := r.runBck(bck, listRemote); err != nil {
				glog.Error(err)
			}
			return false // keep going
		})
	}
	r.updRes(err)
}

func (r *bsummXact) runBck(bck *cluster.Bck, listRemote bool) (err error) {
	var (
		msg  cmn.BsummCtrlMsg
		summ = cmn.NewBsummResult(bck.Bucket(), r.totalDisksSize)
	)
	cos.CopyStruct(&msg, r.msg) // each bucket to have it's own copy of the msg (we may update it)
	if bck.IsRemote() {
		msg.ObjCached = msg.ObjCached || !listRemote
		if bck.IsHTTP() && !msg.ObjCached {
			glog.Warningf("cannot list %s buckets, assuming 'cached'", apc.DisplayProvider(bck.Provider))
			msg.ObjCached = true
		}
	} else {
		msg.ObjCached = true
	}
	if err = r._run(bck, summ, &msg); err == nil {
		r.summaries = append(r.summaries, summ)
	}
	return
}

// TODO: `msg.Fast` might be a bit crude, usability-wise - consider adding (best effort) max-time limitation
func (r *bsummXact) _run(bck *cluster.Bck, summ *cmn.BsummResult, msg *cmn.BsummCtrlMsg) (err error) {
	summ.Bck.Copy(bck.Bucket())

	// 1. always estimate on-disk size (is fast)
	summ.TotalSize.OnDisk = r.sizeOnDisk(bck)
	if msg.Fast {
		return
	}

	// 2. walk local pages
	lsmsg := &apc.LsoMsg{Props: apc.GetPropsSize, Flags: apc.LsObjCached}
	npg := newNpgCtx(r.t, bck, lsmsg, r.LomAdd)
	for {
		npg.page.Entries = allocLsoEntries()
		if err := npg.nextPageA(); err != nil {
			return err
		}
		for _, v := range npg.page.Entries {
			summ.TotalSize.PresentObjs += uint64(v.Size)
			if v.Size < summ.ObjSize.Min {
				summ.ObjSize.Min = v.Size
			}
			if v.Size > summ.ObjSize.Max {
				summ.ObjSize.Max = v.Size
			}
			summ.ObjCount.Present++
		}
		freeLsoEntries(npg.page.Entries)
		if npg.page.ContinuationToken == "" {
			break
		}
		lsmsg.ContinuationToken = npg.page.ContinuationToken
	}

	if msg.ObjCached {
		return nil
	}
	debug.Assert(bck.IsRemote())

	// 3. npg remote
	lsmsg = &apc.LsoMsg{Props: apc.GetPropsSize}
	for {
		npg := newNpgCtx(r.t, bck, lsmsg, noopCb)
		nentries := allocLsoEntries()
		lst, err := npg.nextPageR(nentries)
		if err != nil {
			return err
		}
		for _, v := range lst.Entries {
			summ.TotalSize.RemoteObjs += uint64(v.Size)
			summ.ObjCount.Remote++
		}
		freeLsoEntries(lst.Entries)
		if lsmsg.ContinuationToken = lst.ContinuationToken; lsmsg.ContinuationToken == "" {
			break
		}
	}
	return nil
}

func (*bsummXact) sizeOnDisk(bck *cluster.Bck) (size uint64) {
	var (
		avail = fs.GetAvail()
		wg    = cos.NewLimitedWaitGroup(4, len(avail))
		psize = &size
		b     = bck.Bucket()
	)
	for _, mi := range avail {
		bdir := mi.MakePathBck(b)
		wg.Add(1)
		go addDU(bdir, psize, wg)
	}
	wg.Wait()
	return
}

func addDU(bdir string, psize *uint64, wg cos.WG) {
	sz, err := ios.DirSizeOnDisk(bdir)
	if err != nil {
		glog.Errorf("dir-size %q: %v", bdir, err)
		debug.Assertf(false, "dir-size %q: %v", bdir, err)
	}
	gatomic.AddUint64(psize, sz)
	wg.Done()
}

func (r *bsummXact) updRes(err error) {
	res := &taskState{Err: err}
	if err == nil {
		res.Result = r.summaries
	}
	r.res.Store(unsafe.Pointer(res))
	r.Finish(err)
}

func (r *bsummXact) Result() (any, error) {
	ts := (*taskState)(r.res.Load())
	if ts == nil {
		return nil, errors.New("no result to load")
	}
	return ts.Result, ts.Err
}

func (r *bsummXact) Snap() (snap *cluster.Snap) {
	snap = &cluster.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}
