// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"errors"
	"path/filepath"
	"sync"
	ratomic "sync/atomic"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
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
		msg  *apc.BsummCtrlMsg
	}
	bsummXact struct {
		t         cluster.Target
		msg       *apc.BsummCtrlMsg
		res       ratomic.Pointer[taskState]
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

func (*bsummFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	msg := args.Custom.(*apc.BsummCtrlMsg)
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
		si  *meta.Snode
	)
	rwg.Done()
	if r.Bck() == nil || r.Bck().IsEmpty() {
		nlog.Infof("%s - all buckets", r.Name())
	} else {
		nlog.Infof("%s - bucket(s) %s", r.Name(), r.Bck().Bucket())
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

		bmd.Range(pq, nil, func(bck *meta.Bck) bool {
			if err := r.runBck(bck, listRemote); err != nil {
				nlog.Errorln(err)
			}
			return false // keep going
		})
	}
	r.updRes(err)
}

func (r *bsummXact) runBck(bck *meta.Bck, listRemote bool) (err error) {
	var (
		msg  apc.BsummCtrlMsg
		summ = cmn.NewBsummResult(bck.Bucket(), r.totalDisksSize)
	)
	cos.CopyStruct(&msg, r.msg) // each bucket to have it's own copy of the msg (we may update it)
	if bck.IsRemote() {
		msg.ObjCached = msg.ObjCached || !listRemote
		if bck.IsHTTP() && !msg.ObjCached {
			nlog.Warningf("cannot list %s buckets, assuming 'cached'", apc.DisplayProvider(bck.Provider))
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
func (r *bsummXact) _run(bck *meta.Bck, summ *cmn.BsummResult, msg *apc.BsummCtrlMsg) (err error) {
	summ.Bck.Copy(bck.Bucket())

	// 1. always estimate on-disk size (is fast)
	var errCount uint64
	summ.TotalSize.OnDisk, errCount = r.sizeOnDisk(bck, msg.Prefix)
	if errCount != 0 && msg.Fast {
		return
	}

	// 2. walk local pages
	lsmsg := &apc.LsoMsg{Props: apc.GetPropsSize, Prefix: msg.Prefix, Flags: apc.LsObjCached}
	npg := newNpgCtx(r.t, bck, lsmsg, r.LomAdd)
	for {
		npg.page.Entries = allocLsoEntries()
		if err := npg.nextPageA(); err != nil {
			return err
		}
		summ.ObjCount.Present += uint64(len(npg.page.Entries))
		for _, v := range npg.page.Entries {
			summ.TotalSize.PresentObjs += uint64(v.Size)
			if v.Size < summ.ObjSize.Min {
				summ.ObjSize.Min = v.Size
			}
			if v.Size > summ.ObjSize.Max {
				summ.ObjSize.Max = v.Size
			}
		}
		freeLsoEntries(npg.page.Entries)
		if npg.page.ContinuationToken == "" {
			break
		}
		lsmsg.ContinuationToken = npg.page.ContinuationToken
	}

	if summ.ObjCount.Present == 0 {
		summ.TotalSize.OnDisk = 0 // fixup (is correct here)
	}

	if msg.ObjCached {
		return nil
	}
	debug.Assert(bck.IsRemote())

	// 3. npg remote
	lsmsg = &apc.LsoMsg{Props: apc.GetPropsSize, Prefix: msg.Prefix}
	for {
		npg := newNpgCtx(r.t, bck, lsmsg, noopCb)
		nentries := allocLsoEntries()
		lst, err := npg.nextPageR(nentries)
		if err != nil {
			return err
		}
		summ.ObjCount.Remote += uint64(len(lst.Entries))
		for _, v := range lst.Entries {
			summ.TotalSize.RemoteObjs += uint64(v.Size)
		}
		freeLsoEntries(lst.Entries)
		if lsmsg.ContinuationToken = lst.ContinuationToken; lsmsg.ContinuationToken == "" {
			break
		}
	}
	return nil
}

func (*bsummXact) sizeOnDisk(bck *meta.Bck, prefix string) (size, ecnt uint64) {
	var (
		avail = fs.GetAvail()
		wg    = cos.NewLimitedWaitGroup(4, len(avail))
		psize = &size
		pecnt = &ecnt
		b     = bck.Bucket()
	)
	for _, mi := range avail {
		var (
			dirPath          string
			withNonDirPrefix bool
		)
		if prefix == "" {
			dirPath = mi.MakePathBck(b)
		} else {
			dirPath = filepath.Join(mi.MakePathCT(b, fs.ObjectType), prefix)
			if cos.Stat(dirPath) != nil {
				dirPath += "*"          // prefix is _not_ a directory
				withNonDirPrefix = true // ok to fail matching
			}
		}
		wg.Add(1)
		go addDU(dirPath, psize, pecnt, wg, withNonDirPrefix)
	}
	wg.Wait()
	return
}

func addDU(dirPath string, psize, pecnt *uint64, wg cos.WG, withNonDirPrefix bool) {
	sz, err := ios.DirSizeOnDisk(dirPath, withNonDirPrefix)
	if err != nil {
		nlog.Errorln(err)
		ratomic.AddUint64(pecnt, 1)
	}
	ratomic.AddUint64(psize, sz)
	wg.Done()
}

func (r *bsummXact) updRes(err error) {
	res := &taskState{Err: err}
	r.AddErr(err)
	if err == nil {
		res.Result = r.summaries
	}
	r.res.Store(res)
	r.Finish()
}

func (r *bsummXact) Result() (any, error) {
	ts := r.res.Load()
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
