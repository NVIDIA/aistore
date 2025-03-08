// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"errors"
	"fmt"
	"math"
	"sync"
	ratomic "sync/atomic"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	nsummFactory struct {
		xreg.RenewBase
		xctn *XactNsumm
		msg  *apc.BsummCtrlMsg
	}
	XactNsumm struct {
		p             *nsummFactory
		oneRes        cmn.BsummResult
		mapRes        map[uint64]*cmn.BsummResult
		buckets       []*meta.Bck
		_nam, _str    string
		totalDiskSize uint64
		xact.BckJog
		single     bool
		listRemote bool
	}
)

// interface guard
var (
	_ xreg.Renewable = (*nsummFactory)(nil)
	_ core.Xact      = (*XactNsumm)(nil)
)

//////////////////
// nsummFactory //
//////////////////

func (*nsummFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	msg := args.Custom.(*apc.BsummCtrlMsg)
	p := &nsummFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, msg: msg}
	return p
}

func (p *nsummFactory) Start() (err error) {
	p.xctn, err = newSumm(p)
	if err == nil {
		xact.GoRunW(p.xctn)
	}
	return
}

func (*nsummFactory) Kind() string     { return apc.ActSummaryBck }
func (p *nsummFactory) Get() core.Xact { return p.xctn }

func (*nsummFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

func newSumm(p *nsummFactory) (r *XactNsumm, err error) {
	r = &XactNsumm{p: p}

	r.totalDiskSize = fs.GetDiskSize()
	if r.totalDiskSize < cos.KiB {
		err = fmt.Errorf("invalid disk size (%d bytes)", r.totalDiskSize)
		debug.AssertNoErr(err)
		return nil, err
	}

	listRemote := lsoIsRemote(p.Bck, p.msg.ObjCached)
	if listRemote {
		var (
			smap = core.T.Sowner().Get()
			tsi  *meta.Snode
		)
		if tsi, err = smap.HrwTargetTask(p.UUID()); err != nil {
			return r, err
		}
		r.listRemote = listRemote && tsi.ID() == core.T.SID() // this target
	}

	opts := &mpather.JgroupOpts{
		CTs:         []string{fs.ObjectType},
		Prefix:      p.msg.Prefix,
		VisitObj:    r.visitObj,
		DoLoad:      mpather.LoadUnsafe,
		IncludeCopy: true,
	}
	if !p.Bck.IsQuery() {
		r.initRes(&r.oneRes, p.Bck) // init single result-set
		r.single = true
		opts.Bck = p.Bck.Clone()
	} else {
		var single *meta.Bck
		r.mapRes = make(map[uint64]*cmn.BsummResult, 8)

		opts.Buckets, single = r.initResQbck() // init multiple result-sets (one per bucket)

		nb := len(opts.Buckets)
		switch nb {
		case 0:
			if p.Bck.IsEmpty() {
				return r, errors.New("no buckets in the cluster, nothing to do")
			}
			return r, fmt.Errorf("no buckets matching %q", p.Bck.Bucket())
		case 1:
			// change of mind: single result-set even though spec-ed as qbck
			p.Bck = single
			opts.Buckets = nil
			r.buckets = nil

			r.single = true
			opts.Bck = p.Bck.Clone()
		default:
			// inc num joggers to boost
			nmps := fs.NumAvail()
			if nmps == 0 {
				return r, cmn.ErrNoMountpaths
			}
			opts.PerBucket = nb*nmps <= sys.NumCPU()
		}
	}

	ctlmsg := p.msg.Str(p.Bck.Cname(p.msg.Prefix))
	r.BckJog.Init(p.UUID(), p.Kind(), ctlmsg, p.Bck, opts, cmn.GCO.Get())

	r._nam = r.Base.Name() + "-" + ctlmsg
	r._str = r.Base.String() + "-" + ctlmsg
	return r, nil
}

func (r *XactNsumm) Run(started *sync.WaitGroup) {
	started.Done()
	var (
		bname    string
		rwg, lwg cos.WG
	)
	if !r.p.Bck.IsEmpty() {
		bname = r.p.Bck.Cname("")
	}
	nlog.Infoln(r.Name(), bname)

	// (I) remote
	if r.listRemote {
		// _this_ target to list-and-summ remote pages, in parallel
		if r.single {
			rwg = &sync.WaitGroup{}
			rwg.Add(1)
			go func(wg cos.WG) {
				r.runCloudBck(r.p.Bck, &r.oneRes)
				wg.Done()
			}(rwg)
		} else {
			debug.Assert(len(r.buckets) > 1)
			rwg = cos.NewLimitedWaitGroup(sys.NumCPU(), len(r.buckets))
			for _, bck := range r.buckets {
				res, ok := r.mapRes[bck.Props.BID]
				debug.Assert(ok, r.Name(), bck.Cname(""))
				rwg.Add(1)
				go func(bck *meta.Bck, res *cmn.BsummResult, wg cos.WG) {
					r.runCloudBck(bck, res)
					wg.Done()
				}(bck, res, rwg)
			}
		}
	}

	// (II) calculate on-disk size.
	if r.single {
		lwg = &sync.WaitGroup{}
		lwg.Add(1)
		go func(wg cos.WG) {
			res := &r.oneRes
			res.TotalSize.OnDisk = fs.OnDiskSize(r.p.Bck.Bucket(), r.p.msg.Prefix)
			wg.Done()
		}(lwg)
	} else {
		lwg = cos.NewLimitedWaitGroup(sys.NumCPU(), len(r.buckets))
		for _, bck := range r.buckets {
			res, ok := r.mapRes[bck.Props.BID]
			debug.Assert(ok, r.Name(), bck.Cname(""))
			lwg.Add(1)
			go func(bck *meta.Bck, res *cmn.BsummResult, wg cos.WG) {
				res.TotalSize.OnDisk = fs.OnDiskSize(bck.Bucket(), r.p.msg.Prefix)
				wg.Done()
			}(bck, res, lwg)
		}
	}

	// (III) visit objects
	r.BckJog.Run()

	err := r.BckJog.Wait()
	if err != nil {
		r.AddErr(err)
	}

	lwg.Wait()
	if rwg != nil {
		debug.Assert(r.listRemote)
		rwg.Wait()
	}

	r.Finish()
}

// to add all `res` pointers up front
func (r *XactNsumm) initResQbck() (cmn.Bcks, *meta.Bck) {
	var (
		bmd      = core.T.Bowner().Get()
		qbck     = (*cmn.QueryBcks)(r.p.Bck)
		provider *string
		ns       *cmn.Ns
		buckets  = make(cmn.Bcks, 0, 8) // => jogger opts
		single   *meta.Bck
	)
	if r.listRemote {
		r.buckets = make([]*meta.Bck, 0, 8)
	}
	if qbck.Provider != "" {
		provider = &qbck.Provider
	}
	if !qbck.Ns.IsGlobal() {
		ns = &qbck.Ns
	}
	bmd.Range(provider, ns, func(bck *meta.Bck) bool {
		res := &cmn.BsummResult{}
		r.initRes(res, bck)
		debug.Assert(bck.Props.BID != 0)
		r.mapRes[bck.Props.BID] = res
		buckets = append(buckets, res.Bck)

		if r.listRemote {
			r.buckets = append(r.buckets, bck)
		}
		single = bck
		return false
	})
	return buckets, single
}

func (r *XactNsumm) initRes(res *cmn.BsummResult, bck *meta.Bck) {
	debug.Assert(r.totalDiskSize > 0)
	res.Bck = bck.Clone()
	res.TotalSize.Disks = r.totalDiskSize
	res.ObjSize.Min = math.MaxInt64
}

func (r *XactNsumm) String() string { return r._str }
func (r *XactNsumm) Name() string   { return r._nam }

func (r *XactNsumm) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)
	snap.IdleX = r.IsIdle()
	return
}

func (r *XactNsumm) Result() (cmn.AllBsummResults, error) {
	if r.single {
		var dst cmn.BsummResult
		r.cloneRes(&dst, &r.oneRes)
		return cmn.AllBsummResults{&dst}, r.Err()
	}

	all := make(cmn.AllBsummResults, 0, len(r.mapRes))
	for _, src := range r.mapRes {
		var dst cmn.BsummResult
		r.cloneRes(&dst, src)
		all = append(all, &dst)
	}
	return all, r.Err()
}

func (r *XactNsumm) cloneRes(dst, src *cmn.BsummResult) {
	dst.Bck = src.Bck
	dst.TotalSize.OnDisk = src.TotalSize.OnDisk

	dst.ObjCount.Present = ratomic.LoadUint64(&src.ObjCount.Present)
	dst.TotalSize.PresentObjs = ratomic.LoadUint64(&src.TotalSize.PresentObjs)

	if r.listRemote {
		dst.ObjCount.Remote = ratomic.LoadUint64(&src.ObjCount.Remote)
		dst.TotalSize.RemoteObjs = ratomic.LoadUint64(&src.TotalSize.RemoteObjs)
	}

	dst.ObjSize.Max = ratomic.LoadInt64(&src.ObjSize.Max)
	dst.ObjSize.Min = ratomic.LoadInt64(&src.ObjSize.Min)
	if dst.ObjSize.Max > 0 {
		dst.ObjSize.Min = min(dst.ObjSize.Min, dst.ObjSize.Max)
	}
	// compute the current (maybe, running-and-changing) average and used %%
	if dst.ObjCount.Present > 0 {
		dst.ObjSize.Avg = int64(cos.DivRoundU64(dst.TotalSize.PresentObjs, dst.ObjCount.Present))
	}
	debug.Assert(r.totalDiskSize == src.TotalSize.Disks || (src.TotalSize.Disks == 0 && cmn.Rom.TestingEnv()),
		r.totalDiskSize, " vs ", src.TotalSize.Disks)
	dst.TotalSize.Disks = r.totalDiskSize
	dst.UsedPct = cos.DivRoundU64(dst.TotalSize.OnDisk*100, r.totalDiskSize)
}

func (r *XactNsumm) visitObj(lom *core.LOM, _ []byte) error {
	var res *cmn.BsummResult
	if r.single {
		res = &r.oneRes
	} else {
		s, ok := r.mapRes[lom.Bprops().BID]
		debug.Assert(ok, r.Name(), lom.Cname()) // j.opts.Buckets above
		res = s
	}
	if !lom.IsCopy() {
		ratomic.AddUint64(&res.ObjCount.Present, 1)
	}
	size := lom.Lsize()
	if cmin := ratomic.LoadInt64(&res.ObjSize.Min); cmin > size {
		ratomic.CompareAndSwapInt64(&res.ObjSize.Min, cmin, size)
	}
	if cmax := ratomic.LoadInt64(&res.ObjSize.Max); cmax < size {
		ratomic.CompareAndSwapInt64(&res.ObjSize.Max, cmax, size)
	}
	ratomic.AddUint64(&res.TotalSize.PresentObjs, uint64(size))

	// generic stats (same as base.LomAdd())
	r.ObjsAdd(1, size)
	return nil
}

//
// listRemote
//

func (r *XactNsumm) runCloudBck(bck *meta.Bck, res *cmn.BsummResult) {
	lsmsg := &apc.LsoMsg{Props: apc.GetPropsSize, Prefix: r.p.msg.Prefix}
	lsmsg.SetFlag(apc.LsNameSize | apc.LsNoDirs)
	bp := core.T.Backend(bck)
	for !r.IsAborted() {
		npg := newNpgCtx(bck, lsmsg, noopCb, nil, bp) // TODO: inventory offset
		nentries := allocLsoEntries()
		lst, err := npg.nextPageR(nentries)
		if err != nil {
			r.AddErr(err)
			return
		}
		ratomic.AddUint64(&res.ObjCount.Remote, uint64(len(lst.Entries)))
		for _, v := range lst.Entries {
			ratomic.AddUint64(&res.TotalSize.RemoteObjs, uint64(v.Size))
		}
		freeLsoEntries(lst.Entries)
		if lsmsg.ContinuationToken = lst.ContinuationToken; lsmsg.ContinuationToken == "" {
			return
		}
	}
}
