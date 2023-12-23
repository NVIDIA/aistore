// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"math"
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
	"github.com/NVIDIA/aistore/fs/glob"
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
		totalDiskSize uint64
		xact.BckJog
		single     bool
		listRemote bool
	}
)

// interface guard
var (
	_ xreg.Renewable = (*nsummFactory)(nil)
	_ cluster.Xact   = (*XactNsumm)(nil)
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

func (*nsummFactory) Kind() string        { return apc.ActSummaryBck }
func (p *nsummFactory) Get() cluster.Xact { return p.xctn }

func (*nsummFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

func newSumm(p *nsummFactory) (r *XactNsumm, err error) {
	r = &XactNsumm{p: p}

	r.totalDiskSize = fs.GetDiskSize()

	listRemote := p.Bck.IsCloud() && !p.msg.ObjCached
	if listRemote {
		var (
			smap = glob.T.Sowner().Get()
			tsi  *meta.Snode
		)
		if tsi, err = smap.HrwTargetTask(p.UUID()); err != nil {
			return
		}
		r.listRemote = listRemote && tsi.ID() == glob.T.SID() // this target
	}

	opts := &mpather.JgroupOpts{
		CTs:         []string{fs.ObjectType},
		Prefix:      p.msg.Prefix,
		VisitObj:    r.visitObj,
		DoLoad:      mpather.LoadUnsafe,
		IncludeCopy: true,
	}
	if p.Bck.IsQuery() {
		var single *meta.Bck
		r.mapRes = make(map[uint64]*cmn.BsummResult, 8)
		opts.Buckets, single = r.initResQbck()

		nb := len(opts.Buckets)
		switch nb {
		case 0:
			return r, fmt.Errorf("no %q matching buckets", p.Bck)
		case 1:
			// change of mind: single even though spec-ed as qbck
			p.Bck = single
			opts.Buckets = nil
			r.buckets = nil
			goto single
		default:
			// inc num joggers to boost
			nmps := fs.NumAvail()
			if nmps == 0 {
				return r, cmn.ErrNoMountpaths
			}
			opts.PerBucket = nb*nmps <= sys.NumCPU()
			goto ini
		}
	}
single:
	r.initRes(&r.oneRes, p.Bck)
	r.single = true
	opts.Bck = p.Bck.Clone()
ini:
	r.BckJog.Init(p.UUID(), p.Kind(), p.Bck, opts, cmn.GCO.Get())
	return r, nil
}

func (r *XactNsumm) Run(started *sync.WaitGroup) {
	started.Done()
	nlog.Infoln(r.Name(), r.p.Bck.Cname(""))

	var wg cos.WG
	if r.listRemote {
		// _this_ target to list-and-summ remote pages, in parallel
		if r.single {
			wg = &sync.WaitGroup{}
			wg.Add(1)
			go func(wg cos.WG) {
				r.runCloudBck(r.p.Bck, &r.oneRes)
				wg.Done()
			}(wg)
		} else {
			debug.Assert(len(r.buckets) > 1)
			wg = cos.NewLimitedWaitGroup(sys.NumCPU(), len(r.buckets))
			for _, bck := range r.buckets {
				res, ok := r.mapRes[bck.Props.BID]
				debug.Assert(ok, r.Name(), bck.Cname(""))
				wg.Add(1)
				go func(bck *meta.Bck, wg cos.WG) {
					r.runCloudBck(bck, res)
					wg.Done()
				}(bck, wg)
			}
		}
	}
	r.BckJog.Run()

	err := r.BckJog.Wait()
	r.AddErr(err)

	if wg != nil {
		debug.Assert(r.listRemote)
		wg.Wait()
	}

	r.Finish()
}

// to add all `res` pointers up front
func (r *XactNsumm) initResQbck() (cmn.Bcks, *meta.Bck) {
	var (
		bmd      = glob.T.Bowner().Get()
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
	res.TotalSize.OnDisk = fs.OnDiskSize(bck.Bucket(), r.p.msg.Prefix)
}

func (r *XactNsumm) _str(s string) string { return fmt.Sprintf("%s %+v", s, r.p.msg) }
func (r *XactNsumm) String() string       { return r._str(r.Base.String()) }
func (r *XactNsumm) Name() string         { return r._str(r.Base.Name()) }

func (r *XactNsumm) Snap() (snap *cluster.Snap) {
	snap = &cluster.Snap{}
	r.ToSnap(snap)
	snap.IdleX = r.IsIdle()
	return
}

func (r *XactNsumm) Result() (cmn.AllBsummResults, error) {
	if r.single {
		var res cmn.BsummResult
		r.cloneRes(&res, &r.oneRes)
		return cmn.AllBsummResults{&res}, r.Err()
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

	dst.ObjSize.Min = ratomic.LoadInt64(&src.ObjSize.Min)
	if dst.ObjSize.Min == math.MaxInt64 {
		dst.ObjSize.Min = 0
	}
	dst.ObjSize.Max = ratomic.LoadInt64(&src.ObjSize.Max)

	// compute the current (maybe, running-and-changing) average and used %%
	if dst.ObjCount.Present > 0 {
		dst.ObjSize.Avg = int64(cos.DivRoundU64(dst.TotalSize.PresentObjs, dst.ObjCount.Present))
	}
	debug.Assert(r.totalDiskSize == src.TotalSize.Disks)
	dst.TotalSize.Disks = r.totalDiskSize
	dst.UsedPct = cos.DivRoundU64(dst.TotalSize.OnDisk*100, r.totalDiskSize)
}

func (r *XactNsumm) visitObj(lom *cluster.LOM, _ []byte) error {
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
	size := lom.SizeBytes()
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
	lsmsg.SetFlag(apc.LsNameSize)
	for !r.IsAborted() {
		npg := newNpgCtx(bck, lsmsg, noopCb)
		nentries := allocLsoEntries()
		lst, err := npg.nextPageR(nentries, false /*load LOMs to include status and local MD*/)
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
