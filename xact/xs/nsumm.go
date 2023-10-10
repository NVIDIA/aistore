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
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	nsummFactory struct {
		xreg.RenewBase
		xctn *nsummXact
		msg  *apc.BsummCtrlMsg
	}
	nsummXact struct {
		p *nsummFactory
		xact.BckJog
		res cmn.BsummResult
	}
)

// interface guard
var (
	_ xreg.Renewable = (*nsummFactory)(nil)
	_ cluster.Xact   = (*nsummXact)(nil)
)

//////////////////
// nsummFactory //
//////////////////

func (*nsummFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	msg := args.Custom.(*apc.BsummCtrlMsg)
	p := &nsummFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, msg: msg}
	return p
}

func (p *nsummFactory) Start() error {
	p.xctn = newSumm(p)
	xact.GoRunW(p.xctn)
	return nil
}

func (*nsummFactory) Kind() string        { return apc.ActSummaryBck }
func (p *nsummFactory) Get() cluster.Xact { return p.xctn }

func (*nsummFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

func newSumm(p *nsummFactory) (r *nsummXact) {
	r = &nsummXact{p: p}
	mpopts := &mpather.JgroupOpts{
		T:        p.T,
		CTs:      []string{fs.ObjectType},
		Prefix:   p.msg.Prefix,
		VisitObj: r.visitObj,
		Slab:     nil, // TODO -- FIXME: check not needed
		DoLoad:   mpather.LoadUnsafe,
	}
	mpopts.Bck.Copy(p.Bck.Bucket())
	r.BckJog.Init(p.UUID(), p.Kind(), p.Bck, mpopts, cmn.GCO.Get())

	// TODO -- FIXME: temp limitations
	if !p.msg.ObjCached || !p.msg.BckPresent {
		nlog.Errorln(r.Name(), "niy", p.msg)
	}
	return
}

func (r *nsummXact) Run(wg *sync.WaitGroup) {
	debug.Assert(r.p.Bck != nil && !r.p.Bck.IsQuery()) // TODO -- FIXME: implement 1) all-buckets and 2) QueryBck
	wg.Done()

	nlog.Infoln(r.Name(), r.p.Bck.Cname(""))

	r.res.Bck = r.p.Bck.Clone()
	r.res.TotalSize.Disks = fs.GetDiskSize()
	r.res.ObjSize.Min = math.MaxInt64

	// first, estimate on-disk size
	r.res.TotalSize.OnDisk = fs.OnDiskSize(r.p.Bck.Bucket(), r.p.msg.Prefix)

	// second, joggers to walk and visit
	r.BckJog.Run()

	err := r.BckJog.Wait()
	r.AddErr(err)
	r.Finish()
}

func (r *nsummXact) str(s string) string { return fmt.Sprintf("%s %+v", s, r.p.msg) }
func (r *nsummXact) String() string      { return r.str(r.Base.String()) }
func (r *nsummXact) Name() string        { return r.str(r.Base.Name()) }

func (r *nsummXact) Snap() (snap *cluster.Snap) {
	snap = &cluster.Snap{}
	r.ToSnap(snap)
	snap.IdleX = r.IsIdle()
	return
}

func (r *nsummXact) Result() (any, error) {
	res := r.res
	res.ObjCount.Present = ratomic.LoadUint64(&r.res.ObjCount.Present)
	res.TotalSize.PresentObjs = ratomic.LoadUint64(&r.res.TotalSize.PresentObjs)

	res.ObjSize.Min = ratomic.LoadInt64(&r.res.ObjSize.Min)
	if res.ObjSize.Min == math.MaxInt64 {
		res.ObjSize.Min = 0
	}
	res.ObjSize.Max = ratomic.LoadInt64(&r.res.ObjSize.Max)

	// compute the current (maybe, running-and-changing) average and used %%
	if res.ObjCount.Present > 0 {
		res.ObjSize.Avg = int64(cos.DivRoundU64(res.TotalSize.PresentObjs, res.ObjCount.Present))
	}
	res.UsedPct = cos.DivRoundU64(res.TotalSize.OnDisk*100, r.res.TotalSize.Disks)

	// TODO -- FIXME: single-bucket limitation, here and elsewhere
	summaries := cmn.AllBsummResults{&res}

	return summaries, r.Err()
}

func (r *nsummXact) visitObj(lom *cluster.LOM, buf []byte) error {
	debug.Assert(buf == nil) // TODO -- FIXME: remove
	if !lom.IsCopy() {
		ratomic.AddUint64(&r.res.ObjCount.Present, 1)
	}
	size := lom.SizeBytes()
	if cmin := ratomic.LoadInt64(&r.res.ObjSize.Min); cmin > size {
		ratomic.CompareAndSwapInt64(&r.res.ObjSize.Min, cmin, size)
	}
	if cmax := ratomic.LoadInt64(&r.res.ObjSize.Max); cmax < size {
		ratomic.CompareAndSwapInt64(&r.res.ObjSize.Min, cmax, size)
	}
	ratomic.AddUint64(&r.res.TotalSize.PresentObjs, uint64(size))
	return nil
}
