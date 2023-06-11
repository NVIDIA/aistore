// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"os"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	mncFactory struct {
		xreg.RenewBase
		xctn *xactMNC
		args xreg.MNCArgs
	}

	// xactMNC runs in a background, traverses all local mountpaths, and makes sure
	// the bucket is N-way replicated (where N >= 1).
	xactMNC struct {
		xact.BckJog
		tag    string
		copies int
	}
)

// interface guard
var (
	_ cluster.Xact   = (*xactMNC)(nil)
	_ xreg.Renewable = (*mncFactory)(nil)
)

////////////////
// mncFactory //
////////////////

func (*mncFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	p := &mncFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, args: *args.Custom.(*xreg.MNCArgs)}
	return p
}

func (p *mncFactory) Start() error {
	slab, err := p.T.PageMM().GetSlab(memsys.MaxPageSlabSize)
	debug.AssertNoErr(err)
	p.xctn = newXactMNC(p.Bck, p, slab)
	return nil
}

func (*mncFactory) Kind() string        { return apc.ActMakeNCopies }
func (p *mncFactory) Get() cluster.Xact { return p.xctn }

func (p *mncFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	err = fmt.Errorf("%s is currently running, cannot start a new %q",
		prevEntry.Get(), p.Str(p.Kind()))
	return
}

/////////////
// xactMNC //
/////////////

func newXactMNC(bck *meta.Bck, p *mncFactory, slab *memsys.Slab) (r *xactMNC) {
	r = &xactMNC{tag: p.args.Tag, copies: p.args.Copies}
	debug.Assert(r.tag != "" && r.copies > 0)
	mpopts := &mpather.JgroupOpts{
		T:        p.T,
		CTs:      []string{fs.ObjectType},
		VisitObj: r.visitObj,
		Slab:     slab,
		DoLoad:   mpather.Load, // to support `NumCopies()` config
		Throttle: true,         // NOTE: always throttling
	}
	mpopts.Bck.Copy(bck.Bucket())
	r.BckJog.Init(p.UUID(), apc.ActMakeNCopies, bck, mpopts, cmn.GCO.Get())
	return
}

func (r *xactMNC) Run(wg *sync.WaitGroup) {
	wg.Done()
	tname := r.T.String()
	if err := fs.ValidateNCopies(tname, r.copies); err != nil {
		r.Finish(err)
		return
	}
	r.BckJog.Run()
	glog.Infoln(r.Name())
	err := r.BckJog.Wait()
	r.Finish(err)
}

func (r *xactMNC) visitObj(lom *cluster.LOM, buf []byte) (err error) {
	var (
		size int64
		n    = lom.NumCopies()
	)
	switch {
	case n == r.copies:
		return nil
	case n > r.copies:
		size, err = delCopies(lom, r.copies)
	default:
		size, err = addCopies(lom, r.copies, buf)
	}

	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		if cos.IsErrOOS(err) {
			return cmn.NewErrAborted(r.Name(), "mnc", err)
		}
		if cs := fs.Cap(); cs.Err != nil {
			return cmn.NewErrAborted(r.Name(), "mnc, orig err: ["+err.Error()+"]", cs.Err)
		}
		if r.BckJog.Config.FastV(5, glog.SmoduleMirror) {
			glog.Infof("%s: Error %v (%s, %d, %d, %d)", r.Base.Name(), err, lom.Cname(), n, r.copies, size)
		}
		return
	}

	if r.BckJog.Config.FastV(5, glog.SmoduleMirror) {
		glog.Infof("%s: %s, copies %d=>%d, size=%d", r.Base.Name(), lom.Cname(), n, r.copies, size)
	}
	r.ObjsAdd(1, size)
	if cnt := r.Objs(); cnt%128 == 0 { // TODO: tuneup
		if cs := fs.Cap(); cs.Err != nil {
			err = cmn.NewErrAborted(r.Name(), "mnc", cs.Err)
		}
	}
	return
}

func (r *xactMNC) String() string {
	return fmt.Sprintf("%s tag=%s, copies=%d", r.Base.String(), r.tag, r.copies)
}

func (r *xactMNC) Name() string {
	return fmt.Sprintf("%s tag=%s, copies=%d", r.Base.Name(), r.tag, r.copies)
}

func (r *xactMNC) Snap() (snap *cluster.Snap) {
	snap = &cluster.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}
