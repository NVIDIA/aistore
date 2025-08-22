// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	mncFactory struct {
		xreg.RenewBase
		xctn *mncXact
		args xreg.MNCArgs
	}

	// mncXact runs in a background, traverses all local mountpaths, and makes sure
	// the bucket is N-way replicated (where N >= 1).
	mncXact struct {
		p *mncFactory
		xact.BckJog
		_nam, _str string
	}
)

// interface guard
var (
	_ core.Xact      = (*mncXact)(nil)
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
	slab, err := core.T.PageMM().GetSlab(memsys.MaxPageSlabSize)
	debug.AssertNoErr(err)
	p.xctn = newMNC(p, slab)
	return nil
}

func (*mncFactory) Kind() string     { return apc.ActMakeNCopies }
func (p *mncFactory) Get() core.Xact { return p.xctn }

func (p *mncFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	err = fmt.Errorf("%s is currently running, cannot start a new %q", prevEntry.Get(), p.Str(p.Kind()))
	return
}

/////////////
// mncXact //
/////////////

// NOTE: always throttling
func newMNC(p *mncFactory, slab *memsys.Slab) (r *mncXact) {
	debug.Assert(p.args.Tag != "" && p.args.Copies > 0)
	r = &mncXact{p: p}
	mpopts := &mpather.JgroupOpts{
		CTs:      []string{fs.ObjCT},
		VisitObj: r.visitObj,
		Slab:     slab,
		DoLoad:   mpather.Load,
		Throttle: true,
	}
	mpopts.Bck.Copy(p.Bck.Bucket())
	s := fmt.Sprintf("%s-copies-%d", r.p.args.Tag, r.p.args.Copies)
	r.BckJog.Init(p.UUID(), apc.ActMakeNCopies, s /*ctlmsg*/, p.Bck, mpopts, cmn.GCO.Get())

	// name
	r._nam = r.Base.Name() + "-" + s
	r._str = r.Base.String() + "-" + s
	return r
}

func (r *mncXact) Run(wg *sync.WaitGroup) {
	wg.Done()
	tname := core.T.String()
	if err := fs.ValidateNCopies(tname, r.p.args.Copies); err != nil {
		r.AddErr(err)
		r.Finish()
		return
	}
	r.BckJog.Run()
	nlog.Infoln(r.Name())
	err := r.BckJog.Wait()
	if err != nil {
		r.AddErr(err)
	}
	r.Finish()
}

func (r *mncXact) visitObj(lom *core.LOM, buf []byte) (err error) {
	var (
		size   int64
		n      = lom.NumCopies()
		copies = r.p.args.Copies
	)
	switch {
	case n == copies:
		return nil
	case n > copies:
		lom.Lock(true)
		size, err = delCopies(lom, copies)
		lom.Unlock(true)
	default:
		lom.Lock(true)
		size, err = addCopies(lom, copies, buf)
		lom.Unlock(true)
	}

	if err != nil {
		if cos.IsNotExist(err) {
			return nil
		}
		if cos.IsErrOOS(err) {
			r.Abort(err)
		} else {
			cs := fs.Cap()
			if errCap := cs.Err(); errCap != nil {
				r.Abort(fmt.Errorf("errors: [%w] and [%w]", err, errCap))
			} else {
				r.AddErr(err)
			}
		}
		return err
	}

	if cmn.Rom.FastV(5, cos.SmoduleMirror) {
		nlog.Infof("%s: %s, copies %d=>%d, size=%d", r.Base.Name(), lom.Cname(), n, copies, size)
	}
	r.ObjsAdd(1, size)
	if cnt := r.Objs(); cnt%128 == 0 { // TODO: configurable
		cs := fs.Cap()
		if errCap := cs.Err(); errCap != nil {
			r.Abort(errCap)
			err = errCap
		}
	}
	return err
}

func (r *mncXact) String() string { return r._str }
func (r *mncXact) Name() string   { return r._nam }

func (r *mncXact) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}
