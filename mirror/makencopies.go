// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"os"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xreg"
)

type (
	mncFactory struct {
		xreg.RenewBase
		xact *xactMNC
		args xreg.MNCArgs
	}

	// xactMNC runs in a background, traverses all local mountpaths, and makes sure
	// the bucket is N-way replicated (where N >= 1).
	xactMNC struct {
		xaction.XactBckJog
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

func (*mncFactory) New(args xreg.Args, bck *cluster.Bck) xreg.Renewable {
	p := &mncFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, args: *args.Custom.(*xreg.MNCArgs)}
	return p
}

func (p *mncFactory) Start() error {
	slab, err := p.T.PageMM().GetSlab(memsys.MaxPageSlabSize)
	cos.AssertNoErr(err)
	p.xact = newXactMNC(p.Bck, p, slab)
	return nil
}

func (*mncFactory) Kind() string        { return cmn.ActMakeNCopies }
func (p *mncFactory) Get() cluster.Xact { return p.xact }

func (p *mncFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	err = fmt.Errorf("%s is currently running, cannot start a new %q",
		prevEntry.Get(), p.Str(p.Kind()))
	return
}

/////////////
// xactMNC //
/////////////

func (r *xactMNC) String() string {
	return fmt.Sprintf("%s tag=%s, copies=%d", r.XactBase.String(), r.tag, r.copies)
}

func (r *xactMNC) Name() string {
	return fmt.Sprintf("%s tag=%s, copies=%d", r.XactBase.Name(), r.tag, r.copies)
}

func newXactMNC(bck *cluster.Bck, p *mncFactory, slab *memsys.Slab) (r *xactMNC) {
	r = &xactMNC{tag: p.args.Tag, copies: p.args.Copies}
	debug.Assert(r.tag != "" && r.copies > 0)
	mpopts := &mpather.JoggerGroupOpts{
		Bck:      bck.Bck,
		T:        p.T,
		CTs:      []string{fs.ObjectType},
		VisitObj: r.visitObj,
		Slab:     slab,
		DoLoad:   mpather.Load, // Required to fetch `NumCopies()` and skip copies.
		Throttle: true,
	}
	r.XactBckJog.Init(p.UUID(), cmn.ActMakeNCopies, bck, mpopts)
	return
}

func (r *xactMNC) Run(wg *sync.WaitGroup) {
	wg.Done()
	if err := fs.ValidateNCopies(r.Target().Sname(), r.copies); err != nil {
		r.Finish(err)
		return
	}
	r.XactBckJog.Run()
	glog.Infoln(r.Name())
	err := r.XactBckJog.Wait()
	r.Finish(err)
}

func (r *xactMNC) visitObj(lom *cluster.LOM, buf []byte) (err error) {
	var size int64
	if n := lom.NumCopies(); n == r.copies {
		return nil
	} else if n > r.copies {
		size, err = delCopies(lom, r.copies)
	} else {
		size, err = addCopies(lom, r.copies, buf)
	}

	if os.IsNotExist(err) {
		return nil
	}
	if err != nil && cos.IsErrOOS(err) {
		return cmn.NewErrAborted(r.Name(), "visit-obj", err)
	}

	r.ObjsInc()
	r.BytesAdd(size)

	if r.Objs()%100 == 0 {
		if cs := fs.GetCapStatus(); cs.Err != nil {
			return cmn.NewErrAborted(r.Name(), "visit-obj", cs.Err)
		}
	}
	return nil
}
