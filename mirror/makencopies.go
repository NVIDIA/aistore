// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

type (
	mncFactory struct {
		xreg.BaseBckEntry
		xact *xactMNC
		t    cluster.Target
		uuid string
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
	_ cluster.Xact    = (*xactMNC)(nil)
	_ xreg.BckFactory = (*mncFactory)(nil)
)

////////////////
// mncFactory //
////////////////

func (*mncFactory) New(args xreg.Args) xreg.Renewable {
	return &mncFactory{t: args.T, uuid: args.UUID, args: *args.Custom.(*xreg.MNCArgs)}
}

func (p *mncFactory) Start(bck cmn.Bck) error {
	slab, err := p.t.MMSA().GetSlab(memsys.MaxPageSlabSize)
	cos.AssertNoErr(err)
	p.xact = newXactMNC(bck, p, slab)
	return nil
}

func (*mncFactory) Kind() string        { return cmn.ActMakeNCopies }
func (p *mncFactory) Get() cluster.Xact { return p.xact }

/////////////
// xactMNC //
/////////////

func (r *xactMNC) String() string {
	return fmt.Sprintf("%s tag=%s, copies=%d", r.XactBase.String(), r.tag, r.copies)
}

func newXactMNC(bck cmn.Bck, p *mncFactory, slab *memsys.Slab) (r *xactMNC) {
	r = &xactMNC{tag: p.args.Tag, copies: p.args.Copies}
	debug.Assert(r.tag != "" && r.copies > 0)
	mpopts := &mpather.JoggerGroupOpts{
		Bck:      bck,
		T:        p.t,
		CTs:      []string{fs.ObjectType},
		VisitObj: r.visitObj,
		Slab:     slab,
		DoLoad:   mpather.Load, // Required to fetch `NumCopies()` and skip copies.
		Throttle: true,
	}
	r.XactBckJog.Init(p.uuid, cmn.ActMakeNCopies, bck, mpopts)
	return
}

func (r *xactMNC) Run() {
	if err := fs.ValidateNCopies(r.Target().Sname(), r.copies); err != nil {
		r.Finish(err)
		return
	}
	r.XactBckJog.Run()
	glog.Infoln(r.String())
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
		what := fmt.Sprintf("%s(%q)", r.Kind(), r.ID())
		return cmn.NewAbortedError(what, err.Error())
	}

	r.ObjectsInc()
	r.BytesAdd(size)

	if r.ObjCount()%100 == 0 {
		if cs := fs.GetCapStatus(); cs.Err != nil {
			what := fmt.Sprintf("%s(%q)", r.Kind(), r.ID())
			return cmn.NewAbortedError(what, cs.Err.Error())
		}
	}
	return nil
}
