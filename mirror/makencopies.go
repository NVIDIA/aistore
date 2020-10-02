// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"os"
	"runtime"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xaction/registry"
)

const (
	throttleNumObjects = 16                      // unit of self-throttling
	minThrottleSize    = 256 * cmn.KiB           // throttle every 4MB (= 16K*256) or greater
	logNumProcessed    = throttleNumObjects * 16 // unit of house-keeping
	MaxNCopies         = 16                      // validation
)

type (
	mncProvider struct {
		registry.BaseBckEntry
		xact *xactMNC

		t      cluster.Target
		uuid   string
		copies int
	}

	// xactMNC runs in a background, traverses all local mountpaths, and makes sure
	// the bucket is N-way replicated (where N >= 1).
	xactMNC struct {
		xactBckBase
		slab   *memsys.Slab
		copies int
	}
	mncJogger struct { // one per mountpath
		joggerBckBase
		parent *xactMNC
		buf    []byte
	}
)

func (*mncProvider) New(args registry.XactArgs) registry.BucketEntry {
	return &mncProvider{t: args.T, uuid: args.UUID, copies: args.Custom.(int)}
}
func (p *mncProvider) Start(bck cmn.Bck) error {
	slab, err := p.t.MMSA().GetSlab(memsys.MaxPageSlabSize)
	cmn.AssertNoErr(err)
	p.xact = newXactMNC(bck, p.t, slab, p.uuid, p.copies)
	return nil
}
func (*mncProvider) Kind() string        { return cmn.ActMakeNCopies }
func (p *mncProvider) Get() cluster.Xact { return p.xact }

func newXactMNC(bck cmn.Bck, t cluster.Target, slab *memsys.Slab, id string, copies int) *xactMNC {
	return &xactMNC{
		xactBckBase: *newXactBckBase(id, cmn.ActMakeNCopies, bck, t),
		slab:        slab,
		copies:      copies,
	}
}

func (r *xactMNC) Run() (err error) {
	var mpathersCount int
	if mpathersCount, err = r.runJoggers(); err != nil {
		return
	}
	glog.Infoln(r.String(), "copies=", r.copies)
	err = r.xactBckBase.waitDone(mpathersCount)
	r.Finish(err)
	return
}

func ValidateNCopies(prefix string, copies int) error {
	if _, err := cmn.CheckI64Range(int64(copies), 1, MaxNCopies); err != nil {
		return fmt.Errorf("number of copies (%d) %s", copies, err.Error())
	}
	availablePaths, _ := fs.Get()
	mpathCount := len(availablePaths)
	if mpathCount == 0 {
		return fmt.Errorf("%s: %s", prefix, cmn.NoMountpaths)
	}
	if copies > mpathCount {
		return fmt.Errorf("%s: number of copies (%d) exceeds the number of mountpaths (%d)", prefix, copies, mpathCount)
	}
	return nil
}

//
// private methods
//

func (r *xactMNC) runJoggers() (mpathCount int, err error) {
	tname := r.Target().Snode().Name()
	if err = ValidateNCopies(tname, r.copies); err != nil {
		return
	}

	var (
		availablePaths, _ = fs.Get()
		config            = cmn.GCO.Get()
	)
	mpathCount = len(availablePaths)

	r.xactBckBase.init(mpathCount)
	for _, mpathInfo := range availablePaths {
		mncJogger := newMNCJogger(r, mpathInfo, config)
		mpathLC := mpathInfo.MakePathCT(r.Bck(), fs.ObjectType)
		r.mpathers[mpathLC] = mncJogger
	}
	for _, mpather := range r.mpathers {
		mncJogger := mpather.(*mncJogger)
		go mncJogger.jog()
	}
	return
}

//
// mpath mncJogger - main
//

func newMNCJogger(parent *xactMNC, mpathInfo *fs.MountpathInfo, config *cmn.Config) *mncJogger {
	j := &mncJogger{
		joggerBckBase: joggerBckBase{
			parent:    &parent.xactBckBase,
			bck:       parent.Bck(),
			mpathInfo: mpathInfo,
			config:    config,
		},
		parent: parent,
	}
	j.joggerBckBase.callback = j.delAddCopies
	return j
}

func (j *mncJogger) jog() {
	glog.Infof("jogger[%s/%s] started", j.mpathInfo, j.parent.Bck())
	j.buf = j.parent.slab.Alloc()
	j.joggerBckBase.jog()
	j.parent.slab.Free(j.buf)
}

func (j *mncJogger) delAddCopies(lom *cluster.LOM) (err error) {
	if j.parent.Aborted() {
		return cmn.NewAbortedError("makenaction xaction")
	}

	var size int64
	if n := lom.NumCopies(); n == j.parent.copies {
		return nil
	} else if n > j.parent.copies {
		size, err = delCopies(lom, j.parent.copies)
	} else {
		size, err = addCopies(lom, j.parent.copies, j.parent.Mpathers(), j.buf)
	}

	if os.IsNotExist(err) {
		return nil
	}
	if err != nil && cmn.IsErrOOS(err) {
		what := fmt.Sprintf("%s(%q)", j.parent.Kind(), j.parent.ID())
		return cmn.NewAbortedErrorDetails(what, err.Error())
	}

	j.num++
	j.size += size

	j.parent.ObjectsInc()
	j.parent.BytesAdd(size)

	if (j.num % throttleNumObjects) == 0 {
		if j.size > minThrottleSize*throttleNumObjects {
			j.size = 0
			if errstop := j.yieldTerm(); errstop != nil {
				return errstop
			}
		}
		if (j.num % logNumProcessed) == 0 {
			glog.Infof("jogger[%s/%s] processed %d objects...", j.mpathInfo, j.parent.Bck(), j.num)
			j.config = cmn.GCO.Get()
		}
		if cs := fs.GetCapStatus(); cs.Err != nil {
			what := fmt.Sprintf("%s(%q)", j.parent.Kind(), j.parent.ID())
			return cmn.NewAbortedErrorDetails(what, cs.Err.Error())
		}
	} else {
		runtime.Gosched()
	}
	return
}
