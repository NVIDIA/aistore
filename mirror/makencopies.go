// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"runtime"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

const pkgName = "mirror"
const (
	throttleNumObjects = 16                      // unit of self-throttling
	minThrottleSize    = 256 * cmn.KiB           // throttle every 4MB (= 16K*256) or greater
	logNumProcessed    = throttleNumObjects * 16 // unit of house-keeping
	MaxNCopies         = 16                      // validation
)

// XactBckMakeNCopies runs in a background, traverses all local mountpaths, and makes sure
// the bucket is N-way replicated (where N >= 1)

type (
	XactBckMakeNCopies struct {
		xactBckBase
		slab   *memsys.Slab2
		copies int
	}
	mncJogger struct { // one per mountpath
		joggerBckBase
		parent *XactBckMakeNCopies
		buf    []byte
	}
)

func init() {
	if logLvl, ok := cmn.CheckDebug(pkgName); ok {
		glog.SetV(glog.SmoduleMirror, logLvl)
	}
}

//
// public methods
//

func NewXactMNC(id int64, bucket string, t cluster.Target, slab *memsys.Slab2, copies int, local bool) *XactBckMakeNCopies {
	return &XactBckMakeNCopies{
		xactBckBase: *newXactBckBase(id, cmn.ActMakeNCopies, bucket, t, local),
		slab:        slab,
		copies:      copies,
	}
}

func (r *XactBckMakeNCopies) Run() (err error) {
	var numjs int
	if numjs, err = r.init(); err != nil {
		return
	}
	glog.Infoln(r.String(), "copies=", r.copies)
	return r.xactBckBase.run(numjs)
}

func ValidateNCopies(copies int) error {
	const s = "number of local copies"
	if _, err := cmn.CheckI64Range(int64(copies), 1, MaxNCopies); err != nil {
		return fmt.Errorf("%s (%d) %s", s, copies, err.Error())
	}
	availablePaths, _ := fs.Mountpaths.Get()
	nmp := len(availablePaths)
	if copies > nmp {
		return fmt.Errorf("%s (%d) exceeds the number of mountpaths (%d)", s, copies, nmp)
	}
	return nil
}

//
// private methods
//

func (r *XactBckMakeNCopies) init() (numjs int, err error) {
	if err = ValidateNCopies(r.copies); err != nil {
		return
	}
	availablePaths, _ := fs.Mountpaths.Get()
	r.xactBckBase.init(availablePaths)
	numjs = len(availablePaths)
	config := cmn.GCO.Get()
	for _, mpathInfo := range availablePaths {
		mncJogger := newMncJogger(r, mpathInfo, config)
		mpathLC := mpathInfo.MakePath(fs.ObjectType, r.BckProvider())
		r.mpathers[mpathLC] = mncJogger
	}
	for _, mpather := range r.mpathers {
		mncJogger := mpather.(*mncJogger)
		go mncJogger.jog()
	}
	return
}

func (r *XactBckMakeNCopies) Description() string {
	return "delete or add local object copies (replicas)"
}

//
// mpath mncJogger - as mpather
//

func newMncJogger(parent *XactBckMakeNCopies, mpathInfo *fs.MountpathInfo, config *cmn.Config) *mncJogger {
	jbase := joggerBckBase{parent: &parent.xactBckBase, mpathInfo: mpathInfo, config: config}
	j := &mncJogger{joggerBckBase: jbase, parent: parent}
	j.joggerBckBase.callback = j.delAddCopies
	return j
}

//
// mpath mncJogger - main
//
func (j *mncJogger) jog() {
	glog.Infof("jogger[%s/%s] started", j.mpathInfo, j.parent.Bucket())
	j.buf = j.parent.slab.Alloc()
	j.joggerBckBase.jog()
	j.parent.slab.Free(j.buf)
}

func (j *mncJogger) delAddCopies(lom *cluster.LOM) (err error) {
	var size int64
	if n := lom.NumCopies(); n == j.parent.copies {
		return nil
	} else if n > j.parent.copies {
		size, err = j.delCopies(lom)
	} else {
		size, err = j.addCopies(lom)
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
			glog.Infof("jogger[%s/%s] processed %d objects...", j.mpathInfo, j.parent.Bucket(), j.num)
			j.config = cmn.GCO.Get()
		}
	} else {
		runtime.Gosched()
	}
	return
}

func (j *mncJogger) delCopies(lom *cluster.LOM) (size int64, err error) {
	lom.Lock(true)
	if j.parent.copies == 1 {
		size += lom.Size() * int64(lom.NumCopies()-1)
		if err = lom.DelAllCopies(); err == nil {
			err = lom.Persist()
		}
	} else {
		var (
			copies  = lom.GetCopies()
			ndel    = len(copies) - j.parent.copies + 1
			persist bool
		)
		for cpyfqn := range copies {
			if ndel <= 0 {
				break
			}
			if err = lom.DelCopy(cpyfqn); err == nil {
				ndel--
				size += lom.Size()
				persist = true
			}
		}
		if persist {
			if ers := lom.Persist(); ers != nil {
				if err == nil {
					err = ers
				} else {
					err = fmt.Errorf("[%v], [%v]", err, ers)
				}
			}
		}
	}
	lom.ReCache()
	lom.Unlock(true)
	return
}

func (j *mncJogger) addCopies(lom *cluster.LOM) (size int64, err error) {
	for i := lom.NumCopies() + 1; i <= j.parent.copies; i++ {
		if mpather := findLeastUtilized(lom, j.parent.Mpathers()); mpather != nil {
			lom.Lock(false)
			var clone *cluster.LOM
			if clone, err = copyTo(lom, mpather.mountpathInfo(), j.buf); err != nil {
				glog.Errorln(err)
				lom.Unlock(false)
				return
			}
			size += lom.Size()
			if glog.FastV(4, glog.SmoduleMirror) {
				glog.Infof("copied %s=>%s", lom, clone)
			}
			lom.Unlock(false)
		} else {
			err = fmt.Errorf("%s (copies=%d): cannot find dst mountpath", lom, lom.NumCopies())
			return
		}
	}
	return
}

// static helper
func checkErrNumMp(xx cmn.Xact, l int) error {
	if l < 2 {
		return fmt.Errorf("%s: number of mountpaths (%d) is insufficient for local mirroring, exiting", xx, l)
	}
	return nil
}
