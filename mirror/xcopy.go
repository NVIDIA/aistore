// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"errors"
	"fmt"
	"runtime"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

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
	xcopyJogger struct { // one per mountpath
		joggerBckBase
		parent *XactBckMakeNCopies
		buf    []byte
	}
)

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
		xcopyJogger := newXcopyJogger(r, mpathInfo, config)
		mpathLC := mpathInfo.MakePath(fs.ObjectType, r.BckIsLocal())
		r.mpathers[mpathLC] = xcopyJogger
		go xcopyJogger.jog()
	}
	return
}

func (r *XactBckMakeNCopies) Description() string {
	return "responsible for deleting or adding object's copies within a target"
}

//
// mpath xcopyJogger - as mpather
//

func newXcopyJogger(parent *XactBckMakeNCopies, mpathInfo *fs.MountpathInfo, config *cmn.Config) *xcopyJogger {
	jbase := joggerBckBase{parent: &parent.xactBckBase, mpathInfo: mpathInfo, config: config}
	j := &xcopyJogger{joggerBckBase: jbase, parent: parent}
	j.joggerBckBase.callback = j.delAddCopies
	return j
}

//
// mpath xcopyJogger - main
//
func (j *xcopyJogger) jog() {
	glog.Infof("jogger[%s/%s] started", j.mpathInfo, j.parent.Bucket())
	j.buf = j.parent.slab.Alloc()
	j.joggerBckBase.jog()
	j.parent.slab.Free(j.buf)
}

func (j *xcopyJogger) delAddCopies(lom *cluster.LOM) (err error) {
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

func (j *xcopyJogger) delCopies(lom *cluster.LOM) (size int64, err error) {
	cluster.ObjectLocker.Lock(lom.Uname(), true)
	if j.parent.copies == 1 {
		size += lom.Size() * int64(lom.NumCopies()-1)
		if errstr := lom.DelAllCopies(); errstr != "" {
			err = errors.New(errstr)
		} else {
			err = lom.Persist()
		}
	} else {
		copies := lom.CopyFQN()
		for i := len(copies) - 1; i >= j.parent.copies-1; i-- {
			cpyfqn := copies[i]
			if errstr := lom.DelCopy(cpyfqn); errstr != "" {
				err = errors.New(errstr)
				break
			}

			if err = lom.Persist(); err != nil {
				break
			}

			size += lom.Size()
		}
	}
	lom.ReCache()
	cluster.ObjectLocker.Unlock(lom.Uname(), true)
	return
}

func (j *xcopyJogger) addCopies(lom *cluster.LOM) (size int64, err error) {
	for i := lom.NumCopies() + 1; i <= j.parent.copies; i++ {
		if mpather := findLeastUtilized(lom, j.parent.Mpathers()); mpather != nil {
			cluster.ObjectLocker.Lock(lom.Uname(), false)
			if err = copyTo(lom, mpather.mountpathInfo(), j.buf); err != nil {
				glog.Errorln(err)
				cluster.ObjectLocker.Unlock(lom.Uname(), false)
				return
			}
			size += lom.Size()
			if glog.V(4) {
				glog.Infof("%s: %s=>%s", lom, lom.ParsedFQN.MpathInfo, mpather.mountpathInfo())
			}
			cluster.ObjectLocker.Unlock(lom.Uname(), false)
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
