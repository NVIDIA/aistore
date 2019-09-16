// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
)

type (
	XactBckLoadLomCache struct {
		xactBckBase
	}
	xwarmJogger struct { // one per mountpath
		joggerBckBase
		parent *XactBckLoadLomCache
	}
)

//
// public methods
//

func NewXactLLC(id int64, bucket string, t cluster.Target, local bool) *XactBckLoadLomCache {
	return &XactBckLoadLomCache{xactBckBase: *newXactBckBase(id, cmn.ActLoadLomCache, bucket, t, local)}
}

func (r *XactBckLoadLomCache) Run() (err error) {
	var numjs int
	if numjs, err = r.init(); err != nil {
		return
	}
	glog.Infoln(r.String())
	return r.xactBckBase.run(numjs)
}

//
// private methods
//

func (r *XactBckLoadLomCache) init() (numjs int, err error) {
	availablePaths, _ := fs.Mountpaths.Get()
	r.xactBckBase.init(availablePaths)
	numjs = len(availablePaths)
	config := cmn.GCO.Get()
	for _, mpathInfo := range availablePaths {
		xwarmJogger := newXwarmJogger(r, mpathInfo, config)
		mpathLC := mpathInfo.MakePath(fs.ObjectType, r.BckProvider())
		r.mpathers[mpathLC] = xwarmJogger
		go xwarmJogger.jog()
	}
	return
}

func (r *XactBckLoadLomCache) Description() string {
	return "load object metadata into in-memory cache"
}

//
// mpath xwarmJogger - as mpather
//

func newXwarmJogger(parent *XactBckLoadLomCache, mpathInfo *fs.MountpathInfo, config *cmn.Config) *xwarmJogger {
	jbase := joggerBckBase{parent: &parent.xactBckBase, mpathInfo: mpathInfo, config: config}
	j := &xwarmJogger{joggerBckBase: jbase, parent: parent}
	j.joggerBckBase.callback = j.noop
	return j
}

//
// mpath xwarmJogger - main
//
func (j *xwarmJogger) jog() {
	glog.Infof("jogger[%s/%s] started", j.mpathInfo, j.parent.Bucket())
	j.joggerBckBase.jog()
}

func (j *xwarmJogger) noop(*cluster.LOM) error { return nil }
