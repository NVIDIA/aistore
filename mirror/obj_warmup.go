// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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

func NewXactLLC(t cluster.Target, bck cmn.Bck) *XactBckLoadLomCache {
	return &XactBckLoadLomCache{xactBckBase: *newXactBckBase("", cmn.ActLoadLomCache, bck, t)}
}

func (r *XactBckLoadLomCache) Run() (err error) {
	var mpathCount int
	if mpathCount, err = r.init(); err != nil {
		return
	}
	glog.Infoln(r.String())
	return r.xactBckBase.run(mpathCount)
}

//
// private methods
//

func (r *XactBckLoadLomCache) init() (mpathCount int, err error) {
	var (
		availablePaths, _ = fs.Mountpaths.Get()
		config            = cmn.GCO.Get()
	)
	mpathCount = len(availablePaths)

	r.xactBckBase.init(mpathCount)
	for _, mpathInfo := range availablePaths {
		xwarmJogger := newXwarmJogger(r, mpathInfo, config)
		mpathLC := mpathInfo.MakePathCT(r.Bck(), fs.ObjectType)
		r.mpathers[mpathLC] = xwarmJogger
		go xwarmJogger.jog()
	}
	return
}

//
// mpath xwarmJogger - main
//

func newXwarmJogger(parent *XactBckLoadLomCache, mpathInfo *fs.MountpathInfo, config *cmn.Config) *xwarmJogger {
	j := &xwarmJogger{
		joggerBckBase: joggerBckBase{
			parent:    &parent.xactBckBase,
			bck:       parent.Bck(),
			mpathInfo: mpathInfo,
			config:    config,
		},
		parent: parent,
	}
	j.joggerBckBase.callback = j.noop
	return j
}

func (j *xwarmJogger) jog() {
	glog.Infof("jogger[%s/%s] started", j.mpathInfo, j.parent.Bck())
	j.joggerBckBase.jog()
}

func (j *xwarmJogger) noop(*cluster.LOM) error { return nil }
