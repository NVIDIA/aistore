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
	"github.com/NVIDIA/aistore/xaction/registry"
)

type (
	loadLomCacheProvider struct {
		t    cluster.Target
		xact *XactBckLoadLomCache
	}
	XactBckLoadLomCache struct {
		xactBckBase
	}
	xwarmJogger struct { // one per mountpath
		joggerBckBase
		parent *XactBckLoadLomCache
	}
)

func (*loadLomCacheProvider) New(args registry.XactArgs) registry.BucketEntry {
	return &loadLomCacheProvider{t: args.T}
}
func (e *loadLomCacheProvider) Start(bck cmn.Bck) error {
	x := NewXactLLC(e.t, bck)
	go x.Run()
	e.xact = x
	return nil
}
func (*loadLomCacheProvider) Kind() string                                        { return cmn.ActLoadLomCache }
func (e *loadLomCacheProvider) Get() cluster.Xact                                 { return e.xact }
func (e *loadLomCacheProvider) PreRenewHook(_ registry.BucketEntry) (bool, error) { return true, nil }
func (e *loadLomCacheProvider) PostRenewHook(_ registry.BucketEntry)              {}

//
// public methods
//

func NewXactLLC(t cluster.Target, bck cmn.Bck) *XactBckLoadLomCache {
	return &XactBckLoadLomCache{xactBckBase: *newXactBckBase("", cmn.ActLoadLomCache, bck, t)}
}

func (r *XactBckLoadLomCache) Run() (err error) {
	mpathCount := r.runJoggers()
	glog.Infoln(r.String())
	return r.xactBckBase.waitDone(mpathCount)
}

//
// private methods
//

func (r *XactBckLoadLomCache) runJoggers() (mpathCount int) {
	var (
		availablePaths, _ = fs.Get()
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

// note: consider j.parent.ObjectsInc() here
func (j *xwarmJogger) noop(*cluster.LOM) error { return nil }
