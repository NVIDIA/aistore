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
	llcProvider struct {
		t    cluster.Target
		xact *xactLLC
	}
	xactLLC struct {
		xactBckBase
	}
	llcJogger struct { // one per mountpath
		joggerBckBase
		parent *xactLLC
	}
)

func (*llcProvider) New(args registry.XactArgs) registry.BucketEntry {
	return &llcProvider{t: args.T}
}

func (p *llcProvider) Start(bck cmn.Bck) error {
	xact := newXactLLC(p.t, bck)
	go xact.Run()
	p.xact = xact
	return nil
}
func (*llcProvider) Kind() string        { return cmn.ActLoadLomCache }
func (p *llcProvider) Get() cluster.Xact { return p.xact }

// NOTE: Not using registry.BaseBckEntry because it would return `false, nil`.
func (p *llcProvider) PreRenewHook(_ registry.BucketEntry) (bool, error) { return true, nil }
func (p *llcProvider) PostRenewHook(_ registry.BucketEntry)              {}

func newXactLLC(t cluster.Target, bck cmn.Bck) *xactLLC {
	return &xactLLC{xactBckBase: *newXactBckBase("", cmn.ActLoadLomCache, bck, t)}
}

func (r *xactLLC) Run() (err error) {
	mpathCount := r.runJoggers()
	glog.Infoln(r.String())
	return r.xactBckBase.waitDone(mpathCount)
}

func (r *xactLLC) runJoggers() (mpathCount int) {
	var (
		availablePaths, _ = fs.Get()
		config            = cmn.GCO.Get()
	)
	mpathCount = len(availablePaths)
	r.xactBckBase.init(mpathCount)
	for _, mpathInfo := range availablePaths {
		jogger := newLLCJogger(r, mpathInfo, config)
		mpathLC := mpathInfo.MakePathCT(r.Bck(), fs.ObjectType)
		r.mpathers[mpathLC] = jogger
		go jogger.jog()
	}
	return
}

//
// mpath llcJogger - main
//

func newLLCJogger(parent *xactLLC, mpathInfo *fs.MountpathInfo, config *cmn.Config) *llcJogger {
	j := &llcJogger{
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

func (j *llcJogger) jog() {
	glog.Infof("jogger[%s/%s] started", j.mpathInfo, j.parent.Bck())
	j.joggerBckBase.jog()
}

// note: consider j.parent.ObjectsInc() here
func (j *llcJogger) noop(*cluster.LOM) error { return nil }
