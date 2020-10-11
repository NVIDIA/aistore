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
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

type (
	llcProvider struct {
		t    cluster.Target
		xact *xactLLC
	}
	xactLLC struct {
		xactBckBase
	}
)

// interface guard
var _ cluster.Xact = &xactLLC{}

func (*llcProvider) New(args xreg.XactArgs) xreg.BucketEntry {
	return &llcProvider{t: args.T}
}

func (p *llcProvider) Start(bck cmn.Bck) error {
	xact := newXactLLC(p.t, bck)
	p.xact = xact
	go xact.Run()
	return nil
}
func (*llcProvider) Kind() string        { return cmn.ActLoadLomCache }
func (p *llcProvider) Get() cluster.Xact { return p.xact }

// NOTE: Not using xreg.BaseBckEntry because it would return `false, nil`.
func (p *llcProvider) PreRenewHook(_ xreg.BucketEntry) (bool, error) { return true, nil }
func (p *llcProvider) PostRenewHook(_ xreg.BucketEntry)              {}

func newXactLLC(t cluster.Target, bck cmn.Bck) *xactLLC {
	return &xactLLC{xactBckBase: *newXactBckBase("", cmn.ActLoadLomCache, &mpather.JoggerGroupOpts{
		T:        t,
		Bck:      bck,
		CTs:      []string{fs.ObjectType},
		Callback: func(_ *cluster.LOM, _ []byte) error { return nil },
		DoLoad:   mpather.Load,
	})}
}

func (r *xactLLC) Run() (err error) {
	r.xactBckBase.runJoggers()
	glog.Infoln(r.String())
	return r.xactBckBase.waitDone()
}
