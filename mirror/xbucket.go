// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

type (
	XactBck interface {
		cluster.Xact
		DoneCh() chan struct{}
		Target() cluster.Target
	}
	xactBckBase struct {
		xaction.XactBase
		joggers *mpather.JoggerGroup

		t      cluster.Target
		doneCh chan struct{}
	}
)

func init() {
	xreg.RegisterBucketXact(&transferBckProvider{kind: cmn.ActCopyBucket})
	xreg.RegisterBucketXact(&transferBckProvider{kind: cmn.ActETLBucket})
	xreg.RegisterBucketXact(&dirPromoteProvider{})
	xreg.RegisterBucketXact(&mncProvider{})
	xreg.RegisterBucketXact(&llcProvider{})
	xreg.RegisterBucketXact(&putMirrorProvider{})
}

func newXactBckBase(id, kind string, bck cmn.Bck, opts *mpather.JoggerGroupOpts) *xactBckBase {
	base := &xactBckBase{
		XactBase: *xaction.NewXactBaseBck(id, kind, bck),
		t:        opts.T,
	}
	base.joggers = mpather.NewJoggerGroup(opts)
	return base
}

//
// as XactBck interface
//
func (r *xactBckBase) Run()                   { cmn.Assert(false) }
func (r *xactBckBase) DoneCh() chan struct{}  { return r.doneCh }
func (r *xactBckBase) Target() cluster.Target { return r.t }

func (r *xactBckBase) runJoggers() {
	r.joggers.Run()
}

func (r *xactBckBase) waitDone() error {
	for {
		select {
		case <-r.ChanAbort():
			r.joggers.Stop()
			return cmn.NewAbortedError(r.String())
		case <-r.joggers.ListenFinished():
			return r.joggers.Stop()
		}
	}
}
