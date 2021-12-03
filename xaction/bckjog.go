// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs/mpather"
)

type XactBckJog struct {
	XactBase
	t       cluster.Target
	joggers *mpather.JoggerGroup
}

func (r *XactBckJog) Init(id, kind string, bck *cluster.Bck, opts *mpather.JoggerGroupOpts) {
	r.t = opts.T
	r.InitBase(id, kind, bck)
	r.joggers = mpather.NewJoggerGroup(opts)
}

func (r *XactBckJog) Run()                   { r.joggers.Run() }
func (r *XactBckJog) Target() cluster.Target { return r.t }

func (r *XactBckJog) Wait() error {
	for {
		select {
		case <-r.ChanAbort():
			r.joggers.Stop()
			return cmn.NewErrAborted(r.Name(), "x-bck-jog", nil)
		case <-r.joggers.ListenFinished():
			return r.joggers.Stop()
		}
	}
}
