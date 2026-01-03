// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs/mpather"
)

type BckJog struct {
	Config  *cmn.Config
	joggers *mpather.Jgroup
	Base
}

func (r *BckJog) Init(id, kind string, bck *meta.Bck, opts *mpather.JgroupOpts, config *cmn.Config) {
	r.InitBase(id, kind, bck)
	r.joggers = mpather.NewJgroup(opts, config, nil)
	r.Config = config
}

func (r *BckJog) Run() { r.joggers.Run() }

func (r *BckJog) NumJoggers() int {
	if r.joggers == nil {
		return 0
	}
	return r.joggers.NumJ()
}

func (r *BckJog) NumVisits() int64 {
	if r.joggers == nil {
		return 0
	}
	return r.joggers.NumVisits()
}

func (r *BckJog) Wait() error {
	select {
	case errCause := <-r.ChanAbort():
		r.joggers.Stop()
		if cmn.IsErrAborted(errCause) {
			return errCause
		}
		return cmn.NewErrAborted(r.Name(), "x-bck-jog", errCause)
	case <-r.joggers.ListenFinished():
		return r.joggers.Stop()
	}
}
