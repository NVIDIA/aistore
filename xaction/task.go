// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"context"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
)

type bckListTaskEntry struct {
	ctx  context.Context
	xact *bckListTask
	t    cluster.Target
	id   string
	msg  *cmn.SelectMsg
}

func (e *bckListTaskEntry) Start(_ string, bck cmn.Bck) error {
	xact := &bckListTask{
		XactBase: *cmn.NewXactBaseWithBucket(e.id, cmn.ActAsyncTask, bck),
		ctx:      e.ctx,
		t:        e.t,
		msg:      e.msg,
	}
	e.xact = xact
	go xact.Run()
	return nil
}

func (e *bckListTaskEntry) Kind() string   { return cmn.ActAsyncTask }
func (e *bckListTaskEntry) IsGlobal() bool { return false }
func (e *bckListTaskEntry) IsTask() bool   { return true }
func (e *bckListTaskEntry) Get() cmn.Xact  { return e.xact }
func (e *bckListTaskEntry) Stats(xact cmn.Xact) stats.XactStats {
	cmn.Assert(e.xact == xact)
	return stats.NewXactStats(e.xact)
}

type bckSummaryTaskEntry struct {
	ctx  context.Context
	xact *bckSummaryTask
	t    cluster.Target
	id   string
	msg  *cmn.SelectMsg
}

func (e *bckSummaryTaskEntry) Start(_ string, bck cmn.Bck) error {
	xact := &bckSummaryTask{
		XactBase: *cmn.NewXactBaseWithBucket(e.id, cmn.ActAsyncTask, bck),
		t:        e.t,
		msg:      e.msg,
		ctx:      e.ctx,
	}
	e.xact = xact
	go xact.Run()
	return nil
}

func (e *bckSummaryTaskEntry) Kind() string   { return cmn.ActAsyncTask }
func (e *bckSummaryTaskEntry) IsGlobal() bool { return false }
func (e *bckSummaryTaskEntry) IsTask() bool   { return true }
func (e *bckSummaryTaskEntry) Get() cmn.Xact  { return e.xact }
func (e *bckSummaryTaskEntry) Stats(xact cmn.Xact) stats.XactStats {
	cmn.Assert(e.xact == xact)
	return stats.NewXactStats(e.xact)
}
