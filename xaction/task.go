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
	baseEntry
	ctx  context.Context
	xact *bckListTask
	t    cluster.Target
	id   string
	msg  *cmn.SelectMsg
	bck  *cluster.Bck
}

func (e *bckListTaskEntry) Start(_ string) error {
	xact := &bckListTask{
		XactBase: *cmn.NewXactBase(e.id, cmn.ActAsyncTask),
		ctx:      e.ctx,
		t:        e.t,
		msg:      e.msg,
		bck:      e.bck,
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
	return e.stats.FillFromXact(e.xact)
}

type bckSummaryTaskEntry struct {
	baseEntry
	ctx  context.Context
	xact *bckSummaryTask
	t    cluster.Target
	id   string
	msg  *cmn.SelectMsg
	bck  *cluster.Bck
}

func (e *bckSummaryTaskEntry) Start(_ string) error {
	xact := &bckSummaryTask{
		XactBase: *cmn.NewXactBase(e.id, cmn.ActAsyncTask),
		t:        e.t,
		msg:      e.msg,
		bck:      e.bck,
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
	return e.stats.FillFromXact(e.xact)
}
