// Package runners provides implementation for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package runners

import (
	"context"
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/xaction"
)

type (
	FastRen struct {
		xaction.XactBase
		t         cluster.Target
		bckFrom   *cluster.Bck
		bckTo     *cluster.Bck
		rebStatsF func() ([]cluster.XactStats, error)
	}
)

func NewFastRen(uuid, kind string, bck cmn.Bck, t cluster.Target,
	bckFrom, bckTo *cluster.Bck, rebF func() ([]cluster.XactStats, error)) *FastRen {
	return &FastRen{
		XactBase:  *xaction.NewXactBaseBck(uuid, kind, bck),
		t:         t,
		bckFrom:   bckFrom,
		bckTo:     bckTo,
		rebStatsF: rebF,
	}
}

func (r *FastRen) IsMountpathXact() bool { return true }
func (r *FastRen) String() string        { return fmt.Sprintf("%s <= %s", r.XactBase.String(), r.bckFrom) }

func (r *FastRen) Run() error {
	glog.Infoln(r.String())

	// FIXME: smart wait for resilver. For now assuming that rebalance takes longer than resilver.
	var finished bool
	for !finished {
		time.Sleep(10 * time.Second)
		rebStats, err := r.rebStatsF()
		cmn.AssertNoErr(err)
		for _, stat := range rebStats {
			finished = finished || stat.Finished()
		}
	}

	r.t.BMDVersionFixup(nil, r.bckFrom.Bck, false) // piggyback bucket renaming (last step) on getting updated BMD
	r.Finish()
	return nil
}

//
//  EvictDelete
//
type (
	listRangeBase struct {
		xaction.XactBase
		args *DeletePrefetchArgs
		t    cluster.Target
	}
	EvictDelete struct {
		listRangeBase
	}
	DeletePrefetchArgs struct {
		Ctx      context.Context
		RangeMsg *cmn.RangeMsg
		ListMsg  *cmn.ListMsg
		UUID     string
		Evict    bool
	}
	objCallback = func(args *DeletePrefetchArgs, objName string) error
)

func (r *EvictDelete) IsMountpathXact() bool { return false }

func (r *EvictDelete) Run() error {
	var err error
	if r.args.RangeMsg != nil {
		err = r.iterateBucketRange(r.args)
	} else {
		err = r.listOperation(r.args, r.args.ListMsg)
	}
	r.Finish()
	return err
}

func NewEvictDelete(uuid, kind string, bck cmn.Bck, t cluster.Target, args *DeletePrefetchArgs) *EvictDelete {
	return &EvictDelete{
		listRangeBase: listRangeBase{
			XactBase: *xaction.NewXactBaseBck(uuid, kind, bck),
			t:        t,
			args:     args,
		},
	}
}

//
// Prefetch
//
type (
	Prefetch struct {
		listRangeBase
	}
)

func NewPrefetch(uuid, kind string, bck cmn.Bck, t cluster.Target, args *DeletePrefetchArgs) *Prefetch {
	return &Prefetch{
		listRangeBase: listRangeBase{
			XactBase: *xaction.NewXactBaseBck(uuid, kind, bck),
			t:        t,
			args:     args,
		},
	}
}

func (r *Prefetch) IsMountpathXact() bool { return false }

func (r *Prefetch) Run() error {
	var err error
	if r.args.RangeMsg != nil {
		err = r.iterateBucketRange(r.args)
	} else {
		err = r.listOperation(r.args, r.args.ListMsg)
	}
	r.Finish()
	return err
}
