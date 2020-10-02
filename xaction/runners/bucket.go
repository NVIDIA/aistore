// Package runners provides implementation for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package runners

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/registry"
)

func init() {
	registry.Registry.RegisterBucketXact(&FastRenProvider{})
	registry.Registry.RegisterBucketXact(&EvictDeleteProvider{kind: cmn.ActEvictObjects})
	registry.Registry.RegisterBucketXact(&EvictDeleteProvider{kind: cmn.ActDelete})
	registry.Registry.RegisterBucketXact(&PrefetchProvider{})
}

type (
	FastRenProvider struct {
		xact *FastRen

		t     cluster.Target
		uuid  string
		phase string
		args  *registry.FastRenameArgs
	}

	FastRen struct {
		xaction.XactBase
		t         cluster.Target
		bckFrom   *cluster.Bck
		bckTo     *cluster.Bck
		rebStatsF func() ([]cluster.XactStats, error)
	}
)

func (r *FastRenProvider) New(args registry.XactArgs) registry.BucketEntry {
	return &FastRenProvider{
		t:     args.T,
		uuid:  args.UUID,
		phase: args.Phase,
		args:  args.Custom.(*registry.FastRenameArgs),
	}
}
func (r *FastRenProvider) Start(bck cmn.Bck) error {
	f := func() ([]cluster.XactStats, error) {
		onlyRunning := false
		return registry.Registry.GetStats(registry.XactFilter{
			ID:          r.args.RebID.String(),
			Kind:        cmn.ActRebalance,
			OnlyRunning: &onlyRunning,
		})
	}
	r.xact = NewFastRen(r.uuid, r.Kind(), bck, r.t, r.args.BckFrom, r.args.BckTo, f)
	return nil
}
func (r *FastRenProvider) Kind() string      { return cmn.ActRenameLB }
func (r *FastRenProvider) Get() cluster.Xact { return r.xact }
func (r *FastRenProvider) PreRenewHook(previousEntry registry.BucketEntry) (keep bool, err error) {
	if r.phase == cmn.ActBegin {
		if !previousEntry.Get().Finished() {
			err = fmt.Errorf("%s: cannot(%s=>%s) older rename still in progress", r.Kind(), r.args.BckFrom, r.args.BckTo)
			return
		}
		// TODO: more checks
	}
	prev := previousEntry.(*FastRenProvider)
	bckEq := prev.args.BckTo.Equal(r.args.BckTo, false /*sameID*/, false /* same backend */)
	if prev.phase == cmn.ActBegin && r.phase == cmn.ActCommit && bckEq {
		prev.phase = cmn.ActCommit // transition
		keep = true
		return
	}
	err = fmt.Errorf("%s(%s=>%s, phase %s): cannot %s(=>%s)",
		r.Kind(), prev.args.BckFrom, prev.args.BckTo, prev.phase, r.phase, r.args.BckFrom)
	return
}
func (r *FastRenProvider) PostRenewHook(_ registry.BucketEntry) {}

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
// EvictDelete
//

type (
	listRangeBase struct {
		xaction.XactBase
		t    cluster.Target
		args *registry.DeletePrefetchArgs
	}
	EvictDeleteProvider struct {
		xact *EvictDelete

		t    cluster.Target
		kind string
		args *registry.DeletePrefetchArgs
	}
	EvictDelete struct {
		listRangeBase
	}
	objCallback = func(args *registry.DeletePrefetchArgs, objName string) error
)

func (r *EvictDeleteProvider) New(args registry.XactArgs) registry.BucketEntry {
	return &EvictDeleteProvider{
		t:    args.T,
		kind: r.kind,
		args: args.Custom.(*registry.DeletePrefetchArgs),
	}
}
func (r *EvictDeleteProvider) Start(bck cmn.Bck) error {
	r.xact = NewEvictDelete(r.args.UUID, r.kind, bck, r.t, r.args)
	return nil
}
func (r *EvictDeleteProvider) Kind() string                                      { return r.kind }
func (r *EvictDeleteProvider) Get() cluster.Xact                                 { return r.xact }
func (r *EvictDeleteProvider) PreRenewHook(_ registry.BucketEntry) (bool, error) { return false, nil }
func (r *EvictDeleteProvider) PostRenewHook(_ registry.BucketEntry)              {}

func NewEvictDelete(uuid, kind string, bck cmn.Bck, t cluster.Target, args *registry.DeletePrefetchArgs) *EvictDelete {
	return &EvictDelete{
		listRangeBase: listRangeBase{
			XactBase: *xaction.NewXactBaseBck(uuid, kind, bck),
			t:        t,
			args:     args,
		},
	}
}

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

//
// Prefetch
//

type (
	PrefetchProvider struct {
		xact *Prefetch

		t    cluster.Target
		args *registry.DeletePrefetchArgs
	}
	Prefetch struct {
		listRangeBase
	}
)

func (r *PrefetchProvider) New(args registry.XactArgs) registry.BucketEntry {
	return &PrefetchProvider{
		t:    args.T,
		args: args.Custom.(*registry.DeletePrefetchArgs),
	}
}
func (r *PrefetchProvider) Start(bck cmn.Bck) error {
	r.xact = NewPrefetch(r.args.UUID, r.Kind(), bck, r.t, r.args)
	return nil
}
func (r *PrefetchProvider) Kind() string                                      { return cmn.ActPrefetch }
func (r *PrefetchProvider) Get() cluster.Xact                                 { return r.xact }
func (r *PrefetchProvider) PreRenewHook(_ registry.BucketEntry) (bool, error) { return false, nil }
func (r *PrefetchProvider) PostRenewHook(_ registry.BucketEntry)              {}

func NewPrefetch(uuid, kind string, bck cmn.Bck, t cluster.Target, args *registry.DeletePrefetchArgs) *Prefetch {
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
