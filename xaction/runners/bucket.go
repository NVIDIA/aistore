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
	registry.Registry.RegisterBucketXact(&BckRenameProvider{})
	registry.Registry.RegisterBucketXact(&evictDeleteProvider{kind: cmn.ActEvictObjects})
	registry.Registry.RegisterBucketXact(&evictDeleteProvider{kind: cmn.ActDelete})
	registry.Registry.RegisterBucketXact(&PrefetchProvider{})
}

type (
	BckRenameProvider struct {
		xact *bckRename

		t     cluster.Target
		uuid  string
		phase string
		args  *registry.BckRenameArgs
	}

	bckRename struct {
		xaction.XactBase
		t       cluster.Target
		bckFrom *cluster.Bck
		bckTo   *cluster.Bck
		rebID   xaction.RebID
	}
)

func (*BckRenameProvider) New(args registry.XactArgs) registry.BucketEntry {
	return &BckRenameProvider{
		t:     args.T,
		uuid:  args.UUID,
		phase: args.Phase,
		args:  args.Custom.(*registry.BckRenameArgs),
	}
}

func (p *BckRenameProvider) Start(bck cmn.Bck) error {
	p.xact = newBckRename(p.uuid, p.Kind(), bck, p.t, p.args.BckFrom, p.args.BckTo, p.args.RebID)
	return nil
}
func (*BckRenameProvider) Kind() string        { return cmn.ActRenameLB }
func (p *BckRenameProvider) Get() cluster.Xact { return p.xact }
func (p *BckRenameProvider) PreRenewHook(previousEntry registry.BucketEntry) (keep bool, err error) {
	if p.phase == cmn.ActBegin {
		if !previousEntry.Get().Finished() {
			err = fmt.Errorf("%s: cannot(%s=>%s) older rename still in progress", p.Kind(), p.args.BckFrom, p.args.BckTo)
			return
		}
		// TODO: more checks
	}
	prev := previousEntry.(*BckRenameProvider)
	bckEq := prev.args.BckTo.Equal(p.args.BckTo, false /*sameID*/, false /* same backend */)
	if prev.phase == cmn.ActBegin && p.phase == cmn.ActCommit && bckEq {
		prev.phase = cmn.ActCommit // transition
		keep = true
		return
	}
	err = fmt.Errorf("%s(%s=>%s, phase %s): cannot %s(=>%s)",
		p.Kind(), prev.args.BckFrom, prev.args.BckTo, prev.phase, p.phase, p.args.BckFrom)
	return
}
func (p *BckRenameProvider) PostRenewHook(_ registry.BucketEntry) {}

func newBckRename(uuid, kind string, bck cmn.Bck, t cluster.Target,
	bckFrom, bckTo *cluster.Bck, rebID xaction.RebID) *bckRename {
	return &bckRename{
		XactBase: *xaction.NewXactBaseBck(uuid, kind, bck),
		t:        t,
		bckFrom:  bckFrom,
		bckTo:    bckTo,
		rebID:    rebID,
	}
}

func (r *bckRename) IsMountpathXact() bool { return true }
func (r *bckRename) String() string        { return fmt.Sprintf("%s <= %s", r.XactBase.String(), r.bckFrom) }

func (r *bckRename) Run() error {
	glog.Infoln(r.String())
	// FIXME: smart wait for resilver. For now assuming that rebalance takes longer than resilver.
	var (
		onlyRunning, finished bool

		flt = registry.XactFilter{ID: r.rebID.String(), Kind: cmn.ActRebalance, OnlyRunning: &onlyRunning}
	)
	for !finished {
		time.Sleep(10 * time.Second)
		rebStats, err := registry.Registry.GetStats(flt)
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
// evictDelete
//

type (
	listRangeBase struct {
		xaction.XactBase
		t    cluster.Target
		args *registry.DeletePrefetchArgs
	}
	evictDeleteProvider struct {
		registry.BaseBckEntry
		xact *evictDelete

		t    cluster.Target
		kind string
		args *registry.DeletePrefetchArgs
	}
	evictDelete struct {
		listRangeBase
	}
	objCallback = func(args *registry.DeletePrefetchArgs, objName string) error
)

func (p *evictDeleteProvider) New(args registry.XactArgs) registry.BucketEntry {
	return &evictDeleteProvider{
		t:    args.T,
		kind: p.kind,
		args: args.Custom.(*registry.DeletePrefetchArgs),
	}
}

func (p *evictDeleteProvider) Start(bck cmn.Bck) error {
	p.xact = newEvictDelete(p.args.UUID, p.kind, bck, p.t, p.args)
	return nil
}
func (p *evictDeleteProvider) Kind() string      { return p.kind }
func (p *evictDeleteProvider) Get() cluster.Xact { return p.xact }

func newEvictDelete(uuid, kind string, bck cmn.Bck, t cluster.Target, args *registry.DeletePrefetchArgs) *evictDelete {
	return &evictDelete{
		listRangeBase: listRangeBase{
			XactBase: *xaction.NewXactBaseBck(uuid, kind, bck),
			t:        t,
			args:     args,
		},
	}
}

func (r *evictDelete) IsMountpathXact() bool { return false }

func (r *evictDelete) Run() error {
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
// prefetch
//

type (
	PrefetchProvider struct {
		registry.BaseBckEntry
		xact *prefetch

		t    cluster.Target
		args *registry.DeletePrefetchArgs
	}
	prefetch struct {
		listRangeBase
	}
)

func (*PrefetchProvider) New(args registry.XactArgs) registry.BucketEntry {
	return &PrefetchProvider{
		t:    args.T,
		args: args.Custom.(*registry.DeletePrefetchArgs),
	}
}

func (p *PrefetchProvider) Start(bck cmn.Bck) error {
	p.xact = newPrefetch(p.args.UUID, p.Kind(), bck, p.t, p.args)
	return nil
}
func (*PrefetchProvider) Kind() string        { return cmn.ActPrefetch }
func (p *PrefetchProvider) Get() cluster.Xact { return p.xact }

func newPrefetch(uuid, kind string, bck cmn.Bck, t cluster.Target, args *registry.DeletePrefetchArgs) *prefetch {
	return &prefetch{
		listRangeBase: listRangeBase{
			XactBase: *xaction.NewXactBaseBck(uuid, kind, bck),
			t:        t,
			args:     args,
		},
	}
}

func (r *prefetch) IsMountpathXact() bool { return false }

func (r *prefetch) Run() error {
	var err error
	if r.args.RangeMsg != nil {
		err = r.iterateBucketRange(r.args)
	} else {
		err = r.listOperation(r.args, r.args.ListMsg)
	}
	r.Finish()
	return err
}
