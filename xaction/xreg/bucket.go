// Package registry provides core functionality for the AIStore extended actions xreg.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"context"
	"fmt"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/query"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction"
)

type (
	BaseBckEntry struct{}

	BucketEntry interface {
		BaseEntry
		// pre-renew: returns true iff the current active one exists and is either
		// - ok to keep running as is, or
		// - has been renew(ed) and is still ok
		PreRenewHook(previousEntry BucketEntry) (keep bool, err error)
		// post-renew hook
		PostRenewHook(previousEntry BucketEntry)
	}

	// BckFactory is an interface to provider a new instance of BucketEntry interface.
	BckFactory interface {
		// New should create empty stub for bucket xaction that could be started
		// with `Start()` method.
		New(args *XactArgs) BucketEntry
		Kind() string
	}

	XactArgs struct {
		Ctx    context.Context
		T      cluster.Target
		UUID   string
		Phase  string
		Custom interface{} // Additional arguments that are specific for a given xaction.
	}

	DirPromoteArgs struct {
		Dir    string
		Params *cmn.ActValPromote
	}

	TransferBckArgs struct {
		BckFrom *cluster.Bck
		BckTo   *cluster.Bck
		DM      *bundle.DataMover
		DP      cluster.LomReaderProvider
		Meta    *cmn.Bck2BckMsg
	}

	PutArchiveArgs struct {
		BckFrom *cluster.Bck
		BckTo   *cluster.Bck
	}

	DeletePrefetchArgs struct {
		Ctx      context.Context
		UUID     string
		RangeMsg *cmn.RangeMsg
		ListMsg  *cmn.ListMsg
		Evict    bool
	}

	BckRenameArgs struct {
		RebID   xaction.RebID
		BckFrom *cluster.Bck
		BckTo   *cluster.Bck
	}
)

//////////////////
// BaseBckEntry //
//////////////////

func (b *BaseBckEntry) PreRenewHook(previousEntry BucketEntry) (keep bool, err error) {
	e := previousEntry.Get()
	_, keep = e.(xaction.XactDemand)
	return
}

func (b *BaseBckEntry) PostRenewHook(_ BucketEntry) {}

//////////////
// registry //
//////////////

func RegBckXact(entry BckFactory) { defaultReg.regBckXact(entry) }

func (r *registry) regBckXact(entry BckFactory) {
	debug.Assert(xaction.XactsDtor[entry.Kind()].Type == xaction.XactTypeBck)

	// It is expected that registrations happen at the init time. Therefore, it
	// is safe to assume that no `RenewXYZ` will happen before all xactions
	// are registered. Thus, no locking is needed.
	r.bckXacts[entry.Kind()] = entry
}

// RenewBucketXact is general function to renew bucket xaction without any
// additional or specific parameters.
func RenewBucketXact(kind string, bck *cluster.Bck, args *XactArgs) (cluster.Xact, error) {
	return defaultReg.renewBucketXact(kind, bck, args)
}

func (r *registry) renewBucketXact(kind string, bck *cluster.Bck, args *XactArgs) (cluster.Xact, error) {
	e := r.bckXacts[kind].New(args)
	res := r.renewBckXact(e, bck)
	if res.err != nil {
		return nil, res.err
	}
	xact := res.entry.Get()
	if !res.isNew {
		// NOTE: make sure existing on-demand is active to prevent it from (idle) expiration
		//       (see demand.go hkcb())
		if xactDemand, ok := xact.(xaction.XactDemand); ok {
			xactDemand.IncPending()
			xactDemand.DecPending()
		}
	}
	return xact, nil
}

func RenewECEncode(t cluster.Target, bck *cluster.Bck, uuid, phase string) (cluster.Xact, error) {
	return defaultReg.renewECEncode(t, bck, uuid, phase)
}

func (r *registry) renewECEncode(t cluster.Target, bck *cluster.Bck, uuid, phase string) (cluster.Xact, error) {
	return r.renewBucketXact(cmn.ActECEncode, bck, &XactArgs{
		T:     t,
		UUID:  uuid,
		Phase: phase,
	})
}

func RenewMakeNCopies(t cluster.Target, tag string) { defaultReg.renewMakeNCopies(t, tag) }

func (r *registry) renewMakeNCopies(t cluster.Target, tag string) {
	var (
		cfg      = cmn.GCO.Get()
		bmd      = t.Bowner().Get()
		provider = cmn.ProviderAIS
	)
	bmd.Range(&provider, nil, func(bck *cluster.Bck) bool {
		if bck.Props.Mirror.Enabled {
			xact, err := r.renewBckMakeNCopies(t, bck, tag, int(bck.Props.Mirror.Copies))
			if err == nil {
				go xact.Run()
			}
		}
		return false
	})
	// TODO: remote ais
	for name, ns := range cfg.Backend.Providers {
		bmd.Range(&name, &ns, func(bck *cluster.Bck) bool {
			if bck.Props.Mirror.Enabled {
				xact, err := r.renewBckMakeNCopies(t, bck, tag, int(bck.Props.Mirror.Copies))
				if err == nil {
					go xact.Run()
				}
			}
			return false
		})
	}
}

func RenewBckMakeNCopies(t cluster.Target, bck *cluster.Bck, uuid string, copies int) (cluster.Xact, error) {
	return defaultReg.renewBckMakeNCopies(t, bck, uuid, copies)
}

func (r *registry) renewBckMakeNCopies(t cluster.Target, bck *cluster.Bck, uuid string,
	copies int) (cluster.Xact, error) {
	e := r.bckXacts[cmn.ActMakeNCopies].New(&XactArgs{T: t, UUID: uuid, Custom: copies})
	res := r.renewBckXact(e, bck)
	if res.err != nil {
		return nil, res.err
	}
	if !res.isNew {
		return nil, fmt.Errorf("%s xaction already running", e.Kind())
	}
	return res.entry.Get(), nil
}

func RenewDirPromote(t cluster.Target, bck *cluster.Bck, dir string,
	params *cmn.ActValPromote) (cluster.Xact, error) {
	return defaultReg.renewDirPromote(t, bck, dir, params)
}

func (r *registry) renewDirPromote(t cluster.Target, bck *cluster.Bck, dir string,
	params *cmn.ActValPromote) (cluster.Xact, error) {
	return r.renewBucketXact(cmn.ActPromote, bck, &XactArgs{
		T: t,
		Custom: &DirPromoteArgs{
			Dir:    dir,
			Params: params,
		},
	})
}

func RenewBckLoadLomCache(t cluster.Target, uuid string, bck *cluster.Bck) (err error) {
	_, err = defaultReg.renewBckLoadLomCache(t, uuid, bck)
	return
}

func (r *registry) renewBckLoadLomCache(t cluster.Target, uuid string, bck *cluster.Bck) (cluster.Xact, error) {
	return r.renewBucketXact(cmn.ActLoadLomCache, bck, &XactArgs{T: t, UUID: uuid})
}

func RenewPutMirror(t cluster.Target, lom *cluster.LOM) cluster.Xact {
	return defaultReg.renewPutMirror(t, lom)
}

func (r *registry) renewPutMirror(t cluster.Target, lom *cluster.LOM) cluster.Xact {
	xact, err := r.renewBucketXact(cmn.ActPutCopies, lom.Bck(), &XactArgs{T: t, Custom: lom})
	debug.AssertNoErr(err)
	return xact
}

func RenewPutArchive(uuid string, t cluster.Target, bckFrom, bckTo *cluster.Bck) cluster.Xact {
	xact, err := defaultReg.renewPutArchive(uuid, t, bckFrom, bckTo)
	debug.AssertNoErr(err)
	return xact
}

func (r *registry) renewPutArchive(uuid string, t cluster.Target, bckFrom, bckTo *cluster.Bck) (cluster.Xact, error) {
	return r.renewBucketXact(cmn.ActArchive, bckFrom, &XactArgs{
		T:      t,
		UUID:   uuid,
		Custom: &PutArchiveArgs{BckFrom: bckFrom, BckTo: bckTo},
	})
}

func RenewTransferBck(t cluster.Target, bckFrom, bckTo *cluster.Bck, uuid, kind,
	phase string, dm *bundle.DataMover, dp cluster.LomReaderProvider, meta *cmn.Bck2BckMsg) (cluster.Xact, error) {
	return defaultReg.renewTransferBck(t, bckFrom, bckTo, uuid, kind, phase, dm, dp, meta)
}

func (r *registry) renewTransferBck(t cluster.Target, bckFrom, bckTo *cluster.Bck, uuid, kind,
	phase string, dm *bundle.DataMover, dp cluster.LomReaderProvider, meta *cmn.Bck2BckMsg) (cluster.Xact, error) {
	return r.renewBucketXact(kind, bckTo, &XactArgs{
		T:     t,
		UUID:  uuid,
		Phase: phase,
		Custom: &TransferBckArgs{
			BckFrom: bckFrom,
			BckTo:   bckTo,
			DM:      dm,
			DP:      dp,
			Meta:    meta,
		},
	})
}

func RenewEvictDelete(t cluster.Target, bck *cluster.Bck, args *DeletePrefetchArgs) (cluster.Xact, error) {
	return defaultReg.renewEvictDelete(t, bck, args)
}

func (r *registry) renewEvictDelete(t cluster.Target, bck *cluster.Bck, args *DeletePrefetchArgs) (cluster.Xact, error) {
	kind := cmn.ActDelete
	if args.Evict {
		kind = cmn.ActEvictObjects
	}
	return r.renewBucketXact(kind, bck, &XactArgs{T: t, UUID: args.UUID, Custom: args})
}

func RenewPrefetch(t cluster.Target, bck *cluster.Bck, args *DeletePrefetchArgs) cluster.Xact {
	return defaultReg.renewPrefetch(t, bck, args)
}

func (r *registry) renewPrefetch(t cluster.Target, bck *cluster.Bck, args *DeletePrefetchArgs) cluster.Xact {
	xact, err := r.renewBucketXact(cmn.ActPrefetch, bck, &XactArgs{
		T:      t,
		UUID:   args.UUID,
		Custom: args,
	})
	debug.AssertNoErr(err)
	return xact
}

func RenewBckRename(t cluster.Target, bckFrom, bckTo *cluster.Bck,
	uuid string, rmdVersion int64, phase string) (cluster.Xact, error) {
	return defaultReg.renewBckRename(t, bckFrom, bckTo, uuid, rmdVersion, phase)
}

func (r *registry) renewBckRename(t cluster.Target, bckFrom, bckTo *cluster.Bck,
	uuid string, rmdVersion int64, phase string) (cluster.Xact, error) {
	return r.renewBucketXact(cmn.ActMoveBck, bckTo, &XactArgs{
		T:     t,
		UUID:  uuid,
		Phase: phase,
		Custom: &BckRenameArgs{
			RebID:   xaction.RebID(rmdVersion),
			BckFrom: bckFrom,
			BckTo:   bckTo,
		},
	})
}

func RenewObjList(t cluster.Target, bck *cluster.Bck, uuid string,
	msg *cmn.SelectMsg) (xact cluster.Xact, isNew bool, err error) {
	return defaultReg.renewObjList(t, bck, uuid, msg)
}

func (r *registry) renewObjList(t cluster.Target, bck *cluster.Bck, uuid string,
	msg *cmn.SelectMsg) (xact cluster.Xact, isNew bool, err error) {
	xact = r.getXact(uuid)
	if xact == nil || xact.Finished() {
		e := r.bckXacts[cmn.ActList].New(&XactArgs{
			Ctx:    context.Background(),
			T:      t,
			UUID:   uuid,
			Custom: msg,
		})
		res := r.renewBckXact(e, bck, uuid)
		if res.err != nil {
			return nil, res.isNew, res.err
		}
		return res.entry.Get(), res.isNew, nil
	}
	return xact, false, nil
}

func RenewBckSummary(ctx context.Context, t cluster.Target, bck *cluster.Bck, msg *cmn.BucketSummaryMsg) error {
	return defaultReg.renewBckSummary(ctx, t, bck, msg)
}

func (r *registry) renewBckSummary(ctx context.Context, t cluster.Target, bck *cluster.Bck, msg *cmn.BucketSummaryMsg) error {
	if err := r.removeFinishedByID(msg.UUID); err != nil {
		return err
	}
	e := &bckSummaryTaskEntry{ctx: ctx, t: t, uuid: msg.UUID, msg: msg}
	if err := e.Start(bck.Bck); err != nil {
		return err
	}
	r.storeEntry(e)
	return nil
}

func RenewQuery(ctx context.Context, t cluster.Target, q *query.ObjectsQuery,
	msg *cmn.SelectMsg) (cluster.Xact, bool, error) {
	return defaultReg.RenewQuery(ctx, t, q, msg)
}

func (r *registry) RenewQuery(ctx context.Context, t cluster.Target, q *query.ObjectsQuery,
	msg *cmn.SelectMsg) (cluster.Xact, bool, error) {
	debug.Assert(msg.UUID != "")
	if xact := query.Registry.Get(msg.UUID); xact != nil {
		if !xact.Aborted() {
			return xact, false, nil
		}
		query.Registry.Delete(msg.UUID)
	}
	if err := r.removeFinishedByID(msg.UUID); err != nil {
		return nil, false, err
	}
	e := &queFactory{
		ctx:   ctx,
		t:     t,
		query: q,
		msg:   msg,
	}
	res := r.renewBckXact(e, q.BckSource.Bck)
	if res.err != nil {
		return nil, res.isNew, res.err
	}
	return res.entry.Get(), res.isNew, nil
}
