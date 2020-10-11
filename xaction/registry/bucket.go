// Package registry provides core functionality for the AIStore extended actions registry.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package registry

import (
	"context"
	"fmt"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/query"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction"
)

type BaseBckEntry struct{}

func (b *BaseBckEntry) PreRenewHook(previousEntry BucketEntry) (keep bool, err error) {
	e := previousEntry.Get()
	_, keep = e.(xaction.XactDemand)
	return
}
func (b *BaseBckEntry) PostRenewHook(_ BucketEntry) {}

type (
	BucketEntry interface {
		baseEntry
		// pre-renew: returns true iff the current active one exists and is either
		// - ok to keep running as is, or
		// - has been renew(ed) and is still ok
		PreRenewHook(previousEntry BucketEntry) (keep bool, err error)
		// post-renew hook
		PostRenewHook(previousEntry BucketEntry)
	}

	// BucketEntryProvider is an interface to provider a new instance of BucketEntry interface.
	BucketEntryProvider interface {
		// New should create empty stub for bucket xaction that could be started
		// with `Start()` method.
		New(args XactArgs) BucketEntry
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

func (r *registry) RegisterBucketXact(entry BucketEntryProvider) {
	cmn.Assert(xaction.XactsDtor[entry.Kind()].Type == xaction.XactTypeBck)

	// It is expected that registrations happen at the init time. Therefore, it
	// is safe to assume that no `RenewXYZ` will happen before all xactions
	// are registered. Thus, no locking is needed.
	r.bckXacts[entry.Kind()] = entry
}

// RenewBucketXact is general function to renew bucket xaction without any
// additional or specific parameters.
func (r *registry) RenewBucketXact(kind string, bck *cluster.Bck, args ...XactArgs) (cluster.Xact, error) {
	var xactArgs XactArgs
	if len(args) > 0 {
		xactArgs = args[0]
	}
	e := r.bckXacts[kind].New(xactArgs)
	res := r.renewBucketXaction(e, bck)
	if res.err != nil {
		return nil, res.err
	}
	return res.entry.Get(), nil
}

func (r *registry) RenewECEncode(t cluster.Target, bck *cluster.Bck, uuid, phase string) (cluster.Xact, error) {
	return r.RenewBucketXact(cmn.ActECEncode, bck, XactArgs{
		T:     t,
		UUID:  uuid,
		Phase: phase,
	})
}

// TODO: Restart the EC (#531) in case of mountpath event.
func (r *registry) RenewMakeNCopies(t cluster.Target, tag string) {
	var (
		cfg      = cmn.GCO.Get()
		bmd      = t.Bowner().Get()
		provider = cmn.ProviderAIS
	)
	bmd.Range(&provider, nil, func(bck *cluster.Bck) bool {
		if bck.Props.Mirror.Enabled {
			xact, err := r.RenewBckMakeNCopies(t, bck, tag, int(bck.Props.Mirror.Copies))
			if err == nil {
				go xact.Run()
			}
		}
		return false
	})
	// TODO: remote ais
	for name, ns := range cfg.Cloud.Providers {
		bmd.Range(&name, &ns, func(bck *cluster.Bck) bool {
			if bck.Props.Mirror.Enabled {
				xact, err := r.RenewBckMakeNCopies(t, bck, tag, int(bck.Props.Mirror.Copies))
				if err == nil {
					go xact.Run()
				}
			}
			return false
		})
	}
}

func (r *registry) RenewBckMakeNCopies(t cluster.Target, bck *cluster.Bck, uuid string, copies int) (cluster.Xact, error) {
	e := r.bckXacts[cmn.ActMakeNCopies].New(XactArgs{
		T:      t,
		UUID:   uuid,
		Custom: copies,
	})
	res := r.renewBucketXaction(e, bck)
	if res.err != nil {
		return nil, res.err
	}
	if !res.isNew {
		return nil, fmt.Errorf("%s xaction already running", e.Kind())
	}
	return res.entry.Get(), nil
}

func (r *registry) RenewDirPromote(t cluster.Target, bck *cluster.Bck, dir string, params *cmn.ActValPromote) (cluster.Xact, error) {
	return r.RenewBucketXact(cmn.ActPromote, bck, XactArgs{
		T: t,
		Custom: &DirPromoteArgs{
			Dir:    dir,
			Params: params,
		},
	})
}

func (r *registry) RenewBckLoadLomCache(t cluster.Target, bck *cluster.Bck) {
	_, err := r.RenewBucketXact(cmn.ActLoadLomCache, bck, XactArgs{T: t})
	cmn.AssertNoErr(err)
}

func (r *registry) RenewPutMirror(lom *cluster.LOM) cluster.Xact {
	xact, err := r.RenewBucketXact(cmn.ActPutCopies, lom.Bck(), XactArgs{
		T:      lom.T,
		Custom: lom,
	})
	cmn.AssertNoErr(err)
	return xact
}

func (r *registry) RenewTransferBck(t cluster.Target, bckFrom, bckTo *cluster.Bck, uuid, kind,
	phase string, dm *bundle.DataMover, dp cluster.LomReaderProvider, meta *cmn.Bck2BckMsg) (cluster.Xact, error) {
	return r.RenewBucketXact(kind, bckTo, XactArgs{
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

func (r *registry) RenewEvictDelete(t cluster.Target, bck *cluster.Bck, args *DeletePrefetchArgs) (cluster.Xact, error) {
	kind := cmn.ActDelete
	if args.Evict {
		kind = cmn.ActEvictObjects
	}
	return r.RenewBucketXact(kind, bck, XactArgs{
		T:      t,
		UUID:   args.UUID,
		Custom: args,
	})
}

func (r *registry) RenewPrefetch(t cluster.Target, bck *cluster.Bck, args *DeletePrefetchArgs) cluster.Xact {
	xact, err := r.RenewBucketXact(cmn.ActPrefetch, bck, XactArgs{
		T:      t,
		UUID:   args.UUID,
		Custom: args,
	})
	cmn.AssertNoErr(err)
	return xact
}

func (r *registry) RenewBckRename(t cluster.Target, bckFrom, bckTo *cluster.Bck,
	uuid string, rmdVersion int64, phase string) (cluster.Xact, error) {
	return r.RenewBucketXact(cmn.ActRenameLB, bckTo, XactArgs{
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

//
// Objects list
//

func (r *registry) RenewObjList(t cluster.Target, bck *cluster.Bck, uuid string,
	msg *cmn.SelectMsg) (xact cluster.Xact, isNew bool, err error) {
	xact = r.GetXact(uuid)
	if xact == nil || xact.Finished() {
		e := r.bckXacts[cmn.ActListObjects].New(XactArgs{
			Ctx:    context.Background(),
			T:      t,
			UUID:   uuid,
			Custom: msg,
		})
		res := r.renewBucketXaction(e, bck, uuid)
		if res.err != nil {
			return nil, res.isNew, res.err
		}
		return res.entry.Get(), res.isNew, nil
	}
	return xact, false, nil
}

//
// Objects query
//

type queryEntry struct {
	BaseBckEntry
	xact *query.ObjectsListingXact

	ctx   context.Context
	t     cluster.Target
	query *query.ObjectsQuery
	msg   *cmn.SelectMsg
}

func (e *queryEntry) Start(_ cmn.Bck) error {
	xact := query.NewObjectsListing(e.ctx, e.t, e.query, e.msg)
	e.xact = xact
	if query.Registry.Get(e.msg.UUID) != nil {
		return fmt.Errorf("result set with handle %s already exists", e.msg.UUID)
	}
	return nil
}
func (*queryEntry) Kind() string        { return cmn.ActQueryObjects }
func (e *queryEntry) Get() cluster.Xact { return e.xact }
func (e *queryEntry) PreRenewHook(_ BucketEntry) (keep bool, err error) {
	return query.Registry.Get(e.msg.UUID) != nil, nil
}

func (r *registry) RenewQuery(ctx context.Context, t cluster.Target, q *query.ObjectsQuery, msg *cmn.SelectMsg) (cluster.Xact, bool, error) {
	cmn.Assert(msg.UUID != "")
	if xact := query.Registry.Get(msg.UUID); xact != nil {
		if xact.Aborted() {
			query.Registry.Delete(msg.UUID)
		} else {
			return xact, false, nil
		}
	}

	if err := r.removeFinishedByID(msg.UUID); err != nil {
		return nil, false, err
	}
	e := &queryEntry{
		ctx:   ctx,
		t:     t,
		query: q,
		msg:   msg,
	}
	res := r.renewBucketXaction(e, q.BckSource.Bck)
	if res.err != nil {
		return nil, res.isNew, res.err
	}
	return res.entry.Get(), res.isNew, nil
}
