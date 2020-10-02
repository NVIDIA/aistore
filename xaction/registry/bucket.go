// Package registry provides core functionality for the AIStore extended actions registry.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package registry

import (
	"context"
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/query"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction"
)

//
// baseBckEntry
//

type baseBckEntry struct {
	uuid string
}

func (b *baseBckEntry) PreRenewHook(previousEntry BucketEntry) (keep bool, err error) {
	e := previousEntry.Get()
	_, keep = e.(xaction.XactDemand)
	return
}

func (b *baseBckEntry) PostRenewHook(_ BucketEntry) {}

type (
	BucketEntry interface {
		BaseEntry
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
func (r *registry) RenewBucketXact(kind string, bck *cluster.Bck) cluster.Xact {
	e := r.bckXacts[kind].New(XactArgs{})
	res := r.renewBucketXaction(e, bck)
	return res.entry.Get()
}

func (r *registry) RenewECEncodeXact(t cluster.Target, bck *cluster.Bck, uuid, phase string) (cluster.Xact, error) {
	e := r.bckXacts[cmn.ActECEncode].New(XactArgs{
		T:     t,
		UUID:  uuid,
		Phase: phase,
	})
	res := r.renewBucketXaction(e, bck)
	if res.err != nil {
		return nil, res.err
	}
	return res.entry.Get(), nil
}

//
// mncEntry
//
type mncEntry struct {
	baseBckEntry
	t      cluster.Target
	xact   *mirror.XactBckMakeNCopies
	copies int
}

func (e *mncEntry) Start(bck cmn.Bck) error {
	slab, err := e.t.MMSA().GetSlab(memsys.MaxPageSlabSize)
	cmn.AssertNoErr(err)
	xmnc := mirror.NewXactMNC(bck, e.t, slab, e.uuid, e.copies)
	e.xact = xmnc
	return nil
}
func (*mncEntry) Kind() string        { return cmn.ActMakeNCopies }
func (e *mncEntry) Get() cluster.Xact { return e.xact }

// TODO: restart the EC (#531) in case of mountpath event
func (r *registry) MakeNCopiesOnMpathEvent(t cluster.Target, tag string) {
	var (
		cfg      = cmn.GCO.Get()
		bmd      = t.Bowner().Get()
		provider = cmn.ProviderAIS
	)
	bmd.Range(&provider, nil, func(bck *cluster.Bck) bool {
		if bck.Props.Mirror.Enabled {
			xact, err := r.RenewBckMakeNCopies(bck, t, tag, int(bck.Props.Mirror.Copies))
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
				xact, err := r.RenewBckMakeNCopies(bck, t, tag, int(bck.Props.Mirror.Copies))
				if err == nil {
					go xact.Run()
				}
			}
			return false
		})
	}
}

func (r *registry) RenewBckMakeNCopies(bck *cluster.Bck, t cluster.Target,
	uuid string, copies int) (*mirror.XactBckMakeNCopies, error) {
	e := &mncEntry{baseBckEntry: baseBckEntry{uuid}, t: t, copies: copies}
	res := r.renewBucketXaction(e, bck)
	if res.err != nil {
		return nil, res.err
	}
	if !res.isNew {
		return nil, fmt.Errorf("%s xaction already running", e.Kind())
	}
	return res.entry.Get().(*mirror.XactBckMakeNCopies), nil
}

//
// dpromoteEntry
//
type dpromoteEntry struct {
	baseBckEntry
	t      cluster.Target
	xact   *mirror.XactDirPromote
	dir    string
	params *cmn.ActValPromote
}

func (e *dpromoteEntry) Start(bck cmn.Bck) error {
	xact := mirror.NewXactDirPromote(e.dir, bck, e.t, e.params)
	go xact.Run()
	e.xact = xact
	return nil
}
func (*dpromoteEntry) Kind() string        { return cmn.ActPromote }
func (e *dpromoteEntry) Get() cluster.Xact { return e.xact }

func (r *registry) RenewDirPromote(dir string, bck *cluster.Bck, t cluster.Target, params *cmn.ActValPromote) (*mirror.XactDirPromote, error) {
	e := &dpromoteEntry{t: t, dir: dir, params: params}
	res := r.renewBucketXaction(e, bck)
	if res.err != nil {
		return nil, res.err
	}
	return res.entry.Get().(*mirror.XactDirPromote), nil
}

//
// loadLomCacheEntry
//
type loadLomCacheEntry struct {
	baseBckEntry
	t    cluster.Target
	xact *mirror.XactBckLoadLomCache
}

func (e *loadLomCacheEntry) Start(bck cmn.Bck) error {
	x := mirror.NewXactLLC(e.t, bck)
	go x.Run()
	e.xact = x

	return nil
}
func (*loadLomCacheEntry) Kind() string        { return cmn.ActLoadLomCache }
func (e *loadLomCacheEntry) Get() cluster.Xact { return e.xact }

func (r *registry) RenewBckLoadLomCache(t cluster.Target, bck *cluster.Bck) {
	e := &loadLomCacheEntry{t: t}
	r.renewBucketXaction(e, bck)
}

func (e *loadLomCacheEntry) PreRenewHook(_ BucketEntry) (bool, error) {
	return true, nil
}

//
// putMirrorEntry
//
type putMirrorEntry struct {
	baseBckEntry
	t    cluster.Target
	lom  *cluster.LOM
	xact *mirror.XactPut
}

func (e *putMirrorEntry) Start(_ cmn.Bck) error {
	slab, err := e.t.MMSA().GetSlab(memsys.MaxPageSlabSize) // TODO: estimate
	cmn.AssertNoErr(err)
	x, err := mirror.RunXactPut(e.lom, slab)

	if err != nil {
		glog.Error(err)
		return err
	}
	e.xact = x
	return nil
}

func (e *putMirrorEntry) Get() cluster.Xact { return e.xact }
func (*putMirrorEntry) Kind() string        { return cmn.ActPutCopies }

func (r *registry) RenewPutMirror(lom *cluster.LOM) *mirror.XactPut {
	e := &putMirrorEntry{t: lom.T, lom: lom}
	res := r.renewBucketXaction(e, lom.Bck())
	if res.err != nil {
		return nil
	}
	return res.entry.Get().(*mirror.XactPut)
}

//
// transferBckEntry
//
type transferBckEntry struct {
	baseBckEntry
	t       cluster.Target
	xact    *mirror.XactTransferBck
	bckFrom *cluster.Bck
	bckTo   *cluster.Bck
	phase   string
	kind    string
	dm      *bundle.DataMover
	dp      cluster.LomReaderProvider
	meta    *cmn.Bck2BckMsg
}

func (e *transferBckEntry) Start(_ cmn.Bck) error {
	slab, err := e.t.MMSA().GetSlab(memsys.MaxPageSlabSize)
	cmn.AssertNoErr(err)
	e.xact = mirror.NewXactTransferBck(e.uuid, e.kind, e.bckFrom, e.bckTo, e.t, slab, e.dm, e.dp, e.meta)
	return nil
}
func (e *transferBckEntry) Kind() string      { return e.kind }
func (e *transferBckEntry) Get() cluster.Xact { return e.xact }

func (e *transferBckEntry) PreRenewHook(previousEntry BucketEntry) (keep bool, err error) {
	prev := previousEntry.(*transferBckEntry)
	bckEq := prev.bckFrom.Equal(e.bckFrom, true /*same BID*/, true /* same backend */)
	if prev.phase == cmn.ActBegin && e.phase == cmn.ActCommit && bckEq {
		prev.phase = cmn.ActCommit // transition
		keep = true
		return
	}
	err = fmt.Errorf("%s(%s=>%s, phase %s): cannot %s(%s=>%s)",
		prev.xact, prev.bckFrom, prev.bckTo, prev.phase, e.phase, e.bckFrom, e.bckTo)
	return
}

func (r *registry) RenewTransferBck(t cluster.Target, bckFrom, bckTo *cluster.Bck, uuid, kind,
	phase string, dm *bundle.DataMover, dp cluster.LomReaderProvider, meta *cmn.Bck2BckMsg) (*mirror.XactTransferBck, error) {
	e := &transferBckEntry{
		baseBckEntry: baseBckEntry{uuid},
		t:            t,
		bckFrom:      bckFrom,
		bckTo:        bckTo,
		phase:        phase,
		dm:           dm,
		dp:           dp,
		meta:         meta,
		kind:         kind,
	}
	res := r.renewBucketXaction(e, bckTo)
	if res.err != nil {
		return nil, res.err
	}
	return res.entry.Get().(*mirror.XactTransferBck), nil
}

type (
	DeletePrefetchArgs struct {
		Ctx      context.Context
		UUID     string
		RangeMsg *cmn.RangeMsg
		ListMsg  *cmn.ListMsg
		Evict    bool
	}
	FastRenameArgs struct {
		RebID   xaction.RebID
		BckFrom *cluster.Bck
		BckTo   *cluster.Bck
	}
)

func (r *registry) RenewEvictDelete(t cluster.Target, bck *cluster.Bck, args *DeletePrefetchArgs) (cluster.Xact, error) {
	kind := cmn.ActDelete
	if args.Evict {
		kind = cmn.ActEvictObjects
	}
	e := r.bckXacts[kind].New(XactArgs{
		T:      t,
		UUID:   args.UUID,
		Custom: args,
	})
	res := r.renewBucketXaction(e, bck)
	if res.err != nil {
		return nil, res.err
	}
	return res.entry.Get(), nil
}

func (r *registry) RenewPrefetch(t cluster.Target, bck *cluster.Bck, args *DeletePrefetchArgs) (cluster.Xact, error) {
	e := r.bckXacts[cmn.ActPrefetch].New(XactArgs{
		T:      t,
		UUID:   args.UUID,
		Custom: args,
	})
	res := r.renewBucketXaction(e, bck)
	if res.err != nil {
		return nil, res.err
	}
	return res.entry.Get(), nil
}

func (r *registry) RenewBckFastRename(t cluster.Target, uuid string, rmdVersion int64,
	bckFrom, bckTo *cluster.Bck, phase string) (cluster.Xact, error) {
	e := r.bckXacts[cmn.ActRenameLB].New(XactArgs{
		T:     t,
		UUID:  uuid,
		Phase: phase,
		Custom: &FastRenameArgs{
			RebID:   xaction.RebID(rmdVersion),
			BckFrom: bckFrom,
			BckTo:   bckTo,
		},
	})
	res := r.renewBucketXaction(e, bckTo)
	if res.err != nil {
		return nil, res.err
	}
	return res.entry.Get(), nil
}

//
// Objects query
//

type queryEntry struct {
	baseBckEntry
	t     cluster.Target
	xact  *query.ObjectsListingXact
	query *query.ObjectsQuery
	ctx   context.Context
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

func (e *queryEntry) Kind() string      { return cmn.ActQueryObjects }
func (e *queryEntry) Get() cluster.Xact { return e.xact }

func (r *registry) RenewObjectsListingXact(ctx context.Context, t cluster.Target, q *query.ObjectsQuery, msg *cmn.SelectMsg) (*query.ObjectsListingXact, bool, error) {
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
		t:     t,
		query: q,
		ctx:   ctx,
		msg:   msg,
	}
	res := r.renewBucketXaction(e, q.BckSource.Bck)
	if res.err != nil {
		return nil, res.isNew, res.err
	}
	xact := res.entry.Get().(*query.ObjectsListingXact)
	return xact, res.isNew, nil
}

func (e *queryEntry) PreRenewHook(_ BucketEntry) (keep bool, err error) {
	return query.Registry.Get(e.msg.UUID) != nil, nil
}

//
// Objects list
//

func (r *registry) RenewBckListXact(t cluster.Target, bck *cluster.Bck, uuid string,
	msg *cmn.SelectMsg) (listXact cluster.Xact, isNew bool, err error) {
	xact := r.GetXact(uuid)
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
