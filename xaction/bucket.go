// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"context"
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/stats"
)

type (
	bucketEntry interface {
		baseEntry
		// pre-renew: returns true iff the current active one exists and is either
		// - ok to keep running as is, or
		// - has been renew(ed) and is still ok
		preRenewHook(previousEntry bucketEntry) (keep bool, err error)
		// post-renew hook
		postRenewHook(previousEntry bucketEntry)
	}
)

var (
	_ ec.XactRegistry = &registry{}
)

//
// ecGetEntry
//
type ecGetEntry struct {
	baseBckEntry
	xact *ec.XactGet
}

func (e *ecGetEntry) Start(id string, bck cmn.Bck) error {
	xec := ec.ECM.NewGetXact(bck)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECGet, bck)
	e.xact = xec
	go xec.Run()
	return nil
}

func (*ecGetEntry) Kind() string    { return cmn.ActECGet }
func (e *ecGetEntry) Get() cmn.Xact { return e.xact }
func (r *registry) RenewGetEC(bck *cluster.Bck) *ec.XactGet {
	ee, _ := r.renewBucketXaction(&ecGetEntry{}, bck) // TODO -- FIXME: handle error
	return ee.Get().(*ec.XactGet)
}

//
// ecPutEntry
//
type ecPutEntry struct {
	baseBckEntry
	xact *ec.XactPut
}

func (e *ecPutEntry) Start(id string, bck cmn.Bck) error {
	xec := ec.ECM.NewPutXact(bck)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECPut, bck)
	go xec.Run()
	e.xact = xec
	return nil
}
func (*ecPutEntry) Kind() string    { return cmn.ActECPut }
func (e *ecPutEntry) Get() cmn.Xact { return e.xact }
func (r *registry) RenewPutEC(bck *cluster.Bck) *ec.XactPut {
	e := &ecPutEntry{}
	ee, _ := r.renewBucketXaction(e, bck) // TODO: handle error
	return ee.Get().(*ec.XactPut)
}

//
// ecRespondEntry
//
type ecRespondEntry struct {
	baseBckEntry
	xact *ec.XactRespond
}

func (e *ecRespondEntry) Start(id string, bck cmn.Bck) error {
	xec := ec.ECM.NewRespondXact(bck)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECRespond, bck)
	go xec.Run()
	e.xact = xec
	return nil
}
func (*ecRespondEntry) Kind() string    { return cmn.ActECRespond }
func (e *ecRespondEntry) Get() cmn.Xact { return e.xact }
func (r *registry) RenewRespondEC(bck *cluster.Bck) *ec.XactRespond {
	e := &ecRespondEntry{}
	ee, _ := r.renewBucketXaction(e, bck)
	return ee.Get().(*ec.XactRespond)
}

//
// ecEncodeEntry
//
type ecEncodeEntry struct {
	baseBckEntry
	t     cluster.Target
	xact  *ec.XactBckEncode
	phase string
}

func (e *ecEncodeEntry) Start(id string, bck cmn.Bck) error {
	xec := ec.NewXactBckEncode(id, bck, e.t)
	e.xact = xec
	return nil
}

func (*ecEncodeEntry) Kind() string    { return cmn.ActECEncode }
func (e *ecEncodeEntry) Get() cmn.Xact { return e.xact }
func (r *registry) RenewECEncodeXact(t cluster.Target, bck *cluster.Bck, phase string) (*ec.XactBckEncode, error) {
	e := &ecEncodeEntry{t: t, phase: phase}
	ee, err := r.renewBucketXaction(e, bck)
	if err == nil {
		return ee.Get().(*ec.XactBckEncode), nil
	}
	return nil, err
}

func (e *ecEncodeEntry) preRenewHook(previousEntry bucketEntry) (keep bool, err error) {
	// TODO: add more checks?
	prev := previousEntry.(*ecEncodeEntry)
	if prev.phase == cmn.ActBegin && e.phase == cmn.ActCommit {
		prev.phase = cmn.ActCommit // transition
		keep = true
		return
	}
	err = fmt.Errorf("%s(%s, phase %s): cannot %s", e.Kind(), prev.xact.Bck().Name, prev.phase, e.phase)
	return
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

func (e *mncEntry) Start(id string, bck cmn.Bck) error {
	slab, err := e.t.GetMMSA().GetSlab(memsys.MaxPageSlabSize)
	cmn.AssertNoErr(err)
	xmnc := mirror.NewXactMNC(id, bck, e.t, slab, e.copies)
	go xmnc.Run()
	e.xact = xmnc
	return nil
}
func (*mncEntry) Kind() string    { return cmn.ActMakeNCopies }
func (e *mncEntry) Get() cmn.Xact { return e.xact }

// TODO: restart the EC (#531) in case of mountpath event
func (r *registry) RenewObjsRedundancy(t cluster.Target) {
	var (
		cfg      = cmn.GCO.Get()
		bmd      = t.GetBowner().Get()
		provider = cmn.ProviderAIS
	)
	bmd.Range(&provider, nil, func(bck *cluster.Bck) bool {
		if bck.Props.Mirror.Enabled {
			r.RenewBckMakeNCopies(bck, t, int(bck.Props.Mirror.Copies))
		}
		return false
	})
	if cfg.Cloud.Supported {
		provider = cfg.Cloud.Provider
		namespace := cfg.Cloud.Ns
		bmd.Range(&provider, &namespace, func(bck *cluster.Bck) bool {
			if bck.Props.Mirror.Enabled {
				r.RenewBckMakeNCopies(bck, t, int(bck.Props.Mirror.Copies))
			}
			return false
		})
	}
}

func (r *registry) RenewBckMakeNCopies(bck *cluster.Bck, t cluster.Target, copies int) {
	e := &mncEntry{t: t, copies: copies}
	_, _ = r.renewBucketXaction(e, bck)
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

func (e *dpromoteEntry) Start(id string, bck cmn.Bck) error {
	xact := mirror.NewXactDirPromote(id, e.dir, bck, e.t, e.params)
	go xact.Run()
	e.xact = xact
	return nil
}
func (*dpromoteEntry) Kind() string    { return cmn.ActPromote }
func (e *dpromoteEntry) Get() cmn.Xact { return e.xact }

func (r *registry) RenewDirPromote(dir string, bck *cluster.Bck, t cluster.Target, params *cmn.ActValPromote) (*mirror.XactDirPromote, error) {
	e := &dpromoteEntry{t: t, dir: dir, params: params}
	ee, err := r.renewBucketXaction(e, bck)
	if err == nil {
		return ee.Get().(*mirror.XactDirPromote), nil
	}
	return nil, err
}

//
// loadLomCacheEntry
//
type loadLomCacheEntry struct {
	baseBckEntry
	t    cluster.Target
	xact *mirror.XactBckLoadLomCache
}

func (e *loadLomCacheEntry) Start(id string, bck cmn.Bck) error {
	x := mirror.NewXactLLC(e.t, id, bck)
	go x.Run()
	e.xact = x

	return nil
}
func (*loadLomCacheEntry) Kind() string    { return cmn.ActLoadLomCache }
func (e *loadLomCacheEntry) Get() cmn.Xact { return e.xact }

func (r *registry) RenewBckLoadLomCache(t cluster.Target, bck *cluster.Bck) {
	e := &loadLomCacheEntry{t: t}
	r.renewBucketXaction(e, bck)
}

func (e *loadLomCacheEntry) preRenewHook(_ bucketEntry) (bool, error) {
	return true, nil
}

//
// putLocReplicasEntry
//
type putLocReplicasEntry struct {
	baseBckEntry
	t    cluster.Target
	lom  *cluster.LOM
	xact *mirror.XactPutLRepl
}

func (e *putLocReplicasEntry) Start(id string, _ cmn.Bck) error {
	slab, err := e.t.GetMMSA().GetSlab(memsys.MaxPageSlabSize) // TODO: estimate
	cmn.AssertNoErr(err)
	x, err := mirror.RunXactPutLRepl(id, e.lom, slab)

	if err != nil {
		glog.Error(err)
		return err
	}
	e.xact = x
	return nil
}

func (e *putLocReplicasEntry) Get() cmn.Xact { return e.xact }
func (*putLocReplicasEntry) Kind() string    { return cmn.ActPutCopies }

func (r *registry) RenewPutLocReplicas(lom *cluster.LOM) *mirror.XactPutLRepl {
	e := &putLocReplicasEntry{t: lom.T, lom: lom}
	ee, err := r.renewBucketXaction(e, lom.Bck())
	if err != nil {
		return nil
	}
	return ee.Get().(*mirror.XactPutLRepl)
}

//
// bccEntry
//
type bccEntry struct {
	baseBckEntry
	t       cluster.Target
	xact    *mirror.XactBckCopy
	bckFrom *cluster.Bck
	bckTo   *cluster.Bck
	phase   string
}

func (e *bccEntry) Start(id string, _ cmn.Bck) error {
	slab, err := e.t.GetMMSA().GetSlab(memsys.MaxPageSlabSize)
	cmn.AssertNoErr(err)
	e.xact = mirror.NewXactBCC(id, e.bckFrom, e.bckTo, e.t, slab)
	return nil
}
func (e *bccEntry) Kind() string  { return cmn.ActCopyBucket }
func (e *bccEntry) Get() cmn.Xact { return e.xact }

func (e *bccEntry) preRenewHook(previousEntry bucketEntry) (keep bool, err error) {
	prev := previousEntry.(*bccEntry)
	if prev.phase == cmn.ActBegin && e.phase == cmn.ActCommit && prev.bckFrom.Equal(e.bckFrom, true /*same BID*/) {
		prev.phase = cmn.ActCommit // transition
		keep = true
		return
	}
	err = fmt.Errorf("%s(%s=>%s, phase %s): cannot %s(%s=>%s)",
		prev.xact, prev.bckFrom, prev.bckTo, prev.phase, e.phase, e.bckFrom, e.bckTo)
	return
}

func (r *registry) RenewBckCopy(t cluster.Target, bckFrom, bckTo *cluster.Bck, phase string) (*mirror.XactBckCopy, error) {
	e := &bccEntry{
		t:       t,
		bckFrom: bckFrom,
		bckTo:   bckTo,
		phase:   phase,
	}
	ee, err := r.renewBucketXaction(e, bckTo)
	if err != nil {
		return nil, err
	}
	return ee.Get().(*mirror.XactBckCopy), nil
}

//
// FastRenEntry & FastRen
//
type (
	FastRenEntry struct {
		baseBckEntry
		t          cluster.Target
		rebManager cluster.RebManager
		xact       *FastRen
		bckFrom    *cluster.Bck
		bckTo      *cluster.Bck
		phase      string
	}
	FastRen struct {
		cmn.XactBase
		rebManager cluster.RebManager
		t          cluster.Target
		bckFrom    *cluster.Bck
		bckTo      *cluster.Bck
	}
)

func (r *FastRen) IsMountpathXact() bool { return true }
func (r *FastRen) String() string        { return fmt.Sprintf("%s <= %s", r.XactBase.String(), r.bckFrom) }

func (r *FastRen) Run(rmdVersion int64) {
	var (
		allFinished bool

		deadline  = time.Now().Add(cmn.GCO.Get().Timeout.MaxHostBusy) // wait at most `MaxHostBusy` for rebalance/resilver to start
		xactState = map[string]*struct{ finished bool }{
			cmn.ActResilver:  {},
			cmn.ActRebalance: {},
		}
	)

	glog.Infoln(r.String())

	// Wait for particular rebalance to start.
	var running bool
	for !running && time.Now().Before(deadline) {
		time.Sleep(10 * time.Second)
		rebStats, err := Registry.GetStats(cmn.ActRebalance, nil, false /*onlyRecent*/)
		cmn.AssertNoErr(err)
		for _, stat := range rebStats {
			rebStats := stat.(*stats.RebalanceTargetStats)
			running = running || (stat.Running() && rebStats.Ext.RebID >= rmdVersion)
		}
	}

	// Wait for rebalance or resilver to finish.
	for running && !allFinished {
		time.Sleep(5 * time.Second)
		allFinished = true
		for kind, state := range xactState {
			if !state.finished {
				state.finished = true
				rebStats, err := Registry.GetStats(kind, nil, false /*onlyRecent*/)
				cmn.AssertNoErr(err)
				for _, stat := range rebStats {
					state.finished = state.finished && stat.Finished()
				}
			}
			allFinished = allFinished && state.finished
		}
	}

	r.t.BMDVersionFixup(nil, r.bckFrom.Bck, false) // piggyback bucket renaming (last step) on getting updated BMD
	r.EndTime(time.Now())
}

func (e *FastRenEntry) Start(id string, bck cmn.Bck) error {
	e.xact = &FastRen{
		XactBase:   *cmn.NewXactBaseWithBucket(id, e.Kind(), bck),
		t:          e.t,
		bckFrom:    e.bckFrom,
		bckTo:      e.bckTo,
		rebManager: e.rebManager,
	}
	return nil
}
func (e *FastRenEntry) Kind() string  { return cmn.ActRenameLB }
func (e *FastRenEntry) Get() cmn.Xact { return e.xact }

func (e *FastRenEntry) preRenewHook(previousEntry bucketEntry) (keep bool, err error) {
	if e.phase == cmn.ActBegin {
		if !previousEntry.Get().Finished() {
			err = fmt.Errorf("%s: cannot(%s=>%s) older rename still in progress", e.Kind(), e.bckFrom, e.bckTo)
			return
		}
		// TODO: more checks
	}
	prev := previousEntry.(*FastRenEntry)
	if prev.phase == cmn.ActBegin && e.phase == cmn.ActCommit && prev.bckTo.Equal(e.bckTo, false /*sameID*/) {
		prev.phase = cmn.ActCommit // transition
		keep = true
		return
	}
	err = fmt.Errorf("%s(%s=>%s, phase %s): cannot %s(=>%s)",
		e.Kind(), prev.bckFrom, prev.bckTo, prev.phase, e.phase, e.bckFrom)
	return
}

func (r *registry) RenewBckFastRename(t cluster.Target, bckFrom, bckTo *cluster.Bck, phase string,
	mgr cluster.RebManager) (*FastRen, error) {
	e := &FastRenEntry{
		t:          t,
		rebManager: mgr,
		bckFrom:    bckFrom,
		bckTo:      bckTo,
		phase:      phase,
	}
	ee, err := r.renewBucketXaction(e, bckTo)
	if err == nil {
		return ee.Get().(*FastRen), nil
	}
	return nil, err
}

//
// EvictDeleteEntry & EvictDelete
//
type (
	evictDeleteEntry struct {
		baseBckEntry
		t    cluster.Target
		xact *EvictDelete
		args *DeletePrefetchArgs
	}
	listRangeBase struct {
		cmn.XactBase
		t cluster.Target
	}
	EvictDelete struct {
		listRangeBase
	}
	DeletePrefetchArgs struct {
		Ctx      context.Context
		RangeMsg *cmn.RangeMsg
		ListMsg  *cmn.ListMsg
		Evict    bool
	}
	objCallback = func(args *DeletePrefetchArgs, objName string) error
)

func (r *EvictDelete) IsMountpathXact() bool { return false }

func (r *EvictDelete) Run(args *DeletePrefetchArgs) {
	if args.RangeMsg != nil {
		r.iterateBucketRange(args)
	} else {
		r.listOperation(args, args.ListMsg)
	}
	r.EndTime(time.Now())
}

func (e *evictDeleteEntry) Start(id string, bck cmn.Bck) error {
	e.xact = &EvictDelete{
		listRangeBase: listRangeBase{
			XactBase: *cmn.NewXactBaseWithBucket(id, e.Kind(), bck),
			t:        e.t,
		},
	}
	return nil
}
func (e *evictDeleteEntry) Kind() string {
	if e.args.Evict {
		return cmn.ActEvictObjects
	}
	return cmn.ActDelete
}
func (e *evictDeleteEntry) Get() cmn.Xact { return e.xact }

func (e *evictDeleteEntry) preRenewHook(_ bucketEntry) (keep bool, err error) {
	return false, nil
}

func (r *registry) RenewEvictDelete(t cluster.Target, bck *cluster.Bck, args *DeletePrefetchArgs) (*EvictDelete, error) {
	e := &evictDeleteEntry{
		t:    t,
		args: args,
	}
	ee, err := r.renewBucketXaction(e, bck)
	if err == nil {
		return ee.Get().(*EvictDelete), nil
	}
	return nil, err
}

//
// Prefetch
//
type (
	prefetchEntry struct {
		baseBckEntry
		t    cluster.Target
		xact *Prefetch
		args *DeletePrefetchArgs
	}
	Prefetch struct {
		listRangeBase
	}
)

func (e *prefetchEntry) Kind() string  { return cmn.ActPrefetch }
func (e *prefetchEntry) Get() cmn.Xact { return e.xact }
func (e *prefetchEntry) preRenewHook(_ bucketEntry) (keep bool, err error) {
	return false, nil
}
func (e *prefetchEntry) Start(id string, bck cmn.Bck) error {
	e.xact = &Prefetch{
		listRangeBase: listRangeBase{
			XactBase: *cmn.NewXactBaseWithBucket(id, e.Kind(), bck),
			t:        e.t,
		},
	}
	return nil
}

func (r *Prefetch) IsMountpathXact() bool { return false }

func (r *Prefetch) Run(args *DeletePrefetchArgs) {
	if args.RangeMsg != nil {
		r.iterateBucketRange(args)
	} else {
		r.listOperation(args, args.ListMsg)
	}
	r.EndTime(time.Now())
}

func (r *registry) RenewPrefetch(t cluster.Target, bck *cluster.Bck, args *DeletePrefetchArgs) (*Prefetch, error) {
	e := &prefetchEntry{
		t:    t,
		args: args,
	}
	ee, err := r.renewBucketXaction(e, bck)
	if err == nil {
		return ee.Get().(*Prefetch), nil
	}
	return nil, err
}

//
// baseBckEntry
//
type baseBckEntry struct{}

// nolint:unparam // `err` is set in different implementations
func (b *baseBckEntry) preRenewHook(previousEntry bucketEntry) (keep bool, err error) {
	e := previousEntry.Get()
	if demandEntry, ok := e.(cmn.XactDemand); ok {
		demandEntry.Renew()
		keep = true
	}
	return
}

func (b *baseBckEntry) postRenewHook(_ bucketEntry) {}

func (b *baseBckEntry) Stats(xact cmn.Xact) stats.XactStats {
	return stats.NewXactStats(xact)
}
