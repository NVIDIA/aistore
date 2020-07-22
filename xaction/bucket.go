// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"context"
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/bcklist"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/objwalk/walkinfo"
	"github.com/NVIDIA/aistore/query"
	"github.com/NVIDIA/aistore/tar2tf"
	"github.com/NVIDIA/aistore/xaction/demand"
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

func (e *ecGetEntry) Start(bck cmn.Bck) error {
	var (
		xec      = ec.ECM.NewGetXact(bck)
		idleTime = cmn.GCO.Get().Timeout.SendFile
	)
	xec.XactDemandBase = *demand.NewXactDemandBaseBck(cmn.ActECGet, bck, idleTime)
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

func (e *ecPutEntry) Start(bck cmn.Bck) error {
	var (
		xec      = ec.ECM.NewPutXact(bck)
		idleTime = cmn.GCO.Get().Timeout.SendFile
	)
	xec.XactDemandBase = *demand.NewXactDemandBaseBck(cmn.ActECPut, bck, idleTime)
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

func (e *ecRespondEntry) Start(bck cmn.Bck) error {
	var (
		xec      = ec.ECM.NewRespondXact(bck)
		idleTime = cmn.GCO.Get().Timeout.SendFile
	)
	xec.XactDemandBase = *demand.NewXactDemandBaseBck(cmn.ActECRespond, bck, idleTime)
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

func (e *ecEncodeEntry) Start(bck cmn.Bck) error {
	xec := ec.NewXactBckEncode(bck, e.t, e.uuid)
	e.xact = xec
	return nil
}

func (*ecEncodeEntry) Kind() string    { return cmn.ActECEncode }
func (e *ecEncodeEntry) Get() cmn.Xact { return e.xact }
func (r *registry) RenewECEncodeXact(t cluster.Target, bck *cluster.Bck, uuid, phase string) (*ec.XactBckEncode, error) {
	e := &ecEncodeEntry{baseBckEntry: baseBckEntry{uuid}, t: t, phase: phase}
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

func (e *mncEntry) Start(bck cmn.Bck) error {
	slab, err := e.t.GetMMSA().GetSlab(memsys.MaxPageSlabSize)
	cmn.AssertNoErr(err)
	xmnc := mirror.NewXactMNC(bck, e.t, slab, e.uuid, e.copies)
	e.xact = xmnc
	return nil
}
func (*mncEntry) Kind() string    { return cmn.ActMakeNCopies }
func (e *mncEntry) Get() cmn.Xact { return e.xact }

// TODO: restart the EC (#531) in case of mountpath event
func (r *registry) MakeNCopiesOnMpathEvent(t cluster.Target, tag string) {
	var (
		cfg      = cmn.GCO.Get()
		bmd      = t.GetBowner().Get()
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
	if cfg.Cloud.Provider != "" {
		provider = cfg.Cloud.Provider
		namespace := cfg.Cloud.Ns
		bmd.Range(&provider, &namespace, func(bck *cluster.Bck) bool {
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
	ne := &mncEntry{baseBckEntry: baseBckEntry{uuid}, t: t, copies: copies}
	oe, err := r.renewBucketXaction(ne, bck)
	if err != nil {
		return nil, err
	}
	xact, _ := oe.Get().(*mirror.XactBckMakeNCopies)
	if ne != oe {
		err = fmt.Errorf("%s already running", xact)
	}
	return xact, err
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

func (e *loadLomCacheEntry) Start(bck cmn.Bck) error {
	x := mirror.NewXactLLC(e.t, bck)
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
// putMirrorEntry
//
type putMirrorEntry struct {
	baseBckEntry
	t    cluster.Target
	lom  *cluster.LOM
	xact *mirror.XactPut
}

func (e *putMirrorEntry) Start(_ cmn.Bck) error {
	slab, err := e.t.GetMMSA().GetSlab(memsys.MaxPageSlabSize) // TODO: estimate
	cmn.AssertNoErr(err)
	x, err := mirror.RunXactPut(e.lom, slab)

	if err != nil {
		glog.Error(err)
		return err
	}
	e.xact = x
	return nil
}

func (e *putMirrorEntry) Get() cmn.Xact { return e.xact }
func (*putMirrorEntry) Kind() string    { return cmn.ActPutCopies }

func (r *registry) RenewPutMirror(lom *cluster.LOM) *mirror.XactPut {
	e := &putMirrorEntry{t: lom.T, lom: lom}
	ee, err := r.renewBucketXaction(e, lom.Bck())
	if err != nil {
		return nil
	}
	return ee.Get().(*mirror.XactPut)
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

func (e *bccEntry) Start(_ cmn.Bck) error {
	slab, err := e.t.GetMMSA().GetSlab(memsys.MaxPageSlabSize)
	cmn.AssertNoErr(err)
	e.xact = mirror.NewXactBCC(e.uuid, e.bckFrom, e.bckTo, e.t, slab)
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

func (r *registry) RenewBckCopy(t cluster.Target, bckFrom, bckTo *cluster.Bck, uuid, phase string) (*mirror.XactBckCopy, error) {
	e := &bccEntry{
		baseBckEntry: baseBckEntry{uuid},
		t:            t,
		bckFrom:      bckFrom,
		bckTo:        bckTo,
		phase:        phase,
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
		t       cluster.Target
		xact    *FastRen
		rebID   RebID
		bckFrom *cluster.Bck
		bckTo   *cluster.Bck
		phase   string
	}
	FastRen struct {
		cmn.XactBase
		t       cluster.Target
		rebID   RebID
		bckFrom *cluster.Bck
		bckTo   *cluster.Bck
	}
)

func (r *FastRen) IsMountpathXact() bool { return true }
func (r *FastRen) String() string        { return fmt.Sprintf("%s <= %s", r.XactBase.String(), r.bckFrom) }

func (r *FastRen) Run() error {
	glog.Infoln(r.String())

	// FIXME: smart wait for resilver. For now assuming that rebalance takes longer than resilver.
	var finished bool
	for !finished {
		time.Sleep(10 * time.Second)
		rebStats, err := Registry.GetStats(RegistryXactFilter{
			ID:          r.rebID.String(),
			Kind:        cmn.ActRebalance,
			OnlyRunning: api.Bool(false),
		})
		cmn.AssertNoErr(err)
		for _, stat := range rebStats {
			finished = finished || stat.Finished()
		}
	}

	r.t.BMDVersionFixup(nil, r.bckFrom.Bck, false) // piggyback bucket renaming (last step) on getting updated BMD
	r.Finish()
	return nil
}

func (e *FastRenEntry) Start(bck cmn.Bck) error {
	e.xact = &FastRen{
		XactBase: *cmn.NewXactBaseBck(e.uuid, e.Kind(), bck),
		t:        e.t,
		rebID:    e.rebID,
		bckFrom:  e.bckFrom,
		bckTo:    e.bckTo,
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

func (r *registry) RenewBckFastRename(t cluster.Target, uuid string, rmdVersion int64, bckFrom, bckTo *cluster.Bck, phase string) (*FastRen, error) {
	e := &FastRenEntry{
		baseBckEntry: baseBckEntry{uuid},
		t:            t,
		rebID:        RebID(rmdVersion),
		bckFrom:      bckFrom,
		bckTo:        bckTo,
		phase:        phase,
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

func (e *evictDeleteEntry) Start(bck cmn.Bck) error {
	e.xact = &EvictDelete{
		listRangeBase: listRangeBase{
			XactBase: *cmn.NewXactBaseBck(e.uuid, e.Kind(), bck),
			t:        e.t,
			args:     e.args,
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
		baseBckEntry: baseBckEntry{args.UUID},
		t:            t,
		args:         args,
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
func (e *prefetchEntry) Start(bck cmn.Bck) error {
	e.xact = &Prefetch{
		listRangeBase: listRangeBase{
			XactBase: *cmn.NewXactBaseBck(e.uuid, e.Kind(), bck),
			t:        e.t,
			args:     e.args,
		},
	}
	return nil
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

func (r *registry) RenewPrefetch(t cluster.Target, bck *cluster.Bck, args *DeletePrefetchArgs) (*Prefetch, error) {
	e := &prefetchEntry{
		baseBckEntry: baseBckEntry{args.UUID},
		t:            t,
		args:         args,
	}
	ee, err := r.renewBucketXaction(e, bck)
	if err == nil {
		return ee.Get().(*Prefetch), nil
	}
	return nil, err
}

//
// Tar2Tf
//

type tar2TfEntry struct {
	baseBckEntry
	t    cluster.Target
	xact *tar2tf.Xact
	id   string
	job  *tar2tf.SamplesStreamJob
}

func (e *tar2TfEntry) Start(bck cmn.Bck) error {
	xact := &tar2tf.Xact{
		XactBase: *cmn.NewXactBaseBck(e.id, cmn.ActTar2Tf, bck),
		T:        e.t,
		Job:      e.job,
	}
	e.xact = xact
	go xact.Run()
	return nil
}

func (e *tar2TfEntry) Kind() string  { return cmn.ActTar2Tf }
func (e *tar2TfEntry) Get() cmn.Xact { return e.xact }

func (r *registry) NewTar2TfXact(job *tar2tf.SamplesStreamJob, t cluster.Target, bck *cluster.Bck) (*tar2tf.Xact, error) {
	id := cmn.GenUUID()
	if err := r.removeFinishedByID(id); err != nil {
		return nil, err
	}
	e := &tar2TfEntry{
		id:  id,
		t:   t,
		job: job,
	}
	job.Wg.Add(1)

	ee, err := r.renewBucketXaction(e, bck)
	if err == nil {
		return ee.Get().(*tar2tf.Xact), nil
	}
	return nil, err
}

func (e *tar2TfEntry) preRenewHook(_ bucketEntry) (keep bool, err error) {
	// always create new tar2tf xaction
	return false, nil
}

//
// Objects query
//

type queryEntry struct {
	baseBckEntry
	t     cluster.Target
	xact  *query.ObjectsListingXact
	query *query.ObjectsQuery
	wi    *walkinfo.WalkInfo
	id    string // serves as well as a query handle
}

func (e *queryEntry) Start(_ cmn.Bck) error {
	xact := query.NewObjectsListing(e.t, e.query, e.wi, e.id)
	e.xact = xact
	if query.Registry.Get(e.id) != nil {
		return fmt.Errorf("result set with handle %s already exists", e.id)
	}
	return nil
}

func (e *queryEntry) Kind() string  { return cmn.ActQueryObjects }
func (e *queryEntry) Get() cmn.Xact { return e.xact }

func (r *registry) RenewObjectsListingXact(t cluster.Target, q *query.ObjectsQuery, wi *walkinfo.WalkInfo, handle string) (*query.ObjectsListingXact, bool, error) {
	cmn.Assert(handle != "")
	if xact := query.Registry.Get(handle); xact != nil {
		if xact.Aborted() {
			query.Registry.Delete(handle)
		} else {
			return xact, false, nil
		}
	}

	id := handle
	if err := r.removeFinishedByID(id); err != nil {
		return nil, false, err
	}
	e := &queryEntry{
		id:    id,
		t:     t,
		query: q,
		wi:    wi,
	}

	ee, err := r.renewBucketXaction(e, q.BckSource.Bck)
	if err == nil {
		x := ee.Get().(*query.ObjectsListingXact)
		return x, true, nil
	}
	return nil, false, err
}

func (e *queryEntry) preRenewHook(_ bucketEntry) (keep bool, err error) {
	return query.Registry.Get(e.id) != nil, nil
}

//
// baseBckEntry
//
type baseBckEntry struct {
	uuid string
}

// nolint:unparam // `err` is set in different implementations
func (b *baseBckEntry) preRenewHook(previousEntry bucketEntry) (keep bool, err error) {
	e := previousEntry.Get()
	_, keep = e.(demand.XactDemand)
	return
}

func (b *baseBckEntry) postRenewHook(_ bucketEntry) {}

func (r *registry) RenewBckListNewXact(t cluster.Target, bck *cluster.Bck, uuid string, msg *cmn.SelectMsg) (*bcklist.BckListTask, error) {
	xact := r.GetXact(uuid)
	if xact == nil || xact.Finished() {
		e := &bckListTaskEntry{baseBckEntry: baseBckEntry{uuid}, t: t, msg: msg}
		ee, err := r.renewBucketXaction(e, bck, uuid)
		if err == nil {
			listXact := ee.Get().(*bcklist.BckListTask)
			return listXact, nil
		}
		return nil, err
	}

	listXact := xact.(*bcklist.BckListTask)
	return listXact, nil
}
