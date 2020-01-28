// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"fmt"
	"sync"
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
		entry
		// pre-renew: returns true iff the current active one exists and is either
		// - ok to keep running as is, or
		// - has been renew(ed) and is still ok
		preRenewHook(previousEntry bucketEntry) (keep bool, err error)
		// post-renew hook
		postRenewHook(previousEntry bucketEntry)
	}
	bucketXactions struct {
		sync.RWMutex
		r       *registry
		entries map[string]bucketEntry
	}
)

var (
	_ ec.XactRegistry = &registry{}
)

//
// bucketXactions
//

func newBucketXactions(r *registry) *bucketXactions {
	return &bucketXactions{r: r, entries: make(map[string]bucketEntry)}
}

func (b *bucketXactions) GetL(kind string) bucketEntry {
	b.RLock()
	defer b.RUnlock()
	return b.entries[kind]
}

func (b *bucketXactions) Stats() map[int64]stats.XactStats {
	statsList := make(map[int64]stats.XactStats, len(b.entries))
	b.RLock()
	for _, e := range b.entries {
		xact := e.Get()
		statsList[xact.ID()] = e.Stats(xact)
	}
	b.RUnlock()
	return statsList
}

// AbortAll tries to (async) abort each bucket xaction; returns true if the caller is advised to wait
func (b *bucketXactions) AbortAll() bool {
	sleep := false
	wg := &sync.WaitGroup{}
	b.RLock()
	for _, e := range b.entries {
		xact := e.Get()
		if xact.Finished() {
			continue
		}
		sleep = true
		wg.Add(1)
		go func(xact cmn.Xact) {
			xact.Abort()
			wg.Done()
		}(xact)
	}
	b.RUnlock()
	wg.Wait()
	return sleep
}

func (b *bucketXactions) uniqueID() int64 { return b.r.uniqueID() }
func (b *bucketXactions) len() int        { return len(b.entries) }

// generic bucket-xaction renewal
func (b *bucketXactions) renewBucketXaction(e bucketEntry) (bucketEntry, error) {
	b.RLock()
	previousEntry := b.entries[e.Kind()]
	running := previousEntry != nil && !previousEntry.Get().Finished()
	if running {
		if keep, err := e.preRenewHook(previousEntry); keep || err != nil {
			b.RUnlock()
			return previousEntry, err
		}
	}
	b.RUnlock()

	b.Lock()
	defer b.Unlock()
	previousEntry = b.entries[e.Kind()]
	running = previousEntry != nil && !previousEntry.Get().Finished()
	if running {
		if keep, err := e.preRenewHook(previousEntry); keep || err != nil {
			return previousEntry, err
		}
	}
	if err := e.Start(b.uniqueID()); err != nil {
		return nil, err
	}
	b.entries[e.Kind()] = e
	b.r.storeByID(e.Get().ID(), e)
	if running {
		e.postRenewHook(previousEntry)
	}
	return e, nil
}

//
// ecGetEntry
//
type ecGetEntry struct {
	baseBckEntry
	xact *ec.XactGet
}

func (e *ecGetEntry) Start(id int64) error {
	xec := ec.ECM.NewGetXact(e.bck.Name)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECGet, e.bck.Bck)
	e.xact = xec
	go xec.Run()

	return nil
}

func (*ecGetEntry) Kind() string    { return cmn.ActECGet }
func (e *ecGetEntry) Get() cmn.Xact { return e.xact }
func (r *registry) RenewGetEC(bck *cluster.Bck) *ec.XactGet {
	b := r.BucketsXacts(bck)
	e := &ecGetEntry{baseBckEntry: baseBckEntry{bck: bck}}
	ee, _ := b.renewBucketXaction(e) // TODO: handle error
	return ee.Get().(*ec.XactGet)
}

//
// ecPutEntry
//
type ecPutEntry struct {
	baseBckEntry
	xact *ec.XactPut
}

func (e *ecPutEntry) Start(id int64) error {
	xec := ec.ECM.NewPutXact(e.bck.Name)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECPut, e.bck.Bck)
	go xec.Run()
	e.xact = xec
	return nil
}
func (*ecPutEntry) Kind() string    { return cmn.ActECPut }
func (e *ecPutEntry) Get() cmn.Xact { return e.xact }
func (r *registry) RenewPutEC(bck *cluster.Bck) *ec.XactPut {
	b := r.BucketsXacts(bck)
	e := &ecPutEntry{baseBckEntry: baseBckEntry{bck: bck}}
	ee, _ := b.renewBucketXaction(e) // TODO: handle error
	return ee.Get().(*ec.XactPut)
}

//
// ecRespondEntry
//
type ecRespondEntry struct {
	baseBckEntry
	xact *ec.XactRespond
}

func (e *ecRespondEntry) Start(id int64) error {
	xec := ec.ECM.NewRespondXact(e.bck.Name)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECRespond, e.bck.Bck)
	go xec.Run()
	e.xact = xec
	return nil
}
func (*ecRespondEntry) Kind() string    { return cmn.ActECRespond }
func (e *ecRespondEntry) Get() cmn.Xact { return e.xact }
func (r *registry) RenewRespondEC(bck *cluster.Bck) *ec.XactRespond {
	b := r.BucketsXacts(bck)
	e := &ecRespondEntry{baseBckEntry: baseBckEntry{bck: bck}}
	ee, _ := b.renewBucketXaction(e)
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

func (e *ecEncodeEntry) Start(id int64) error {
	xec := ec.NewXactBckEncode(id, e.bck, e.t)
	e.xact = xec
	return nil
}

func (*ecEncodeEntry) Kind() string    { return cmn.ActECEncode }
func (e *ecEncodeEntry) Get() cmn.Xact { return e.xact }
func (r *registry) RenewECEncodeXact(t cluster.Target, bck *cluster.Bck, phase string) (*ec.XactBckEncode, error) {
	b := r.BucketsXacts(bck)
	e := &ecEncodeEntry{baseBckEntry: baseBckEntry{bck: bck}, t: t, phase: phase}
	ee, err := b.renewBucketXaction(e)
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
	err = fmt.Errorf("%s(%s, phase %s): cannot %s", e.Kind(), prev.bck.Name, prev.phase, e.phase)
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

func (e *mncEntry) Start(id int64) error {
	slab, err := e.t.GetMem2().GetSlab2(memsys.MaxSlabSize)
	cmn.AssertNoErr(err)
	xmnc := mirror.NewXactMNC(id, e.bck, e.t, slab, e.copies)
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
		bmd.Range(&provider, nil, func(bck *cluster.Bck) bool {
			if bck.Props.Mirror.Enabled {
				r.RenewBckMakeNCopies(bck, t, int(bck.Props.Mirror.Copies))
			}
			return false
		})
	}
}

func (r *registry) RenewBckMakeNCopies(bck *cluster.Bck, t cluster.Target, copies int) {
	b := r.BucketsXacts(bck)
	e := &mncEntry{t: t, copies: copies, baseBckEntry: baseBckEntry{bck: bck}}
	_, _ = b.renewBucketXaction(e)
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

func (e *dpromoteEntry) Start(id int64) error {
	xact := mirror.NewXactDirPromote(id, e.dir, e.bck, e.t, e.params)
	go xact.Run()
	e.xact = xact
	return nil
}
func (*dpromoteEntry) Kind() string    { return cmn.ActPromote }
func (e *dpromoteEntry) Get() cmn.Xact { return e.xact }

func (r *registry) RenewDirPromote(dir string, bck *cluster.Bck, t cluster.Target, params *cmn.ActValPromote) (*mirror.XactDirPromote, error) {
	b := r.BucketsXacts(bck)
	e := &dpromoteEntry{t: t, dir: dir, baseBckEntry: baseBckEntry{bck: bck}, params: params}
	ee, err := b.renewBucketXaction(e)
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

func (e *loadLomCacheEntry) Start(id int64) error {
	x := mirror.NewXactLLC(e.t, id, e.bck)
	go x.Run()
	e.xact = x

	return nil
}
func (*loadLomCacheEntry) Kind() string    { return cmn.ActLoadLomCache }
func (e *loadLomCacheEntry) Get() cmn.Xact { return e.xact }

func (r *registry) RenewBckLoadLomCache(t cluster.Target, bck *cluster.Bck) {
	b := r.BucketsXacts(bck)
	e := &loadLomCacheEntry{t: t, baseBckEntry: baseBckEntry{bck: bck}}
	b.renewBucketXaction(e)
}

func (e *loadLomCacheEntry) preRenewHook(previousEntry bucketEntry) (bool, error) {
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

func (e *putLocReplicasEntry) Start(id int64) error {
	slab, err := e.t.GetMem2().GetSlab2(memsys.MaxSlabSize) // TODO: estimate
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
	b := r.BucketsXacts(lom.Bck())
	e := &putLocReplicasEntry{t: lom.T, lom: lom, baseBckEntry: baseBckEntry{bck: lom.Bck()}}
	ee, err := b.renewBucketXaction(e)
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
	action  string
	phase   string
}

func (e *bccEntry) Start(id int64) error {
	slab, err := e.t.GetMem2().GetSlab2(memsys.MaxSlabSize)
	cmn.AssertNoErr(err)
	e.xact = mirror.NewXactBCC(id, e.bckFrom, e.bckTo, e.action, e.t, slab)
	return nil
}
func (e *bccEntry) Kind() string  { return e.action }
func (e *bccEntry) Get() cmn.Xact { return e.xact }

func (e *bccEntry) preRenewHook(previousEntry bucketEntry) (keep bool, err error) {
	prev := previousEntry.(*bccEntry)
	if prev.phase == cmn.ActBegin && e.phase == cmn.ActCommit && prev.bckFrom.Equal(e.bckFrom, true /*same BID*/) {
		prev.phase = cmn.ActCommit // transition
		keep = true
		return
	}
	err = fmt.Errorf("%s(%s=>%s, phase %s): cannot %s(%s=>%s)", prev.xact, prev.bckFrom, prev.bckTo, prev.phase, e.phase, e.bckFrom, e.bckTo)
	return
}

func (r *registry) RenewBckCopy(t cluster.Target, bckFrom, bckTo *cluster.Bck, phase string) (*mirror.XactBckCopy, error) {
	b := r.BucketsXacts(bckTo)
	e := &bccEntry{baseBckEntry: baseBckEntry{bck: bckTo},
		t:       t,
		bckFrom: bckFrom,
		bckTo:   bckTo,
		action:  cmn.ActCopyBucket, // kind
		phase:   phase,
	}
	ee, err := b.renewBucketXaction(e)
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
		bucketTo   string
		action     string
		phase      string
	}
	FastRen struct {
		cmn.XactBase
		rebManager cluster.RebManager
		t          cluster.Target
		bucketTo   string
	}
)

func (r *FastRen) Description() string {
	return "rename bucket via 2-phase local-rename followed by global-rebalance"
}
func (r *FastRen) IsMountpathXact() bool { return false }

//
// TODO -- FIXME: if rebalancing don't "scope" it to a bucket
//
func (r *FastRen) Run(globRebID int64) {
	var (
		wg               = &sync.WaitGroup{}
		gbucket, lbucket = r.bucketTo, r.bucketTo // scoping
		_, lrunning      = Registry.IsRebalancing(cmn.ActLocalReb)
	)
	glog.Infoln(r.String(), r.Bck(), "=>", r.bucketTo)
	if Registry.AbortGlobalXact(cmn.ActGlobalReb) {
		glog.Infof("%s: restarting global rebalance upon rename...", r)
		gbucket = ""
	}
	if lrunning {
		lbucket = ""
	}
	// run in parallel
	wg.Add(1)
	go func() {
		r.rebManager.RunLocalReb(true /*skipGlobMisplaced*/, lbucket)
		wg.Done()
	}()
	r.rebManager.RunGlobalReb(r.t.GetSowner().Get(), globRebID, gbucket)
	wg.Wait()

	r.t.BMDVersionFixup(r.Bck(), false) // piggyback bucket renaming (last step) on getting updated BMD
	r.EndTime(time.Now())
}

func (e *FastRenEntry) Start(id int64) error {
	e.xact = &FastRen{
		XactBase:   *cmn.NewXactBaseWithBucket(id, e.Kind(), e.bck.Bck),
		t:          e.t,
		bucketTo:   e.bucketTo,
		rebManager: e.rebManager,
	}
	return nil
}
func (e *FastRenEntry) Kind() string   { return e.action }
func (e *FastRenEntry) Get() cmn.Xact  { return e.xact }
func (r *FastRenEntry) Bucket() string { return r.bck.Name }

func (e *FastRenEntry) preRenewHook(previousEntry bucketEntry) (keep bool, err error) {
	if e.phase == cmn.ActBegin {
		bckTo := cluster.NewBck(e.bucketTo, cmn.ProviderAIS, cmn.NsGlobal)
		bb := Registry.BucketsXacts(bckTo)
		if num := bb.len(); num > 0 {
			err = fmt.Errorf("%s: cannot(%s=>%s) with %d in progress", e.Kind(), e.bck.Name, e.bucketTo, num)
			return
		}
		// TODO: more checks
	}
	prev := previousEntry.(*FastRenEntry)
	if prev.phase == cmn.ActBegin && e.phase == cmn.ActCommit && prev.bucketTo == e.bucketTo {
		prev.phase = cmn.ActCommit // transition
		keep = true
		return
	}
	err = fmt.Errorf("%s(%s=>%s, phase %s): cannot %s(=>%s)",
		e.Kind(), prev.bck.Name, prev.bucketTo, prev.phase, e.phase, e.bucketTo)
	return
}

func (r *registry) RenewBckFastRename(t cluster.Target, bckFrom, bckTo *cluster.Bck, phase string,
	mgr cluster.RebManager) (*FastRen, error) {
	b := r.BucketsXacts(bckFrom)
	e := &FastRenEntry{baseBckEntry: baseBckEntry{bck: bckFrom},
		t:          t,
		rebManager: mgr,
		bucketTo:   bckTo.Name,
		action:     cmn.ActRenameLB, // kind
		phase:      phase,
	}
	ee, err := b.renewBucketXaction(e)
	if err == nil {
		return ee.Get().(*FastRen), nil
	}
	return nil, err
}

//
// baseBckEntry
//
type baseBckEntry struct {
	baseEntry
	bck *cluster.Bck
}

func (*baseBckEntry) IsGlobal() bool { return false }
func (*baseBckEntry) IsTask() bool   { return false }

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
	return b.stats.FillFromXact(xact, b.bck)
}
