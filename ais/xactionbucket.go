// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

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
	xactionBucketEntry interface {
		xactionEntry
		// pre-renew: returns true iff the current active one exists and is either
		// - ok to keep running as is, or
		// - has been renew(ed) and is still ok
		preRenewHook(previousEntry xactionBucketEntry) (keep bool, err error)
		// post-renew hook
		postRenewHook(previousEntry xactionBucketEntry)
	}
	bucketXactions struct {
		sync.RWMutex
		r       *xactionsRegistry
		entries map[string]xactionBucketEntry
	}
)

//
// bucketXactions
//

func newBucketXactions(r *xactionsRegistry) *bucketXactions {
	return &bucketXactions{r: r, entries: make(map[string]xactionBucketEntry)}
}

func (b *bucketXactions) GetL(kind string) xactionBucketEntry {
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
func (b *bucketXactions) renewBucketXaction(e xactionBucketEntry) (xactionBucketEntry, error) {
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
	previousEntry = b.entries[e.Kind()]
	running = previousEntry != nil && !previousEntry.Get().Finished()
	if running {
		if keep, err := e.preRenewHook(previousEntry); keep || err != nil {
			b.Unlock()
			return previousEntry, err
		}
	}
	if err := e.Start(b.uniqueID()); err != nil {
		b.Unlock()
		return nil, err
	}
	b.entries[e.Kind()] = e
	b.r.storeByID(e.Get().ID(), e)
	if running {
		e.postRenewHook(previousEntry)
	}
	b.Unlock()
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
	xec := ECM.newGetXact(e.bck.Name)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECGet, e.bck.Name, e.bck.IsAIS())
	e.xact = xec
	go xec.Run()

	return nil
}

func (*ecGetEntry) Kind() string    { return cmn.ActECGet }
func (e *ecGetEntry) Get() cmn.Xact { return e.xact }
func (r *xactionsRegistry) renewGetEC(bck *cluster.Bck) *ec.XactGet {
	b := r.bucketsXacts(bck)
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
	xec := ECM.newPutXact(e.bck.Name)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECPut, e.bck.Name, e.bck.IsAIS())
	go xec.Run()
	e.xact = xec
	return nil
}
func (*ecPutEntry) Kind() string    { return cmn.ActECPut }
func (e *ecPutEntry) Get() cmn.Xact { return e.xact }
func (r *xactionsRegistry) renewPutEC(bck *cluster.Bck) *ec.XactPut {
	b := r.bucketsXacts(bck)
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
	xec := ECM.newRespondXact(e.bck.Name)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECRespond, e.bck.Name, e.bck.IsAIS())
	go xec.Run()
	e.xact = xec
	return nil
}
func (*ecRespondEntry) Kind() string    { return cmn.ActECRespond }
func (e *ecRespondEntry) Get() cmn.Xact { return e.xact }
func (r *xactionsRegistry) renewRespondEC(bck *cluster.Bck) *ec.XactRespond {
	b := r.bucketsXacts(bck)
	e := &ecRespondEntry{baseBckEntry: baseBckEntry{bck: bck}}
	ee, _ := b.renewBucketXaction(e)
	return ee.Get().(*ec.XactRespond)
}

//
// mncEntry
//
type mncEntry struct {
	baseBckEntry
	t      *targetrunner
	xact   *mirror.XactBckMakeNCopies
	copies int
}

func (e *mncEntry) Start(id int64) error {
	slab, err := nodeCtx.mm.GetSlab2(memsys.MaxSlabSize)
	cmn.AssertNoErr(err)
	xmnc := mirror.NewXactMNC(id, e.bck, e.t, slab, e.copies)
	go xmnc.Run()
	e.xact = xmnc
	return nil
}
func (*mncEntry) Kind() string    { return cmn.ActMakeNCopies }
func (e *mncEntry) Get() cmn.Xact { return e.xact }

func (r *xactionsRegistry) renewBckMakeNCopies(bck *cluster.Bck, t *targetrunner, copies int) {
	b := r.bucketsXacts(bck)
	e := &mncEntry{t: t, copies: copies, baseBckEntry: baseBckEntry{bck: bck}}
	_, _ = b.renewBucketXaction(e)
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

func (r *xactionsRegistry) renewBckLoadLomCache(t cluster.Target, bck *cluster.Bck) {
	b := r.bucketsXacts(bck)
	e := &loadLomCacheEntry{t: t, baseBckEntry: baseBckEntry{bck: bck}}
	b.renewBucketXaction(e)
}

func (e *loadLomCacheEntry) preRenewHook(previousEntry xactionBucketEntry) (bool, error) {
	return true, nil
}

//
// putLocReplicasEntry
//
type putLocReplicasEntry struct {
	baseBckEntry
	lom  *cluster.LOM
	xact *mirror.XactPutLRepl
}

func (e *putLocReplicasEntry) Start(id int64) error {
	slab, err := nodeCtx.mm.GetSlab2(memsys.MaxSlabSize) // TODO: estimate
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

func (r *xactionsRegistry) renewPutLocReplicas(lom *cluster.LOM) *mirror.XactPutLRepl {
	b := r.bucketsXacts(lom.Bck())
	e := &putLocReplicasEntry{lom: lom, baseBckEntry: baseBckEntry{bck: lom.Bck()}}
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
	t      *targetrunner
	xact   *mirror.XactBckCopy
	bckTo  *cluster.Bck
	action string
	phase  string
}

func (e *bccEntry) Start(id int64) error {
	slab, err := nodeCtx.mm.GetSlab2(memsys.MaxSlabSize)
	cmn.AssertNoErr(err)
	e.xact = mirror.NewXactBCC(id, e.bck, e.bckTo, e.action, e.t, slab)
	return nil
}
func (e *bccEntry) Kind() string  { return e.action }
func (e *bccEntry) Get() cmn.Xact { return e.xact }

func (e *bccEntry) preRenewHook(previousEntry xactionBucketEntry) (keep bool, err error) {
	prev := previousEntry.(*bccEntry)
	if prev.phase == cmn.ActBegin && e.phase == cmn.ActCommit && prev.bckTo.Equal(e.bckTo) {
		prev.phase = cmn.ActCommit // transition
		keep = true
		return
	}
	err = fmt.Errorf("%s(%s=>%s, phase %s): cannot %s(=>%s)", prev.xact, prev.bck, prev.bckTo, prev.phase, e.phase, e.bckTo)
	return
}

func (r *xactionsRegistry) renewBckCopy(t *targetrunner, bckFrom, bckTo *cluster.Bck, phase string) (*mirror.XactBckCopy, error) {
	b := r.bucketsXacts(bckFrom)
	e := &bccEntry{baseBckEntry: baseBckEntry{bck: bckFrom},
		t:      t,
		bckTo:  bckTo,
		action: cmn.ActCopyLB, // kind
		phase:  phase,
	}
	ee, err := b.renewBucketXaction(e)
	if err != nil {
		return nil, err
	}
	return ee.Get().(*mirror.XactBckCopy), nil
}

//
// fastRenEntry & xactFastRen
//
type (
	fastRenEntry struct {
		baseBckEntry
		t        *targetrunner
		xact     *xactFastRen
		bucketTo string
		action   string
		phase    string
	}
	xactFastRen struct {
		cmn.XactBase
		t        *targetrunner
		bucketTo string
	}
)

func (r *xactFastRen) Description() string {
	return "rename bucket via 2-phase local-rename followed by global-rebalance"
}
func (r *xactFastRen) IsMountpathXact() bool { return false }

//
// TODO -- FIXME: if rebalancing don't "scope" it to a bucket
//
func (r *xactFastRen) run(globRebID int64) {
	var (
		wg               = &sync.WaitGroup{}
		gbucket, lbucket = r.bucketTo, r.bucketTo // scoping
		_, lrunning      = r.t.xactions.isRebalancing(cmn.ActLocalReb)
	)
	glog.Infoln(r.String(), r.Bucket(), "=>", r.bucketTo)
	if r.t.xactions.abortGlobalXact(cmn.ActGlobalReb) {
		glog.Infof("%s: restarting global rebalance upon rename...", r)
		gbucket = ""
	}
	if lrunning {
		lbucket = ""
	}
	// run in parallel
	wg.Add(1)
	go func() {
		r.t.rebManager.runLocalReb(true /*skipGlobMisplaced*/, lbucket)
		wg.Done()
	}()
	r.t.rebManager.runGlobalReb(r.t.smapowner.get(), globRebID, gbucket)
	wg.Wait()

	r.t.bmdVersionFixup(r.Bucket()) // piggyback bucket renaming (last step) on getting updated BMD
	r.EndTime(time.Now())
}

func (e *fastRenEntry) Start(id int64) error {
	e.xact = &xactFastRen{
		XactBase: *cmn.NewXactBaseWithBucket(id, e.Kind(), e.bck.Name, e.bck.IsAIS()),
		t:        e.t,
		bucketTo: e.bucketTo,
	}
	return nil
}
func (e *fastRenEntry) Kind() string  { return e.action }
func (e *fastRenEntry) Get() cmn.Xact { return e.xact }

func (e *fastRenEntry) preRenewHook(previousEntry xactionBucketEntry) (keep bool, err error) {
	if e.phase == cmn.ActBegin {
		bckTo := &cluster.Bck{Name: e.bucketTo, Provider: cmn.ProviderFromBool(true)}
		bb := e.t.xactions.bucketsXacts(bckTo)
		if num := bb.len(); num > 0 {
			err = fmt.Errorf("%s: cannot(%s=>%s) with %d in progress", e.Kind(), e.bck.Name, e.bucketTo, num)
			return
		}
		// TODO: more checks
	}
	prev := previousEntry.(*fastRenEntry)
	if prev.phase == cmn.ActBegin && e.phase == cmn.ActCommit && prev.bucketTo == e.bucketTo {
		prev.phase = cmn.ActCommit // transition
		keep = true
		return
	}
	err = fmt.Errorf("%s(%s=>%s, phase %s): cannot %s(=>%s)", e.Kind(), prev.bck.Name, prev.bucketTo, prev.phase, e.phase, e.bucketTo)
	return
}

func (r *xactionsRegistry) renewBckFastRename(t *targetrunner, bckFrom, bckTo *cluster.Bck, phase string) (*xactFastRen, error) {
	b := r.bucketsXacts(bckFrom)
	e := &fastRenEntry{baseBckEntry: baseBckEntry{bck: bckFrom},
		t:        t,
		bucketTo: bckTo.Name,
		action:   cmn.ActRenameLB, // kind
		phase:    phase,
	}
	ee, err := b.renewBucketXaction(e)
	if err == nil {
		return ee.Get().(*xactFastRen), nil
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

//nolint:unparam
func (b *baseBckEntry) preRenewHook(previousEntry xactionBucketEntry) (keep bool, err error) {
	e := previousEntry.Get()
	if demandEntry, ok := e.(cmn.XactDemand); ok {
		demandEntry.Renew()
		keep = true
	}
	return
}

func (b *baseBckEntry) postRenewHook(_ xactionBucketEntry) {}

func (b *baseBckEntry) Stats(xact cmn.Xact) stats.XactStats {
	return b.stats.FillFromXact(xact, b.bck)
}
