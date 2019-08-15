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
		bckName string
		entries map[string]xactionBucketEntry
	}
)

//
// bucketXactions
//

func newBucketXactions(r *xactionsRegistry, bckName string) *bucketXactions {
	return &bucketXactions{r: r, entries: make(map[string]xactionBucketEntry), bckName: bckName}
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
		if !e.Get().Finished() {
			sleep = true
			wg.Add(1)
			go func() {
				e.Get().Abort()
				wg.Done()
			}()
		}
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
	xec := ECM.newGetXact(e.bckName)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECGet, e.bckName, true /* local*/) // TODO: EC support for Cloud
	e.xact = xec
	go xec.Run()

	return nil
}

func (*ecGetEntry) Kind() string    { return cmn.ActECGet }
func (e *ecGetEntry) Get() cmn.Xact { return e.xact }
func (r *xactionsRegistry) renewGetEC(bucket string) *ec.XactGet {
	b := r.bucketsXacts(bucket)
	e := &ecGetEntry{baseBckEntry: baseBckEntry{bckName: bucket}}
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
	xec := ECM.newPutXact(e.bckName)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECPut, e.bckName, true /* local */) // TODO: EC support for Cloud
	go xec.Run()
	e.xact = xec
	return nil
}
func (*ecPutEntry) Kind() string    { return cmn.ActECPut }
func (e *ecPutEntry) Get() cmn.Xact { return e.xact }
func (r *xactionsRegistry) renewPutEC(bucket string) *ec.XactPut {
	b := r.bucketsXacts(bucket)
	e := &ecPutEntry{baseBckEntry: baseBckEntry{bckName: bucket}}
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
	xec := ECM.newRespondXact(e.bckName)
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECRespond, e.bckName, true /* local */) // TODO: EC support for Cloud
	go xec.Run()
	e.xact = xec
	return nil
}
func (*ecRespondEntry) Kind() string    { return cmn.ActECRespond }
func (e *ecRespondEntry) Get() cmn.Xact { return e.xact }
func (r *xactionsRegistry) renewRespondEC(bucket string) *ec.XactRespond {
	b := r.bucketsXacts(bucket)
	e := &ecRespondEntry{baseBckEntry: baseBckEntry{bckName: bucket}}
	ee, _ := b.renewBucketXaction(e)
	return ee.Get().(*ec.XactRespond)
}

//
// mncEntry
//
type mncEntry struct {
	baseBckEntry
	t          *targetrunner
	xact       *mirror.XactBckMakeNCopies
	copies     int
	bckIsLocal bool
}

func (e *mncEntry) Start(id int64) error {
	slab, err := nodeCtx.mm.GetSlab2(memsys.MaxSlabSize)
	cmn.AssertNoErr(err)
	xmnc := mirror.NewXactMNC(id, e.bckName, e.t, slab, e.copies, e.bckIsLocal)
	go xmnc.Run()
	e.xact = xmnc
	return nil
}
func (*mncEntry) Kind() string    { return cmn.ActMakeNCopies }
func (e *mncEntry) Get() cmn.Xact { return e.xact }

func (r *xactionsRegistry) renewBckMakeNCopies(bucket string, t *targetrunner, copies int, bckIsLocal bool) {
	b := r.bucketsXacts(bucket)
	e := &mncEntry{t: t, copies: copies, bckIsLocal: bckIsLocal, baseBckEntry: baseBckEntry{bckName: bucket}}
	_, _ = b.renewBucketXaction(e)
}

//
// loadLomCacheEntry
//
type loadLomCacheEntry struct {
	baseBckEntry
	t          cluster.Target
	bckIsLocal bool
	xact       *mirror.XactBckLoadLomCache
}

func (e *loadLomCacheEntry) Start(id int64) error {
	x := mirror.NewXactLLC(id, e.bckName, e.t, e.bckIsLocal)
	go x.Run()
	e.xact = x

	return nil
}
func (*loadLomCacheEntry) Kind() string    { return cmn.ActLoadLomCache }
func (e *loadLomCacheEntry) Get() cmn.Xact { return e.xact }

func (r *xactionsRegistry) renewBckLoadLomCache(bucket string, t cluster.Target, bckIsLocal bool) {
	b := r.bucketsXacts(bucket)
	e := &loadLomCacheEntry{t: t, bckIsLocal: bckIsLocal, baseBckEntry: baseBckEntry{bckName: bucket}}
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
	b := r.bucketsXacts(lom.Bucket)
	e := &putLocReplicasEntry{lom: lom, baseBckEntry: baseBckEntry{bckName: lom.Bucket}}
	ee, err := b.renewBucketXaction(e)
	if err != nil {
		return nil
	}
	return ee.(*putLocReplicasEntry).xact
}

//
// bcrEntry
//
type bcrEntry struct {
	baseBckEntry
	t        *targetrunner
	xact     *mirror.XactBckCopy
	bucketTo string
	action   string
	phase    string
}

func (e *bcrEntry) Start(id int64) error {
	slab, err := nodeCtx.mm.GetSlab2(memsys.MaxSlabSize)
	cmn.AssertNoErr(err)
	e.xact = mirror.NewXactBCR(id, e.bckName, e.bucketTo, e.action, e.t, slab, true /* is local */)
	return nil
}
func (e *bcrEntry) Kind() string  { return e.action }
func (e *bcrEntry) Get() cmn.Xact { return e.xact }

func (e *bcrEntry) preRenewHook(previousEntry xactionBucketEntry) (keep bool, err error) {
	prev := previousEntry.(*bcrEntry)
	if prev.phase == cmn.ActBegin && e.phase == cmn.ActCommit && prev.bucketTo == e.bucketTo {
		prev.phase = cmn.ActCommit // transition
		keep = true
		return
	}
	err = fmt.Errorf("%s(%s=>%s, phase %s): cannot %s(=>%s)", prev.xact, prev.bckName, prev.bucketTo, prev.phase, e.phase, e.bucketTo)
	return
}

func (r *xactionsRegistry) renewBckCopy(bucketFrom, bucketTo string, t *targetrunner,
	action, phase string) (*mirror.XactBckCopy, error) {
	b := r.bucketsXacts(bucketFrom)
	e := &bcrEntry{baseBckEntry: baseBckEntry{bckName: bucketFrom},
		t:        t,
		bucketTo: bucketTo,
		action:   action, // kind
		phase:    phase,
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

func (r *xactFastRen) run() {
	glog.Infoln(r.String(), r.Bucket(), "=>", r.bucketTo)
	if aborted := r.t.rebManager.abortGlobalReb(); aborted {
		glog.Infof("%s: restarting global rebalance upon rename...", r)
	}
	r.t.rebManager.runLocalReb(true /*skipGplaced*/, r.bucketTo)
	r.t.rebManager.runGlobalReb(r.t.smapowner.get())

	r.t.bmdVersionFixup() // and let proxy handle pending renames

	r.EndTime(time.Now())
}

func (e *fastRenEntry) Start(id int64) error {
	e.xact = &xactFastRen{
		XactBase: *cmn.NewXactBaseWithBucket(id, e.Kind(), e.bckName, true /* is local */),
		t:        e.t,
		bucketTo: e.bucketTo,
	}
	return nil
}
func (e *fastRenEntry) Kind() string  { return e.action }
func (e *fastRenEntry) Get() cmn.Xact { return e.xact }

func (e *fastRenEntry) preRenewHook(previousEntry xactionBucketEntry) (keep bool, err error) {
	if e.phase == cmn.ActBegin {
		bb := e.t.xactions.bucketsXacts(e.bucketTo)
		if num := bb.len(); num > 0 {
			err = fmt.Errorf("%s: cannot(%s=>%s) with %d in progress", e.Kind(), e.bckName, e.bucketTo, num)
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
	err = fmt.Errorf("%s(%s=>%s, phase %s): cannot %s(=>%s)", e.Kind(), prev.bckName, prev.bucketTo, prev.phase, e.phase, e.bucketTo)
	return
}

func (r *xactionsRegistry) renewBckFastRename(bucketFrom, bucketTo string, t *targetrunner, action, phase string) (*xactFastRen, error) {
	b := r.bucketsXacts(bucketFrom)
	e := &fastRenEntry{baseBckEntry: baseBckEntry{bckName: bucketFrom},
		t:        t,
		bucketTo: bucketTo,
		action:   action, // kind
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
	bckName string
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
	return b.stats.FillFromXact(xact, b.bckName)
}
