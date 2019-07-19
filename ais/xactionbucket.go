// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/stats"
)

type (
	bucketXactions struct {
		r       *xactionsRegistry
		bckName string
		sync.RWMutex
		// entires
		entries map[string]xactionBucketEntry
	}

	xactionBucketEntry interface {
		xactionEntry
		// returns if renew should be canceled when previous entry exists
		EndRenewOnPrevious(entry xactionBucketEntry) (end bool)
		// performs some actions before returning from renew, when previous entry exists, for example Renew()
		ActOnPrevious(entry xactionBucketEntry)
	}
)

// bucketXactions entries methods

func (b *bucketXactions) uniqueID() int64 {
	return b.r.uniqueID()
}

func newBucketXactions(r *xactionsRegistry, bckName string) *bucketXactions {
	return &bucketXactions{r: r, entries: make(map[string]xactionBucketEntry), bckName: bckName}
}

func (b *bucketXactions) GetL(kind string) xactionBucketEntry {
	b.RLock()
	defer b.RUnlock()
	return b.entries[kind]
}

func (b *bucketXactions) Update(e xactionBucketEntry) error {
	b.Lock()
	defer b.Unlock()

	previousEntry := b.entries[e.Kind()]
	if previousEntry != nil && e.EndRenewOnPrevious(previousEntry) {
		e.ActOnPrevious(previousEntry)
		return nil
	}

	if err := e.Start(b.uniqueID()); err != nil {
		return err
	}

	b.entries[e.Kind()] = e
	b.r.storeByID(e.Get().ID(), e)
	return nil
}

func (b *bucketXactions) renewBucketXaction(e xactionBucketEntry) (err error) {
	b.RLock()
	previousEntry := b.entries[e.Kind()]

	if previousEntry != nil && e.EndRenewOnPrevious(previousEntry) {
		e.ActOnPrevious(previousEntry)
		b.RUnlock()
		return nil
	}

	b.RUnlock()
	return b.Update(e)
}

func (b *bucketXactions) Stats() map[int64]stats.XactStats {
	statsList := make(map[int64]stats.XactStats, len(b.entries))
	b.RLock()
	for _, e := range b.entries {
		statsList[e.Get().ID()] = e.Stats()
	}
	b.RUnlock()
	return statsList
}

// AbortAll waits until abort of all bucket's xactions is finished
// Every abort is done asynchronously
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

// Buckets entries

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
	_ = b.renewBucketXaction(e)

	return b.GetL(e.Kind()).Get().(*ec.XactGet)
}

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
	_ = b.renewBucketXaction(e)
	return b.GetL(e.Kind()).Get().(*ec.XactPut)
}

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
	_ = b.renewBucketXaction(e)
	return b.GetL(e.Kind()).Get().(*ec.XactRespond)
}

type mncEntry struct {
	baseBckEntry
	t          *targetrunner
	copies     int
	bckIsLocal bool
	xact       *mirror.XactBckMakeNCopies
}

func (e *mncEntry) Start(id int64) error {
	slab, err := nodeCtx.mm.GetSlab2(memsys.MaxSlabSize) // TODO: estimate
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
	_ = b.renewBucketXaction(e)
}

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
	if true {
		return
	}

	b := r.bucketsXacts(bucket)
	e := &loadLomCacheEntry{t: t, bckIsLocal: bckIsLocal, baseBckEntry: baseBckEntry{bckName: bucket}}
	b.renewBucketXaction(e)
}

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

	if err := b.renewBucketXaction(e); err != nil {
		return nil
	}

	putLocRepEntry := b.GetL(e.Kind())
	return putLocRepEntry.(*putLocReplicasEntry).xact
}

// Base implementation

type baseBckEntry struct {
	baseEntry
	bckName string
}

func (*baseBckEntry) IsGlobal() bool  { return false }
func (*baseBckEntry) IsTask() bool    { return false }
func (b *baseBckEntry) IsEmpty() bool { return b == nil }
func (b *baseBckEntry) EndRenewOnPrevious(entry xactionBucketEntry) (end bool) {
	return !entry.Get().Finished()
}
func (b *baseBckEntry) ActOnPrevious(entry xactionBucketEntry) {
	if demandEntry, ok := entry.Get().(cmn.XactDemand); ok {
		demandEntry.Renew()
	}
}

// STATS
func (e *ecGetEntry) Stats() stats.XactStats          { return e.stats.FromXact(e.xact, e.bckName) }
func (e *ecPutEntry) Stats() stats.XactStats          { return e.stats.FromXact(e.xact, e.bckName) }
func (e *ecRespondEntry) Stats() stats.XactStats      { return e.stats.FromXact(e.xact, e.bckName) }
func (e *mncEntry) Stats() stats.XactStats            { return e.stats.FromXact(e.xact, e.bckName) }
func (e *putLocReplicasEntry) Stats() stats.XactStats { return e.stats.FromXact(e.xact, e.bckName) }
func (e *loadLomCacheEntry) Stats() stats.XactStats   { return e.stats.FromXact(e.xact, e.bckName) }
