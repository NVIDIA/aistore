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
		bck     cmn.Bck
		entries map[string]bucketEntry
	}
)

var (
	_ ec.XactRegistry = &registry{}
)

//
// bucketXactions
//

func newBucketXactions(r *registry, bck cmn.Bck) *bucketXactions {
	return &bucketXactions{r: r, bck: bck, entries: make(map[string]bucketEntry)}
}

func (b *bucketXactions) GetL(kind string) bucketEntry {
	b.RLock()
	defer b.RUnlock()
	return b.entries[kind]
}

func (b *bucketXactions) Stats() map[string]stats.XactStats {
	statsList := make(map[string]stats.XactStats, len(b.entries))
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

func (b *bucketXactions) uniqueID() string { return b.r.uniqueID() }
func (b *bucketXactions) len() int         { return len(b.entries) }

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
	if err := e.Start(b.uniqueID(), b.bck); err != nil {
		return nil, err
	}
	b.entries[e.Kind()] = e
	b.r.storeEntry(e)
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

func (e *ecGetEntry) Start(id string, bck cmn.Bck) error {
	xec := ec.ECM.NewGetXact(bck.Name) // TODO: we should pass whole `cmn.Bck`
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECGet, bck)
	e.xact = xec
	go xec.Run()
	return nil
}

func (*ecGetEntry) Kind() string    { return cmn.ActECGet }
func (e *ecGetEntry) Get() cmn.Xact { return e.xact }
func (r *registry) RenewGetEC(bck *cluster.Bck) *ec.XactGet {
	b := r.BucketsXacts(bck)
	ee, _ := b.renewBucketXaction(&ecGetEntry{}) // TODO: handle error
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
	xec := ec.ECM.NewPutXact(bck.Name) // TODO: we should pass whole `cmn.Bck`
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECPut, bck)
	go xec.Run()
	e.xact = xec
	return nil
}
func (*ecPutEntry) Kind() string    { return cmn.ActECPut }
func (e *ecPutEntry) Get() cmn.Xact { return e.xact }
func (r *registry) RenewPutEC(bck *cluster.Bck) *ec.XactPut {
	b := r.BucketsXacts(bck)
	e := &ecPutEntry{}
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

func (e *ecRespondEntry) Start(id string, bck cmn.Bck) error {
	xec := ec.ECM.NewRespondXact(bck.Name) // TODO: we should pass whole `cmn.Bck`
	xec.XactDemandBase = *cmn.NewXactDemandBase(id, cmn.ActECRespond, bck)
	go xec.Run()
	e.xact = xec
	return nil
}
func (*ecRespondEntry) Kind() string    { return cmn.ActECRespond }
func (e *ecRespondEntry) Get() cmn.Xact { return e.xact }
func (r *registry) RenewRespondEC(bck *cluster.Bck) *ec.XactRespond {
	b := r.BucketsXacts(bck)
	e := &ecRespondEntry{}
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

func (e *ecEncodeEntry) Start(id string, bck cmn.Bck) error {
	xec := ec.NewXactBckEncode(id, bck, e.t)
	e.xact = xec
	return nil
}

func (*ecEncodeEntry) Kind() string    { return cmn.ActECEncode }
func (e *ecEncodeEntry) Get() cmn.Xact { return e.xact }
func (r *registry) RenewECEncodeXact(t cluster.Target, bck *cluster.Bck, phase string) (*ec.XactBckEncode, error) {
	b := r.BucketsXacts(bck)
	e := &ecEncodeEntry{t: t, phase: phase}
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
	b := r.BucketsXacts(bck)
	e := &mncEntry{t: t, copies: copies}
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

func (e *dpromoteEntry) Start(id string, bck cmn.Bck) error {
	xact := mirror.NewXactDirPromote(id, e.dir, bck, e.t, e.params)
	go xact.Run()
	e.xact = xact
	return nil
}
func (*dpromoteEntry) Kind() string    { return cmn.ActPromote }
func (e *dpromoteEntry) Get() cmn.Xact { return e.xact }

func (r *registry) RenewDirPromote(dir string, bck *cluster.Bck, t cluster.Target, params *cmn.ActValPromote) (*mirror.XactDirPromote, error) {
	b := r.BucketsXacts(bck)
	e := &dpromoteEntry{t: t, dir: dir, params: params}
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

func (e *loadLomCacheEntry) Start(id string, bck cmn.Bck) error {
	x := mirror.NewXactLLC(e.t, id, bck)
	go x.Run()
	e.xact = x

	return nil
}
func (*loadLomCacheEntry) Kind() string    { return cmn.ActLoadLomCache }
func (e *loadLomCacheEntry) Get() cmn.Xact { return e.xact }

func (r *registry) RenewBckLoadLomCache(t cluster.Target, bck *cluster.Bck) {
	b := r.BucketsXacts(bck)
	e := &loadLomCacheEntry{t: t}
	b.renewBucketXaction(e)
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
	b := r.BucketsXacts(lom.Bck())
	e := &putLocReplicasEntry{t: lom.T, lom: lom}
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

func (e *bccEntry) Start(id string, _ cmn.Bck) error {
	slab, err := e.t.GetMMSA().GetSlab(memsys.MaxPageSlabSize)
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
	e := &bccEntry{
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
		bckFrom    *cluster.Bck
		bckTo      *cluster.Bck
		action     string
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

func (r *FastRen) Description() string {
	return "rename bucket via 2-phase local-rename followed by global-rebalance"
}
func (r *FastRen) IsMountpathXact() bool { return false }

func (r *FastRen) Run(waiter *sync.WaitGroup, globRebID int64) {
	var (
		wg               = &sync.WaitGroup{}
		gbucket, lbucket = r.bckTo.Name, r.bckTo.Name // scoping
	)

	glog.Infoln(r.String(), r.bckFrom, "=>", r.bckTo)

	if Registry.DoAbort(cmn.ActLocalReb, nil) {
		glog.Infof("%s: restarting local rebalance upon rename...", r)
		lbucket = ""
	}
	if Registry.DoAbort(cmn.ActGlobalReb, nil) {
		glog.Infof("%s: restarting global rebalance upon rename...", r)
		gbucket = ""
	}

	waiter.Done() // notify the waiter that we are ready to start

	// run in parallel
	wg.Add(1)
	go func() {
		r.rebManager.RunLocalReb(true /*skipGlobMisplaced*/, lbucket)
		wg.Done()
	}()
	r.rebManager.RunGlobalReb(r.t.GetSowner().Get(), globRebID, gbucket)
	wg.Wait()

	r.t.BMDVersionFixup(nil, r.Bck(), false) // piggyback bucket renaming (last step) on getting updated BMD
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
func (e *FastRenEntry) Kind() string  { return e.action }
func (e *FastRenEntry) Get() cmn.Xact { return e.xact }

func (e *FastRenEntry) preRenewHook(previousEntry bucketEntry) (keep bool, err error) {
	if e.phase == cmn.ActBegin {
		bb := Registry.BucketsXacts(e.bckTo)
		if num := bb.len(); num > 0 {
			err = fmt.Errorf("%s: cannot(%s=>%s) with %d in progress", e.Kind(), e.bckFrom, e.bckTo, num)
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
	b := r.BucketsXacts(bckFrom)
	e := &FastRenEntry{
		t:          t,
		rebManager: mgr,
		bckFrom:    bckFrom,
		bckTo:      bckTo,
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
type baseBckEntry struct{}

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
	return stats.NewXactStats(xact)
}
