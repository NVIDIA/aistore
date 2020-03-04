// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/housekeep/lru"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

// Smoke tests for xactions

func TestXactionRenewLRU(t *testing.T) {
	xactions := newRegistry()
	defer xactions.AbortAll()

	ch := make(chan *lru.Xaction, 10)
	wg := &sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			ch <- xactions.RenewLRU()
		}()
	}

	wg.Wait()
	close(ch)

	notNilCount := 0
	for xact := range ch {
		if xact != nil {
			notNilCount++
		}
	}

	tassert.Errorf(t, notNilCount == 1, "expected just one LRU xaction to be created, got %d", notNilCount)
}

func TestXactionRenewPrefetch(t *testing.T) {
	xactions := newRegistry()
	defer xactions.AbortAll()

	ch := make(chan *prefetch, 10)
	wg := &sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			ch <- xactions.RenewPrefetch(nil)
		}()
	}

	wg.Wait()
	close(ch)

	notNilCount := 0
	for xact := range ch {
		if xact != nil {
			notNilCount++
		}
	}

	tassert.Errorf(t, notNilCount == 1, "expected just one Prefetch xaction to be created, got %d", notNilCount)
}

func TestXactionRenewEvictDelete(t *testing.T) {
	xactions := newRegistry()
	bmd := cluster.NewBaseBownerMock()
	props := &cmn.BucketProps{Cksum: cmn.CksumConf{Type: cmn.ChecksumXXHash}}
	bckFrom := cluster.NewBck("test", cmn.ProviderAIS, cmn.NsGlobal, props)
	bmd.Add(bckFrom)
	evArgs := &EvictDeleteArgs{}
	tMock := cluster.NewTargetMock(bmd)

	defer xactions.AbortAll()

	ch := make(chan *EvictDelete, 10)
	wg := &sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			xact, _ := xactions.RenewEvictDelete(tMock, bckFrom, evArgs)
			ch <- xact
		}()
	}

	wg.Wait()
	close(ch)

	res := make(map[*EvictDelete]struct{}, 10)
	for xact := range ch {
		if xact != nil {
			res[xact] = struct{}{}
		}
	}

	tassert.Errorf(t, len(res) > 0, "expected some EvictDelete xactions to be created, got %d", len(res))
}

func TestXactionAbortAll(t *testing.T) {
	xactions := newRegistry()
	bckFrom := cluster.NewBck("test", cmn.ProviderAIS, cmn.NsGlobal)
	bckTo := cluster.NewBck("test", cmn.ProviderAIS, cmn.NsGlobal)

	xactGlob := xactions.RenewLRU()
	tassert.Errorf(t, xactGlob != nil, "Xaction must be created")
	xactBck, err := xactions.RenewBckFastRename(nil, bckFrom, bckTo, "phase", nil)
	tassert.Errorf(t, err == nil && xactBck != nil, "Xaction must be created")

	xactions.AbortAll()

	tassert.Errorf(t, xactGlob != nil && xactGlob.Aborted(),
		"AbortAllGlobal: expected global xaction to be aborted")
	tassert.Errorf(t, xactBck != nil && xactBck.Aborted(),
		"AbortAllGlobal: expected bucket xaction to be aborted")
}

func TestXactionAbortAllGlobal(t *testing.T) {
	xactions := newRegistry()
	bckFrom := cluster.NewBck("test", cmn.ProviderAIS, cmn.NsGlobal)
	bckTo := cluster.NewBck("test", cmn.ProviderAIS, cmn.NsGlobal)
	xactGlob := xactions.RenewLRU()
	tassert.Errorf(t, xactGlob != nil, "Xaction must be created")
	xactBck, err := xactions.RenewBckFastRename(nil, bckFrom, bckTo, "phase", nil)
	tassert.Errorf(t, err == nil && xactBck != nil, "Xaction must be created")

	xactions.AbortAll(cmn.XactTypeGlobal)

	tassert.Errorf(t, xactGlob != nil && xactGlob.Aborted(),
		"AbortAllGlobal: expected global xaction to be aborted")
	tassert.Errorf(t, xactBck != nil && !xactBck.Aborted(),
		"AbortAllGlobal: expected bucket xaction to be running")
	xactions.AbortAll()
}

func TestXactionAbortBuckets(t *testing.T) {
	xactions := newRegistry()
	bckFrom := cluster.NewBck("test", cmn.ProviderAIS, cmn.NsGlobal)
	bckTo := cluster.NewBck("test", cmn.ProviderAIS, cmn.NsGlobal)

	xactGlob := xactions.RenewLRU()
	tassert.Errorf(t, xactGlob != nil, "Xaction must be created")
	xactBck, err := xactions.RenewBckFastRename(nil, bckFrom, bckTo, "phase", nil)
	tassert.Errorf(t, err == nil && xactBck != nil, "Xaction must be created")

	xactions.AbortAllBuckets(bckFrom)

	tassert.Errorf(t, xactGlob != nil && !xactGlob.Aborted(),
		"AbortAllGlobal: expected global xaction to be running")
	tassert.Errorf(t, xactBck != nil && xactBck.Aborted(),
		"AbortAllGlobal: expected bucket xaction to be aborted")
	xactions.AbortAll()
}
