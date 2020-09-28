// Package registry provides core functionality for the AIStore extended actions registry.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package registry

import (
	"fmt"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/lru"
	"github.com/NVIDIA/aistore/tutils/tassert"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/runners"
)

// Smoke tests for xactions
func TestXactionRenewLRU(t *testing.T) {
	var (
		num      = 10
		xactions = newRegistry()
		xactCh   = make(chan *lru.Xaction, num)
		wg       = &sync.WaitGroup{}
	)
	defer xactions.AbortAll()
	cmn.InitShortID(0)
	wg.Add(num)
	for i := 0; i < num; i++ {
		go func() {
			xactCh <- xactions.RenewLRU("")
			wg.Done()
		}()
	}
	wg.Wait()
	close(xactCh)

	notNilCount := 0
	for xact := range xactCh {
		if xact != nil {
			notNilCount++
		}
	}

	tassert.Errorf(t, notNilCount == 1, "expected just one LRU xaction to be created, got %d", notNilCount)
}

func TestXactionRenewEvictDelete(t *testing.T) {
	var (
		xactions = newRegistry()
		evArgs   = &runners.DeletePrefetchArgs{}

		bmd     = cluster.NewBaseBownerMock()
		bckFrom = cluster.NewBck(
			"test", cmn.ProviderAIS, cmn.NsGlobal,
			&cmn.BucketProps{Cksum: cmn.CksumConf{Type: cmn.ChecksumXXHash}},
		)
		tMock = cluster.NewTargetMock(bmd)
	)
	bmd.Add(bckFrom)

	defer xactions.AbortAll()

	ch := make(chan *runners.EvictDelete, 10)
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

	res := make(map[*runners.EvictDelete]struct{}, 10)
	for xact := range ch {
		if xact != nil {
			res[xact] = struct{}{}
		}
	}

	tassert.Errorf(t, len(res) > 0, "expected some EvictDelete xactions to be created, got %d", len(res))
}

func TestXactionAbortAll(t *testing.T) {
	var (
		xactions = newRegistry()

		bmd     = cluster.NewBaseBownerMock()
		bckFrom = cluster.NewBck("test", cmn.ProviderAIS, cmn.NsGlobal)
		bckTo   = cluster.NewBck("test", cmn.ProviderAIS, cmn.NsGlobal)
		tMock   = cluster.NewTargetMock(bmd)
	)
	bmd.Add(bckFrom)
	bmd.Add(bckTo)

	xactGlob := xactions.RenewLRU("")
	tassert.Errorf(t, xactGlob != nil, "Xaction must be created")
	xactBck, err := xactions.RenewBckFastRename(tMock, "uuid", 123, bckFrom, bckTo, "phase")
	tassert.Errorf(t, err == nil && xactBck != nil, "Xaction must be created")

	xactions.AbortAll()

	tassert.Errorf(t, xactGlob != nil && xactGlob.Aborted(),
		"AbortAllGlobal: expected global xaction to be aborted")
	tassert.Errorf(t, xactBck != nil && xactBck.Aborted(),
		"AbortAllGlobal: expected bucket xaction to be aborted")
}

func TestXactionAbortAllGlobal(t *testing.T) {
	var (
		xactions = newRegistry()

		bmd     = cluster.NewBaseBownerMock()
		bckFrom = cluster.NewBck("test", cmn.ProviderAIS, cmn.NsGlobal)
		bckTo   = cluster.NewBck("test", cmn.ProviderAIS, cmn.NsGlobal)
		tMock   = cluster.NewTargetMock(bmd)
	)
	bmd.Add(bckFrom)
	bmd.Add(bckTo)

	xactGlob := xactions.RenewLRU("")
	tassert.Errorf(t, xactGlob != nil, "Xaction must be created")
	xactBck, err := xactions.RenewBckFastRename(tMock, "uuid", 123, bckFrom, bckTo, "phase")
	tassert.Errorf(t, err == nil && xactBck != nil, "Xaction must be created")

	xactions.AbortAll(xaction.XactTypeGlobal)

	tassert.Errorf(t, xactGlob != nil && xactGlob.Aborted(),
		"AbortAllGlobal: expected global xaction to be aborted")
	tassert.Errorf(t, xactBck != nil && !xactBck.Aborted(),
		"AbortAllGlobal: expected bucket xaction to be running")
	xactions.AbortAll()
}

func TestXactionAbortBuckets(t *testing.T) {
	var (
		xactions = newRegistry()
		bmd      = cluster.NewBaseBownerMock()
		bckFrom  = cluster.NewBck("test", cmn.ProviderAIS, cmn.NsGlobal)
		bckTo    = cluster.NewBck("test", cmn.ProviderAIS, cmn.NsGlobal)
		tMock    = cluster.NewTargetMock(bmd)
	)
	bmd.Add(bckFrom)
	bmd.Add(bckTo)

	xactGlob := xactions.RenewLRU("")
	tassert.Errorf(t, xactGlob != nil, "Xaction must be created")
	xactBck, err := xactions.RenewBckFastRename(tMock, "uuid", 123, bckFrom, bckTo, "phase")
	tassert.Errorf(t, err == nil && xactBck != nil, "Xaction must be created")

	xactions.AbortAllBuckets(bckFrom)

	tassert.Errorf(t, xactGlob != nil && !xactGlob.Aborted(),
		"AbortAllGlobal: expected global xaction to be running")
	tassert.Errorf(t, xactBck != nil && xactBck.Aborted(),
		"AbortAllGlobal: expected bucket xaction to be aborted")
	xactions.AbortAll()
}

// TODO: extend this to include all cases of the Query
func TestXactionQueryFinished(t *testing.T) {
	type testConfig struct {
		bckNil           bool
		kindNil          bool
		showActive       *bool
		expectedStatsLen int
	}
	var (
		xactions = newRegistry()
		bmd      = cluster.NewBaseBownerMock()
		bck1     = cluster.NewBck("test1", cmn.ProviderAIS, cmn.NsGlobal)
		bck2     = cluster.NewBck("test2", cmn.ProviderAIS, cmn.NsGlobal)
		tMock    = cluster.NewTargetMock(bmd)
	)
	bmd.Add(bck1)
	bmd.Add(bck2)

	xactBck1, err := xactions.RenewBckFastRename(tMock, "uuid", 123, bck1, bck1, "phase")
	tassert.Errorf(t, err == nil && xactBck1 != nil, "Xaction must be created")
	xactBck2, err := xactions.RenewBckFastRename(tMock, "uuid", 123, bck2, bck2, "phase")
	tassert.Errorf(t, err == nil && xactBck2 != nil, "Xaction must be created %v", err)
	xactBck1.Finish()
	xactBck1, err = xactions.RenewBckFastRename(tMock, "uuid", 123, bck1, bck1, "phase")
	tassert.Errorf(t, err == nil && xactBck1 != nil, "Xaction must be created")
	_, err = xactions.RenewEvictDelete(tMock, bck1, &runners.DeletePrefetchArgs{})
	tassert.Errorf(t, err == nil && xactBck2 != nil, "Xaction must be created %v", err)

	printStates := func(showActive *bool) string {
		s := ""
		if showActive == nil {
			s += "|RUNNING|FINISHED"
		} else {
			if *showActive {
				s += "|RUNNING"
			} else {
				s += "|FINISHED"
			}
		}
		return s
	}
	scenarioName := func(tc testConfig) string {
		name := ""
		if tc.bckNil {
			name += "bck:empty"
		} else {
			name += "bck:set"
		}
		if tc.kindNil {
			name += "/Kind:empty"
		} else {
			name += "/Kind:set"
		}
		name += fmt.Sprintf("/States:%s", printStates(tc.showActive))
		return name
	}

	f := func(t *testing.T, tc testConfig) {
		t.Run(scenarioName(tc), func(t *testing.T) {
			query := RegistryXactFilter{}
			if !tc.bckNil {
				query.Bck = bck1
			}
			if !tc.kindNil {
				query.Kind = xactBck1.Kind()
			}
			query.OnlyRunning = tc.showActive
			stats, err := xactions.GetStats(query)
			tassert.Errorf(t, err == nil, "Error fetching Xact Stats %v for query %v", err, query)
			tassert.Errorf(t, len(stats) == tc.expectedStatsLen, "Length of result: %d != %d", len(stats), tc.expectedStatsLen)
		})
	}
	tests := []testConfig{
		{bckNil: true, kindNil: true, showActive: api.Bool(false), expectedStatsLen: 1},
		{bckNil: true, kindNil: false, showActive: api.Bool(false), expectedStatsLen: 1},
		{bckNil: false, kindNil: true, showActive: api.Bool(false), expectedStatsLen: 1},
		{bckNil: false, kindNil: false, showActive: api.Bool(false), expectedStatsLen: 1},
	}
	for _, test := range tests {
		f(t, test)
	}
}
