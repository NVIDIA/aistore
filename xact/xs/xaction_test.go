// Package xs_test contains xs unit test.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xs_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/space"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xact/xs"
)

func init() {
	config := cmn.GCO.BeginUpdate()
	config.Log.Level = "3"
	config.ConfigDir = "/tmp/ais-tests"
	config.Timeout.CplaneOperation = cos.Duration(2 * time.Second)
	config.Timeout.MaxKeepalive = cos.Duration(4 * time.Second)
	config.Timeout.MaxHostBusy = cos.Duration(20 * time.Second)
	cmn.GCO.CommitUpdate(config)

	xreg.Init()
	xs.Xreg(false)
}

// Smoke tests for xactions
func TestXactionRenewLRU(t *testing.T) {
	var (
		num    = 10
		xactCh = make(chan xreg.RenewRes, num)
		wg     = &sync.WaitGroup{}
	)
	xreg.TestReset()

	xreg.RegNonBckXact(&space.TestFactory{})
	defer xreg.AbortAll(nil)
	cos.InitShortID(0)

	wg.Add(num)
	for i := 0; i < num; i++ {
		go func() {
			xactCh <- xreg.RenewLRU(cos.GenUUID())
			wg.Done()
		}()
	}
	wg.Wait()
	close(xactCh)

	newCnt := 0
	for rns := range xactCh {
		if !rns.IsRunning() {
			newCnt++
		}
	}
	tassert.Errorf(t, newCnt == 1, "expected just one LRU xaction to be created, got %d", newCnt)
}

func TestXactionRenewPrefetch(t *testing.T) {
	var (
		evArgs = &apc.ListRange{}
		bmd    = mock.NewBaseBownerMock()
		bck    = meta.NewBck(
			"test", apc.GCP, cmn.NsGlobal,
			&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumXXHash}},
		)
		tMock = mock.NewTarget(bmd)
	)
	core.T = tMock
	xreg.TestReset()
	bmd.Add(bck)

	xreg.RegBckXact(&xs.TestXFactory{})
	defer xreg.AbortAll(nil)
	cos.InitShortID(0)

	ch := make(chan xreg.RenewRes, 10)
	wg := &sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			ch <- xreg.RenewPrefetch(cos.GenUUID(), bck, evArgs)
		}()
	}

	wg.Wait()
	close(ch)

	res := make(map[core.Xact]struct{}, 10)
	for rns := range ch {
		if xctn := rns.Entry.Get(); xctn != nil {
			res[xctn] = struct{}{}
		}
	}

	tassert.Errorf(t, len(res) > 0, "expected some evictDelete xactions to be created, got %d", len(res))
}

func TestXactionAbortAll(t *testing.T) {
	var (
		bmd     = mock.NewBaseBownerMock()
		bckFrom = meta.NewBck("test", apc.AIS, cmn.NsGlobal)
		bckTo   = meta.NewBck("test", apc.AIS, cmn.NsGlobal)
		tMock   = mock.NewTarget(bmd)
	)
	core.T = tMock
	xreg.TestReset()
	bmd.Add(bckFrom)
	bmd.Add(bckTo)

	xreg.RegNonBckXact(&space.TestFactory{})
	xreg.RegBckXact(&xs.TestBmvFactory{})
	cos.InitShortID(0)

	rnsLRU := xreg.RenewLRU(cos.GenUUID())
	tassert.Errorf(t, !rnsLRU.IsRunning(), "new LRU must be created")
	rnsRen := xreg.RenewBckRename(bckFrom, bckTo, cos.GenUUID(), 123, "phase")
	xactBck := rnsRen.Entry.Get()
	tassert.Errorf(t, rnsRen.Err == nil && xactBck != nil, "Xaction must be created")

	xreg.AbortAll(errors.New("test-abort-all"))

	tassert.Errorf(t, rnsLRU.Entry.Get().IsAborted(), "AbortAllGlobal: expected global xaction to be aborted")
	tassert.Errorf(t, xactBck.IsAborted(), "AbortAllGlobal: expected bucket xaction to be aborted")
}

func TestXactionAbortAllGlobal(t *testing.T) {
	var (
		bmd     = mock.NewBaseBownerMock()
		bckFrom = meta.NewBck("test", apc.AIS, cmn.NsGlobal)
		bckTo   = meta.NewBck("test", apc.AIS, cmn.NsGlobal)
		tMock   = mock.NewTarget(bmd)
	)
	core.T = tMock
	xreg.TestReset()

	defer xreg.AbortAll(errors.New("test-abort-global"))

	bmd.Add(bckFrom)
	bmd.Add(bckTo)

	xreg.RegNonBckXact(&space.TestFactory{})
	xreg.RegBckXact(&xs.TestBmvFactory{})
	cos.InitShortID(0)

	rnsLRU := xreg.RenewLRU(cos.GenUUID())
	tassert.Errorf(t, !rnsLRU.IsRunning(), "new LRU must be created")
	rnsRen := xreg.RenewBckRename(bckFrom, bckTo, cos.GenUUID(), 123, "phase")
	xactBck := rnsRen.Entry.Get()
	tassert.Errorf(t, rnsRen.Err == nil && xactBck != nil, "Xaction must be created")

	xreg.AbortAll(errors.New("test-abort-g"), xact.ScopeG, xact.ScopeGB)

	tassert.Errorf(t, rnsLRU.Entry.Get().IsAborted(), "AbortAllGlobal: expected global xaction to be aborted")
	tassert.Errorf(t, !xactBck.IsAborted(), "AbortAllGlobal: expected bucket xaction to be running: %s", xactBck)
}

func TestXactionAbortBuckets(t *testing.T) {
	var (
		bmd     = mock.NewBaseBownerMock()
		bckFrom = meta.NewBck("test", apc.AIS, cmn.NsGlobal)
		bckTo   = meta.NewBck("test", apc.AIS, cmn.NsGlobal)
		tMock   = mock.NewTarget(bmd)
	)
	core.T = tMock
	xreg.TestReset()

	defer xreg.AbortAll(errors.New("abort-buckets"))

	bmd.Add(bckFrom)
	bmd.Add(bckTo)

	xreg.RegNonBckXact(&space.TestFactory{})
	xreg.RegBckXact(&xs.TestBmvFactory{})
	cos.InitShortID(0)

	rnsLRU := xreg.RenewLRU(cos.GenUUID())
	tassert.Errorf(t, !rnsLRU.IsRunning(), "new LRU must be created")
	rns := xreg.RenewBckRename(bckFrom, bckTo, cos.GenUUID(), 123, "phase")
	xactBck := rns.Entry.Get()
	tassert.Errorf(t, rns.Err == nil && xactBck != nil, "Xaction must be created")

	xreg.AbortAllBuckets(nil, bckFrom)

	tassert.Errorf(t, !rnsLRU.Entry.Get().IsAborted(), "AbortAllGlobal: expected global xaction to keep running")
	tassert.Errorf(t, xactBck.IsAborted(), "AbortAllGlobal: expected bucket xaction to be aborted")
}

// TODO: extend this to include all cases of the Query
func TestXactionQueryFinished(t *testing.T) {
	type testConfig struct {
		bckNil           bool
		kindNil          bool
		showActive       bool
		expectedStatsLen int
	}
	var (
		bmd   = mock.NewBaseBownerMock()
		bck1  = meta.NewBck("test1", apc.AIS, cmn.NsGlobal)
		bck2  = meta.NewBck("test2", apc.AIS, cmn.NsGlobal)
		bck3  = meta.NewBck("test3", apc.GCP, cmn.NsGlobal)
		tMock = mock.NewTarget(bmd)
	)
	core.T = tMock
	xreg.TestReset()

	defer xreg.AbortAll(nil)

	bmd.Add(bck1)
	bmd.Add(bck2)
	bmd.Add(bck3)

	xreg.RegBckXact(&xs.TestXFactory{})
	xreg.RegBckXact(&xs.TestBmvFactory{})
	cos.InitShortID(0)

	rns1 := xreg.RenewBckRename(bck1, bck1, cos.GenUUID(), 123, "phase")
	tassert.Errorf(t, rns1.Err == nil && rns1.Entry.Get() != nil, "Xaction must be created")
	rns2 := xreg.RenewBckRename(bck2, bck2, cos.GenUUID(), 123, "phase")
	tassert.Errorf(t, rns2.Err == nil && rns2.Entry.Get() != nil, "Xaction must be created %v", rns2.Err)
	rns1.Entry.Get().Finish()

	rns1 = xreg.RenewBckRename(bck1, bck1, cos.GenUUID(), 123, "phase")
	tassert.Errorf(t, rns1.Err == nil && rns1.Entry.Get() != nil, "Xaction must be created")
	rns3 := xreg.RenewPrefetch(cos.GenUUID(), bck3, &apc.ListRange{})
	tassert.Errorf(t, rns3.Entry.Get() != nil, "Xaction must be created %v", rns3.Err)

	xactBck1 := rns1.Entry.Get()

	scenarioName := func(tc testConfig) string {
		name := ""
		if tc.bckNil {
			name += "bck:empty"
		} else {
			name += "bck:set"
		}
		if tc.kindNil {
			name += "/kind:empty"
		} else {
			name += "/kind:set"
		}
		if tc.showActive {
			name += "/state:running"
		} else {
			name += "/state:finished"
		}
		return name
	}

	f := func(t *testing.T, tc testConfig) {
		t.Run(scenarioName(tc), func(t *testing.T) {
			query := xreg.Flt{}
			if !tc.bckNil {
				query.Bck = bck1
			}
			if !tc.kindNil {
				query.Kind = xactBck1.Kind()
			}
			query.OnlyRunning = &tc.showActive
			stats, err := xreg.GetSnap(query)
			tassert.Errorf(t, err == nil, "Error fetching Xact Stats %v for query %v", err, query)
			tassert.Errorf(t, len(stats) == tc.expectedStatsLen, "Length of result: %d != %d", len(stats), tc.expectedStatsLen)
		})
	}
	tests := []testConfig{
		{bckNil: true, kindNil: true, showActive: false, expectedStatsLen: 1},
		{bckNil: true, kindNil: false, showActive: false, expectedStatsLen: 1},
		{bckNil: false, kindNil: true, showActive: false, expectedStatsLen: 1},
		{bckNil: false, kindNil: false, showActive: false, expectedStatsLen: 1},
	}
	for _, test := range tests {
		f(t, test)
	}
}

func TestBeid(t *testing.T) {
	const div = uint64(100 * time.Millisecond)
	num := 100
	if testing.Short() {
		num = 10
	}
	now := time.Now()
	xreg.PrimeTime.Store(now.UnixNano())
	xreg.MyTime.Store(now.Add(time.Second).UnixNano())

	var (
		ids  = make(map[string]struct{}, num)
		tags = []string{"tag1", "tag2"}
		cnt  int
	)
	for i := 0; i < num; i++ {
		beid, _, _ := xreg.GenBEID(div, tags[i%2])
		if _, ok := ids[beid]; ok {
			t.Fatalf("%d: %s duplicated", i, beid)
		}
		ids[beid] = struct{}{}

		time.Sleep(time.Millisecond)
		id, _, _ := xreg.GenBEID(div, tags[i%2])
		if beid != id {
			cnt++
		}

		time.Sleep(time.Duration(div))
	}
	if cnt > 0 {
		fmt.Printf("Warning: failed to reproduce %d time%s out of %d\n", cnt, cos.Plural(cnt), num)
	}
}
