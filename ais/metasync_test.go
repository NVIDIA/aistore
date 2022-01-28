// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/mock"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/memsys"
	jsoniter "github.com/json-iterator/go"
)

type (
	// syncf is the sync function this test uses to control what to do when a metasync call
	// is received, for example, accepts or rejects the request.
	syncf func(w http.ResponseWriter, r *http.Request, cnt int) (int, error)

	// metaSyncServer represents one test metaSyncServer object, proxy or target
	metaSyncServer struct {
		id      string
		isProxy bool
		sf      syncf
		failCnt []int
	}

	// transportData records information about metasync calls including called for which server, how many
	// times it is called.
	transportData struct {
		isProxy bool
		id      string
		cnt     int
	}

	// helper for sorting []transportData
	msgSortHelper []transportData
)

// serverTCPAddr takes a string in format of "http://ip:port" and returns its ip and port
func serverTCPAddr(u string) cluster.NetInfo {
	s := strings.TrimPrefix(u, "http://")
	addr, _ := net.ResolveTCPAddr("tcp", s)
	return *cluster.NewNetInfo("http", addr.IP.String(), strconv.Itoa(addr.Port))
}

// newPrimary returns a proxy runner after initializing the fields that are needed by this test
func newPrimary() *proxy {
	var (
		p       = &proxy{}
		tracker = mock.NewStatsTracker()
		smap    = newSmap()
	)

	p.owner.smap = newSmapOwner(cmn.GCO.Get())
	p.si = cluster.NewSnode("primary", cmn.Proxy, cluster.NetInfo{}, cluster.NetInfo{}, cluster.NetInfo{})

	smap.addProxy(p.si)
	smap.Primary = p.si
	p.owner.smap.put(smap)

	config := cmn.GCO.BeginUpdate()
	config.ConfigDir = "/tmp/ais-tests"
	config.Periodic.RetrySyncTime = cos.Duration(time.Millisecond * 100)
	config.Keepalive.Proxy.Name = "heartbeat"
	config.Keepalive.Proxy.Interval = cos.Duration(3 * time.Second)
	config.Timeout.CplaneOperation = cos.Duration(2 * time.Second)
	config.Timeout.MaxKeepalive = cos.Duration(4 * time.Second)
	config.Client.Timeout = cos.Duration(10 * time.Second)
	config.Client.TimeoutLong = cos.Duration(10 * time.Second)
	config.Cksum.Type = cos.ChecksumXXHash
	cmn.GCO.CommitUpdate(config)
	cmn.GCO.SetInitialGconfPath("/tmp/ais-tests/ais.config")

	p.client.data = &http.Client{}
	p.client.control = &http.Client{}
	p.keepalive = newProxyKeepalive(p, tracker, atomic.NewBool(true))

	o := newBMDOwnerPrx(config)
	o.put(newBucketMD())
	p.owner.bmd = o

	e := newEtlMDOwnerPrx(config)
	e.put(newEtlMD())
	p.owner.etl = e

	p.gmm = memsys.PageMM()
	return p
}

func newSecondary(name string) *proxy {
	p := &proxy{}
	p.si = cluster.NewSnode(name, cmn.Proxy, cluster.NetInfo{}, cluster.NetInfo{}, cluster.NetInfo{})
	p.owner.smap = newSmapOwner(cmn.GCO.Get())
	p.owner.smap.put(newSmap())
	p.client.data = &http.Client{}
	p.client.control = &http.Client{}

	config := cmn.GCO.BeginUpdate()
	config.Periodic.RetrySyncTime = cos.Duration(100 * time.Millisecond)
	config.Keepalive.Proxy.Name = "heartbeat"
	config.Keepalive.Proxy.Interval = cos.Duration(3 * time.Second)
	config.Timeout.CplaneOperation = cos.Duration(2 * time.Second)
	config.Timeout.MaxKeepalive = cos.Duration(4 * time.Second)
	config.Cksum.Type = cos.ChecksumXXHash
	cmn.GCO.CommitUpdate(config)

	o := newBMDOwnerPrx(cmn.GCO.Get())
	o.put(newBucketMD())
	p.owner.bmd = o
	return p
}

// newTransportServer creates a http test server to simulate a proxy or a target, it is used to test the
// transport of metasync, which is making sync calls, retry failed calls, etc. it doesn't involve the actual
// content of the meta data received.
// newTransportServer's http handler calls the sync function which decide how to respond to the sync call,
// counts number of times sync call received, sends result to the result channel on each sync (error or
// no error), completes the http request with the status returned by the sync function.
func newTransportServer(primary *proxy, s *metaSyncServer, ch chan<- transportData) *httptest.Server {
	cnt := 0
	// notes: needs to assign these from 's', otherwise 'f' captures what in 's' which changes from call to call
	isProxy := s.isProxy
	id := s.id
	sf := s.sf

	// entry point for metasyncer's sync call
	f := func(w http.ResponseWriter, r *http.Request) {
		cnt++
		status, err := sf(w, r, cnt)
		ch <- transportData{isProxy, id, cnt}
		if err == nil {
			return
		}
		http.Error(w, err.Error(), status)
	}

	// creates the test proxy/target server and add to primary proxy's smap
	ts := httptest.NewServer(http.HandlerFunc(f))
	addrInfo := serverTCPAddr(ts.URL)
	clone := primary.owner.smap.get().clone()
	if s.isProxy {
		clone.Pmap[id] = cluster.NewSnode(id, cmn.Proxy, addrInfo, addrInfo, addrInfo)
	} else {
		clone.Tmap[id] = cluster.NewSnode(id, cmn.Target, addrInfo, addrInfo, addrInfo)
	}
	clone.Version++
	primary.owner.smap.put(clone)

	return ts
}

func TestMetasyncDeepCopy(t *testing.T) {
	bmd := newBucketMD()
	bmd.add(cluster.NewBck("bucket1", cmn.ProviderAIS, cmn.NsGlobal), &cmn.BucketProps{
		Cksum: cmn.CksumConf{
			Type: cos.ChecksumXXHash,
		},
	})
	bmd.add(cluster.NewBck("bucket2", cmn.ProviderAIS, cmn.NsGlobal), &cmn.BucketProps{
		Cksum: cmn.CksumConf{
			Type: cos.ChecksumXXHash,
		},
	})
	bmd.add(cluster.NewBck("bucket3", cmn.ProviderAmazon, cmn.NsGlobal), &cmn.BucketProps{
		Cksum: cmn.CksumConf{
			Type: cos.ChecksumXXHash,
		},
	})
	bmd.add(cluster.NewBck("bucket4", cmn.ProviderAmazon, cmn.NsGlobal), &cmn.BucketProps{
		Cksum: cmn.CksumConf{
			Type: cos.ChecksumXXHash,
		},
	})

	clone := bmd.clone()
	s1 := string(cos.MustMarshal(bmd))
	s2 := string(cos.MustMarshal(clone))
	if s1 == "" || s2 == "" || s1 != s2 {
		t.Log(s1)
		t.Log(s2)
		t.Fatal("marshal(bucketmd) != marshal(clone(bucketmd))")
	}
}

// TestMetasyncTransport is the driver for metasync transport tests.
// for each test case, it creates a primary proxy, starts the metasync instance, run the test case,
// verifies the result, and stop the syncer.
func TestMetasyncTransport(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	tcs := []struct {
		name  string
		testf func(*testing.T, *proxy, *metasyncer) ([]transportData, []transportData)
	}{
		{"SyncOnce", syncOnce},
		{"SyncOnceWait", syncOnceWait},
		{"SyncOnceNoWait", syncOnceNoWait},
		{"Retry", retry},
		{"MultipleSync", multipleSync},
		{"Refused", refused},
	}

	for _, tc := range tcs {
		primary := newPrimary()
		syncer := testSyncer(primary)

		var wg sync.WaitGroup
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			syncer.Run()
		}(&wg)

		t.Run(tc.name, func(t *testing.T) {
			exp, act := tc.testf(t, primary, syncer)
			if !reflect.DeepEqual(exp, act) {
				t.Fatalf("exp = %+v, act = %+v", exp, act)
			}
		})

		syncer.Stop(nil)
		wg.Wait()
	}
}

// collectResult reads N sync call results from the channel, sort the results and returns.
// sorting is to make result checking easier as sync calls to different servers run in paraller so
// the calls are received in random order.
func collectResult(n int, ch <-chan transportData) []transportData {
	msgs := make([]transportData, n)
	for i := 0; i < n; i++ {
		msgs[i] = <-ch
	}

	sort.Sort(msgSortHelper(msgs))
	return msgs
}

// alwaysOk accepts the sync call
func alwaysOk(http.ResponseWriter, *http.Request, int) (int, error) { return 0, nil }

// deletedOk accepts the sync call after a short wait
func delayedOk(http.ResponseWriter, *http.Request, int) (int, error) {
	time.Sleep(time.Second)
	return 0, nil
}

// failFirst rejects the first sync call, accept all other calls
func failFirst(_ http.ResponseWriter, _ *http.Request, cnt int) (int, error) {
	if cnt == 1 {
		return http.StatusForbidden, fmt.Errorf("fail first call")
	}
	return 0, nil
}

// syncOnce checks a mixed number of proxy and targets accept one sync call
func syncOnce(_ *testing.T, primary *proxy, syncer *metasyncer) ([]transportData, []transportData) {
	var (
		servers = []metaSyncServer{
			{"p1", true, alwaysOk, nil},
			{"p2", true, alwaysOk, nil},
			{"t1", false, alwaysOk, nil},
			{"t2", false, alwaysOk, nil},
		}
		ch = make(chan transportData, len(servers))
	)

	for _, v := range servers {
		s := newTransportServer(primary, &v, ch)
		defer s.Close()
	}

	smap := primary.owner.smap.get()
	msg := primary.newAmsgStr("", nil)
	wg := syncer.sync(revsPair{smap, msg})
	wg.Wait()
	return []transportData{
		{true, "p1", 1},
		{true, "p2", 1},
		{false, "t1", 1},
		{false, "t2", 1},
	}, collectResult(len(servers), ch)
}

// syncOnceWait checks sync(wait = true) doesn't return before all servers receive the call
func syncOnceWait(t *testing.T, primary *proxy, syncer *metasyncer) ([]transportData, []transportData) {
	var (
		servers = []metaSyncServer{
			{"p1", true, delayedOk, nil},
			{"t1", false, alwaysOk, nil},
		}
		ch = make(chan transportData, len(servers))
	)

	for _, v := range servers {
		s := newTransportServer(primary, &v, ch)
		defer s.Close()
	}

	smap := primary.owner.smap.get()
	msg := primary.newAmsgStr("", nil)
	wg := syncer.sync(revsPair{smap, msg})
	wg.Wait()
	if len(ch) != len(servers) {
		t.Fatalf("sync call wait returned before sync is completed")
	}

	return []transportData{
		{true, "p1", 1},
		{false, "t1", 1},
	}, collectResult(len(servers), ch)
}

// syncOnceNoWait checks sync(wait = false) returns before all servers receive the call
func syncOnceNoWait(t *testing.T, primary *proxy, syncer *metasyncer) ([]transportData, []transportData) {
	var (
		servers = []metaSyncServer{
			{"p1", true, delayedOk, nil},
			{"t1", false, alwaysOk, nil},
		}
		ch = make(chan transportData, len(servers))
	)

	for _, v := range servers {
		s := newTransportServer(primary, &v, ch)
		defer s.Close()
	}

	smap := primary.owner.smap.get()
	msg := primary.newAmsgStr("", nil)
	syncer.sync(revsPair{smap, msg})
	if len(ch) == len(servers) {
		t.Fatalf("sync call no wait returned after sync is completed")
	}

	return []transportData{
		{true, "p1", 1},
		{false, "t1", 1},
	}, collectResult(len(servers), ch)
}

// retry checks a failed sync call is retried
func retry(_ *testing.T, primary *proxy, syncer *metasyncer) ([]transportData, []transportData) {
	var (
		servers = []metaSyncServer{
			{"p1", true, failFirst, nil},
			{"p2", true, alwaysOk, nil},
			{"t1", false, failFirst, nil},
		}
		ch = make(chan transportData, len(servers)+2)
	)

	for _, v := range servers {
		s := newTransportServer(primary, &v, ch)
		defer s.Close()
	}

	smap := primary.owner.smap.get()
	msg := primary.newAmsgStr("", nil)
	wg := syncer.sync(revsPair{smap, msg})
	wg.Wait()
	return []transportData{
		{true, "p1", 1},
		{true, "p1", 2},
		{true, "p2", 1},
		{false, "t1", 1},
		{false, "t1", 2},
	}, collectResult(len(servers)+2, ch)
}

// multipleSync checks a mixed number of proxy and targets accept multiple sync calls
func multipleSync(_ *testing.T, primary *proxy, syncer *metasyncer) ([]transportData, []transportData) {
	var (
		servers = []metaSyncServer{
			{"p1", true, alwaysOk, nil},
			{"p2", true, alwaysOk, nil},
			{"t1", false, alwaysOk, nil},
			{"t2", false, alwaysOk, nil},
		}
		ch = make(chan transportData, len(servers)*3)
	)

	for _, v := range servers {
		s := newTransportServer(primary, &v, ch)
		defer s.Close()
	}

	smap := primary.owner.smap.get()
	msg := primary.newAmsgStr("", nil)
	syncer.sync(revsPair{smap, msg}).Wait()

	ctx := &smapModifier{
		pre: func(_ *smapModifier, clone *smapX) error {
			clone.Version++
			return nil
		},
		final: func(_ *smapModifier, clone *smapX) {
			msg := primary.newAmsgStr("", nil)
			syncer.sync(revsPair{clone, msg})
		},
	}
	primary.owner.smap.modify(ctx)

	ctx = &smapModifier{
		pre: func(_ *smapModifier, clone *smapX) error {
			clone.Version++
			return nil
		},
		final: func(_ *smapModifier, clone *smapX) {
			msg := primary.newAmsgStr("", nil)
			syncer.sync(revsPair{clone, msg}).Wait()
		},
	}
	primary.owner.smap.modify(ctx)

	return []transportData{
		{true, "p1", 1},
		{true, "p1", 2},
		{true, "p1", 3},
		{true, "p2", 1},
		{true, "p2", 2},
		{true, "p2", 3},
		{false, "t1", 1},
		{false, "t1", 2},
		{false, "t1", 3},
		{false, "t2", 1},
		{false, "t2", 2},
		{false, "t2", 3},
	}, collectResult(len(servers)*3, ch)
}

// refused tests the connection-refused scenario
// it has two test cases: one with a short delay to let metasyncer handle it immediately,
// the other with a longer delay so that metasyncer times out
// retrying connection-refused errors and falls back to the retry-pending "route"
func refused(t *testing.T, primary *proxy, syncer *metasyncer) ([]transportData, []transportData) {
	var (
		ch       = make(chan transportData, 2) // NOTE: Use 2 to avoid unbuffered channel, http handler can return.
		id       = "p"
		addrInfo = *cluster.NewNetInfo(
			httpProto,
			"127.0.0.1",
			"53538", // the lucky port
		)
	)

	// handler for /v1/metasync
	http.HandleFunc(cmn.URLPathMetasync.S, func(w http.ResponseWriter, r *http.Request) {
		ch <- transportData{true, id, 1}
	})

	clone := primary.owner.smap.get().clone()
	clone.Pmap[id] = cluster.NewSnode(id, cmn.Proxy, addrInfo, addrInfo, addrInfo)
	clone.Version++
	primary.owner.smap.put(clone)

	// function shared between the two cases: start proxy, wait for a sync call
	f := func() {
		timer := time.NewTimer(time.Minute)
		defer timer.Stop()

		wg := &sync.WaitGroup{}
		s := &http.Server{Addr: addrInfo.String()}

		wg.Add(1)
		go func() {
			defer wg.Done()
			s.ListenAndServe()
		}()

		select {
		case <-timer.C:
			t.Log("timed out")
		case <-ch:
		}

		s.Close()
		wg.Wait()
	}

	// testcase #1: short delay
	smap := primary.owner.smap.get()
	msg := primary.newAmsgStr("", nil)
	syncer.sync(revsPair{smap, msg})
	time.Sleep(time.Millisecond)
	// sync will return even though the sync actually failed, and there is no error return
	f()

	// testcase #2: long delay
	ctx := &smapModifier{
		pre: func(_ *smapModifier, clone *smapX) error {
			clone.Version++
			return nil
		},
		final: func(_ *smapModifier, clone *smapX) {
			msg := primary.newAmsgStr("", nil)
			syncer.sync(revsPair{clone, msg})
		},
	}
	primary.owner.smap.modify(ctx)

	time.Sleep(2 * time.Second)
	f()

	// only cares if the sync call comes, no need to verify the id and cnt as we are the one
	// filling those in above
	exp := []transportData{{true, id, 1}}
	return exp, exp
}

// TestMetasyncData is the driver for metasync data tests.
func TestMetasyncData(t *testing.T) {
	// data stores the data comes from the http sync call and an error
	type data struct {
		payload msPayload
		err     error
	}

	// newServer simulates a proxy or a target for metasync's data tests
	newServer := func(primary *proxy, s *metaSyncServer, ch chan<- data) *httptest.Server {
		cnt := 0
		id := s.id
		failCnt := s.failCnt

		// entry point for metasyncer's sync call
		f := func(w http.ResponseWriter, r *http.Request) {
			cnt++

			for _, v := range failCnt {
				if v == cnt {
					http.Error(w, "retry", http.StatusUnavailableForLegalReasons)
					return
				}
			}

			d := make(msPayload)
			err := d.unmarshal(r.Body, "")
			ch <- data{d, err}
		}

		// creates the test proxy/target server and add to primary proxy's smap
		ts := httptest.NewServer(http.HandlerFunc(f))
		addrInfo := serverTCPAddr(ts.URL)
		clone := primary.owner.smap.get().clone()
		if s.isProxy {
			clone.Pmap[id] = cluster.NewSnode(id, cmn.Proxy, addrInfo, addrInfo, addrInfo)
		} else {
			clone.Tmap[id] = cluster.NewSnode(id, cmn.Target, addrInfo, addrInfo, addrInfo)
		}
		clone.Version++
		primary.owner.smap.put(clone)

		return ts
	}

	match := func(t *testing.T, exp msPayload, ch <-chan data, cnt int) {
		fail := func(t *testing.T, exp, act msPayload) {
			t.Fatalf("Mismatch: exp = %+v, act = %+v", exp, act)
		}

		for i := 0; i < cnt; i++ {
			act := (<-ch).payload
			for k, e := range act {
				a, ok := exp[k]
				if !ok {
					fail(t, exp, act)
				}

				if !bytes.Equal(e, a) {
					fail(t, exp, act)
				}
			}
		}
	}

	var (
		exp      = make(msPayload)
		expRetry = make(msPayload)
		primary  = newPrimary()
		syncer   = testSyncer(primary)
		ch       = make(chan data, 5)
		bmd      = newBucketMD()
	)

	emptyAisMsg, err := jsoniter.Marshal(aisMsg{})
	if err != nil {
		t.Fatal("Failed to marshal empty cmn.ActionMsg, err =", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		syncer.Run()
	}(&wg)

	proxy := newServer(primary, &metaSyncServer{"proxy", true, nil, []int{3, 4, 5}}, ch)
	defer proxy.Close()

	target := newServer(primary, &metaSyncServer{"target", false, nil, []int{2}}, ch)
	defer target.Close()

	// sync smap
	smap := primary.owner.smap.get()
	smapBody := smap.marshal()

	exp[revsSmapTag] = smapBody
	expRetry[revsSmapTag] = smapBody
	exp[revsSmapTag+revsActionTag] = emptyAisMsg
	expRetry[revsSmapTag+revsActionTag] = emptyAisMsg

	syncer.sync(revsPair{smap, &aisMsg{}})
	match(t, expRetry, ch, 1)

	// sync bucketmd, fail target and retry
	bmd.add(cluster.NewBck("bucket1", cmn.ProviderAIS, cmn.NsGlobal), &cmn.BucketProps{
		Cksum: cmn.CksumConf{
			Type: cos.ChecksumXXHash,
		},
	})
	bmd.add(cluster.NewBck("bucket2", cmn.ProviderAIS, cmn.NsGlobal), &cmn.BucketProps{
		Cksum: cmn.CksumConf{
			Type: cos.ChecksumXXHash,
		},
	})
	primary.owner.bmd.putPersist(bmd, nil)
	bmdBody := bmd.marshal()

	exp[revsBMDTag] = bmdBody
	expRetry[revsBMDTag] = bmdBody
	exp[revsBMDTag+revsActionTag] = emptyAisMsg
	expRetry[revsBMDTag+revsActionTag] = emptyAisMsg

	syncer.sync(revsPair{bmd, &aisMsg{}})
	match(t, exp, ch, 1)
	match(t, expRetry, ch, 1)

	// sync bucketmd, fail proxy, sync new bucketmd, expect proxy to receive the new bucketmd
	// after rejecting a few sync requests
	bmd = bmd.clone()
	bprops := &cmn.BucketProps{
		Cksum: cmn.CksumConf{Type: cos.ChecksumXXHash},
		LRU:   cmn.GCO.Get().LRU,
	}
	bmd.add(cluster.NewBck("bucket3", cmn.ProviderAIS, cmn.NsGlobal), bprops)
	primary.owner.bmd.putPersist(bmd, nil)
	bmdBody = bmd.marshal()

	exp[revsBMDTag] = bmdBody
	msg := primary.newAmsgStr("", bmd)
	syncer.sync(revsPair{bmd, msg})
}

// TestMetasyncMembership tests metasync's logic when accessing proxy's smap directly
func TestMetasyncMembership(t *testing.T) {
	{
		// pending server dropped without sync
		primary := newPrimary()
		syncer := testSyncer(primary)

		var wg sync.WaitGroup
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			syncer.Run()
		}(&wg)

		var cnt atomic.Int32
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cnt.Add(1)
			http.Error(w, "i don't know how to deal with you", http.StatusNotAcceptable)
		}))

		defer s.Close()

		id := "t"
		addrInfo := serverTCPAddr(s.URL)
		clone := primary.owner.smap.get().clone()
		clone.addTarget(cluster.NewSnode(id, cmn.Target, addrInfo, addrInfo, addrInfo))
		primary.owner.smap.put(clone)
		msg := primary.newAmsgStr("", nil)
		wg1 := syncer.sync(revsPair{clone, msg})
		wg1.Wait()
		time.Sleep(time.Millisecond * 300)

		clone = primary.owner.smap.get().clone()
		clone.delTarget(id)
		primary.owner.smap.put(clone)

		time.Sleep(time.Millisecond * 300)
		savedCnt := cnt.Load()
		time.Sleep(time.Millisecond * 300)
		if cnt.Load() != savedCnt {
			t.Fatal("Sync call didn't stop after traget is deleted")
		}

		syncer.Stop(nil)
		wg.Wait()
	}

	primary := newPrimary()
	syncer := testSyncer(primary)

	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		syncer.Run()
	}(&wg)

	ch := make(chan struct{}, 10)
	f := func(w http.ResponseWriter, r *http.Request) {
		ch <- struct{}{}
	}

	{
		// sync before smap sync (no previous sync saved in metasyncer)
		s1 := httptest.NewServer(http.HandlerFunc(f))
		defer s1.Close()

		id := "t1111"
		addrInfo := serverTCPAddr(s1.URL)
		di := cluster.NewSnode(id, cmn.Target, addrInfo, addrInfo, addrInfo)
		clone := primary.owner.smap.get().clone()
		clone.addTarget(di)
		primary.owner.smap.put(clone)
		bmd := primary.owner.bmd.get()
		msg := primary.newAmsgStr("", bmd)
		wg := syncer.sync(revsPair{bmd, msg})
		wg.Wait()
		<-ch

		// sync smap so metasyncer has a smap
		wg = syncer.sync(revsPair{clone, msg})
		wg.Wait()
		<-ch
	}

	{
		// add a new target but new smap is not synced
		// metasyncer picks up the new target directly from primary's smap
		// and metasyncer will also add the new target to pending to sync all previously synced data
		// that's why the extra channel read
		s2 := httptest.NewServer(http.HandlerFunc(f))
		defer s2.Close()

		id := "t22222"
		addrInfo := serverTCPAddr(s2.URL)
		di := cluster.NewSnode(id, cmn.Target, addrInfo, addrInfo, addrInfo)
		clone := primary.owner.smap.get().clone()
		clone.addTarget(di)
		primary.owner.smap.put(clone)

		bmd := primary.owner.bmd.get()
		msg := primary.newAmsgStr("", bmd)
		wg := syncer.sync(revsPair{bmd, msg})
		wg.Wait()
		<-ch // target 1
		<-ch // target 2
		if len(ch) != 0 {
			t.Fatal("Too many sync calls received")
		}

		syncer.Stop(nil)
		wg.Wait()
	}
}

// TestMetasyncReceive tests extracting received sync data.
func TestMetasyncReceive(t *testing.T) {
	{
		emptyAisMsg := func(a *aisMsg) {
			if a.Action != "" || a.Name != "" || a.Value != nil {
				t.Fatal("Expecting empty action message", a)
			}
		}

		nilSMap := func(m *smapX) {
			if m != nil {
				t.Fatal("Expecting nil Smap", m)
			}
		}

		matchSMap := func(a, b *smapX) {
			_, sameUUID, sameVersion, eq := a.Compare(&b.Smap)
			if !sameUUID || !sameVersion || !eq {
				t.Fatal("Smap mismatch", a.StringEx(), b.StringEx())
			}
		}

		primary := newPrimary()
		syncer := testSyncer(primary)

		var wg sync.WaitGroup
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			syncer.Run()
		}(&wg)

		chProxy := make(chan msPayload, 10)
		fProxy := func(w http.ResponseWriter, r *http.Request) {
			d := make(msPayload)
			err := d.unmarshal(r.Body, "")
			cos.AssertNoErr(err)
			chProxy <- d
		}

		// the only difference is the channel
		s := httptest.NewServer(http.HandlerFunc(fProxy))
		defer s.Close()
		addrInfo := serverTCPAddr(s.URL)
		clone := primary.owner.smap.get().clone()
		clone.addProxy(cluster.NewSnode("p1", cmn.Proxy, addrInfo, addrInfo, addrInfo))
		primary.owner.smap.put(clone)

		proxy1 := newSecondary("p1")

		// empty payload
		newSMap, msg, err := proxy1.extractSmap(make(msPayload), "")
		if newSMap != nil || msg != nil || err != nil {
			t.Fatal("Extract smap from empty payload returned data")
		}

		wg1 := syncer.sync(revsPair{primary.owner.smap.get(), &aisMsg{}})
		wg1.Wait()
		payload := <-chProxy

		newSMap, msg, err = proxy1.extractSmap(payload, "")
		tassert.CheckFatal(t, err)
		emptyAisMsg(msg)
		matchSMap(primary.owner.smap.get(), newSMap)
		proxy1.owner.smap.put(newSMap)

		// same version of smap received
		newSMap, msg, err = proxy1.extractSmap(payload, "")
		tassert.CheckFatal(t, err)
		emptyAisMsg(msg)
		nilSMap(newSMap)
	}
}

func testSyncer(p *proxy) (syncer *metasyncer) {
	syncer = newMetasyncer(p)
	return
}

///////////////////
// msgSortHelper //
///////////////////

func (m msgSortHelper) Len() int {
	return len(m)
}

func (m msgSortHelper) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func (m msgSortHelper) Less(i, j int) bool {
	if m[i].isProxy != m[j].isProxy {
		return m[i].isProxy
	}

	if m[i].id != m[j].id {
		return m[i].id < m[j].id
	}

	return m[i].cnt < m[j].cnt
}
