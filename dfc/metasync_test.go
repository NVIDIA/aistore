/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package dfc

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/json-iterator/go"
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
)

// helper for sorting []transportData
type msgSortHelper []transportData

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

// serverTCPAddr takes a string in format of "http://ip:port" and returns its ip and port
func serverTCPAddr(u string) *net.TCPAddr {
	s := strings.TrimPrefix(u, "http://")
	addr, _ := net.ResolveTCPAddr("tcp", s)
	return addr
}

// newPrimary returns a proxy runner after initializing the fields that are needed by this test
func newPrimary() *proxyrunner {
	p := proxyrunner{}
	p.smapowner = &smapowner{}
	p.si = newDaemonInfo("primary", httpProto, &net.TCPAddr{}, &net.TCPAddr{}, &net.TCPAddr{})
	smap := newSmap()
	smap.addProxy(p.si)
	smap.ProxySI = p.si
	p.smapowner.put(smap)

	p.httpclientLongTimeout = &http.Client{}
	ctx.config.Periodic.RetrySyncTime = time.Millisecond * 100
	ctx.config.KeepaliveTracker.Proxy.Name = "heartbeat"
	ctx.config.KeepaliveTracker.Proxy.IntervalStr = "as"
	p.keepalive = newProxyKeepaliveRunner(&p)

	p.bmdowner = &bmdowner{}
	p.bmdowner.put(newBucketMD())
	return &p
}

// newTransportServer creates a http test server to simulate a proxy or a target, it is used to test the
// transport of mets sync, which is making sync calls, retry failed calls, etc. it doesn't involve the actual
// content of the meta data received.
// newTransportServer's http handler calls the sync funtion which decide how to respond to the sync call,
// counts number of times sync call received, sends result to the result channel on each sync (error or
// no error), completes the http request with the status returned by the sync function.
func newTransportServer(primary *proxyrunner, s *metaSyncServer, ch chan<- transportData) *httptest.Server {
	cnt := 0
	// notes: needs to assgin these from 's', otherwise 'f' captures what in 's' which changes from call to call
	isProxy := s.isProxy
	id := s.id
	sf := s.sf

	// entry point for metasyncer's sync call
	f := func(w http.ResponseWriter, r *http.Request) {
		cnt++
		status, err := sf(w, r, cnt)
		ch <- transportData{isProxy, id, cnt}
		if err != nil {
			http.Error(w, err.Error(), status)
		}
	}

	// creates the test proxy/target server and add to primary proxy's smap
	ts := httptest.NewServer(http.HandlerFunc(f))
	addrInfo := serverTCPAddr(ts.URL)
	di := newDaemonInfo(id, httpProto, addrInfo, &net.TCPAddr{}, &net.TCPAddr{})
	clone := primary.smapowner.get().clone()
	if s.isProxy {
		clone.Pmap[id] = di
	} else {
		clone.Tmap[id] = di
	}
	clone.Version++
	primary.smapowner.put(clone)

	return ts
}

func TestMetaSyncDeepCopy(t *testing.T) {
	bucketmd := newBucketMD()
	bucketmd.add("bucket1", true, BucketProps{
		CloudProvider: api.ProviderDFC,
		NextTierURL:   "http://foo.com",
		CksumConf: cksumconfig{
			Checksum: ChecksumInherit,
		},
	})
	bucketmd.add("bucket2", true, BucketProps{
		CksumConf: cksumconfig{
			Checksum: ChecksumInherit,
		},
	})
	bucketmd.add("bucket3", false, BucketProps{
		CloudProvider: api.ProviderDFC,
		CksumConf: cksumconfig{
			Checksum: ChecksumInherit,
		},
	})
	bucketmd.add("bucket4", false, BucketProps{
		CksumConf: cksumconfig{
			Checksum: ChecksumInherit,
		},
	})

	clone := &bucketMD{}
	bucketmd.deepcopy(clone)

	var jsonCompat = jsoniter.ConfigCompatibleWithStandardLibrary
	b1, _ := jsonCompat.Marshal(bucketmd)
	s1 := string(b1)
	b2, _ := jsonCompat.Marshal(clone)
	s2 := string(b2)
	if s1 == "" || s2 == "" || s1 != s2 {
		t.Log(s1)
		t.Log(s2)
		t.Fatal("marshal(bucketmd) != marshal(clone(bucketmd))")
	}
}

// TestMetaSyncTransport is the driver for metasync transport tests.
// for each test case, it creates a primary proxy, starts the metasync instance, run the test case,
// verifies the result, and stop the syncer.
func TestMetaSyncTransport(t *testing.T) {
	tcs := []struct {
		name  string
		testf func(*testing.T, *proxyrunner, *metasyncer) ([]transportData, []transportData)
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
		syncer := newmetasyncer(primary)

		var wg sync.WaitGroup
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			syncer.run()
		}(&wg)

		t.Run(tc.name, func(t *testing.T) {
			exp, act := tc.testf(t, primary, syncer)
			if !reflect.DeepEqual(exp, act) {
				t.Fatalf("exp = %+v, act = %+v", exp, act)
			}
		})

		syncer.stop(nil)
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
func alwaysOk(w http.ResponseWriter, r *http.Request, cnt int) (int, error) {
	return 0, nil
}

// deletedOk accepts the sync call after a short wait
func delayedOk(w http.ResponseWriter, r *http.Request, cnt int) (int, error) {
	time.Sleep(time.Second)
	return 0, nil
}

// failFirst rejects the first sync call, accept all other calls
func failFirst(w http.ResponseWriter, r *http.Request, cnt int) (int, error) {
	if cnt == 1 {
		return http.StatusForbidden, fmt.Errorf("fail first call")
	}

	return 0, nil
}

// syncOnce checks a mixed number of proxy and targets accept one sync call
func syncOnce(t *testing.T, primary *proxyrunner, syncer *metasyncer) ([]transportData, []transportData) {
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

	syncer.sync(true, primary.smapowner.get(), "")
	return []transportData{
		{true, "p1", 1},
		{true, "p2", 1},
		{false, "t1", 1},
		{false, "t2", 1},
	}, collectResult(len(servers), ch)
}

// syncOnceWait checks sync(wait = true) doesn't return before all servers receive the call
func syncOnceWait(t *testing.T, primary *proxyrunner, syncer *metasyncer) ([]transportData, []transportData) {
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

	syncer.sync(true, primary.smapowner.get(), "")
	if len(ch) != len(servers) {
		t.Fatalf("sync call wait returned before sync is completed")
	}

	return []transportData{
		{true, "p1", 1},
		{false, "t1", 1},
	}, collectResult(len(servers), ch)
}

// syncOnceNoWait checks sync(wait = false) returns before all servers receive the call
func syncOnceNoWait(t *testing.T, primary *proxyrunner, syncer *metasyncer) ([]transportData, []transportData) {
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

	syncer.sync(false, primary.smapowner.get(), "")
	if len(ch) == len(servers) {
		t.Fatalf("sync call no wait returned after sync is completed")
	}

	return []transportData{
		{true, "p1", 1},
		{false, "t1", 1},
	}, collectResult(len(servers), ch)
}

// retry checks a failed sync call is retryed
func retry(t *testing.T, primary *proxyrunner, syncer *metasyncer) ([]transportData, []transportData) {
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

	syncer.sync(true, primary.smapowner.get(), "")
	return []transportData{
		{true, "p1", 1},
		{true, "p1", 2},
		{true, "p2", 1},
		{false, "t1", 1},
		{false, "t1", 2},
	}, collectResult(len(servers)+2, ch)
}

// multipleSync checks a mixed number of proxy and targets accept multiple sync calls
func multipleSync(t *testing.T, primary *proxyrunner, syncer *metasyncer) ([]transportData, []transportData) {
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

	syncer.sync(true, primary.smapowner.get(), "")
	syncer.sync(false, primary.smapowner.get(), "")
	syncer.sync(true, primary.smapowner.get(), "")
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
// it has two test cases: one wih a short delay to let metasyncer handle it immediately,
// the other with a longer delay so that metasyncer times out
// retrying connection-refused errors and falls back to the retry-pending "route"
func refused(t *testing.T, primary *proxyrunner, syncer *metasyncer) ([]transportData, []transportData) {
	var (
		ch       = make(chan transportData, 2) // note: use 2 to avoid unbuffered channel, http handler can return
		id       = "p"
		addrInfo = &net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 53538, // the lucky port
		}
		s = &http.Server{Addr: addrInfo.String()}
	)

	// handler for /v1/metasync
	http.HandleFunc(api.URLPath(api.Version, api.Metasync), func(w http.ResponseWriter, r *http.Request) {
		ch <- transportData{true, id, 1}
	})

	clone := primary.smapowner.get().clone()
	clone.Pmap[id] = newDaemonInfo(id, httpProto, addrInfo, &net.TCPAddr{}, &net.TCPAddr{})
	clone.Version++
	primary.smapowner.put(clone)

	// function shared between the two cases: start proxy, wait for a sync call
	f := func(n int) {
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			s.ListenAndServe()
		}()

		<-ch
		s.Close()
		wg.Wait()
	}

	// testcase #1: short delay
	syncer.sync(false, primary.smapowner.get(), "")
	time.Sleep(time.Millisecond)
	// sync will return even though the sync actually failed, and there is no error return
	f(1)

	// testcase #2: long delay
	clone = primary.smapowner.get().clone()
	clone.Version++
	primary.smapowner.put(clone)
	syncer.sync(false, primary.smapowner.get(), "")
	time.Sleep(time.Second * 2)
	f(2)

	// only cares if the sync call comes, no need to verify the id and cnt as we are the one
	// filling those in above
	exp := []transportData{{true, id, 1}}
	return exp, exp
}

// TestMetaSyncData is the driver for metasync data tests.
func TestMetaSyncData(t *testing.T) {
	// data stores the data comes from the http sync call and an error
	type data struct {
		payload map[string]string
		err     error
	}

	// newDataServer simulates a proxt or a target for metasync's data tests
	newServer := func(primary *proxyrunner, s *metaSyncServer, ch chan<- data) *httptest.Server {
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

			d := make(map[string]string)
			err := primary.readJSON(w, r, &d)
			ch <- data{d, err}
		}

		// creates the test proxy/target server and add to primary proxy's smap
		ts := httptest.NewServer(http.HandlerFunc(f))
		addrInfo := serverTCPAddr(ts.URL)
		di := newDaemonInfo(id, httpProto, addrInfo, &net.TCPAddr{}, &net.TCPAddr{})
		clone := primary.smapowner.get().clone()
		if s.isProxy {
			clone.Pmap[id] = di
		} else {
			clone.Tmap[id] = di
		}
		clone.Version++
		primary.smapowner.put(clone)

		return ts
	}

	match := func(t *testing.T, exp map[string]string, ch <-chan data, cnt int) {
		fail := func(t *testing.T, exp, act map[string]string) {
			t.Fatalf("Mismatch: exp = %+v, act = %+v", exp, act)
		}

		for i := 0; i < cnt; i++ {
			act := (<-ch).payload
			for k, e := range act {
				a, ok := exp[k]
				if !ok {
					fail(t, exp, act)
				}

				if strings.Compare(e, a) != 0 {
					fail(t, exp, act)
				}
			}
		}
	}

	var (
		exp            = make(map[string]string)
		expRetry       = make(map[string]string)
		primary        = newPrimary()
		syncer         = newmetasyncer(primary)
		ch             = make(chan data, 5)
		bucketmd       = newBucketMD()
		emptyActionMsg string
	)

	b, err := jsoniter.Marshal(api.ActionMsg{})
	if err != nil {
		t.Fatal("Failed to marshal empty api.ActionMsg, err =", err)
	}

	emptyActionMsg = string(b)

	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		syncer.run()
	}(&wg)

	proxy := newServer(primary, &metaSyncServer{"proxy", true, nil, []int{3, 4, 5}}, ch)
	defer proxy.Close()

	target := newServer(primary, &metaSyncServer{"target", false, nil, []int{2}}, ch)
	defer target.Close()

	// sync smap
	smap := primary.smapowner.get()
	b, err = smap.marshal()
	if err != nil {
		t.Fatal("Failed to marshal smap, err =", err)
	}

	exp[smaptag] = string(b)
	expRetry[smaptag] = string(b)
	exp[smaptag+actiontag] = string(emptyActionMsg)
	expRetry[smaptag+actiontag] = string(emptyActionMsg)

	syncer.sync(false, smap, "")
	match(t, expRetry, ch, 1)

	// sync bucketmd, fail target and retry
	bucketmd.add("bucket1", true, BucketProps{
		CloudProvider: api.ProviderDFC,
		CksumConf: cksumconfig{
			Checksum: ChecksumInherit,
		},
	})
	bucketmd.add("bucket2", true, BucketProps{
		CloudProvider: api.ProviderDFC,
		NextTierURL:   "http://localhost:8082",
		CksumConf: cksumconfig{
			Checksum: ChecksumInherit,
		},
	})
	b, err = bucketmd.marshal()
	if err != nil {
		t.Fatal("Failed to marshal bucketmd, err =", err)
	}

	exp[bucketmdtag] = string(b)
	expRetry[bucketmdtag] = string(b)
	exp[bucketmdtag+actiontag] = string(emptyActionMsg)
	expRetry[bucketmdtag+actiontag] = string(emptyActionMsg)

	syncer.sync(false, bucketmd, "")
	match(t, exp, ch, 1)
	match(t, expRetry, ch, 1)

	// sync bucketmd, fail proxy, sync new bucketmd, expect proxy to receive the new bucketmd
	// after rejecting a few sync requests
	bucketmd = bucketmd.clone()
	bucketmd.add("bucket3", true, *NewBucketProps())
	b, err = bucketmd.marshal()
	if err != nil {
		t.Fatal("Failed to marshal bucketmd, err =", err)
	}

	exp[bucketmdtag] = string(b)
	syncer.sync(false, bucketmd, "")

	return

	match(t, exp, ch, 2)

	bucketmd = bucketmd.clone()
	bucketmd.add("bucket4", true, BucketProps{
		CloudProvider: api.ProviderDFC,
		NextTierURL:   "http://foo.com",
		CksumConf: cksumconfig{
			Checksum: ChecksumInherit,
		},
	})
	b, err = bucketmd.marshal()
	if err != nil {
		t.Fatal("Failed to marshal bucketmd, err =", err)
	}

	bucketmdString := string(b)
	exp[bucketmdtag] = bucketmdString
	syncer.sync(false, bucketmd, "")
	match(t, exp, ch, 2)

	// sync smap
	delete(exp, bucketmdtag)
	delete(exp, bucketmdtag+actiontag)

	proxy = newServer(primary, &metaSyncServer{"another proxy", true, nil, nil}, ch)
	defer proxy.Close()

	b, err = primary.smapowner.get().marshal()
	if err != nil {
		t.Fatal("Failed to marshal smap, err =", err)
	}

	exp[smaptag] = string(b)
	exp[smaptag+actiontag] = string(emptyActionMsg)

	syncer.sync(false, primary.smapowner.get(), "")
	match(t, exp, ch, 3)

	// sync smap pair
	msgSMap := &api.ActionMsg{Action: "msg8"}
	b, err = jsoniter.Marshal(msgSMap)
	if err != nil {
		t.Fatal("Failed to marshal action message, err =", err)
	}

	exp[smaptag+actiontag] = string(b)
	clone := primary.smapowner.get().clone()
	clone.Version++
	primary.smapowner.put(clone)
	b, err = primary.smapowner.get().marshal()
	if err != nil {
		t.Fatal("Failed to marshal smap, err =", err)
	}
	exp[smaptag] = string(b)

	syncer.sync(false, primary.smapowner.get(), msgSMap)
	match(t, exp, ch, 2)

	// NOTE: will cause an error "duplicated - already sync-ed or pending" - expected
	msgBMD := &api.ActionMsg{Action: "msg-duplicated"}
	msgSMap = msgBMD
	delete(exp, bucketmdtag)
	delete(exp, bucketmdtag+actiontag)
	syncer.sync(true, primary.smapowner.get(), msgSMap, bucketmd, msgBMD)
	match(t, exp, ch, 1)

	syncer.stop(nil)
	wg.Wait()
}

// TestMetaSyncMembership tests metasync's logic when accessing proxy's smap directly
func TestMetaSyncMembership(t *testing.T) {
	{
		// pending server dropped without sync
		primary := newPrimary()
		syncer := newmetasyncer(primary)

		var wg sync.WaitGroup
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			syncer.run()
		}(&wg)

		var cnt int32
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&cnt, 1)
			http.Error(w, "i don't know how to deal with you", http.StatusNotAcceptable)
		}))

		defer s.Close()

		id := "t"
		addrInfo := serverTCPAddr(s.URL)
		clone := primary.smapowner.get().clone()
		clone.addTarget(newDaemonInfo(id, httpProto, addrInfo, &net.TCPAddr{}, &net.TCPAddr{}))
		primary.smapowner.put(clone)
		syncer.sync(true, clone, "")
		time.Sleep(time.Millisecond * 300)

		clone = primary.smapowner.get().clone()
		clone.delTarget(id)
		primary.smapowner.put(clone)

		time.Sleep(time.Millisecond * 300)
		cnt1 := atomic.LoadInt32(&cnt)
		time.Sleep(time.Millisecond * 300)
		if atomic.LoadInt32(&cnt) != cnt1 {
			t.Fatal("Sync call didn't stop after traget is deleted")
		}

		syncer.stop(nil)
		wg.Wait()
	}

	primary := newPrimary()
	syncer := newmetasyncer(primary)

	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		syncer.run()
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
		di := newDaemonInfo(id, httpProto, addrInfo, &net.TCPAddr{}, &net.TCPAddr{})
		clone := primary.smapowner.get().clone()
		clone.addTarget(di)
		primary.smapowner.put(clone)
		syncer.sync(true, primary.bmdowner.get(), "")
		<-ch

		// sync smap so metasyncer has a smap
		syncer.sync(true, clone, "")
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
		di := newDaemonInfo(id, httpProto, addrInfo, &net.TCPAddr{}, &net.TCPAddr{})
		clone := primary.smapowner.get().clone()
		clone.addTarget(di)
		primary.smapowner.put(clone)

		syncer.sync(true, primary.bmdowner.get(), "")
		<-ch // target 1
		<-ch // target 2
		if len(ch) != 0 {
			t.Fatal("Too many sync calls received")
		}

		syncer.stop(nil)
		wg.Wait()
	}
}

// TestMetaSyncReceive tests extracting received sync data.
func TestMetaSyncReceive(t *testing.T) {
	{
		noErr := func(s string) {
			if s != "" {
				t.Fatal("Unexpected error", s)
			}
		}

		hasErr := func(s string) {
			if s == "" {
				t.Fatal("Expecting error")
			}
		}

		emptyActionMsg := func(a *api.ActionMsg) {
			if a.Action != "" || a.Name != "" || a.Value != nil {
				t.Fatal("Expecting empty action message", a)
			}
		}

		matchActionMsg := func(a, b *api.ActionMsg) {
			if !reflect.DeepEqual(a, b) {
				t.Fatal("api.ActionMsg mismatch ", a, b)
			}
		}

		nilSMap := func(m *Smap) {
			if m != nil {
				t.Fatal("Expecting nil smap", m)
			}
		}

		matchSMap := func(a, b *Smap) {
			if !a.Equals(b) {
				t.Fatal("SMap mismatch", a, b)
			}
		}

		nilBMD := func(l *bucketMD) {
			if l != nil {
				t.Fatal("Expecting nil bucketmd", l)
			}
		}

		matchBMD := func(a, b *bucketMD) {
			if !reflect.DeepEqual(a, b) {
				t.Fatal("bucketmd mismatch", a, b)
			}
		}

		primary := newPrimary()
		syncer := newmetasyncer(primary)

		var wg sync.WaitGroup
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			syncer.run()
		}(&wg)

		chProxy := make(chan map[string]string, 10)
		fProxy := func(w http.ResponseWriter, r *http.Request) {
			d := make(map[string]string)
			primary.readJSON(w, r, &d) // ignore json error
			chProxy <- d
		}

		// the only difference is the channel
		chTarget := make(chan map[string]string, 10)
		fTarget := func(w http.ResponseWriter, r *http.Request) {
			d := make(map[string]string)
			primary.readJSON(w, r, &d) // ignore json error
			chTarget <- d
		}

		s := httptest.NewServer(http.HandlerFunc(fProxy))
		defer s.Close()
		addrInfo := serverTCPAddr(s.URL)
		clone := primary.smapowner.get().clone()
		clone.addProxy(newDaemonInfo("proxy1", httpProto, addrInfo, &net.TCPAddr{}, &net.TCPAddr{}))
		primary.smapowner.put(clone)

		proxy1 := proxyrunner{}
		proxy1.smapowner = &smapowner{}
		proxy1.smapowner.put(newSmap())
		proxy1.bmdowner = &bmdowner{}
		proxy1.bmdowner.put(newBucketMD())

		// empty payload
		newSMap, actMsg, errStr := proxy1.extractSmap(make(map[string]string))
		if newSMap != nil || actMsg != nil || errStr != "" {
			t.Fatal("Extract smap from empty payload returned data")
		}

		syncer.sync(true, primary.smapowner.get(), "")
		payload := <-chProxy

		newSMap, actMsg, errStr = proxy1.extractSmap(payload)
		noErr(errStr)
		emptyActionMsg(actMsg)
		matchSMap(primary.smapowner.get(), newSMap)
		proxy1.smapowner.put(newSMap)

		// same version of smap received
		newSMap, actMsg, errStr = proxy1.extractSmap(payload)
		noErr(errStr)
		emptyActionMsg(actMsg)
		nilSMap(newSMap)

		return

		// older version of smap received
		clone = proxy1.smapowner.get().clone()
		clone.Version++
		proxy1.smapowner.put(clone)
		_, _, errStr = proxy1.extractSmap(payload)
		hasErr(errStr)
		clone.Version--
		proxy1.smapowner.put(clone)

		var am api.ActionMsg
		y := &api.ActionMsg{Action: ""}
		b, _ := jsoniter.Marshal(y)
		jsoniter.Unmarshal(b, &am)

		s = httptest.NewServer(http.HandlerFunc(fTarget))
		defer s.Close()
		addrInfo = serverTCPAddr(s.URL)

		clone = primary.smapowner.get().clone()
		clone.addTarget(newDaemonInfo("target1", httpProto, addrInfo, &net.TCPAddr{}, &net.TCPAddr{}))
		primary.smapowner.put(clone)

		target1 := targetrunner{}
		target1.smapowner = &smapowner{}
		target1.smapowner.put(newSmap())
		target1.bmdowner = &bmdowner{}
		target1.bmdowner.put(newBucketMD())

		c := func(n *Smap, m *api.ActionMsg, e string) {
			noErr(e)
			matchActionMsg(&am, m)
			matchSMap(primary.smapowner.get(), n)
		}

		syncer.sync(true, primary.smapowner.get(), "")
		payload = <-chProxy
		newSMap, actMsg, errStr = proxy1.extractSmap(payload)
		c(newSMap, actMsg, errStr)

		payload = <-chTarget
		newSMap, actMsg, errStr = target1.extractSmap(payload)
		c(newSMap, actMsg, errStr)

		// this extra call is not necessary
		// it is new added target, but there is no other meta needs to be synced
		// (smap is the only one and it is already synced)
		payload = <-chTarget

		y = &api.ActionMsg{Action: ""}
		b, _ = jsoniter.Marshal(y)
		jsoniter.Unmarshal(b, &am)

		s = httptest.NewServer(http.HandlerFunc(fTarget))
		defer s.Close()
		addrInfo = serverTCPAddr(s.URL)
		clone = primary.smapowner.get().clone()
		clone.addTarget(newDaemonInfo("target2", httpProto, addrInfo, &net.TCPAddr{}, &net.TCPAddr{}))
		primary.smapowner.put(clone)

		target2 := targetrunner{}
		target2.smapowner = &smapowner{}
		target2.smapowner.put(newSmap())
		target2.bmdowner = &bmdowner{}
		target2.bmdowner.put(newBucketMD())

		syncer.sync(true, primary.smapowner.get(), "")
		payload = <-chProxy
		newSMap, actMsg, errStr = proxy1.extractSmap(payload)
		c(newSMap, actMsg, errStr)

		payload = <-chTarget
		newSMap, actMsg, errStr = target1.extractSmap(payload)
		c(newSMap, actMsg, errStr)

		payload = <-chTarget
		newSMap, actMsg, errStr = target2.extractSmap(payload)
		c(newSMap, actMsg, errStr)

		// same as above, extra sync caused by newly added target
		payload = <-chTarget

		// extract bucketmd
		// empty payload
		lb, actMsg, errStr := proxy1.extractbucketmd(make(simplekvs))
		if lb != nil || actMsg != nil || errStr != "" {
			t.Fatal("Extract bucketmd from empty payload returned data")
		}

		bucketmd := newBucketMD()
		syncer.sync(true, bucketmd, "")
		payload = <-chProxy
		lb, actMsg, errStr = proxy1.extractbucketmd(make(simplekvs))
		if lb != nil || actMsg != nil || errStr != "" {
			t.Fatal("Extract bucketmd from empty payload returned data")
		}

		payload = <-chTarget
		payload = <-chTarget

		bucketmd.add("lb1", true, BucketProps{
			CloudProvider: api.ProviderDFC,
			CksumConf: cksumconfig{
				Checksum: ChecksumInherit,
			},
		})
		bucketmd.add("lb2", true, BucketProps{
			CloudProvider: api.ProviderAmazon,
			CksumConf: cksumconfig{
				Checksum: ChecksumInherit,
			},
		})
		syncer.sync(true, bucketmd, "")
		payload = <-chProxy
		lb, actMsg, errStr = proxy1.extractbucketmd(payload)
		noErr(errStr)
		emptyActionMsg(actMsg)
		matchBMD(bucketmd, lb)
		proxy1.bmdowner.put(lb)

		payload = <-chTarget
		payload = <-chTarget

		// same version
		lb, actMsg, errStr = proxy1.extractbucketmd(payload)
		noErr(errStr)
		nilBMD(lb)
		emptyActionMsg(actMsg)

		// older version
		p1bmd := proxy1.bmdowner.get().clone()
		p1bmd.Version++
		proxy1.bmdowner.put(p1bmd)
		_, _, errStr = proxy1.extractbucketmd(payload)
		hasErr(errStr)
		p1bmd.Version--
		proxy1.bmdowner.put(p1bmd)

		am1 := api.ActionMsg{Action: "Action"}
		syncer.sync(true, bucketmd, &am1)
		payload = <-chTarget
		lb, actMsg, errStr = proxy1.extractbucketmd(payload)
		matchActionMsg(&am1, actMsg)

		payload = <-chProxy
		payload = <-chTarget

		y = &api.ActionMsg{Action: "New proxy"}
		b, _ = jsoniter.Marshal(y)
		jsoniter.Unmarshal(b, &am)

		// new smap and new bucketmd pairs
		s = httptest.NewServer(http.HandlerFunc(fProxy))
		defer s.Close()
		addrInfo = serverTCPAddr(s.URL)
		clone = primary.smapowner.get().clone()
		clone.addProxy(newDaemonInfo("proxy2", httpProto, addrInfo, &net.TCPAddr{}, &net.TCPAddr{}))
		primary.smapowner.put(clone)

		proxy2 := proxyrunner{}
		proxy2.smapowner = &smapowner{}
		proxy2.smapowner.put(newSmap())
		proxy2.bmdowner = &bmdowner{}
		proxy2.bmdowner.put(newBucketMD())

		bucketmd.add("lb3", true, BucketProps{
			CloudProvider: api.ProviderGoogle,
			CksumConf: cksumconfig{
				Checksum: ChecksumInherit,
			},
		})

		c = func(n *Smap, m *api.ActionMsg, e string) {
			noErr(e)
			matchActionMsg(&am, m)
			matchSMap(primary.smapowner.get(), n)
		}

		clb := func(lb *bucketMD, m *api.ActionMsg, e string) {
			noErr(e)
			matchActionMsg(&am1, m)
			matchBMD(bucketmd, lb)
		}

		syncer.sync(true, bucketmd, &am1, primary.smapowner.get(), &api.ActionMsg{Action: "New proxy"})
		payload = <-chProxy
		lb, actMsg, errStr = proxy2.extractbucketmd(payload)
		clb(lb, actMsg, errStr)
		newSMap, actMsg, errStr = proxy2.extractSmap(payload)
		c(newSMap, actMsg, errStr)

		payload = <-chTarget
		lb, actMsg, errStr = target2.extractbucketmd(payload)
		clb(lb, actMsg, errStr)
		newSMap, actMsg, errStr = target2.extractSmap(payload)
		c(newSMap, actMsg, errStr)

		payload = <-chProxy
		lb, actMsg, errStr = proxy1.extractbucketmd(payload)
		clb(lb, actMsg, errStr)
		newSMap, actMsg, errStr = proxy1.extractSmap(payload)
		c(newSMap, actMsg, errStr)

		payload = <-chTarget
		lb, actMsg, errStr = target1.extractbucketmd(payload)
		clb(lb, actMsg, errStr)
		newSMap, actMsg, errStr = target1.extractSmap(payload)
		c(newSMap, actMsg, errStr)

		payload = <-chProxy
		lb, actMsg, errStr = proxy2.extractbucketmd(payload)
		noErr(errStr)
		emptyActionMsg(actMsg)
		matchBMD(bucketmd, lb)
		newSMap, actMsg, errStr = proxy2.extractSmap(payload)
		noErr(errStr)
		emptyActionMsg(actMsg)
		matchSMap(primary.smapowner.get(), newSMap)

		syncer.stop(nil)
		wg.Wait()
	}

	{
		// this is to prove that metasync doesn't store action message
		// in this case, two targets, after a sync call of lb with action message,
		// if one of them fails for once and receives the sync successfully on retry,
		// one of them will receive the sync data with the original action message,
		// the failed one will not, it will only receives the lb but without the action message.
		primary := newPrimary()
		syncer := newmetasyncer(primary)

		var wg sync.WaitGroup
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			syncer.run()
		}(&wg)

		chTarget := make(chan map[string]string, 10)
		cnt := 0
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cnt++
			if cnt == 2 {
				http.Error(w, "call again", http.StatusNotAcceptable)
				return
			}

			d := make(map[string]string)
			primary.readJSON(w, r, &d) // ignore json error
			chTarget <- d
		}))
		defer s.Close()
		addrInfo := serverTCPAddr(s.URL)
		clone := primary.smapowner.get().clone()
		clone.addTarget(newDaemonInfo("target1", httpProto, addrInfo, &net.TCPAddr{}, &net.TCPAddr{}))
		primary.smapowner.put(clone)

		target1 := targetrunner{}
		target1.smapowner = &smapowner{}
		target1.smapowner.put(newSmap())
		target1.bmdowner = &bmdowner{}
		target1.bmdowner.put(newBucketMD())

		s = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			d := make(map[string]string)
			primary.readJSON(w, r, &d) // ignore json error
			chTarget <- d
		}))
		defer s.Close()
		addrInfo = serverTCPAddr(s.URL)
		clone = primary.smapowner.get().clone()
		clone.addTarget(newDaemonInfo("target2", httpProto, addrInfo, &net.TCPAddr{}, &net.TCPAddr{}))
		primary.smapowner.put(clone)

		target2 := targetrunner{}
		target2.smapowner = &smapowner{}
		target2.smapowner.put(newSmap())
		target2.bmdowner = &bmdowner{}
		target2.bmdowner.put(newBucketMD())

		bucketmd := newBucketMD()
		bucketmd.add("lb1", true, *NewBucketProps())

		syncer.sync(true, primary.smapowner.get(), "", bucketmd, "")
		<-chTarget
		<-chTarget

		bucketmd.add("lb2", true, BucketProps{
			CloudProvider: api.ProviderDFC,
			CksumConf: cksumconfig{
				Checksum: ChecksumInherit,
			},
		})
		syncer.sync(false, bucketmd, &api.ActionMsg{Action: "NB"})
		payload := <-chTarget
		_, actMsg, _ := target2.extractbucketmd(payload)
		if actMsg.Action == "" {
			t.Fatal("Expecting action message")
		}

		payload = <-chTarget
		_, actMsg, _ = target1.extractbucketmd(payload)
		if actMsg.Action != "" {
			t.Fatal("Not expecting action message")
		}

		syncer.stop(nil)
		wg.Wait()
	}
}
