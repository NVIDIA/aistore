/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package dfc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type (
	// syncf is the sync function this test uses to control what to do when a meta sync call
	// is received, for example, accepts or rejects the request.
	syncf func(w http.ResponseWriter, r *http.Request, cnt int) (int, error)

	// server represents one test server object, proxy or target
	server struct {
		id      string
		isProxy bool
		sf      syncf
		failCnt []int
	}

	// transportData records information about meta sync calls including called for which server, how many
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

// newPrimary returns a proxy runner after initializing the fields that are needed by this test
func newPrimary() *proxyrunner {
	p := proxyrunner{}
	smap := &Smap{Tmap: make(map[string]*daemonInfo), Pmap: make(map[string]*proxyInfo)}
	p.smap = smap
	p.si = &daemonInfo{DaemonID: "primary"}
	p.primary = true
	p.httpclient = &http.Client{}
	p.httpclientLongTimeout = &http.Client{}
	p.kalive = &proxykalive{}
	ctx.config.Periodic.RetrySyncTime = time.Millisecond * 100
	pi := &proxyInfo{daemonInfo{DaemonID: p.si.DaemonID, DirectURL: "do not care"}, true /* primary */}
	p.smap.addProxy(pi)
	p.smap.ProxySI = pi
	return &p
}

// newTransportServer creates a http test server to simulate a proxy or a target, it is used to test the
// transport of mets sync, which is making sync calls, retry failed calls, etc. it doesn't involve the actual
// content of the meta data received.
// newTransportServer's http handler calls the sync funtion which decide how to respond to the sync call,
// counts number of times sync call received, sends result to the result channel on each sync (error or
// no error), completes the http request with the status returned by the sync function.
func newTransportServer(primary *proxyrunner, s *server, ch chan<- transportData) *httptest.Server {
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
	if s.isProxy {
		primary.smap.Pmap[id] = &proxyInfo{daemonInfo{DaemonID: id, DirectURL: ts.URL}, false /* primary */}
	} else {
		primary.smap.Tmap[id] = &daemonInfo{DaemonID: id, DirectURL: ts.URL}
	}

	return ts
}

// TestMetaSyncTransport is the driver for meta sync transport tests.
// for each test case, it creates a primary proxy, starts the meta sync instance, run the test case,
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

		t.Run(fmt.Sprintf("%s", tc.name), func(t *testing.T) {
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
	var msgs []transportData

	for i := 0; i < n; i++ {
		msgs = append(msgs, <-ch)
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
		servers = []server{
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

	syncer.sync(true, primary.smap)
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
		servers = []server{
			{"p1", true, delayedOk, nil},
			{"t1", false, alwaysOk, nil},
		}
		ch = make(chan transportData, len(servers))
	)

	for _, v := range servers {
		s := newTransportServer(primary, &v, ch)
		defer s.Close()
	}

	syncer.sync(true, primary.smap)
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
		servers = []server{
			{"p1", true, delayedOk, nil},
			{"t1", false, alwaysOk, nil},
		}
		ch = make(chan transportData, len(servers))
	)

	for _, v := range servers {
		s := newTransportServer(primary, &v, ch)
		defer s.Close()
	}

	syncer.sync(false, primary.smap)
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
		servers = []server{
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

	syncer.sync(true, primary.smap)
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
		servers = []server{
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

	syncer.sync(true, primary.smap)
	syncer.sync(false, primary.smap)
	syncer.sync(true, primary.smap)
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

// refused tests the connection refused handling by meta syncer.
// it tells metasyncer to sync before starting the server to cause a connection refused error
// it has two test cases: one wihout delay to let metasyncer handles it immediately,
// the other one has a delay before starting the server, metasyncer fail the direct handling of
// connection refused error and falls back to the retry route
func refused(t *testing.T, primary *proxyrunner, syncer *metasyncer) ([]transportData, []transportData) {
	var (
		ch = make(chan transportData, 2) // note: use 2 to avoid unbuffered channel, http handler can return
		id = "p"
		s  = &http.Server{Addr: "localhost:53538"} // the lucky port
	)

	// handler for /v1/daemon/metasync
	http.HandleFunc(path.Join("/", Rversion, Rdaemon, Rmetasync), func(w http.ResponseWriter, r *http.Request) {
		ch <- transportData{true, id, 1}
	})

	primary.smap.Pmap[id] = &proxyInfo{daemonInfo{DaemonID: id, DirectURL: "http://" + s.Addr}, false /* primary */}

	// function shared by the two cases: no delay and delay
	// starts the proxy server, wait for a sync call
	f := func() {
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

	// testcase #1: no delay, metasync handles this directly
	syncer.sync(false, primary.smap)
	// sync will return even though the sync actually failed, and there is no error return
	f()

	// testcase #2: with delay, metasync tries and falls through pending handling (retry)
	syncer.sync(false, primary.smap)
	time.Sleep(time.Second * 2)
	f()

	// only cares if the sync call comes, no need to verify the id and cnt as we are the one
	// filling those in above
	exp := []transportData{{true, id, 1}}
	return exp, exp
}

// TestMetaSyncData is the driver for meta sync data tests.
func TestMetaSyncData(t *testing.T) {
	// data stores the data comes from the http sync call and an error
	type data struct {
		payload map[string]string
		err     error
	}

	// newDataServer simulates a proxt or a target for meta sync's data tests
	newServer := func(primary *proxyrunner, s *server, ch chan<- data) *httptest.Server {
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
		if s.isProxy {
			primary.smap.addProxy(&proxyInfo{daemonInfo{DaemonID: id, DirectURL: ts.URL}, false /* primary */})
		} else {
			primary.smap.add(&daemonInfo{DaemonID: id, DirectURL: ts.URL})
		}

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
		lbMap          = newLBMap()
		emptyActionMsg string
	)

	b, err := json.Marshal(ActionMsg{})
	if err != nil {
		t.Fatal("Failed to marshal empty ActionMsg, err =", err)
	}

	emptyActionMsg = string(b)

	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		syncer.run()
	}(&wg)

	proxy := newServer(primary, &server{"proxy", true, nil, []int{3, 4, 5}}, ch)
	defer proxy.Close()

	target := newServer(primary, &server{"target", false, nil, []int{2}}, ch)
	defer target.Close()

	// sync smap
	b, err = primary.smap.marshal()
	if err != nil {
		t.Fatal("Failed to marshal smap, err =", err)
	}

	exp[smaptag] = string(b)
	expRetry[smaptag] = string(b)
	exp[smaptag+actiontag] = string(emptyActionMsg)
	expRetry[smaptag+actiontag] = string(emptyActionMsg)

	syncer.sync(false, primary.smap.cloneU())
	match(t, expRetry, ch, 1)

	// sync lbmap, target fail and retry
	lbMap.add("bucket1")
	lbMap.add("bucket2")
	b, err = lbMap.marshal()
	if err != nil {
		t.Fatal("Failed to marshal lbMap, err =", err)
	}

	exp[lbmaptag] = string(b)
	expRetry[lbmaptag] = string(b)
	exp[lbmaptag+actiontag] = string(emptyActionMsg)
	expRetry[lbmaptag+actiontag] = string(emptyActionMsg)

	syncer.sync(false, &lbMap)
	match(t, exp, ch, 1)
	match(t, expRetry, ch, 1)

	// sync lbmap, proxy fail, new lbmap synced, expect proxy to receive the new lbmap after rejecting a few
	// sync request
	lbMap.add("bucket3")
	b, err = lbMap.marshal()
	if err != nil {
		t.Fatal("Failed to marshal lbMap, err =", err)
	}

	exp[lbmaptag] = string(b)
	syncer.sync(false, &lbMap)
	match(t, exp, ch, 1)

	lbMap.add("bucket4")
	b, err = lbMap.marshal()
	if err != nil {
		t.Fatal("Failed to marshal lbMap, err =", err)
	}

	lbMapStr := string(b)
	exp[lbmaptag] = lbMapStr
	syncer.sync(false, &lbMap)
	match(t, exp, ch, 2)

	// sync smap
	delete(exp, lbmaptag)
	delete(exp, lbmaptag+actiontag)

	b, err = json.Marshal(&ActionMsg{"", "", primary.smap})
	if err != nil {
		t.Fatal("Failed to marshal smap, err =", err)
	}

	exp[smaptag+actiontag] = string(b)

	proxy = newServer(primary, &server{"another proxy", true, nil, nil}, ch)
	defer proxy.Close()

	b, err = primary.smap.marshal()
	if err != nil {
		t.Fatal("Failed to marshal smap, err =", err)
	}

	exp[smaptag] = string(b)

	syncer.sync(false, primary.smap)
	match(t, exp, ch, 3)

	// sync smap pair
	msgSMap := &ActionMsg{"who cares", "whatever", primary.smap}
	b, err = json.Marshal(msgSMap)
	if err != nil {
		t.Fatal("Failed to marshal action message, err =", err)
	}

	exp[smaptag+actiontag] = string(b)
	// note: action message's value has to be nil for smap
	msgSMap.Value = nil
	syncer.sync(false, &revspair{primary.smap, msgSMap})
	match(t, exp, ch, 3)

	// sync smap pair and lbmap pair
	msgLBMap := &ActionMsg{"who cares", "whatever", "send me back"}
	b, err = json.Marshal(msgLBMap)
	if err != nil {
		t.Fatal("Failed to marshal action message, err =", err)
	}

	exp[lbmaptag] = lbMapStr
	exp[lbmaptag+actiontag] = string(b)
	// note: 'Value' field was modified by the last call to sync()
	msgSMap.Value = nil
	syncer.sync(true, &revspair{primary.smap, msgSMap}, &revspair{&lbMap, msgLBMap})
	match(t, exp, ch, 3)

	// lbmap pair
	delete(exp, smaptag)
	syncer.sync(true, &revspair{&lbMap, msgLBMap})
	match(t, exp, ch, 3)

	syncer.stop(nil)
	wg.Wait()
}

// TestMetaSyncMembership tests meta sync's logic when accessing proxy's smap directly
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

		cnt := 0
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cnt++
			http.Error(w, "i don't know how to deal with you", http.StatusNotAcceptable)
		}))

		defer s.Close()

		id := "t"
		primary.smap.add(&daemonInfo{DaemonID: id, DirectURL: s.URL})
		syncer.sync(true, primary.smap)
		time.Sleep(time.Millisecond * 300)
		primary.smap.del(id)
		time.Sleep(time.Millisecond * 300)
		cnt1 := cnt
		time.Sleep(time.Millisecond * 300)
		if cnt != cnt1 {
			t.Fatal("Sync call didn't stop after traget is deleted")
		}

		syncer.stop(nil)
		wg.Wait()
	}

	{
		// sync before smap sync (no previous sync saved in meta syncer)
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

		s1 := httptest.NewServer(http.HandlerFunc(f))
		defer s1.Close()

		primary.smap.add(&daemonInfo{DaemonID: "t1", DirectURL: s1.URL})
		syncer.sync(true, primary.lbmap)
		<-ch

		// sync smap so meta syncer has a smap
		syncer.sync(true, primary.smap.cloneU())
		<-ch

		// add a new target but new smap is not synced
		// meta syncer picks up the new target directly from primary's smap
		// and meta syncer will also add the new target to pending to sync all previously synced data
		// that's why the extra channel read
		s2 := httptest.NewServer(http.HandlerFunc(f))
		defer s2.Close()

		primary.smap.add(&daemonInfo{DaemonID: "t2", DirectURL: s2.URL})
		syncer.sync(true, primary.lbmap)
		<-ch // target 1
		<-ch // target 2
		<-ch // all previously synced data to target 2
		if len(ch) != 0 {
			t.Fatal("Too many sync calls received")
		}

		syncer.stop(nil)
		wg.Wait()
	}
}
