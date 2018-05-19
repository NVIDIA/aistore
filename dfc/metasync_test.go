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
	"sync/atomic"
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

// getServerURL takes a string in format of "http://ip:port" and returns its ip and port
func getServerIPAndPort(u string) (string, string) {
	s := strings.TrimPrefix(u, "http://")
	items := strings.Split(s, ":")
	return items[0], items[1]
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
	ip, port := getServerIPAndPort(ts.URL)
	di := daemonInfo{DaemonID: id, DirectURL: ts.URL, NodeIPAddr: ip, DaemonPort: port}
	if s.isProxy {
		primary.smap.Pmap[id] = &proxyInfo{di, false /* primary */}
	} else {
		primary.smap.Tmap[id] = &di
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
// it has two test cases: one wih a short delay to let metasyncer handles it immediately,
// the other one has a longer delay before starting the server, metasyncer fail the direct handling of
// connection refused error and falls back to the retry route
func refused(t *testing.T, primary *proxyrunner, syncer *metasyncer) ([]transportData, []transportData) {
	var (
		ch   = make(chan transportData, 2) // note: use 2 to avoid unbuffered channel, http handler can return
		id   = "p"
		port = "53538" // the lucky port
		ip   = "localhost"
		s    = &http.Server{Addr: ip + ":" + port}
	)

	// handler for /v1/daemon/metasync
	http.HandleFunc(path.Join("/", Rversion, Rdaemon, Rmetasync), func(w http.ResponseWriter, r *http.Request) {
		ch <- transportData{true, id, 1}
	})

	primary.smap.Pmap[id] = &proxyInfo{
		daemonInfo{
			DaemonID:   id,
			DirectURL:  "http://" + s.Addr,
			NodeIPAddr: ip,
			DaemonPort: port,
		},
		false, /* primary */
	}

	// function shared by the two cases: short delay and long delay
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

	// testcase #1: short delay, metasync handles this directly
	syncer.sync(false, primary.smap)
	time.Sleep(time.Millisecond)
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
		ip, port := getServerIPAndPort(ts.URL)
		di := daemonInfo{DaemonID: id, DirectURL: ts.URL, NodeIPAddr: ip, DaemonPort: port}
		if s.isProxy {
			primary.smap.Pmap[id] = &proxyInfo{di, false /* primary */}
		} else {
			primary.smap.Tmap[id] = &di
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

	syncer.sync(false, lbMap)
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
	syncer.sync(false, lbMap)
	match(t, exp, ch, 1)

	lbMap.add("bucket4")
	b, err = lbMap.marshal()
	if err != nil {
		t.Fatal("Failed to marshal lbMap, err =", err)
	}

	lbMapStr := string(b)
	exp[lbmaptag] = lbMapStr
	syncer.sync(false, lbMap)
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

	syncer.sync(false, primary.smap.cloneU())
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
	syncer.sync(false, &revspair{primary.smap.cloneU(), msgSMap})
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
	syncer.sync(true, &revspair{primary.smap.cloneL().(*Smap), msgSMap}, &revspair{lbMap, msgLBMap})
	match(t, exp, ch, 3)

	// lbmap pair
	delete(exp, smaptag)
	syncer.sync(true, &revspair{lbMap, msgLBMap})
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

		var cnt int32
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&cnt, 1)
			http.Error(w, "i don't know how to deal with you", http.StatusNotAcceptable)
		}))

		defer s.Close()

		id := "t"
		ip, port := getServerIPAndPort(s.URL)
		primary.smap.add(
			&daemonInfo{
				DaemonID:   id,
				DirectURL:  s.URL,
				NodeIPAddr: ip,
				DaemonPort: port,
			})
		syncer.sync(true, primary.smap.cloneU())
		time.Sleep(time.Millisecond * 300)
		smapLock.Lock()
		primary.smap.del(id)
		smapLock.Unlock()
		time.Sleep(time.Millisecond * 300)
		cnt1 := atomic.LoadInt32(&cnt)
		time.Sleep(time.Millisecond * 300)
		if atomic.LoadInt32(&cnt) != cnt1 {
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

		id := "t1"
		ip, port := getServerIPAndPort(s1.URL)
		di := daemonInfo{DaemonID: id, DirectURL: s1.URL, NodeIPAddr: ip, DaemonPort: port}
		primary.smap.add(&di)
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

		id = "t2"
		ip, port = getServerIPAndPort(s2.URL)
		di = daemonInfo{DaemonID: id, DirectURL: s2.URL, NodeIPAddr: ip, DaemonPort: port}
		primary.smap.add(&di)
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

		emptyActionMsg := func(a *ActionMsg) {
			if a.Action != "" || a.Name != "" || a.Value != nil {
				t.Fatal("Expecting empty action message", a)
			}
		}

		matchActionMsg := func(a, b *ActionMsg) {
			if !reflect.DeepEqual(a, b) {
				t.Fatal("ActionMsg mismatch ", a, b)
			}
		}

		nilSMap := func(m *Smap) {
			if m != nil {
				t.Fatal("Expecting nil smap", m)
			}
		}

		emptySMap := func(m *Smap) {
			if m.Tmap != nil || m.Pmap != nil || m.ProxySI != nil || m.Version != 0 {
				t.Fatal("Expecting empty smap", m)
			}
		}

		matchSMap := func(a, b *Smap) {
			if !reflect.DeepEqual(a, b) {
				t.Fatal("SMap mismatch", a, b)
			}
		}

		// matchPrevSMap matches two smap partially
		// previous version of the smap is only partially extracted from a sync call
		matchPrevSMap := func(a, b *Smap) {
			if a.Version != b.Version {
				t.Fatal("Previous smap's version mismatch", a.Version, b.Version)
			}

			for _, v := range a.Pmap {
				_, ok := b.Pmap[v.DaemonID]
				if !ok {
					t.Fatal("Missing proxy", v.DaemonID)
				}
			}

			for _, v := range a.Tmap {
				_, ok := b.Tmap[v.DaemonID]
				if !ok {
					t.Fatal("Missing target", v.DaemonID)
				}
			}
		}

		nilLBMap := func(l *lbmap) {
			if l != nil {
				t.Fatal("Expecting nil lbmap", l)
			}
		}

		matchLBMap := func(a, b *lbmap) {
			if !reflect.DeepEqual(a, b) {
				t.Fatal("LBMap mismatch", a, b)
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

		// note: only difference is the channel, hard to share the code, but only a few lines
		chTarget := make(chan map[string]string, 10)
		fTarget := func(w http.ResponseWriter, r *http.Request) {
			d := make(map[string]string)
			primary.readJSON(w, r, &d) // ignore json error
			chTarget <- d
		}

		s := httptest.NewServer(http.HandlerFunc(fProxy))
		defer s.Close()
		ip, port := getServerIPAndPort(s.URL)
		primary.smap.addProxy(
			&proxyInfo{
				daemonInfo{
					DaemonID:   "proxy1",
					DirectURL:  s.URL,
					NodeIPAddr: ip,
					DaemonPort: port,
				},
				false, /* primary */
			},
		)
		proxy1 := proxyrunner{}
		proxy1.smap = &Smap{Tmap: make(map[string]*daemonInfo), Pmap: make(map[string]*proxyInfo)}
		proxy1.lbmap = newLBMap()

		// empty payload
		newSMap, oldSMap, actMsg, errStr := proxy1.extractsmap(make(map[string]string))
		if newSMap != nil || oldSMap != nil || actMsg != nil || errStr != "" {
			t.Fatal("Extract smap from empty payload returned data")
		}

		syncer.sync(true, primary.smap.cloneU())
		payload := <-chProxy

		newSMap, oldSMap, actMsg, errStr = proxy1.extractsmap(payload)
		noErr(errStr)
		emptyActionMsg(actMsg)
		emptySMap(oldSMap)
		matchSMap(primary.smap, newSMap)
		proxy1.smap = newSMap

		// same version of smap received
		newSMap, oldSMap, actMsg, errStr = proxy1.extractsmap(payload)
		noErr(errStr)
		emptyActionMsg(actMsg)
		emptySMap(oldSMap)
		nilSMap(newSMap)

		// older version of smap received
		proxy1.smap.Version++
		_, _, _, errStr = proxy1.extractsmap(payload)
		hasErr(errStr)
		proxy1.smap.Version--

		var am ActionMsg
		y := &ActionMsg{"", "", primary.smap.cloneU()}
		b, _ := json.Marshal(y)
		json.Unmarshal(b, &am)
		prevSMap := primary.smap.cloneU()

		s = httptest.NewServer(http.HandlerFunc(fTarget))
		defer s.Close()
		ip, port = getServerIPAndPort(s.URL)
		primary.smap.add(
			&daemonInfo{
				DaemonID:   "target1",
				DirectURL:  s.URL,
				NodeIPAddr: ip,
				DaemonPort: port,
			})
		target1 := targetrunner{}
		target1.smap = &Smap{Tmap: make(map[string]*daemonInfo), Pmap: make(map[string]*proxyInfo)}
		target1.lbmap = newLBMap()

		c := func(n, o *Smap, m *ActionMsg, e string) {
			noErr(e)
			matchActionMsg(&am, m)
			matchPrevSMap(prevSMap, o)
			matchSMap(primary.smap, n)
		}

		syncer.sync(true, primary.smap.cloneU())
		payload = <-chProxy
		newSMap, oldSMap, actMsg, errStr = proxy1.extractsmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		payload = <-chTarget
		newSMap, oldSMap, actMsg, errStr = target1.extractsmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		// note: this extra call is not necessary by meta syncer
		// it picked up the target as it is new added target, but there is no other meta needs to be synced
		// (smap is the only one and it is already synced)
		payload = <-chTarget

		y = &ActionMsg{"", "", primary.smap}
		b, _ = json.Marshal(y)
		json.Unmarshal(b, &am)
		prevSMap = primary.smap.cloneU()

		s = httptest.NewServer(http.HandlerFunc(fTarget))
		defer s.Close()
		ip, port = getServerIPAndPort(s.URL)
		primary.smap.add(
			&daemonInfo{
				DaemonID:   "target2",
				DirectURL:  s.URL,
				NodeIPAddr: ip,
				DaemonPort: port,
			})
		target2 := targetrunner{}
		target2.smap = &Smap{Tmap: make(map[string]*daemonInfo), Pmap: make(map[string]*proxyInfo)}
		target2.lbmap = newLBMap()

		syncer.sync(true, primary.smap.cloneU())
		payload = <-chProxy
		newSMap, oldSMap, actMsg, errStr = proxy1.extractsmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		payload = <-chTarget
		newSMap, oldSMap, actMsg, errStr = target1.extractsmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		payload = <-chTarget
		newSMap, oldSMap, actMsg, errStr = target2.extractsmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		// same as above, extra sync caused by newly added target
		payload = <-chTarget

		// extract lbmap
		// empty payload
		lb, actMsg, errStr := proxy1.extractlbmap(make(map[string]string))
		if lb != nil || actMsg != nil || errStr != "" {
			t.Fatal("Extract lbmap from empty payload returned data")
		}

		lbMap := newLBMap()
		syncer.sync(true, lbMap)
		payload = <-chProxy
		lb, actMsg, errStr = proxy1.extractlbmap(make(map[string]string))
		if lb != nil || actMsg != nil || errStr != "" {
			t.Fatal("Extract lbmap from empty payload returned data")
		}

		payload = <-chTarget
		payload = <-chTarget

		lbMap.add("lb1")
		lbMap.add("lb2")
		syncer.sync(true, lbMap)
		payload = <-chProxy
		lb, actMsg, errStr = proxy1.extractlbmap(payload)
		noErr(errStr)
		emptyActionMsg(actMsg)
		matchLBMap(lbMap, lb)
		proxy1.lbmap = lb

		payload = <-chTarget
		payload = <-chTarget

		// same version
		lb, actMsg, errStr = proxy1.extractlbmap(payload)
		noErr(errStr)
		nilLBMap(lb)
		emptyActionMsg(actMsg)

		// older version
		proxy1.lbmap.Version++
		_, _, errStr = proxy1.extractlbmap(payload)
		hasErr(errStr)
		proxy1.lbmap.Version--

		am1 := ActionMsg{"Action", "Name", "Expecting this back"}
		syncer.sync(true, &revspair{lbMap, &am1})
		payload = <-chTarget
		lb, actMsg, errStr = proxy1.extractlbmap(payload)
		matchActionMsg(&am1, actMsg)

		payload = <-chProxy
		payload = <-chTarget

		y = &ActionMsg{"New proxy", "", primary.smap}
		b, _ = json.Marshal(y)
		json.Unmarshal(b, &am)
		prevSMap = primary.smap.cloneU()

		// new smap and new lbmap pairs
		s = httptest.NewServer(http.HandlerFunc(fProxy))
		defer s.Close()
		ip, port = getServerIPAndPort(s.URL)
		primary.smap.addProxy(
			&proxyInfo{
				daemonInfo{
					DaemonID:   "proxy2",
					DirectURL:  s.URL,
					NodeIPAddr: ip,
					DaemonPort: port},
				false,
			})
		proxy2 := proxyrunner{}
		proxy2.smap = &Smap{Tmap: make(map[string]*daemonInfo), Pmap: make(map[string]*proxyInfo)}
		proxy2.lbmap = newLBMap()

		lbMap.add("lb3")

		c = func(n, o *Smap, m *ActionMsg, e string) {
			noErr(e)
			matchActionMsg(&am, m)
			matchPrevSMap(prevSMap, o)
			matchSMap(primary.smap, n)
		}

		clb := func(lb *lbmap, m *ActionMsg, e string) {
			noErr(e)
			matchActionMsg(&am1, m)
			matchLBMap(lbMap, lb)
		}

		syncer.sync(true, &revspair{lbMap, &am1}, &revspair{primary.smap.cloneU(), &ActionMsg{"New proxy", "", nil}})
		payload = <-chProxy
		lb, actMsg, errStr = proxy2.extractlbmap(payload)
		clb(lb, actMsg, errStr)
		newSMap, oldSMap, actMsg, errStr = proxy2.extractsmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		payload = <-chTarget
		lb, actMsg, errStr = target2.extractlbmap(payload)
		clb(lb, actMsg, errStr)
		newSMap, oldSMap, actMsg, errStr = target2.extractsmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		payload = <-chProxy
		lb, actMsg, errStr = proxy1.extractlbmap(payload)
		clb(lb, actMsg, errStr)
		newSMap, oldSMap, actMsg, errStr = proxy1.extractsmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		payload = <-chTarget
		lb, actMsg, errStr = target1.extractlbmap(payload)
		clb(lb, actMsg, errStr)
		newSMap, oldSMap, actMsg, errStr = target1.extractsmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		// this is the extra send to the newly added proxy.
		// FIXME: meta sync has a bug here i believe unless it is by design.
		//        the previous sync has lbmap and an action message, meta sync only saves the lbmap but not
		//        the action message, so on this extra send, it picks up only the lbmap.
		//        not a big deal for this case, but for retry case, retry proxy/target will not receive the
		//        action message but only receives the lbmap.
		//        same for smap, no old smap is picked up during retry or for newly added servers
		payload = <-chProxy
		lb, actMsg, errStr = proxy2.extractlbmap(payload)
		noErr(errStr)
		emptyActionMsg(actMsg)
		matchLBMap(lbMap, lb)
		newSMap, oldSMap, actMsg, errStr = proxy2.extractsmap(payload)
		noErr(errStr)
		emptyActionMsg(actMsg)
		emptySMap(oldSMap)
		matchSMap(primary.smap, newSMap)

		syncer.stop(nil)
		wg.Wait()
	}

	{
		// this is to prove that meta sync doesn't store action message
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
		ip, port := getServerIPAndPort(s.URL)
		primary.smap.add(
			&daemonInfo{
				DaemonID:   "target1",
				DirectURL:  s.URL,
				NodeIPAddr: ip,
				DaemonPort: port,
			})
		target1 := targetrunner{}
		target1.smap = &Smap{Tmap: make(map[string]*daemonInfo), Pmap: make(map[string]*proxyInfo)}
		target1.lbmap = newLBMap()

		s = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			d := make(map[string]string)
			primary.readJSON(w, r, &d) // ignore json error
			chTarget <- d
		}))
		defer s.Close()
		ip, port = getServerIPAndPort(s.URL)
		primary.smap.add(
			&daemonInfo{
				DaemonID:   "target2",
				DirectURL:  s.URL,
				NodeIPAddr: ip,
				DaemonPort: port,
			})
		target2 := targetrunner{}
		target2.smap = &Smap{Tmap: make(map[string]*daemonInfo), Pmap: make(map[string]*proxyInfo)}
		target2.lbmap = newLBMap()

		lbMap := newLBMap()
		lbMap.add("lb1")

		syncer.sync(true, primary.smap.cloneU(), lbMap)
		<-chTarget
		<-chTarget

		lbMap.add("lb2")
		syncer.sync(false, &revspair{lbMap, &ActionMsg{Action: "NB"}})
		payload := <-chTarget
		_, actMsg, _ := target2.extractlbmap(payload)
		if actMsg.Action == "" {
			t.Fatal("Expecting action message")
		}

		payload = <-chTarget
		_, actMsg, _ = target1.extractlbmap(payload)
		if actMsg.Action != "" {
			t.Fatal("Not expecting action message")
		}

		syncer.stop(nil)
		wg.Wait()
	}
}
