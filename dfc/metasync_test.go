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

	"github.com/NVIDIA/dfcpub/dfc/statsd"
)

type (
	// syncf is the sync function this test uses to control what to do when a meta sync call
	// is received, for example, accepts or rejects the request.
	syncf func(w http.ResponseWriter, r *http.Request, cnt int) (int, error)

	// metaSyncServer represents one test metaSyncServer object, proxy or target
	metaSyncServer struct {
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

// getServerIPAndPort takes a string in format of "http://ip:port" and returns its ip and port
func getServerIPAndPort(u string) (string, string) {
	s := strings.TrimPrefix(u, "http://")
	items := strings.Split(s, ":")
	return items[0], items[1]
}

// newPrimary returns a proxy runner after initializing the fields that are needed by this test
func newPrimary() *proxyrunner {
	p := proxyrunner{}
	p.smapowner = &smapowner{}
	p.si = &daemonInfo{DaemonID: "primary", DirectURL: "do not care"}
	smap := newSmap()
	smap.addProxy(p.si)
	smap.ProxySI = p.si
	p.smapowner.put(smap)

	p.httpclientLongTimeout = &http.Client{}
	ctx.config.Periodic.RetrySyncTime = time.Millisecond * 100
	ctx.config.KeepaliveTracker.Proxy.Name = "heartbeat"
	ctx.config.KeepaliveTracker.Proxy.MaxStr = "20s"
	ctx.config.KeepaliveTracker.Proxy.IntervalStr = "as"
	p.kalive = newproxykalive(&p)
	p.callStatsServer = NewCallStatsServer(nil, 1, &statsd.Client{})
	p.callStatsServer.Start()

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
	ip, port := getServerIPAndPort(ts.URL)
	di := daemonInfo{DaemonID: id, DirectURL: ts.URL, NodeIPAddr: ip, DaemonPort: port}
	clone := primary.smapowner.get().clone()
	if s.isProxy {
		clone.Pmap[id] = &di
	} else {
		clone.Tmap[id] = &di
	}
	primary.smapowner.put(clone)

	return ts
}

func TestMetaSyncDeepCopy(t *testing.T) {
	bucketmd := newBucketMD()
	bucketmd.add("bucket1", true, BucketProps{CloudProvider: ProviderDfc, NextTierURL: "http://foo.com"})
	bucketmd.add("bucket2", true, BucketProps{})
	bucketmd.add("bucket3", false, BucketProps{CloudProvider: ProviderDfc})
	bucketmd.add("bucket4", false, BucketProps{})

	clone := &bucketMD{}
	bucketmd.deepcopy(clone)

	b1, _ := json.Marshal(bucketmd)
	s1 := string(b1)
	b2, _ := json.Marshal(clone)
	s2 := string(b2)
	if s1 == "" || s2 == "" || s1 != s2 {
		t.Log(s1)
		t.Log(s2)
		t.Fatal("marshal(bucketmd) != marshal(clone(bucketmd))")
	}
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
		defer primary.callStatsServer.Stop()
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

	syncer.sync(true, primary.smapowner.get())
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

	syncer.sync(true, primary.smapowner.get())
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

	syncer.sync(false, primary.smapowner.get())
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

	syncer.sync(true, primary.smapowner.get())
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

	syncer.sync(true, primary.smapowner.get())
	syncer.sync(false, primary.smapowner.get())
	syncer.sync(true, primary.smapowner.get())
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

	clone := primary.smapowner.get().clone()
	clone.Pmap[id] = &daemonInfo{
		DaemonID:   id,
		DirectURL:  "http://" + s.Addr,
		NodeIPAddr: ip,
		DaemonPort: port,
	}
	primary.smapowner.put(clone)

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
	syncer.sync(false, primary.smapowner.get())
	time.Sleep(time.Millisecond)
	// sync will return even though the sync actually failed, and there is no error return
	f()

	// testcase #2: with delay, metasync tries and falls through pending handling (retry)
	syncer.sync(false, primary.smapowner.get())
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
		ip, port := getServerIPAndPort(ts.URL)
		di := daemonInfo{DaemonID: id, DirectURL: ts.URL, NodeIPAddr: ip, DaemonPort: port}
		clone := primary.smapowner.get().clone()
		if s.isProxy {
			clone.Pmap[id] = &di
		} else {
			clone.Tmap[id] = &di
		}
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

	defer primary.callStatsServer.Stop()

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

	syncer.sync(false, smap)
	match(t, expRetry, ch, 1)

	// sync bucketmd, fail target and retry
	bucketmd.add("bucket1", true, BucketProps{CloudProvider: ProviderDfc})
	bucketmd.add("bucket2", true, BucketProps{CloudProvider: ProviderDfc,
		NextTierURL: "http://localhost:8082"})
	b, err = bucketmd.marshal()
	if err != nil {
		t.Fatal("Failed to marshal bucketmd, err =", err)
	}

	exp[bucketmdtag] = string(b)
	expRetry[bucketmdtag] = string(b)
	exp[bucketmdtag+actiontag] = string(emptyActionMsg)
	expRetry[bucketmdtag+actiontag] = string(emptyActionMsg)

	syncer.sync(false, bucketmd)
	match(t, exp, ch, 1)
	match(t, expRetry, ch, 1)

	// sync bucketmd, fail proxy, sync new bucketmd, expect proxy to receive the new bucketmd
	// after rejecting a few sync requests
	bucketmd.add("bucket3", true, BucketProps{})
	b, err = bucketmd.marshal()
	if err != nil {
		t.Fatal("Failed to marshal bucketmd, err =", err)
	}

	exp[bucketmdtag] = string(b)
	syncer.sync(false, bucketmd)
	match(t, exp, ch, 1)

	bucketmd.add("bucket4", true, BucketProps{CloudProvider: ProviderDfc, NextTierURL: "http://foo.com"})
	b, err = bucketmd.marshal()
	if err != nil {
		t.Fatal("Failed to marshal bucketmd, err =", err)
	}

	bucketmdString := string(b)
	exp[bucketmdtag] = bucketmdString
	syncer.sync(false, bucketmd)
	match(t, exp, ch, 2)

	// sync smap
	delete(exp, bucketmdtag)
	delete(exp, bucketmdtag+actiontag)

	b, err = json.Marshal(&ActionMsg{"", "", primary.smapowner.get()})
	if err != nil {
		t.Fatal("Failed to marshal smap, err =", err)
	}

	exp[smaptag+actiontag] = string(b)

	proxy = newServer(primary, &metaSyncServer{"another proxy", true, nil, nil}, ch)
	defer proxy.Close()

	b, err = primary.smapowner.get().marshal()
	if err != nil {
		t.Fatal("Failed to marshal smap, err =", err)
	}

	exp[smaptag] = string(b)

	syncer.sync(false, primary.smapowner.get())
	match(t, exp, ch, 3)

	// sync smap pair
	msgSMap := &ActionMsg{"who cares", "whatever", primary.smapowner.get()}
	b, err = json.Marshal(msgSMap)
	if err != nil {
		t.Fatal("Failed to marshal action message, err =", err)
	}

	exp[smaptag+actiontag] = string(b)
	// note: action message's value has to be nil for smap
	msgSMap.Value = nil
	syncer.sync(false, &revspair{primary.smapowner.get(), msgSMap})
	match(t, exp, ch, 3)

	// sync smap pair and bucketmd pair
	msgBMD := &ActionMsg{"who cares", "whatever", "send me back"}
	b, err = json.Marshal(msgBMD)
	if err != nil {
		t.Fatal("Failed to marshal action message, err =", err)
	}

	exp[bucketmdtag] = bucketmdString
	exp[bucketmdtag+actiontag] = string(b)
	// note: 'Value' field was modified by the last call to sync()
	msgSMap.Value = nil
	syncer.sync(true, &revspair{primary.smapowner.get(), msgSMap}, &revspair{bucketmd, msgBMD})
	match(t, exp, ch, 3)

	// bucketmd pair
	delete(exp, smaptag)
	syncer.sync(true, &revspair{bucketmd, msgBMD})
	match(t, exp, ch, 3)

	syncer.stop(nil)
	wg.Wait()
}

// TestMetaSyncMembership tests meta sync's logic when accessing proxy's smap directly
func TestMetaSyncMembership(t *testing.T) {
	{
		// pending server dropped without sync
		primary := newPrimary()
		defer primary.callStatsServer.Stop()
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
		clone := primary.smapowner.get().clone()
		clone.addTarget(
			&daemonInfo{
				DaemonID:   id,
				DirectURL:  s.URL,
				NodeIPAddr: ip,
				DaemonPort: port,
			})
		primary.smapowner.put(clone)
		syncer.sync(true, clone)
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

	{
		// sync before smap sync (no previous sync saved in meta syncer)
		primary := newPrimary()
		defer primary.callStatsServer.Stop()
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
		clone := primary.smapowner.get().clone()
		clone.addTarget(&di)
		primary.smapowner.put(clone)
		syncer.sync(true, primary.bmdowner.get())
		<-ch

		// sync smap so meta syncer has a smap
		syncer.sync(true, clone)
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
		clone = primary.smapowner.get().clone()
		clone.addTarget(&di)
		primary.smapowner.put(clone)

		syncer.sync(true, primary.bmdowner.get())
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
		defer primary.callStatsServer.Stop()
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
		clone := primary.smapowner.get().clone()
		clone.addProxy(
			&daemonInfo{
				DaemonID:   "proxy1",
				DirectURL:  s.URL,
				NodeIPAddr: ip,
				DaemonPort: port,
			},
		)
		primary.smapowner.put(clone)

		proxy1 := proxyrunner{}
		proxy1.smapowner = &smapowner{}
		proxy1.smapowner.put(newSmap())
		proxy1.bmdowner = &bmdowner{}
		proxy1.bmdowner.put(newBucketMD())

		// empty payload
		newSMap, oldSMap, actMsg, errStr := proxy1.extractSmap(make(map[string]string))
		if newSMap != nil || oldSMap != nil || actMsg != nil || errStr != "" {
			t.Fatal("Extract smap from empty payload returned data")
		}

		syncer.sync(true, primary.smapowner.get())
		payload := <-chProxy

		newSMap, oldSMap, actMsg, errStr = proxy1.extractSmap(payload)
		noErr(errStr)
		emptyActionMsg(actMsg)
		emptySMap(oldSMap)
		matchSMap(primary.smapowner.get(), newSMap)
		proxy1.smapowner.put(newSMap)

		// same version of smap received
		newSMap, oldSMap, actMsg, errStr = proxy1.extractSmap(payload)
		noErr(errStr)
		emptyActionMsg(actMsg)
		emptySMap(oldSMap)
		nilSMap(newSMap)

		// older version of smap received
		clone = proxy1.smapowner.get().clone()
		clone.Version++
		proxy1.smapowner.put(clone)
		_, _, _, errStr = proxy1.extractSmap(payload)
		hasErr(errStr)
		clone.Version--
		proxy1.smapowner.put(clone)

		var am ActionMsg
		y := &ActionMsg{"", "", primary.smapowner.get()}
		b, _ := json.Marshal(y)
		json.Unmarshal(b, &am)
		prevSMap := primary.smapowner.get()

		s = httptest.NewServer(http.HandlerFunc(fTarget))
		defer s.Close()
		ip, port = getServerIPAndPort(s.URL)

		clone = primary.smapowner.get().clone()
		clone.addTarget(
			&daemonInfo{
				DaemonID:   "target1",
				DirectURL:  s.URL,
				NodeIPAddr: ip,
				DaemonPort: port,
			})
		primary.smapowner.put(clone)

		target1 := targetrunner{}
		target1.smapowner = &smapowner{}
		target1.smapowner.put(newSmap())
		target1.bmdowner = &bmdowner{}
		target1.bmdowner.put(newBucketMD())

		c := func(n, o *Smap, m *ActionMsg, e string) {
			noErr(e)
			matchActionMsg(&am, m)
			matchPrevSMap(prevSMap, o)
			matchSMap(primary.smapowner.get(), n)
		}

		syncer.sync(true, primary.smapowner.get())
		payload = <-chProxy
		newSMap, oldSMap, actMsg, errStr = proxy1.extractSmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		payload = <-chTarget
		newSMap, oldSMap, actMsg, errStr = target1.extractSmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		// note: this extra call is not necessary by meta syncer
		// it picked up the target as it is new added target, but there is no other meta needs to be synced
		// (smap is the only one and it is already synced)
		payload = <-chTarget

		y = &ActionMsg{"", "", primary.smapowner.get()}
		b, _ = json.Marshal(y)
		json.Unmarshal(b, &am)
		prevSMap = primary.smapowner.get()

		s = httptest.NewServer(http.HandlerFunc(fTarget))
		defer s.Close()
		ip, port = getServerIPAndPort(s.URL)
		clone = primary.smapowner.get().clone()
		clone.addTarget(
			&daemonInfo{
				DaemonID:   "target2",
				DirectURL:  s.URL,
				NodeIPAddr: ip,
				DaemonPort: port,
			})
		primary.smapowner.put(clone)

		target2 := targetrunner{}
		target2.smapowner = &smapowner{}
		target2.smapowner.put(newSmap())
		target2.bmdowner = &bmdowner{}
		target2.bmdowner.put(newBucketMD())

		syncer.sync(true, primary.smapowner.get())
		payload = <-chProxy
		newSMap, oldSMap, actMsg, errStr = proxy1.extractSmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		payload = <-chTarget
		newSMap, oldSMap, actMsg, errStr = target1.extractSmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		payload = <-chTarget
		newSMap, oldSMap, actMsg, errStr = target2.extractSmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		// same as above, extra sync caused by newly added target
		payload = <-chTarget

		// extract bucketmd
		// empty payload
		lb, actMsg, errStr := proxy1.extractbucketmd(make(simplekvs))
		if lb != nil || actMsg != nil || errStr != "" {
			t.Fatal("Extract bucketmd from empty payload returned data")
		}

		bucketmd := newBucketMD()
		syncer.sync(true, bucketmd)
		payload = <-chProxy
		lb, actMsg, errStr = proxy1.extractbucketmd(make(simplekvs))
		if lb != nil || actMsg != nil || errStr != "" {
			t.Fatal("Extract bucketmd from empty payload returned data")
		}

		payload = <-chTarget
		payload = <-chTarget

		bucketmd.add("lb1", true, BucketProps{CloudProvider: ProviderDfc})
		bucketmd.add("lb2", true, BucketProps{CloudProvider: ProviderAmazon})
		syncer.sync(true, bucketmd)
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

		am1 := ActionMsg{"Action", "Name", "Expecting this back"}
		syncer.sync(true, &revspair{bucketmd, &am1})
		payload = <-chTarget
		lb, actMsg, errStr = proxy1.extractbucketmd(payload)
		matchActionMsg(&am1, actMsg)

		payload = <-chProxy
		payload = <-chTarget

		y = &ActionMsg{"New proxy", "", primary.smapowner.get()}
		b, _ = json.Marshal(y)
		json.Unmarshal(b, &am)
		prevSMap = primary.smapowner.get()

		// new smap and new bucketmd pairs
		s = httptest.NewServer(http.HandlerFunc(fProxy))
		defer s.Close()
		ip, port = getServerIPAndPort(s.URL)
		clone = primary.smapowner.get().clone()
		clone.addProxy(
			&daemonInfo{
				DaemonID:   "proxy2",
				DirectURL:  s.URL,
				NodeIPAddr: ip,
				DaemonPort: port,
			})
		primary.smapowner.put(clone)

		proxy2 := proxyrunner{}
		proxy2.smapowner = &smapowner{}
		proxy2.smapowner.put(newSmap())
		proxy2.bmdowner = &bmdowner{}
		proxy2.bmdowner.put(newBucketMD())

		bucketmd.add("lb3", true, BucketProps{CloudProvider: ProviderGoogle})

		c = func(n, o *Smap, m *ActionMsg, e string) {
			noErr(e)
			matchActionMsg(&am, m)
			matchPrevSMap(prevSMap, o)
			matchSMap(primary.smapowner.get(), n)
		}

		clb := func(lb *bucketMD, m *ActionMsg, e string) {
			noErr(e)
			matchActionMsg(&am1, m)
			matchBMD(bucketmd, lb)
		}

		syncer.sync(true, &revspair{bucketmd, &am1}, &revspair{primary.smapowner.get(), &ActionMsg{"New proxy", "", nil}})
		payload = <-chProxy
		lb, actMsg, errStr = proxy2.extractbucketmd(payload)
		clb(lb, actMsg, errStr)
		newSMap, oldSMap, actMsg, errStr = proxy2.extractSmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		payload = <-chTarget
		lb, actMsg, errStr = target2.extractbucketmd(payload)
		clb(lb, actMsg, errStr)
		newSMap, oldSMap, actMsg, errStr = target2.extractSmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		payload = <-chProxy
		lb, actMsg, errStr = proxy1.extractbucketmd(payload)
		clb(lb, actMsg, errStr)
		newSMap, oldSMap, actMsg, errStr = proxy1.extractSmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		payload = <-chTarget
		lb, actMsg, errStr = target1.extractbucketmd(payload)
		clb(lb, actMsg, errStr)
		newSMap, oldSMap, actMsg, errStr = target1.extractSmap(payload)
		c(newSMap, oldSMap, actMsg, errStr)

		// this is the extra send to the newly added proxy.
		// FIXME: meta sync has a bug here i believe unless it is by design.
		//        the previous sync has bucketmd and an action message, meta sync only saves the bucketmd but not
		//        the action message, so on this extra send, it picks up only the bucketmd.
		//        not a big deal for this case, but for retry case, retry proxy/target will not receive the
		//        action message but only receives the bucketmd.
		//        same for smap, no old smap is picked up during retry or for newly added servers
		payload = <-chProxy
		lb, actMsg, errStr = proxy2.extractbucketmd(payload)
		noErr(errStr)
		emptyActionMsg(actMsg)
		matchBMD(bucketmd, lb)
		newSMap, oldSMap, actMsg, errStr = proxy2.extractSmap(payload)
		noErr(errStr)
		emptyActionMsg(actMsg)
		emptySMap(oldSMap)
		matchSMap(primary.smapowner.get(), newSMap)

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
		defer primary.callStatsServer.Stop()
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
		clone := primary.smapowner.get().clone()
		clone.addTarget(
			&daemonInfo{
				DaemonID:   "target1",
				DirectURL:  s.URL,
				NodeIPAddr: ip,
				DaemonPort: port,
			})
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
		ip, port = getServerIPAndPort(s.URL)
		clone = primary.smapowner.get().clone()
		clone.addTarget(
			&daemonInfo{
				DaemonID:   "target2",
				DirectURL:  s.URL,
				NodeIPAddr: ip,
				DaemonPort: port,
			})
		primary.smapowner.put(clone)

		target2 := targetrunner{}
		target2.smapowner = &smapowner{}
		target2.smapowner.put(newSmap())
		target2.bmdowner = &bmdowner{}
		target2.bmdowner.put(newBucketMD())

		bucketmd := newBucketMD()
		bucketmd.add("lb1", true, BucketProps{})

		syncer.sync(true, primary.smapowner.get(), bucketmd)
		<-chTarget
		<-chTarget

		bucketmd.add("lb2", true, BucketProps{CloudProvider: ProviderDfc})
		syncer.sync(false, &revspair{bucketmd, &ActionMsg{Action: "NB"}})
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
