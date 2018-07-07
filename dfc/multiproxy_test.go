/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package dfc

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/dfc/statsd"
)

type (
	discoverServerHandler func(sv int64, lv int64) *httptest.Server

	discoverServer struct {
		id          string
		isProxy     bool
		smapVersion int64
		bmdVersion  int64
		httpHandler discoverServerHandler
	}
)

// newDiscoverServerPrimary returns a proxy runner after initializing the fields that are needed by this test
func newDiscoverServerPrimary() *proxyrunner {
	p := proxyrunner{}
	p.si = &daemonInfo{DaemonID: "primary"}
	p.smapowner = &smapowner{}
	p.httpclientLongTimeout = &http.Client{}
	ctx.config.KeepaliveTracker.Proxy.Name = "heartbeat"
	p.kalive = newproxykalive(&p)
	p.callStatsServer = NewCallStatsServer(nil, 1, &statsd.Client{})
	p.callStatsServer.Start()
	return &p
}

// discoverServerDefaultHandler returns the Smap and bucket-metadata with the given version
func discoverServerDefaultHandler(sv int64, lv int64) *httptest.Server {
	smapVersion := sv
	bmdVersion := lv
	return httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			msg := SmapVoteMsg{
				VoteInProgress: false,
				Smap:           &Smap{Version: smapVersion},
				BucketMD:       &bucketMD{Version: bmdVersion},
			}
			b, _ := json.Marshal(msg)
			w.Write(b)
		},
	))
}

// discoverServerVoteOnceHandler returns vote in progress on the first time it is call, returns
// Smap and bucket-metadata on subsequent calls
func discoverServerVoteOnceHandler(sv int64, lv int64) *httptest.Server {
	cnt := 0
	smapVersion := sv
	bmdVersion := lv
	f := func(w http.ResponseWriter, r *http.Request) {
		cnt++
		msg := SmapVoteMsg{
			VoteInProgress: cnt == 1,
			Smap:           &Smap{Version: smapVersion},
			BucketMD:       &bucketMD{Version: bmdVersion},
		}
		b, _ := json.Marshal(msg)
		w.Write(b)
	}

	return httptest.NewServer(http.HandlerFunc(f))
}

// discoverServerFailTwiceHandler fails the first two calls and returns
// Smap abd bucket-metadata on subsequent calls
func discoverServerFailTwiceHandler(sv int64, lv int64) *httptest.Server {
	cnt := 0
	smapVersion := sv
	bmdVersion := lv
	f := func(w http.ResponseWriter, r *http.Request) {
		cnt++
		if cnt > 2 {
			msg := SmapVoteMsg{
				VoteInProgress: false,
				Smap:           &Smap{Version: smapVersion},
				BucketMD:       &bucketMD{Version: bmdVersion},
			}
			b, _ := json.Marshal(msg)
			w.Write(b)
		} else {
			http.Error(w, "retry", http.StatusUnavailableForLegalReasons)
		}
	}

	return httptest.NewServer(http.HandlerFunc(f))
}

// discoverServerAlwaysFailHandler always responds with error
func discoverServerAlwaysFailHandler(sv int64, lv int64) *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "retry", http.StatusUnavailableForLegalReasons)
	}

	return httptest.NewServer(http.HandlerFunc(f))
}

// discoverServerVoteInProgressHandler always responds with vote in progress
func discoverServerVoteInProgressHandler(sv int64, lv int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			msg := SmapVoteMsg{
				VoteInProgress: true,
				Smap:           &Smap{Version: 12345},
				BucketMD:       &bucketMD{Version: 67890},
			}
			b, _ := json.Marshal(msg)
			w.Write(b)
		},
	))
}

func TestDiscoverServers(t *testing.T) {
	tcs := []struct {
		name        string
		servers     []discoverServer
		duration    time.Duration // how long to wait for discover servers call
		smapVersion int64         // expected return from discover servers
		bmdVersion  int64         // use '0' if expecting nil Smap or bucket-metadata
	}{
		{
			"empty discovery Smap",
			[]discoverServer{},
			time.Millisecond,
			0,
			0,
		},
		{
			"all agreed",
			[]discoverServer{
				{"p1", true, 1, 2, discoverServerDefaultHandler},
				{"t1", false, 1, 2, discoverServerDefaultHandler},
			},
			time.Millisecond,
			1,
			2,
		},
		{
			"mixed",
			[]discoverServer{
				{"p1", true, 1, 2, discoverServerDefaultHandler},
				{"t1", false, 4, 5, discoverServerDefaultHandler},
				{"t2", false, 1, 2, discoverServerDefaultHandler},
			},
			time.Millisecond,
			4,
			5,
		},
		{
			"voting",
			[]discoverServer{
				{"t1", false, 4, 5, discoverServerVoteInProgressHandler},
				{"t2", false, 1, 2, discoverServerVoteInProgressHandler},
			},
			time.Millisecond * 300,
			0,
			0,
		},
		{
			"voting and map mixed",
			[]discoverServer{
				{"t1", false, 4, 5, discoverServerVoteInProgressHandler},
				{"t2", false, 1, 2, discoverServerDefaultHandler},
			},
			time.Millisecond * 300,
			0,
			0,
		},
		{
			"vote once",
			[]discoverServer{
				{"t1", false, 4, 5, discoverServerVoteOnceHandler},
				{"t2", false, 1, 2, discoverServerDefaultHandler},
			},
			time.Millisecond * 3000,
			4,
			5,
		},
		{
			"fail twice",
			[]discoverServer{
				{"t1", false, 4, 5, discoverServerFailTwiceHandler},
				{"t2", false, 1, 2, discoverServerDefaultHandler},
			},
			time.Millisecond * 3000,
			4,
			5,
		},
		{
			"all failed",
			[]discoverServer{
				{"t1", false, 4, 5, discoverServerAlwaysFailHandler},
				{"t2", false, 1, 2, discoverServerAlwaysFailHandler},
			},
			time.Millisecond * 400,
			0,
			0,
		},
		{
			"fail and good mixed",
			[]discoverServer{
				{"t1", false, 4, 5, discoverServerDefaultHandler},
				{"t2", false, 1, 2, discoverServerAlwaysFailHandler},
			},
			time.Millisecond * 400,
			4,
			5,
		},
		{
			"zero Smap version",
			[]discoverServer{
				{"p1", true, 0, 3, discoverServerDefaultHandler},
				{"t1", false, 0, 4, discoverServerDefaultHandler},
			},
			time.Millisecond * 400,
			0,
			4,
		},
		{
			"zero bucket-metadata version",
			[]discoverServer{
				{"p1", true, 1, 0, discoverServerDefaultHandler},
				{"t1", false, 1, 0, discoverServerDefaultHandler},
			},
			time.Millisecond * 400,
			1,
			0,
		},
	}

	for _, tc := range tcs {
		primary := newDiscoverServerPrimary()
		defer primary.callStatsServer.Stop()

		discoverSmap := newSmap()
		for _, s := range tc.servers {
			ts := s.httpHandler(s.smapVersion, s.bmdVersion)
			ip, port := getServerIPAndPort(ts.URL)
			if s.isProxy {
				discoverSmap.addProxy(&daemonInfo{DaemonID: s.id, NodeIPAddr: ip, DaemonPort: port})
			} else {
				discoverSmap.addTarget(&daemonInfo{DaemonID: s.id, NodeIPAddr: ip, DaemonPort: port})
			}
		}
		primary.smapowner.put(discoverSmap)
		smap, bucketmd := primary.meta(time.Now().Add(tc.duration))
		if tc.smapVersion == 0 {
			if smap != nil && smap.version() > 0 {
				t.Errorf("test case %s: expecting nil Smap", tc.name)
			}
		} else {
			if smap == nil || smap.version() == 0 {
				t.Errorf("test case %s: expecting non-empty Smap", tc.name)
			} else if tc.smapVersion != smap.Version {
				t.Errorf("test case %s: expecting %d, got %d", tc.name, tc.smapVersion, smap.Version)
			}
		}

		if tc.bmdVersion == 0 {
			if bucketmd != nil && bucketmd.version() > 0 {
				t.Errorf("test case %s: expecting nil bucket-metadata", tc.name)
			}
		} else {
			if bucketmd == nil || bucketmd.version() == 0 {
				t.Errorf("test case %s: expecting non-empty bucket-metadata", tc.name)
			} else if tc.bmdVersion != bucketmd.Version {
				t.Errorf("test case %s: expecting %d, got %d", tc.name, tc.bmdVersion, bucketmd.Version)
			}
		}
	}
}
