// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cluster/mock"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	jsoniter "github.com/json-iterator/go"
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
func newDiscoverServerPrimary() *proxy {
	var (
		p       = &proxy{}
		tracker = mock.NewStatsTracker()
	)
	p.si = meta.NewSnode("primary", apc.Proxy, meta.NetInfo{}, meta.NetInfo{}, meta.NetInfo{})
	p.client.data = &http.Client{}
	p.client.control = &http.Client{}

	config := cmn.GCO.BeginUpdate()
	config.Keepalive.Proxy.Name = "heartbeat"
	config.Timeout.Startup = cos.Duration(4 * time.Second)
	config.Timeout.CplaneOperation = cos.Duration(2 * time.Second)
	config.Timeout.MaxKeepalive = cos.Duration(4 * time.Second)
	config.Client.Timeout = cos.Duration(10 * time.Second)
	config.Client.TimeoutLong = cos.Duration(10 * time.Second)
	config.Cksum.Type = cos.ChecksumXXHash
	cmn.GCO.CommitUpdate(config)

	p.owner.smap = newSmapOwner(config)
	p.owner.smap.put(newSmap())
	owner := newBMDOwnerPrx(config)
	owner.put(newBucketMD())
	p.owner.bmd = owner
	p.keepalive = newPalive(p, tracker, atomic.NewBool(true))
	return p
}

// discoverServerDefaultHandler returns the Smap and BMD with the given version
func discoverServerDefaultHandler(sv, lv int64) *httptest.Server {
	smapVersion := sv
	bmdVersion := lv
	return httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			msg := cluMeta{
				VoteInProgress: false,
				Smap:           &smapX{Smap: meta.Smap{Version: smapVersion}},
				BMD:            &bucketMD{BMD: meta.BMD{Version: bmdVersion}},
			}
			b, _ := jsoniter.Marshal(msg)
			w.Write(b)
		},
	))
}

// discoverServerVoteOnceHandler returns vote in progress on the first time it is call, returns
// Smap and BMD on subsequent calls
func discoverServerVoteOnceHandler(sv, lv int64) *httptest.Server {
	cnt := 0
	smapVersion := sv
	bmdVersion := lv
	f := func(w http.ResponseWriter, r *http.Request) {
		cnt++
		msg := cluMeta{
			VoteInProgress: cnt == 1,
			Smap:           &smapX{Smap: meta.Smap{Version: smapVersion}},
			BMD:            &bucketMD{BMD: meta.BMD{Version: bmdVersion}},
		}
		b, _ := jsoniter.Marshal(msg)
		w.Write(b)
	}

	return httptest.NewServer(http.HandlerFunc(f))
}

// discoverServerFailTwiceHandler fails the first two calls and returns
// Smap abd BMD on subsequent calls
func discoverServerFailTwiceHandler(sv, lv int64) *httptest.Server {
	cnt := 0
	smapVersion := sv
	bmdVersion := lv
	f := func(w http.ResponseWriter, r *http.Request) {
		cnt++
		if cnt > 2 {
			msg := cluMeta{
				VoteInProgress: false,
				Smap:           &smapX{Smap: meta.Smap{Version: smapVersion}},
				BMD:            &bucketMD{BMD: meta.BMD{Version: bmdVersion}},
			}
			b, _ := jsoniter.Marshal(msg)
			w.Write(b)
		} else {
			http.Error(w, "retry", http.StatusUnavailableForLegalReasons)
		}
	}

	return httptest.NewServer(http.HandlerFunc(f))
}

// discoverServerAlwaysFailHandler always responds with error
func discoverServerAlwaysFailHandler(_, _ int64) *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "retry", http.StatusUnavailableForLegalReasons)
	}

	return httptest.NewServer(http.HandlerFunc(f))
}

// discoverServerVoteInProgressHandler always responds with vote in progress
func discoverServerVoteInProgressHandler(_, _ int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			msg := cluMeta{
				VoteInProgress: true,
				Smap:           &smapX{Smap: meta.Smap{Version: 12345}},
				BMD:            &bucketMD{BMD: meta.BMD{Version: 67890}},
			}
			b, _ := jsoniter.Marshal(msg)
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
		bmdVersion  int64         // use '0' if expecting nil Smap or BMD
		onlyLong    bool          // run only in long mode
	}{
		{
			"empty discovery Smap",
			[]discoverServer{},
			time.Millisecond,
			0,
			0,
			false,
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
			false,
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
			false,
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
			true,
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
			true,
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
			false,
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
			true,
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
			true,
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
			true,
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
			false,
		},
		{
			"zero BMD version",
			[]discoverServer{
				{"p1", true, 1, 0, discoverServerDefaultHandler},
				{"t1", false, 1, 0, discoverServerDefaultHandler},
			},
			time.Millisecond * 400,
			1,
			0,
			false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tools.CheckSkip(t, tools.SkipTestArgs{Long: tc.onlyLong})
			var (
				primary      = newDiscoverServerPrimary()
				discoverSmap = newSmap()
			)

			for _, s := range tc.servers {
				ts := s.httpHandler(s.smapVersion, s.bmdVersion)
				addrInfo := serverTCPAddr(ts.URL)
				if s.isProxy {
					discoverSmap.addProxy(meta.NewSnode(s.id, apc.Proxy, addrInfo, addrInfo, addrInfo))
				} else {
					discoverSmap.addTarget(meta.NewSnode(s.id, apc.Target, addrInfo, addrInfo, addrInfo))
				}
			}
			svm := primary.uncoverMeta(discoverSmap)
			if tc.smapVersion == 0 {
				if svm.Smap != nil && svm.Smap.version() > 0 {
					t.Errorf("test case %q: expecting nil Smap", tc.name)
				}
			} else {
				if svm.Smap == nil || svm.Smap.version() == 0 {
					t.Errorf("test case %q: expecting non-empty Smap", tc.name)
				} else if tc.smapVersion != svm.Smap.Version {
					t.Errorf("test case %q: expecting %d, got %d", tc.name, tc.smapVersion, svm.Smap.Version)
				}
			}

			if tc.bmdVersion == 0 {
				if svm.BMD != nil && svm.BMD.version() > 0 {
					t.Errorf("test case %q: expecting nil BMD", tc.name)
				}
			} else {
				if svm.BMD == nil || svm.BMD.version() == 0 {
					t.Errorf("test case %q: expecting non-empty BMD", tc.name)
				} else if tc.bmdVersion != svm.BMD.Version {
					t.Errorf("test case %q: expecting %d, got %d", tc.name, tc.bmdVersion, svm.BMD.Version)
				}
			}
		})
	}
}
