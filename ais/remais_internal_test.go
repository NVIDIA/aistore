// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestRemaisRefreshPending(t *testing.T) {
	for _, blocked := range []int32{1, remaisRetries} {
		name := "in-flight"
		if blocked == remaisRetries {
			name = "last-attempt"
		}
		t.Run(name, func(t *testing.T) {
			entered, resume := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := calls.Add(1)
				if n < blocked {
					http.Error(w, "retry", http.StatusServiceUnavailable)
					return
				}
				version, remoteURL := int64(2), "http://original"
				if n == blocked {
					close(entered)
					select {
					case <-resume:
					case <-r.Context().Done():
						return
					}
					version, remoteURL = 1, "http://bridge"
				}
				w.Header().Set(cos.HdrContentType, cos.ContentJSON)
				_ = json.NewEncoder(w).Encode(&meta.RemAisVec{
					Ver: version, A: []*meta.RemAis{{UUID: "remote", Alias: "remais", URL: remoteURL}},
				})
			}))
			t.Cleanup(srv.Close)
			t.Cleanup(release)

			p, config := newRemaisProxy(t, srv)
			first := make(chan struct{})
			go func() {
				p.remaisRefresh(&config)
				close(first)
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				release()
				<-first
				t.Fatal("refresh did not reach the target")
			}

			restored := config
			restored.Version = 2
			p.remaisRefresh(&restored)
			release()
			<-first

			waitRemais(t, p, 2, "http://original")
			tassert.Fatalf(t, calls.Load() > blocked, "expected another fetch after the obsolete reply")
		})
	}
}

// the target is yet to apply the requested version: adopt what it has, catch up in the background
func TestRemaisRefreshOnce(t *testing.T) {
	var (
		calls   atomic.Int32
		applied = make(chan struct{})
		release = sync.OnceFunc(func() { close(applied) })
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		version, remoteURL := int64(1), "http://bridge"
		if calls.Add(1) > 1 {
			select {
			case <-applied:
			case <-r.Context().Done():
				return
			}
			version, remoteURL = 2, "http://original"
		}
		w.Header().Set(cos.HdrContentType, cos.ContentJSON)
		_ = json.NewEncoder(w).Encode(&meta.RemAisVec{
			Ver: version, A: []*meta.RemAis{{UUID: "remote", Alias: "remais", URL: remoteURL}},
		})
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(release)

	p, config := newRemaisProxy(t, srv)
	p.remais.want = 2
	p.remaisRefreshOnce(&config)

	p.remais.mu.RLock()
	version, num := p.remais.Ver, len(p.remais.A)
	p.remais.mu.RUnlock()
	tassert.Fatalf(t, version == 1 && num == 1, "expected to adopt v1, have v%d (%d)", version, num)

	release()
	waitRemais(t, p, 2, "http://original")
}

func newRemaisProxy(t *testing.T, srv *httptest.Server) (*proxy, cmn.ClusterConfig) {
	t.Helper()
	p := &proxy{}
	p.si = newSnode("p-remais-test", apc.Proxy)
	p.keepalive = &palive{p: p, keepalive: keepalive{hb: newHB(time.Second)}}
	p.startup.cluster.Store(1)
	p.svs.init()
	peer := newSnode("t-remais-test", apc.Target)
	peer.ControlNet.URL = srv.URL
	smap := newSmap()
	smap.addProxy(p.si)
	smap.addTarget(peer)
	smap.Primary = p.si
	p.owner.smap = &smapOwner{}
	p.owner.smap.smap.Store(smap)

	oldClient := g.client
	t.Cleanup(func() { g.client = oldClient })
	g.client.cplane, g.client.maxkalive = srv.Client(), srv.Client()
	g.client.control, g.client.data = srv.Client(), srv.Client()

	config := cmn.GCO.Get().ClusterConfig
	timeouts := config.Timeout
	timeouts.CplaneOperation = cos.Duration(time.Millisecond)
	timeouts.MaxKeepalive = cos.Duration(time.Millisecond)
	config.Timeout, config.Version = timeouts, 1
	return p, config
}

func waitRemais(t *testing.T, p *proxy, ver int64, remoteURL string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		p.remais.mu.RLock()
		version := p.remais.Ver
		correct := version == ver && len(p.remais.A) == 1 && p.remais.A[0].URL == remoteURL
		p.remais.mu.RUnlock()
		if correct && !p.remais.in.Load() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected remais v%d (%s), have v%d", ver, remoteURL, version)
		}
		time.Sleep(time.Millisecond)
	}
}
