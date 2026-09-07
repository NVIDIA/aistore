// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools/tassert"

	jsoniter "github.com/json-iterator/go"
	"github.com/tinylib/msgp/msgp"
)

func newTestLsoEntries(n int) cmn.LsoEntries {
	entries := make(cmn.LsoEntries, 0, n)
	for i := range n {
		entries = append(entries, &cmn.LsoEnt{Name: fmt.Sprintf("obj-%05d", i)})
	}
	return entries
}

func newTestDupLsoEntries(n, ntargets int) (cmn.LsoEntries, cmn.LsoEntries) {
	entries := newTestLsoEntries(n)
	dup := make(cmn.LsoEntries, 0, n*ntargets)
	for _, en := range entries {
		for range ntargets {
			dup = append(dup, en)
		}
	}
	return entries, dup
}

// apc.LsNoRecursion: de-duplication must not decide truncation on its own.
func TestFinLsoADedup(t *testing.T) {
	const (
		maxSize  = 10
		ntargets = 3
	)

	tests := []struct {
		name      string
		numEnt    int
		hasMore   bool
		wantCnt   int
		wantToken bool
	}{
		{name: "dedup-over-page", numEnt: maxSize + 2, hasMore: false, wantCnt: maxSize, wantToken: true},
		{name: "dedup-exact-page-done", numEnt: maxSize, hasMore: false, wantCnt: maxSize, wantToken: false},
		{name: "dedup-exact-page-more", numEnt: maxSize, hasMore: true, wantCnt: maxSize, wantToken: true},
		{name: "dedup-short-page-done", numEnt: maxSize - 3, hasMore: false, wantCnt: maxSize - 3, wantToken: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lsmsg := &apc.LsoMsg{PageSize: maxSize, Flags: apc.LsNoRecursion}
			entries, dup := newTestDupLsoEntries(test.numEnt, ntargets)
			objs := &cmn.LsoRes{Entries: dup}

			finLsoA(objs, lsmsg, test.hasMore)

			tassert.Fatalf(t, len(objs.Entries) == test.wantCnt,
				"expected %d entries, got %d", test.wantCnt, len(objs.Entries))
			for i, en := range objs.Entries {
				tassert.Fatalf(t, en.Name == entries[i].Name,
					"entry %d: expected %q, got %q", i, entries[i].Name, en.Name)
			}
			if !test.wantToken {
				tassert.Fatalf(t, objs.ContinuationToken == "",
					"expected no continuation token, got %q", objs.ContinuationToken)
				return
			}
			want := entries[test.wantCnt-1].Name
			tassert.Fatalf(t, objs.ContinuationToken == want,
				"expected continuation token %q, got %q", want, objs.ContinuationToken)
		})
	}
}

func TestFinLsoA(t *testing.T) {
	const maxSize = 10

	tests := []struct {
		name      string
		numEnt    int
		hasMore   bool
		wantCnt   int
		wantToken bool
	}{
		{name: "over-page", numEnt: maxSize + 1, hasMore: true, wantCnt: maxSize, wantToken: true},
		{name: "exact-page-done", numEnt: maxSize, hasMore: false, wantCnt: maxSize, wantToken: false},
		{name: "exact-page-more", numEnt: maxSize, hasMore: true, wantCnt: maxSize, wantToken: true},
		{name: "short-page-done", numEnt: maxSize - 1, hasMore: false, wantCnt: maxSize - 1, wantToken: false},
		{name: "short-page-more", numEnt: maxSize - 1, hasMore: true, wantCnt: maxSize - 1, wantToken: true},
		{name: "empty-page", numEnt: 0, hasMore: false, wantCnt: 0, wantToken: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lsmsg := &apc.LsoMsg{PageSize: maxSize}
			objs := &cmn.LsoRes{Entries: newTestLsoEntries(test.numEnt)}

			finLsoA(objs, lsmsg, test.hasMore)

			tassert.Fatalf(t, len(objs.Entries) == test.wantCnt,
				"expected %d entries, got %d", test.wantCnt, len(objs.Entries))
			if !test.wantToken {
				tassert.Fatalf(t, objs.ContinuationToken == "",
					"expected no continuation token, got %q", objs.ContinuationToken)
				return
			}
			want := objs.Entries[test.wantCnt-1].Name
			tassert.Fatalf(t, objs.ContinuationToken == want,
				"expected continuation token %q, got %q", want, objs.ContinuationToken)
		})
	}
}

func lsoTestSigner(t *testing.T, p *proxy) {
	t.Helper()
	pub, priv, err := cos.GenerateNodeKeyPair()
	tassert.CheckFatal(t, err)
	p.si.VerifyingKey = pub
	p.nodeKeyPair = cos.NewNodeKeyPair(priv, pub)
	// A fresh grace window signs with either configured policy.
	p.svs.cur.Store(&_sv{on: true, last: mono.NanoTime()})
}

type lsoRoundTrip func(*http.Request) (*http.Response, error)

func (f lsoRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func lsoTestNodes() (*proxy, *meta.Snode, *smapX) {
	p := &proxy{}
	p.si = &meta.Snode{}
	p.si.Init("ingress", apc.Proxy, nil)
	peer := &meta.Snode{}
	peer.Init("owner", apc.Proxy, nil)
	peer.ControlNet.Init("http", "127.0.0.1", "9081")
	smap := newSmap()
	smap.Version, smap.vstr = 1, "1"
	smap.Primary = p.si
	smap.Pmap[p.SID()], smap.Pmap[peer.ID()] = p.si, peer
	smap.InitDigests()
	return p, peer, smap
}

func TestLsoMappedOwner(t *testing.T) {
	cos.InitShortID(0)
	p, peer, smap := lsoTestNodes()
	msg := &apc.LsoMsg{}
	owner, first, err := p.lsOwner(msg, smap)
	tassert.Fatalf(t, err == nil && first && cos.IsValidUUID(msg.UUID), "first request: owner=%v, new=%t, uuid=%q, err=%v", owner, first, msg.UUID, err)
	uuid := msg.UUID
	next, first, err := p.lsOwner(msg, smap)
	tassert.Fatalf(t, err == nil && !first && next == owner && msg.UUID == uuid, "next page changed ownership or identity: owner=%v, new=%t, err=%v", next, first, err)
	selected, err := smap.HrwProxyTask(uuid)
	tassert.CheckFatal(t, err)
	local := &proxy{}
	local.si = selected
	dst, _, err := local.lsOwner(msg, smap)
	tassert.Fatalf(t, err == nil && dst == nil, "owner must execute locally: dst=%v, err=%v", dst, err)

	p.si.Flags |= meta.SnodeMaint
	dst, _, err = p.lsOwner(msg, smap)
	tassert.Fatalf(t, err == nil && dst == peer, "inactive ingress must route to the sole active peer: dst=%v, err=%v", dst, err)
	peer.Flags |= meta.SnodeMaint
	_, _, err = p.lsOwner(msg, smap)
	tassert.Fatal(t, err != nil, "expected no active proxies error")
}

// Native and unregistered-remote requests preserve the prepared message.
// Signed S3 forwarding is exercised end-to-end below.
func TestLsoMappedForward(t *testing.T) {
	for _, unregistered := range []bool{false, true} {
		p, peer, smap := lsoTestNodes()
		lsoTestSigner(t, p)
		bck := meta.NewBck("bucket", apc.AWS, cmn.NsGlobal, &cmn.Bprops{BID: 1})
		msg := &apc.LsoMsg{UUID: "Xk3nZq4i1", Prefix: "foo*", PageSize: 2}
		want := lsoReq{Bck: bck.Clone(), LsoMsg: msg, New: true}
		want.Bck.Props = nil
		if unregistered {
			bck.Props.BID = 0
			msg.SetFlag(apc.LsDontAddRemote)
			want.Props = bck.Props
		}
		u, err := url.Parse(peer.ControlNet.URL)
		tassert.CheckFatal(t, err)
		rp := httputil.NewSingleHostReverseProxy(u)
		rp.Transport = lsoRoundTrip(func(r *http.Request) (*http.Response, error) {
			var got lsoReq
			tassert.CheckFatal(t, jsoniter.NewDecoder(r.Body).Decode(&got))
			tassert.Fatalf(t, reflect.DeepEqual(got, want), "request changed: got %+v, want %+v", got, want)
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("page"))}, nil
		})
		p.rproxy.nodes.Store(peer.ID(), &singleRProxy{rp: rp, u: u.String()})
		r := httptest.NewRequest(http.MethodGet, "/v1/buckets/bucket", http.NoBody)
		w := httptest.NewRecorder()
		p.forwardLSO(w, r, bck, msg, peer, smap, true, nil)
		tassert.Fatalf(t, w.Code == http.StatusOK && w.Body.String() == "page", "relay: %d %q", w.Code, w.Body.String())
	}
}

func TestLsoMappedRegistration(t *testing.T) {
	config := cmn.GCO.BeginUpdate()
	useCtrl := config.HostNet.UseIntraControl
	config.HostNet.UseIntraControl = true
	cmn.GCO.CommitUpdate(config)
	t.Cleanup(func() {
		config := cmn.GCO.BeginUpdate()
		config.HostNet.UseIntraControl = useCtrl
		cmn.GCO.CommitUpdate(config)
	})
	prev := g.netServ
	t.Cleanup(func() { g.netServ = prev })
	g.netServ.pub = &netServer{muxers: newMuxers(false)}
	g.netServ.control = &netServer{muxers: newMuxers(false)}
	g.netServ.data = &netServer{muxers: newMuxers(false)}
	p := &proxy{}
	p.statsT = mock.NewStatsTracker()
	p.initRecvHandlers() // must not register the public bucket route twice
	for _, srv := range []*netServer{g.netServ.pub, g.netServ.control} {
		r := httptest.NewRequest(http.MethodGet, "/v1/buckets/bucket", http.NoBody)
		_, pattern := srv.muxers[http.MethodGet].Handler(r)
		tassert.Fatalf(t, pattern == "/v1/buckets/", "bucket handler missing: pattern=%q", pattern)
	}
}

// Exercise signed forwarding, R-flow startup, and S3 pagination end-to-end,
// using in-memory transports for both the owner and the targets.
func TestLsoMappedS3Receiver(t *testing.T) {
	config := cmn.GCO.BeginUpdate()
	prevAuth := config.Auth.IntraCluster
	config.Auth.IntraCluster = &cmn.IntraClusterConf{RequestAuth: true}
	cmn.GCO.CommitUpdate(config)
	cmn.Rom.Set(&config.ClusterConfig)
	t.Cleanup(func() {
		config := cmn.GCO.BeginUpdate()
		config.Auth.IntraCluster = prevAuth
		cmn.GCO.CommitUpdate(config)
		cmn.Rom.Set(&config.ClusterConfig)
	})
	sender, peer, smap := lsoTestNodes()
	owner := &proxy{}
	owner.si = peer
	owner.statsT = mock.NewStatsTracker()
	owner.gmm = memsys.PageMM()
	for _, p := range []*proxy{sender, owner} {
		lsoTestSigner(t, p)
	}
	owner.startup.cluster.Store(mono.NanoTime())
	owner.owner.smap = &smapOwner{}
	owner.owner.smap.smap.Store(smap)
	for _, id := range []string{"target-a", "target-b"} {
		si := &meta.Snode{}
		si.Init(id, apc.Target, nil)
		si.ControlNet.Init("http", id, "9080")
		smap.Tmap[id] = si
	}
	smap.InitDigests()
	bck := meta.NewBck("bucket", apc.AWS, cmn.NsGlobal)
	bmd := newBucketMD()
	bmd.add(bck, &cmn.Bprops{})
	bo := newBMDOwnerPrx(cmn.GCO.Get())
	bo.put(bmd)
	owner.owner.bmd = bo

	var begins, commits, pages atomic.Int32
	tr := lsoRoundTrip(func(r *http.Request) (*http.Response, error) {
		var amsg struct {
			Value apc.LsoMsg `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&amsg); err != nil {
			return nil, err
		}
		tassert.Errorf(t, amsg.Value.UUID == "Xk3nZq4i1" && amsg.Value.Prefix == "foo*", "receiver changed prepared UUID/prefix: %+v", amsg.Value)
		resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}
		if strings.HasSuffix(r.URL.Path, "/"+apc.Begin2PC) {
			begins.Add(1)
			return resp, nil
		}
		if strings.HasSuffix(r.URL.Path, "/"+apc.Commit2PC) {
			commits.Add(1)
			tassert.Error(t, begins.Load() == 2, "commit before all target receive paths were initialized")
		}
		pages.Add(1)
		lst := &cmn.LsoRes{UUID: amsg.Value.UUID, ContinuationToken: "opaque\x00next", Entries: cmn.LsoEntries{
			&cmn.LsoEnt{Name: "foo*/" + r.URL.Hostname()},
		}}
		var body bytes.Buffer
		w := msgp.NewWriter(&body)
		if err := lst.EncodeMsg(w); err != nil {
			return nil, err
		}
		err := w.Flush()
		resp.Body = io.NopCloser(&body)
		return resp, err
	})
	prev := g.client
	t.Cleanup(func() { g.client = prev })
	client := &http.Client{Transport: tr, Timeout: time.Minute}
	g.client.control, g.client.cplane, g.client.maxkalive, g.client.data = client, client, client, client

	u, err := url.Parse(peer.ControlNet.URL)
	tassert.CheckFatal(t, err)
	rp := httputil.NewSingleHostReverseProxy(u)
	rp.Transport = lsoRoundTrip(func(r *http.Request) (*http.Response, error) {
		tassert.Fatalf(t, r.URL.Path == "/v1/buckets/bucket" && r.URL.RawQuery == "" && r.URL.Host == u.Host, "internal URL: %s", r.URL)
		r = r.WithContext(context.WithValue(r.Context(), keyReqNet, reqNetCtrl))
		if pages.Load() == 0 {
			bad := r.Clone(r.Context())
			bad.Body = http.NoBody // signature rejection must precede body decoding
			bad.Header.Set(apc.HdrSenderSig, strings.Repeat("A", sigLen()))
			w := httptest.NewRecorder()
			owner.bckCtrlHandler(w, bad)
			tassert.Fatalf(t, w.Code == http.StatusUnauthorized && begins.Load() == 0 && pages.Load() == 0, "bad signature: %d", w.Code)
		}
		w := httptest.NewRecorder()
		owner.bckCtrlHandler(w, r)
		return w.Result(), nil
	})
	sender.rproxy.nodes.Store(peer.ID(), &singleRProxy{rp: rp, u: u.String()})
	token := ""
	for page, size := range []int64{2, 2, 0} {
		msg := &apc.LsoMsg{UUID: "Xk3nZq4i1", Prefix: "foo*", PageSize: size, Flags: apc.LsIsS3}
		msg.AddProps(apc.GetPropsSize, apc.GetPropsChecksum, apc.GetPropsAtime, apc.GetPropsCustom)
		_, msg.ContinuationToken = s3.DecodeToken(token)
		r := httptest.NewRequest(http.MethodGet, "http://public/s3/bucket?prefix=foo*", http.NoBody)
		w := httptest.NewRecorder()
		before := pages.Load()
		sender.forwardLSO(w, r, bck, msg, peer, smap, page == 0, &token)
		tassert.Fatalf(t, w.Code == http.StatusOK && w.Header().Get(cos.HdrContentType) == cos.ContentXML, "S3 relay: %d %s", w.Code, w.Body.String())
		var out s3.ListObjectResult
		tassert.CheckFatal(t, xml.Unmarshal(w.Body.Bytes(), &out))
		tassert.Fatalf(t, out.ContinuationToken == token && int64(out.MaxKeys) == size, "S3 response lost original token/page size: %+v", out)
		if size == 0 {
			tassert.Fatal(t, pages.Load() == before && !out.IsTruncated && out.NextContinuationToken == "", "zero-size request must not fetch another page")
		} else {
			uuid, next := s3.DecodeToken(out.NextContinuationToken)
			tassert.Fatalf(t, uuid == msg.UUID && next == "opaque\x00next" && pages.Load()-before == 2, "incorrect next page: uuid=%q next=%q pages=%d", uuid, next, pages.Load()-before)
		}
		token = out.NextContinuationToken
	}
	tassert.Fatalf(t, begins.Load() == 2 && commits.Load() == 2 && pages.Load() == 4, "phased startup must run only on the first page: begin=%d commit=%d pages=%d", begins.Load(), commits.Load(), pages.Load())
}
