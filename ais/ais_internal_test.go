// Package ais: internal unit tests
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/xact/xreg"

	jsoniter "github.com/json-iterator/go"
)

//
// test setup
//

var mockTarget *target

func init() {
	xreg.Init()
	hk.Init(false)
}

func TestMain(m *testing.M) {
	flag.Parse()

	// file system
	// (idempotent: a previously panicking run may have skipped the cleanup below)
	os.RemoveAll(testMountpath)
	cos.CreateDir(testMountpath)
	defer os.RemoveAll(testMountpath)
	fs.NewTestMFS(nil)

	// config
	config := cmn.GCO.BeginUpdate()
	config.HostNet.Hostname = "localhost"
	config.HostNet.Port = 8080
	config.HostNet.HostnameIntraControl = "localhost"
	config.HostNet.PortIntraControl = 9080
	config.Timeout.CplaneOperation = cos.Duration(2 * time.Second)
	config.Timeout.MaxKeepalive = cos.Duration(4 * time.Second)
	config.Log.Level = "3"
	debug.AssertNoErr(config.HostNet.Validate(config))
	cmn.GCO.CommitUpdate(config)
	cmn.Rom.Set(&config.ClusterConfig)

	co := newConfigOwner(config)

	// target
	t := newTarget(co)
	t.initPhase1(config)
	tid, _ := initTID(config)
	t.si.Init(tid, apc.Target, nil /*verifying key*/)

	// NOTE: without this mountpath nothing downstream works - BMD cannot be
	// persisted, and every fs-dependent test then fails far away from the cause
	if _, err := fs.AddTestMpath(testMountpath, t.SID()); err != nil {
		cos.ExitLog("failed to add test mountpath", testMountpath, err)
	}

	t.htrun.initPhase2(config)
	t.ups.t = t

	t.statsT = mock.NewStatsTracker()
	core.Tinit(t, config, false)

	bck := meta.NewBck(testBucket, apc.AIS, cmn.NsGlobal)
	bmd := newBucketMD()
	bmd.add(bck, &cmn.Bprops{
		Cksum: cmn.CksumConf{
			Type: cos.ChecksumNone,
		},
	})
	t.owner.bmd.putPersist(bmd, nil)
	fs.CreateBucket(bck.Bucket(), false /*nilbmd*/)

	t.owner.smap.put(newSmap()) // required by the transaction timeout test

	mockTarget = t
	m.Run()
}

//
// shared node helper
//

func newSnode(id, daeType string) (snode *meta.Snode) {
	snode = &meta.Snode{}
	snode.Init(id, daeType, nil /*verifying key*/)
	return
}

//
// node ID
//

func TestPodIDSource(t *testing.T) {
	for _, test := range []struct {
		source, scope string
		ok            bool
	}{
		{source: "k8s-pod", ok: true},
		{source: "k8s-pod:6f1c2b7e-3d4a-4c5b-9e8f-0a1b2c3d4e5f", scope: "6f1c2b7e-3d4a-4c5b-9e8f-0a1b2c3d4e5f", ok: true},
		{source: "k8s-pod:a:b", scope: "a:b", ok: true},
		{source: "k8s-pod:"},
		{source: "k8s-node"},
		{source: "k8s-pod-v2"},
		{source: ":scope"},
	} {
		scope, ok := parsePodIDSource(test.source)
		tassert.Errorf(t, ok == test.ok, "%q: expected ok=%t, got %t", test.source, test.ok, ok)
		tassert.Errorf(t, !ok || scope == test.scope, "%q: expected scope %q, got %q", test.source, test.scope, scope)
	}
}

func TestPodDerivedID(t *testing.T) {
	const (
		ns    = "ais"
		pod   = "ais-target-1"
		scope = "6f1c2b7e-3d4a-4c5b-9e8f-0a1b2c3d4e5f"
	)
	// persisted on mountpaths and in cluster maps: the encoding must not change
	for _, test := range []struct {
		daeType, pod, scope, id string
	}{
		{apc.Target, pod, scope, "GBLgBUup"},
		{apc.Proxy, "ais-proxy-0", scope, "FfIXNSzh"},
		{apc.Target, pod, "", "xTKdSUkl"},
	} {
		id := derivePodID(test.daeType, ns, test.pod, test.scope)
		tassert.Errorf(t, id == test.id, "%s %s/%s (scope %q): expected %q, got %q",
			test.daeType, ns, test.pod, test.scope, test.id, id)
	}

	tid := derivePodID(apc.Target, ns, pod, scope)
	// same length and alphabet as a random node ID
	tassert.CheckFatal(t, cos.ValidateDaemonID(tid))
	tassert.Errorf(t, len(tid) == len(cos.GenDaemonID()), "%q: unexpected length", tid)
	tassert.Errorf(t, strings.Trim(tid, cos.LetterRunes) == "", "%q: unexpected alphabet", tid)

	// every input changes the result
	for _, other := range []string{
		derivePodID(apc.Proxy, ns, pod, scope),
		derivePodID(apc.Target, "ais2", pod, scope),
		derivePodID(apc.Target, ns, "ais-target-2", scope),
		derivePodID(apc.Target, ns, pod, "0a1b2c3d-4e5f-4c5b-9e8f-6f1c2b7e3d4a"),
		derivePodID(apc.Target, ns, pod, ""),
		// same concatenation, different fields
		derivePodID(apc.Target, "ais-ais", "target-1", scope),
		derivePodID(apc.Target, ns, pod+scope, ""),
	} {
		tassert.Errorf(t, other != tid, "unexpected collision with %q", tid)
		tassert.CheckError(t, cos.ValidateDaemonID(other))
	}
}

//
// query parsing
//

func TestDpqETLTransformArgs(t *testing.T) {
	for _, args := range []string{
		`{"value":"a+b%20&c"}`,
		`hello world`,
		`a/b?c=d&e=f`,
	} {
		q := url.Values{}
		q.Set(apc.QparamETLTransformArgs, args)

		dpq := dpqAlloc()
		err := dpq.parse(q.Encode())
		tassert.CheckFatal(t, err)

		actual := dpq.get(apc.QparamETLTransformArgs)
		tassert.Errorf(t, actual == args, "expected %q, got %q", args, actual)
		dpqFree(dpq)
	}
}

//
// metasync
//

func TestMetasyncProxyRejectsMissingIntraHeaders(t *testing.T) {
	p := &proxy{}
	p.si = newSnode("p1", apc.Proxy)
	p.owner.smap = newSmapOwner(cmn.GCO.Get(), false /*isTarget*/)
	p.owner.smap.put(newSmap())

	payload := make(msPayload).marshal(memsys.PageMM())
	defer payload.Free()
	req := httptest.NewRequest(http.MethodPut, apc.URLPathMetasync.S, bytes.NewReader(payload.Bytes()))
	req = req.WithContext(context.WithValue(req.Context(), keyReqNet, reqNetCtrl))
	w := httptest.NewRecorder()
	p.metasyncHandler(w, req)

	herr := &cmn.ErrHTTP{}
	tassert.CheckFatal(t, jsoniter.Unmarshal(w.Body.Bytes(), herr))
	tassert.Fatalf(t, w.Code == http.StatusBadRequest && herr.Message == errNotIntraControl.Error(),
		"expected missing intra-control headers to fail with %q, got status %d and %q",
		errNotIntraControl, w.Code, herr.Message)
}

func TestExtractConfigHydratesSparse(t *testing.T) {
	current := cmn.GCO.Get()

	src := &globalConfig{}
	src.Version = current.Version + 1
	src.UUID = current.UUID
	tassert.CheckFatal(t, src.ClusterConfig.HydrateOmittables())

	sgl := src._encode(0)
	defer sgl.Free()

	// Verify that _encode actually produced the representation that caused the
	// regression: default-omittable sections are absent on the wire.
	var sparse globalConfig
	_, err := jsp.Decode(bytes.NewReader(sgl.Bytes()), &sparse, sparse.JspOpts(), "sparse config test")
	tassert.CheckFatal(t, err)

	tassert.Fatalf(t,
		sparse.Log == nil &&
			sparse.Client == nil &&
			sparse.Space == nil &&
			sparse.Transport == nil &&
			sparse.GetBatch == nil &&
			sparse.LRU == nil &&
			sparse.FSHC == nil &&
			sparse.Keepalive == nil &&
			sparse.Rebalance == nil,
		"expected sparse config, got log=%v client=%v space=%v transport=%v get-batch=%v lru=%v fshc=%v keepalive=%v rebalance=%v",
		sparse.Log, sparse.Client, sparse.Space, sparse.Transport, sparse.GetBatch,
		sparse.LRU, sparse.FSHC, sparse.Keepalive, sparse.Rebalance)

	// Exercise the real metasync receive boundary, not HydrateOmittables directly.
	payload := msPayload{revsConfTag: sgl.Bytes()}
	got, _, err := (&htrun{}).extractConfig(payload, "test")
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, got != nil, "extractConfig returned nil config")

	tassert.Fatalf(t,
		got.Log != nil &&
			got.Client != nil &&
			got.Space != nil &&
			got.Transport != nil &&
			got.GetBatch != nil &&
			got.LRU != nil &&
			got.FSHC != nil &&
			got.Keepalive != nil &&
			got.Rebalance != nil,
		"decoded config not hydrated: log=%v client=%v space=%v transport=%v get-batch=%v lru=%v fshc=%v keepalive=%v rebalance=%v",
		got.Log, got.Client, got.Space, got.Transport, got.GetBatch,
		got.LRU, got.FSHC, got.Keepalive, got.Rebalance)

	tassert.Fatalf(t, reflect.DeepEqual(got.Log, src.Log),
		"log mismatch: got %+v, expected %+v", got.Log, src.Log)
	tassert.Fatalf(t, reflect.DeepEqual(got.Client, src.Client),
		"client mismatch: got %+v, expected %+v", got.Client, src.Client)
	tassert.Fatalf(t, reflect.DeepEqual(got.Space, src.Space),
		"space mismatch: got %+v, expected %+v", got.Space, src.Space)
	tassert.Fatalf(t, reflect.DeepEqual(got.Transport, src.Transport),
		"transport mismatch: got %+v, expected %+v", got.Transport, src.Transport)
	tassert.Fatalf(t, reflect.DeepEqual(got.GetBatch, src.GetBatch),
		"get-batch mismatch: got %+v, expected %+v", got.GetBatch, src.GetBatch)
	tassert.Fatalf(t, reflect.DeepEqual(got.LRU, src.LRU),
		"lru mismatch: got %+v, expected %+v", got.LRU, src.LRU)
	tassert.Fatalf(t, reflect.DeepEqual(got.FSHC, src.FSHC),
		"fshc mismatch: got %+v, expected %+v", got.FSHC, src.FSHC)
	tassert.Fatalf(t, reflect.DeepEqual(got.Keepalive, src.Keepalive),
		"keepalive mismatch: got %+v, expected %+v", got.Keepalive, src.Keepalive)
	tassert.Fatalf(t, reflect.DeepEqual(got.Rebalance, src.Rebalance),
		"rebalance mismatch: got %+v, expected %+v", got.Rebalance, src.Rebalance)
}

//
// moss scans
//

func newTestSmapForMossScan(nodeCount int) *smapX {
	smap := newSmap()
	for i := range nodeCount {
		id := fmt.Sprintf("t%d", i)
		si := newSnode(id, apc.Target)
		smap.Tmap[id] = si
	}
	return smap
}

// the proxy must authorize the same bucket the target will read; both sides resolve
// each entry through apc.MossIn, so a case-insensitive JSON key (e.g. "Bucket") maps identically
func TestMossScan_DecodeMatchesTarget(t *testing.T) {
	body := []byte(`{"in":[{"objname":"secret","Bucket":"private"}]}`)
	dflt := &meta.Bck{Name: "public", Provider: apc.AIS}

	// proxy side
	iter := jsoniter.ConfigFastest.BorrowIterator(body)
	bcks, _, err := mossScan(iter, dflt, newTestSmapForMossScan(1), 1, apc.ColocNone)
	jsoniter.ConfigFastest.ReturnIterator(iter)
	tassert.CheckFatal(t, err)

	// target side: how the target actually decodes the same body
	var req apc.MossReq
	tassert.CheckFatal(t, jsoniter.Unmarshal(body, &req))
	tgt, err := meta.BckFromUBP(req.In[0].Uname, req.In[0].Bucket, req.In[0].Provider)
	tassert.CheckFatal(t, err)

	// proxy must authorize the SAME bucket the target will read
	tassert.Fatalf(t, len(bcks) == 1 && bcks[0].Name == tgt.Name,
		"proxy authorized %q but target reads %q", bcks[0].Name, tgt.Name)
}

func TestMossScan_DecodeMatchesTargetTopLevelIn(t *testing.T) {
	body := []byte(`{"In":[{"objname":"secret","bucket":"private"}]}`)
	dflt := &meta.Bck{Name: "public", Provider: apc.AIS}

	iter := jsoniter.ConfigFastest.BorrowIterator(body)
	bcks, _, err := mossScan(iter, dflt, newTestSmapForMossScan(1), 1, apc.ColocNone)
	jsoniter.ConfigFastest.ReturnIterator(iter)
	tassert.CheckFatal(t, err)

	var req apc.MossReq
	tassert.CheckFatal(t, jsoniter.Unmarshal(body, &req))
	tgt, err := meta.BckFromUBP(req.In[0].Uname, req.In[0].Bucket, req.In[0].Provider)
	tassert.CheckFatal(t, err)

	tassert.Fatalf(t, len(bcks) == 1 && bcks[0].Name == tgt.Name,
		"proxy authorized %q but target reads %q", bcks[0].Name, tgt.Name)
}

//
// reverse proxy
//

type rpErrTransport struct {
	err error
}

func (t rpErrTransport) RoundTrip(*http.Request) (*http.Response, error) { return nil, t.err }

func TestReverseProxyErrorHandlers(t *testing.T) {
	p := newTestProxy(t, false /*requestAuthConfigured*/)
	si := newTestSnode(t)
	si.DataNet.Init("http", "dst", "9081")

	rawURL := si.URL(cmn.NetIntraData)
	u, err := url.Parse(rawURL)
	tassert.CheckFatal(t, err)

	relayErr := errors.New("test reverse-proxy failure")
	rproxy := httputil.NewSingleHostReverseProxy(u)
	rproxy.Transport = rpErrTransport{relayErr}
	rproxy.ErrorHandler = p.rpErrHandler
	p.rproxy.nodes.Store(si.ID(), &singleRProxy{rp: rproxy, u: rawURL})

	// The S3 override returns XML without changing the cached native handler.
	r := httptest.NewRequest(http.MethodGet, "/s3/bucket/object", http.NoBody)
	w := httptest.NewRecorder()
	p.s3ReverseRequest(w, r, si, p.owner.smap.get())

	tassert.Fatalf(t, w.Code == http.StatusBadGateway, "S3 relay: expected status %d, got %d", http.StatusBadGateway, w.Code)
	ctype := w.Header().Get(cos.HdrContentType)
	tassert.Fatalf(t, ctype == cos.ContentXML, "S3 relay: expected content type %q, got %q", cos.ContentXML, ctype)
	var serr s3.Error
	err = xml.Unmarshal(w.Body.Bytes(), &serr)
	tassert.Fatalf(t, err == nil, "S3 relay: invalid XML error %q: %v", w.Body.String(), err)
	tassert.Fatalf(t, serr.Message == relayErr.Error(), "S3 relay: expected message %q, got %q", relayErr, serr.Message)

	// Reuse the same cached reverse proxy without the per-request override.
	// Its original handler must still return the native bare status.
	r = httptest.NewRequest(http.MethodGet, "/v1/health", http.NoBody)
	w = httptest.NewRecorder()
	p.reverseRequest(w, r, si.ID(), rawURL)

	tassert.Fatalf(t, w.Code == http.StatusBadGateway, "native relay: expected status %d, got %d", http.StatusBadGateway, w.Code)
	tassert.Fatalf(t, w.Body.Len() == 0, "native relay: expected empty body, got %q", w.Body.String())
	ctype = w.Header().Get(cos.HdrContentType)
	tassert.Fatalf(t, ctype == "", "native relay: expected no content type, got %q", ctype)
}

func TestAltDNSNameTransports(t *testing.T) {
	prevServ, prevClient := g.netServ.control, g.client
	t.Cleanup(func() { g.netServ.control, g.client = prevServ, prevClient })
	g.netServ.control = &netServer{}

	config := &cmn.Config{}
	config.Client = &cmn.ClientConf{}
	config.Net.HTTP.UseHTTPS = true
	config.Net.HTTP.AltDNSName = "ais.example.com"

	tassert.Fatal(t, rpTransportIntra(config).DialTLSContext != nil, "cluster node relay: expected alt name verifier")
	tassert.Fatal(t, rpTransportExt(config).DialTLSContext == nil, "remote AIS and cloud relay: unexpected alt name verifier")

	data := cmn.NewIntraClientTLS(cmn.TransportArgs{}, config).Transport.(*http.Transport)
	tassert.Fatal(t, data.DialTLSContext == nil, "intra-data client: unexpected alt name verifier")

	g.client.control = &http.Client{Transport: rpTransportIntra(config)}
	tr := _x509Client().Transport.(*http.Transport)
	tassert.Fatal(t, tr.TLSClientConfig.InsecureSkipVerify && tr.DialTLSContext == nil, "x509 client: expected verification to be skipped")
}

//
// remote AIS
//

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

//
// backend rate limiting
//

const retryPutBody = "retry body"

type throttleBackend struct {
	core.Backend
	calls int
}

type throttleReaderBackend struct {
	core.Backend
	err   error
	calls int
}

func (bp *throttleBackend) errOnce() error {
	bp.calls++
	if bp.calls == 1 {
		return cmn.NewErrTooManyRequests(errors.New("throttled"), http.StatusTooManyRequests)
	}
	return nil
}

func (bp *throttleBackend) HeadObj(context.Context, *core.LOM, *http.Request) (*cmn.ObjAttrs, int, error) {
	err := bp.errOnce()
	if err != nil {
		return nil, http.StatusTooManyRequests, err
	}
	return &cmn.ObjAttrs{Size: 1}, 0, nil
}

func (bp *throttleBackend) GetObj(context.Context, *core.LOM, cmn.OWT, *http.Request) (int, error) {
	err := bp.errOnce()
	if err != nil {
		return http.StatusTooManyRequests, err
	}
	return 0, nil
}

func (bp *throttleBackend) PutObj(ctx context.Context, r io.ReadCloser, _ *core.LOM, _ *http.Request) (int, error) {
	body, err := io.ReadAll(r)
	cos.Close(r)
	if err != nil {
		return 0, err
	}
	if string(body) != retryPutBody || cmn.IsNoRetry(ctx) != (bp.calls != 0) {
		return 0, errors.New("unexpected PUT body or retry context")
	}
	if err := bp.errOnce(); err != nil {
		return http.StatusTooManyRequests, err
	}
	return 0, nil
}

func (bp *throttleReaderBackend) GetObjReader(context.Context, *core.LOM, int64, int64, ...*core.ObjPin) core.GetReaderResult {
	bp.calls++
	return core.GetReaderResult{Err: bp.err, ErrCode: http.StatusTooManyRequests}
}

func TestRLBackendObjectOps(t *testing.T) {
	tests := []struct {
		name string
		verb string
		do   func(*testing.T, *rlbackend, *core.LOM) error
	}{
		{
			name: http.MethodGet,
			verb: http.MethodGet,
			do: func(_ *testing.T, bp *rlbackend, lom *core.LOM) error {
				_, err := bp.GetObj(context.Background(), lom, cmn.OwtGet, nil)
				return err
			},
		},
		{
			name: http.MethodHead,
			verb: http.MethodHead,
			do: func(_ *testing.T, bp *rlbackend, lom *core.LOM) error {
				oa, _, err := bp.HeadObj(context.Background(), lom, nil)
				if err == nil && (oa == nil || oa.Size != 1) {
					return errors.New("invalid object attributes")
				}
				return err
			},
		},
		{
			name: http.MethodPut,
			verb: http.MethodPut,
			do: func(t *testing.T, bp *rlbackend, lom *core.LOM) error {
				path := filepath.Join(t.TempDir(), "workfile")
				tassert.CheckFatal(t, os.WriteFile(path, []byte(retryPutBody), 0o600))
				r, err := cos.NewFileHandle(path)
				tassert.CheckFatal(t, err)
				_, err = bp.PutObj(t.Context(), r, lom, nil)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lom := core.AllocLOM("rate-limit-" + test.name)
			defer core.FreeLOM(lom)
			err := lom.InitBck(meta.NewBck(testBucket, apc.AIS, cmn.NsGlobal))
			tassert.CheckFatal(t, err)

			conf := &lom.Bck().Props.RateLimit.Backend
			oldEnabled := conf.Enabled
			conf.Enabled = true
			defer func() { conf.Enabled = oldEnabled }()

			arl, err := cos.NewAdaptRateLim(100, 1, time.Second)
			tassert.CheckFatal(t, err)
			key := lom.Bck().HashUname(test.verb)
			mockTarget.ratelim.Store(key, arl)
			defer mockTarget.ratelim.Delete(key)

			backend := &throttleBackend{}
			bp := &rlbackend{Backend: backend, t: mockTarget}
			err = test.do(t, bp, lom)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, backend.calls == 2, "expected one retry, got %d calls", backend.calls)
		})
	}
}

func TestRLBackendHeadReturnsRetryError(t *testing.T) {
	lom := core.AllocLOM("rate-limit-head-retry-error")
	defer core.FreeLOM(lom)
	err := lom.InitBck(meta.NewBck(testBucket, apc.AIS, cmn.NsGlobal))
	tassert.CheckFatal(t, err)

	conf := &lom.Bck().Props.RateLimit.Backend
	oldEnabled := conf.Enabled
	conf.Enabled = true
	defer func() { conf.Enabled = oldEnabled }()

	arl, err := cos.NewAdaptRateLim(100, 1, time.Second)
	tassert.CheckFatal(t, err)
	key := lom.Bck().HashUname(http.MethodHead)
	mockTarget.ratelim.Store(key, arl)
	defer mockTarget.ratelim.Delete(key)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	backend := &throttleBackend{}
	bp := &rlbackend{Backend: backend, t: mockTarget}
	oa, ecode, err := bp.HeadObj(ctx, lom, nil)
	tassert.Fatalf(t, errors.Is(err, context.Canceled), "expected retry error %v, got %v", context.Canceled, err)
	tassert.Fatalf(t, ecode == 0, "expected retry code 0, got %d", ecode)
	tassert.Fatalf(t, oa == nil, "expected no object attributes, got %v", oa)
	tassert.Fatalf(t, backend.calls == 1, "expected no retry call, got %d calls", backend.calls)
}

func TestRLBackendGetObjReaderReturnsRetryError(t *testing.T) {
	lom := core.AllocLOM("rate-limit-reader-retry-error")
	defer core.FreeLOM(lom)
	err := lom.InitBck(meta.NewBck(testBucket, apc.AIS, cmn.NsGlobal))
	tassert.CheckFatal(t, err)

	conf := &lom.Bck().Props.RateLimit.Backend
	oldEnabled := conf.Enabled
	conf.Enabled = true
	defer func() { conf.Enabled = oldEnabled }()

	arl, err := cos.NewAdaptRateLim(100, 1, time.Second)
	tassert.CheckFatal(t, err)
	key := lom.Bck().HashUname(http.MethodGet)
	mockTarget.ratelim.Store(key, arl)
	defer mockTarget.ratelim.Delete(key)

	err429 := cmn.NewErrTooManyRequests(errors.New("throttled"), http.StatusTooManyRequests)
	backend := &throttleReaderBackend{err: err429}
	bp := &rlbackend{Backend: backend, t: mockTarget}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res := bp.GetObjReader(ctx, lom, 0, 0)
	tassert.Fatalf(t, errors.Is(res.Err, context.Canceled), "expected retry error %v, got %v", context.Canceled, res.Err)
	tassert.Fatalf(t, res.ErrCode == 0, "expected retry code 0, got %d", res.ErrCode)
	tassert.Fatalf(t, backend.calls == 1, "expected no retry call, got %d calls", backend.calls)
}

type (
	listResult struct {
		err   error
		ecode int
	}
	listBackend struct {
		core.Backend
		results []listResult
		bcks    []*meta.Bck
		ctxs    []context.Context
		msgs    []*apc.LsoMsg
		lists   []*cmn.LsoRes
		tokens  []string
		calls   int
	}
)

func (bp *listBackend) ListObjects(ctx context.Context, bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoRes) (int, error) {
	bp.bcks = append(bp.bcks, bck)
	bp.ctxs = append(bp.ctxs, ctx)
	bp.msgs = append(bp.msgs, msg)
	bp.lists = append(bp.lists, lst)
	bp.tokens = append(bp.tokens, msg.ContinuationToken)
	result := bp.results[bp.calls]
	bp.calls++
	if result.err == nil {
		msg.PageSize = 17
		lst.Entries = append(lst.Entries[:0], &cmn.LsoEnt{Name: "listed-object"})
		lst.ContinuationToken = "next-page"
	}
	return result.ecode, result.err
}

func TestRLBackendListObjects(t *testing.T) {
	var (
		errThrottle = errors.New("throttled")
		err429      = cmn.NewErrTooManyRequests(errThrottle, http.StatusTooManyRequests)
		err503      = cmn.NewErrTooManyRequests(errThrottle, http.StatusServiceUnavailable)
		errFinal    = errors.New("retry failed")
		tests       = []struct {
			name      string
			results   []listResult
			enabled   bool
			wantCode  int
			wantErr   error
			wantCalls int
		}{
			{
				name:      "success",
				results:   []listResult{{}},
				enabled:   true,
				wantCalls: 1,
			},
			{
				name:      "status 429",
				results:   []listResult{{ecode: http.StatusTooManyRequests, err: err429}, {}},
				enabled:   true,
				wantCalls: 2,
			},
			{
				name:      "status 503",
				results:   []listResult{{ecode: http.StatusServiceUnavailable, err: err503}, {}},
				enabled:   true,
				wantCalls: 2,
			},
			{
				name: "final retry error",
				results: []listResult{
					{ecode: http.StatusTooManyRequests, err: err429},
					{ecode: http.StatusBadGateway, err: errFinal},
				},
				enabled:   true,
				wantCode:  http.StatusBadGateway,
				wantErr:   errFinal,
				wantCalls: 2,
			},
			{
				name:      "non-throttle error",
				results:   []listResult{{ecode: http.StatusBadRequest, err: errFinal}},
				enabled:   true,
				wantCode:  http.StatusBadRequest,
				wantErr:   errFinal,
				wantCalls: 1,
			},
			{
				name:      "disabled",
				results:   []listResult{{ecode: http.StatusTooManyRequests, err: err429}},
				wantCode:  http.StatusTooManyRequests,
				wantErr:   err429,
				wantCalls: 1,
			},
		}
	)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bck := meta.NewBck("rate-limit-list", apc.AIS, cmn.NsGlobal)
			bck.Props = &cmn.Bprops{}
			bck.Props.RateLimit.Backend.Enabled = test.enabled
			if test.enabled {
				arl, err := cos.NewAdaptRateLim(100, 1, time.Second)
				tassert.CheckFatal(t, err)
				key := bck.HashUname(apc.ActList)
				mockTarget.ratelim.Store(key, arl)
				t.Cleanup(func() { mockTarget.ratelim.Delete(key) })
			}

			backend := &listBackend{results: test.results}
			bp := &rlbackend{Backend: backend, t: mockTarget}
			lst := &cmn.LsoRes{}
			msg := &apc.LsoMsg{ContinuationToken: "current-page"}
			ctx := context.Background()
			ecode, err := bp.ListObjects(ctx, bck, msg, lst)
			tassert.Fatalf(t, ecode == test.wantCode, "expected code %d, got %d", test.wantCode, ecode)
			tassert.Fatalf(t, errors.Is(err, test.wantErr), "expected error %v, got %v", test.wantErr, err)
			tassert.Fatalf(t, backend.calls == test.wantCalls, "expected %d calls, got %d", test.wantCalls, backend.calls)
			for i := range backend.calls {
				if i == 0 {
					tassert.Fatalf(t, backend.ctxs[i] == ctx, "call %d: backend received a different context", i+1)
				} else {
					// reactive retry: single attempt (see cmn.WithNoRetry)
					tassert.Fatalf(t, cmn.IsNoRetry(backend.ctxs[i]), "call %d: expected no-retry context", i+1)
				}
				tassert.Fatalf(t, backend.bcks[i] == bck, "call %d: backend received a different bucket", i+1)
				tassert.Fatalf(t, backend.msgs[i] == msg, "call %d: backend received a different list message", i+1)
				tassert.Fatalf(t, backend.lists[i] == lst, "call %d: backend received a different list result", i+1)
				tassert.Fatalf(t, backend.tokens[i] == "current-page", "call %d: continuation token changed to %q",
					i+1, backend.tokens[i])
			}
			if test.wantErr == nil {
				tassert.Fatalf(t, msg.PageSize == 17, "expected backend message update, got page size %d", msg.PageSize)
				tassert.Fatalf(t, len(lst.Entries) == 1 && lst.Entries[0].Name == "listed-object",
					"unexpected entries: %v", lst.Entries)
				tassert.Fatalf(t, lst.ContinuationToken == "next-page", "unexpected continuation token %q", lst.ContinuationToken)
			}
		})
	}
}

func TestRLBackendListObjectsCanceled(t *testing.T) {
	bck := meta.NewBck("rate-limit-list-canceled", apc.AIS, cmn.NsGlobal)
	bck.Props = &cmn.Bprops{}
	bck.Props.RateLimit.Backend.Enabled = true
	arl, err := cos.NewAdaptRateLim(100, 1, time.Second)
	tassert.CheckFatal(t, err)
	key := bck.HashUname(apc.ActList)
	mockTarget.ratelim.Store(key, arl)
	defer mockTarget.ratelim.Delete(key)

	err429 := cmn.NewErrTooManyRequests(errors.New("throttled"), http.StatusTooManyRequests)
	backend := &listBackend{results: []listResult{{ecode: http.StatusTooManyRequests, err: err429}}}
	bp := &rlbackend{Backend: backend, t: mockTarget}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ecode, err := bp.ListObjects(ctx, bck, &apc.LsoMsg{}, &cmn.LsoRes{})
	tassert.Fatalf(t, errors.Is(err, context.Canceled), "expected %v, got %v", context.Canceled, err)
	tassert.Fatalf(t, ecode == 0, "expected code 0, got %d", ecode)
	tassert.Fatalf(t, backend.calls == 1, "expected no retry call, got %d calls", backend.calls)
}

type conditionalWriteBackend struct {
	core.Backend
	enabled bool
}

func (bp *conditionalWriteBackend) CondWrite(*meta.Bck) bool { return bp.enabled }

func TestRLBackendCondWrite(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		wrapped := &conditionalWriteBackend{enabled: enabled}
		bp := &rlbackend{Backend: wrapped}
		tassert.Fatalf(t, bp.CondWrite(nil) == enabled, "lost backend conditional-write capability")
	}
}

//
// transaction timeouts
//

const testMaxHostBusy = 20 * time.Second

func testTxnConfig() *cmn.Config {
	config := &cmn.Config{}
	config.Timeout.MaxHostBusy = cos.Duration(testMaxHostBusy)
	return config
}

// returns the txn interface (not the concrete type) so callers exercise it exactly
// as production does - notably started(), which is interface-dispatched everywhere.
func newETLInitTxn(initTimeout time.Duration) txn {
	msg := &etl.ETLSpecMsg{InitMsgBase: etl.InitMsgBase{InitTimeout: cos.Duration(initTimeout)}}
	c := &txnSrv{t: mockTarget, uuid: "test-etl-init", msg: &actMsgExt{ActMsg: apc.ActMsg{Action: apc.ActETLInline}}}
	return newTxnETLInit(c, msg)
}

// checkTimeout (begin phase): the actual GC decision. This is the regression guard:
// pre-fix, the deadline was a flat 2*max_host_busy (40s) for ALL txns, so a slow ETL
// init was reaped mid-bootstrap. Post-fix, ETL init survives until init_timeout+slack.
func TestCheckTimeoutBeginPhaseETL(t *testing.T) {
	config := testTxnConfig()
	begin := time.Now()
	slack := txnTimeoutMult * testMaxHostBusy

	tests := []struct {
		name        string
		initTimeout time.Duration
		elapsed     time.Duration
		wantReaped  bool
	}{
		// 5m init_timeout, still booting at 2m. Old code reaped at 40s.
		{"etl-5m-at-2m-survives", 5 * time.Minute, 2 * time.Minute, false},
		// boundary: just inside vs just past init_timeout+slack.
		{"etl-5m-just-inside", 5 * time.Minute, 5*time.Minute + slack - time.Second, false},
		{"etl-5m-just-past", 5 * time.Minute, 5*time.Minute + slack + time.Second, true},
		// even a default (45s) ETL init must outlive the old 40s flat deadline.
		{"etl-default-at-41s-survives", etl.DefaultInitTimeout, 41 * time.Second, false},
		{"etl-default-past-deadline", etl.DefaultInitTimeout, etl.DefaultInitTimeout + slack + time.Second, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			txn := newETLInitTxn(tc.initTimeout)
			txn.started(apc.Begin2PC, begin)
			err, _ := checkTimeout(txn, begin.Add(tc.elapsed), config)
			if tc.wantReaped {
				if err == nil {
					t.Fatalf("expected GC reap at elapsed=%v (init_timeout=%v), got nil", tc.elapsed, tc.initTimeout)
				}
				if !strings.Contains(err.Error(), "[begin - start-commit] timeout") {
					t.Fatalf("unexpected reap error: %v", err)
				}
			} else if err != nil {
				t.Fatalf("ETL init must NOT be reaped at elapsed=%v (init_timeout=%v): %v", tc.elapsed, tc.initTimeout, err)
			}
		})
	}
}

//
// head blobs
//

func hdbTestReq(n int) *cmn.HdbReq {
	req := &cmn.HdbReq{}
	bck := cmn.Bck{Name: "hdb", Provider: apc.AIS, Ns: cmn.NsGlobal}
	for i := range n {
		in := cmn.HdbIn{Name: fmt.Sprintf("%064d", i), Bidx: req.AddBck(&bck)}
		in.Size = int64(i)
		in.Cksum = cos.NewCksum(cos.ChecksumOneXxh, "0123456789abcdef")
		in.SetCustomKey(cmn.ETag, "etag-0123456789abcdef")
		in.SetCustomKey(cmn.MD5ObjMD, "md5-0123456789abcdef")
		req.In = append(req.In, in)
	}
	return req
}

// the body comes from the slab (small request) or the heap (large request)
func TestHdbPack(t *testing.T) {
	for _, test := range []struct {
		n    int
		slab bool
	}{
		{apc.HdbSizeDflt, true},
		{apc.HdbSizeMax, false},
	} {
		req := hdbTestReq(test.n)
		body, bodyR := hdbPack(memsys.PageMM(), req)
		tassert.Errorf(t, (bodyR != nil) == test.slab, "%d items: slab %v, expected %v", test.n, bodyR != nil, test.slab)
		tassert.Errorf(t, len(body) == req.PackedSize(), "%d items: body size %d, expected %d", test.n, len(body), req.PackedSize())
		var got cmn.HdbReq
		tassert.CheckFatal(t, got.Unpack(body))
		tassert.Fatalf(t, len(got.In) == test.n, "%d items: unpacked %d", test.n, len(got.In))
		for i := range got.In {
			a, b := &got.In[i], &req.In[i]
			tassert.Errorf(t, a.Name == b.Name && a.Bidx == b.Bidx && a.Size == b.Size &&
				a.Cksum.Equal(b.Cksum) && maps.Equal(a.CustomMD, b.CustomMD), "item %d: %v, expected %v", i, &a.ObjAttrs, &b.ObjAttrs)
		}
		if bodyR != nil {
			tassert.CheckFatal(t, bodyR.Close())
		}
	}
}

func TestHdbBody(t *testing.T) {
	body, bodyR := hdbPack(memsys.PageMM(), hdbTestReq(apc.HdbSizeDflt))
	want := bytes.Clone(body)
	got, err := io.ReadAll(bodyR)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, bytes.Equal(got, want), "body differs from the packed request")

	tassert.CheckFatal(t, bodyR.Close())
	tassert.CheckFatal(t, bodyR.Close()) // must not free the buffer again
	_, err = bodyR.Read(make([]byte, 1))
	tassert.Fatalf(t, err == io.ErrClosedPipe, "read after close: %v", err)
}

// a body that waits for the test before it reads and tells when net/http closes it
type hdbSpyBody struct {
	io.ReadCloser
	gate   chan struct{}
	closed chan struct{}
	once   sync.Once
}

func (s *hdbSpyBody) Read(p []byte) (int, error) {
	<-s.gate
	return s.ReadCloser.Read(p)
}

func (s *hdbSpyBody) Close() error {
	s.once.Do(func() { close(s.closed) })
	return s.ReadCloser.Close()
}

// the peer replies before it reads the body
func TestHdbBodyEarlyReply(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		rc := http.NewResponseController(w)
		rc.EnableFullDuplex() // reply without a read of the body
		w.WriteHeader(http.StatusBadRequest)
		rc.Flush()
	}))
	defer srv.Close()

	body, bodyR := hdbPack(memsys.PageMM(), hdbTestReq(apc.HdbSizeDflt))
	spy := &hdbSpyBody{ReadCloser: bodyR, gate: make(chan struct{}), closed: make(chan struct{})}
	release := sync.OnceFunc(func() { close(spy.gate) })
	defer release()
	args := cmn.HreqArgs{Method: http.MethodPost, Base: srv.URL, Path: apc.URLPathObjects.S, Body: body, BodyR: spy}
	req, err := args.Req()
	tassert.CheckFatal(t, err)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	tassert.CheckFatal(t, err)
	resp.Body.Close()

	select {
	case <-spy.closed:
		t.Fatal("net/http closed the body, so the buffer is free, before it read the body")
	default:
	}
	release()
	select {
	case <-spy.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("net/http did not close the request body")
	}
}

// at critical load the receiver rejects the request with 429 before it reads the body
func TestHdbReceiverBusy(t *testing.T) {
	tgt := &target{}
	tgt.hdb.crit.Store(true)
	tgt.hdb.next.Store(mono.NanoTime() + int64(time.Hour))

	packed := hdbTestReq(apc.HdbSizeDflt).NewPack()
	body := bytes.NewReader(packed)
	ecode, err := tgt.objHeadBatch(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", body))
	tassert.Fatalf(t, err != nil && ecode == http.StatusTooManyRequests, "expecting 429, got %d (%v)", ecode, err)
	tassert.Errorf(t, body.Len() == len(packed), "the body is read: %d of %d bytes left", body.Len(), len(packed))
}

// the sender reads 400 as unsupported (per-object HEADs) and 429 as peer-busy (no fallback)
func TestHdbErr(t *testing.T) {
	tsi := &meta.Snode{DaeID: "t1"}
	for _, test := range []struct {
		status          int
		unsupp, tooMany bool
	}{
		{http.StatusBadRequest, true, false},
		{http.StatusTooManyRequests, false, true},
		{http.StatusServiceUnavailable, false, false},
	} {
		err := _hdbErr(errors.New("cause"), test.status, tsi)
		unsupp, tooMany := cmn.IsErrHdbUnsupported(err), cmn.IsErrTooManyRequests(err)
		tassert.Errorf(t, unsupp == test.unsupp && tooMany == test.tooMany,
			"status %d: unsupported %t, too many requests %t, expected (%t, %t)", test.status, unsupp, tooMany, test.unsupp, test.tooMany)
	}
}

//
// alternate TLS DNS name
//

type altCallTest struct {
	name, base string
	timeout    time.Duration
	known, ok  bool // known: destination node passed in callArgs.si; ok: expect success
}

// - test server certificate covers ais.example.com and loopback IPs, none of the *.internal names
// - every connection goes to the test server regardless of the dialed name
func TestCallAltDNSName(t *testing.T) {
	const (
		altName       = "ais.example.com"
		clientTimeout = 3 * time.Second
		explicitShort = 2500 * time.Millisecond // < client timeout => control client
		explicitLong  = 6 * time.Second         // > client timeout => data client
	)
	p := newTestProxy(t, false /*requestAuthConfigured*/)
	prev := g.client
	t.Cleanup(func() { g.client = prev })

	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(srv.Close)
	ca := filepath.Join(t.TempDir(), "server.pem")
	b := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	tassert.CheckFatal(t, os.WriteFile(ca, b, 0o600))

	_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
	tassert.CheckFatal(t, err)
	si := newTestSnode(t)
	si.PubNet.Init("https", "public.internal", port)
	si.ControlNet.Init("https", "control.internal", port)
	si.DataNet.Init("https", "data.internal", port)
	smap := newSmap()

	newConfig := func(alt string) *cmn.Config {
		config := &cmn.Config{Client: &cmn.ClientConf{
			Timeout:     cos.Duration(clientTimeout),
			TimeoutLong: cos.Duration(30 * time.Second),
		}}
		config.Timeout.CplaneOperation = cos.Duration(2 * time.Second)
		config.Timeout.MaxKeepalive = cos.Duration(5 * time.Second)
		config.Net.HTTP.UseHTTPS = true
		config.Net.HTTP.ClientCA = ca
		config.Net.HTTP.AltDNSName = alt
		return config
	}
	run := func(t *testing.T, tests []altCallTest) {
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				args := allocCargs()
				defer freeCargs(args)
				args.req = cmn.HreqArgs{Method: http.MethodGet, Base: test.base, Path: apc.URLPathHealth.S}
				args.timeout = test.timeout
				if test.known {
					args.si = si
				}
				res := p.call(args, smap)
				defer freeCR(res)
				if test.ok {
					tassert.CheckFatal(t, res.err)
					return
				}
				checkHostnameErr(t, res.err)
			})
		}
	}

	t.Run("with alt name", func(t *testing.T) {
		config := newConfig(altName)
		initAltTestClients(t, config, srv)
		tassert.Fatal(t, g.client.controlLong != nil, "controlLong: expected with alt_tls_dns_name")
		tassert.Fatal(t, g.client.controlLong.Timeout == g.client.data.Timeout, "controlLong: expected long timeout")

		run(t, []altCallTest{
			{name: "public cplane timeout", base: si.PubNet.URL, timeout: 0, known: true, ok: true},
			{name: "public default timeout", base: si.PubNet.URL, timeout: apc.DefaultTimeout, known: true, ok: true},
			{name: "public explicit short timeout", base: si.PubNet.URL, timeout: explicitShort, known: true, ok: true},
			{name: "public long timeout", base: si.PubNet.URL, timeout: apc.LongTimeout, known: true, ok: true},
			{name: "public explicit long timeout", base: si.PubNet.URL, timeout: explicitLong, known: true, ok: true},
			{name: "public retry timeout", base: si.PubNet.URL, timeout: config.Timeout.MaxKeepalive.D() + time.Second, known: true, ok: true},
			{name: "intra-control default timeout", base: si.ControlNet.URL, timeout: apc.DefaultTimeout, known: true, ok: true},
			{name: "intra-data long timeout", base: si.DataNet.URL, timeout: apc.LongTimeout, known: true},
			{name: "intra-data explicit long timeout", base: si.DataNet.URL, timeout: explicitLong, known: true},
			{name: "external long timeout", base: si.PubNet.URL, timeout: apc.LongTimeout},
			{name: "external explicit long timeout", base: si.PubNet.URL, timeout: explicitLong},
			{name: "external matching certificate", base: srv.URL, timeout: apc.LongTimeout, ok: true},
		})

		// external consumers use the data client directly (presigned S3 requests) or clone it (ETL);
		// neither may inherit the alt name
		for _, client := range []*http.Client{g.client.data, cmn.CloneClient(g.client.data, time.Second)} {
			resp, err := client.Get(si.PubNet.URL)
			if resp != nil {
				resp.Body.Close()
			}
			client.CloseIdleConnections()
			checkHostnameErr(t, err)
		}
	})

	t.Run("without alt name", func(t *testing.T) {
		initAltTestClients(t, newConfig(""), srv)
		tassert.Fatal(t, g.client.controlLong == nil, "controlLong: unexpected without alt_tls_dns_name")

		run(t, []altCallTest{
			{name: "public default timeout", base: si.PubNet.URL, timeout: apc.DefaultTimeout, known: true},
			{name: "public long timeout", base: si.PubNet.URL, timeout: apc.LongTimeout, known: true},
			{name: "external matching certificate", base: srv.URL, timeout: apc.LongTimeout, ok: true},
		})
	})
}

// build control and data clients as at startup, then redirect their dials to srv
func initAltTestClients(t *testing.T, config *cmn.Config, srv *httptest.Server) {
	initCtrlClient(config, false /*preferIPv6*/)
	g.client.data = cmn.NewIntraClientTLS(cmn.TransportArgs{ClientTimeout: config.Client.TimeoutLong.D()}, config)
	t.Cleanup(g.client.control.CloseIdleConnections)
	t.Cleanup(g.client.data.CloseIdleConnections)

	dial := func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, srv.Listener.Addr().String())
	}
	ctr := g.client.control.Transport.(*http.Transport)
	ctr.DialContext = dial
	// re-apply TLS: the alt name dialer holds the original DialContext
	tassert.CheckFatal(t, cmn.SetTLS(ctr, config.Net.HTTP.ToTLSAlt(), true /*intra*/))
	g.client.data.Transport.(*http.Transport).DialContext = dial
}

// trusted certificate in all cases: the only acceptable failure is a name mismatch
func checkHostnameErr(t *testing.T, err error) {
	t.Helper()
	var herr x509.HostnameError
	tassert.Fatalf(t, errors.As(err, &herr), "expected hostname error, got %v", err)
}
