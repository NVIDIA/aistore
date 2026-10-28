// Package ais: internal unit tests
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/tassert"
)

// newHTRunWithSmap creates a minimal htrun with a valid smap containing one proxy node.
func newHTRunWithSmap() *htrun {
	h := &htrun{}
	primaryID := "p-primary"
	h.si = newSnode(primaryID, apc.Proxy)

	config := cmn.GCO.BeginUpdate()
	cmn.GCO.CommitUpdate(config)

	h.owner.smap = newSmapOwner(config, false /*isTarget*/)
	smap := newSmap()
	smap.addProxy(h.si)
	smap.Primary = h.si
	h.owner.smap.put(smap)
	h.svs.init()
	return h
}

func newSnode(id, daeType string) (snode *meta.Snode) {
	snode = &meta.Snode{}
	snode.Init(id, daeType, nil /*verifying key*/)
	return
}

func TestSetIntraHdrs_ClearsRelayedSenderHdrs(t *testing.T) {
	spoofed := func() http.Header {
		hdr := http.Header{}
		hdr.Set(apc.HdrSenderID, "p-spoofed")
		hdr.Set(apc.HdrSenderName, "p[spoofed]")
		hdr.Set(apc.HdrSenderIsPrimary, "true")
		hdr.Set(apc.HdrSenderSmapVer, "99999")
		hdr.Set(apc.HdrSenderNonce, "42")
		hdr.Set(apc.HdrSenderSig, "not-a-signature")
		return hdr
	}

	// self is NOT primary; signing disabled and grace expired (svState.init),
	// so signIntra is skipped - exactly the window where a relayed signature would survive
	h := newHTRunWithSmap()
	tassert.Fatalf(t, !h.svs.sign(), "expecting sign() == false")

	smap := h.owner.smap.get().clone()
	other := newSnode("p-other", apc.Proxy)
	smap.addProxy(other)
	smap.Primary = other
	smap.Version++
	h.owner.smap.put(smap)
	smap = h.owner.smap.get()

	req := &http.Request{Header: spoofed()}
	h.setIntraHdrs(req, smap, true /*peer present*/)

	for _, tc := range []struct{ hdr, expected string }{
		{apc.HdrSenderID, h.SID()},
		{apc.HdrSenderName, h.si.Name()},
		{apc.HdrSenderIsPrimary, ""},
		{apc.HdrSenderSmapVer, smap.vstr},
		{apc.HdrSenderNonce, ""},
		{apc.HdrSenderSig, ""},
	} {
		got := req.Header.Get(tc.hdr)
		tassert.Errorf(t, got == tc.expected, "%s: got %q, expected %q", tc.hdr, got, tc.expected)
	}

	// and the legitimate primary case is not broken by the above
	hp := newHTRunWithSmap()
	reqp := &http.Request{Header: spoofed()}
	hp.setIntraHdrs(reqp, hp.owner.smap.get(), true /*peer present*/)
	got := reqp.Header.Get(apc.HdrSenderIsPrimary)
	tassert.Errorf(t, got == "true", "%s: got %q, expected %q", apc.HdrSenderIsPrimary, got, "true")
}

func TestRetryJoin(t *testing.T) {
	var (
		req  = httptest.NewRequest(http.MethodPost, "/", http.NoBody)
		errs = map[string]error{
			"unres": newErrJoinUnresolved(req, errors.New("unresolved")),
			"rej":   newErrJoinRejected(req, errors.New("rejected")),
			"fail":  errors.New("unreachable"),
		}
		unres = []string{"unres", "unres", "unres", "unres"} // joinRounds
	)
	// attempts are instant: only the cases with a non-zero age ever reach the timeout
	const tout = time.Second
	for _, tc := range []struct {
		name   string
		urls   []string
		script map[string][]string // per candidate; the last step repeats; "ok" succeeds
		age    time.Duration       // time already spent before the first attempt
		calls  int
		want   string // kind of the returned result; "" for none
	}{
		{"succeeds at once", []string{"a"}, map[string][]string{"a": {"ok"}}, 0, 1, "ok"},
		{"rejection stops at once", []string{"a"}, map[string][]string{"a": {"fail", "rej"}}, 0, 2, "rej"},
		{"other failures stop after fixed rounds", []string{"a"}, map[string][]string{"a": {"fail"}}, 0, joinRounds, "fail"},
		{"fixed rounds try every candidate", []string{"a", "b"}, map[string][]string{"a": {"fail"}, "b": {"fail"}}, 0, 2 * joinRounds, "fail"},
		{"no candidates", nil, nil, 0, 0, ""},
		{
			"unresolved retries until success", []string{"a"},
			map[string][]string{"a": append(slices.Clone(unres), "unres", "unres", "ok")}, 0, joinRounds + 3, "ok",
		},
		{"unresolved after timeout gets fixed rounds only", []string{"a"}, map[string][]string{"a": {"unres"}}, time.Hour, joinRounds, "unres"},
		{
			"retries stop once failures change", []string{"a"},
			map[string][]string{"a": append(slices.Clone(unres), "fail")}, 0, joinRounds + 1, "fail",
		},
		{
			"unresolved from any candidate extends the round", []string{"a", "b"},
			map[string][]string{"a": {"fail"}, "b": append(slices.Clone(unres), "ok")}, 0, 2 * (joinRounds + 1), "ok",
		},
	} {
		var (
			calls   = map[string]int{}
			total   int
			started = mono.NanoTime() - int64(tc.age)
		)
		try := func(url string) *callResult {
			steps := tc.script[url]
			kind := steps[min(calls[url], len(steps)-1)]
			calls[url]++
			total++
			res := allocCR()
			res.err = errs[kind] // nil for "ok"
			return res
		}
		res := retryJoin(tc.urls, started, tout, 0 /*sleep*/, try)

		got := ""
		if res != nil {
			got = "ok"
			for kind, err := range errs {
				if res.err == err {
					got = kind
				}
			}
			freeCR(res)
		}
		tassert.Errorf(t, got == tc.want, "%s: got result %q, expected %q", tc.name, got, tc.want)
		tassert.Errorf(t, total == tc.calls, "%s: got %d calls, expected %d", tc.name, total, tc.calls)
	}
}

//
// htrun.call client selection vs net.http.alt_tls_dns_name
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
