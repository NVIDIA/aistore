// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
)

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
