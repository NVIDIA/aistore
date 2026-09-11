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
	if err != nil {
		t.Fatal(err)
	}

	relayErr := errors.New("test reverse-proxy failure")
	rproxy := httputil.NewSingleHostReverseProxy(u)
	rproxy.Transport = rpErrTransport{relayErr}
	rproxy.ErrorHandler = p.rpErrHandler
	p.rproxy.nodes.Store(si.ID(), &singleRProxy{rp: rproxy, u: rawURL})

	// The S3 override returns XML without changing the cached native handler.
	r := httptest.NewRequest(http.MethodGet, "/s3/bucket/object", http.NoBody)
	w := httptest.NewRecorder()
	p.s3ReverseRequest(w, r, si, p.owner.smap.get())

	if w.Code != http.StatusBadGateway {
		t.Fatalf("S3 relay: expected status %d, got %d", http.StatusBadGateway, w.Code)
	}
	if ctype := w.Header().Get(cos.HdrContentType); ctype != cos.ContentXML {
		t.Fatalf("S3 relay: expected content type %q, got %q", cos.ContentXML, ctype)
	}
	var serr s3.Error
	if err := xml.Unmarshal(w.Body.Bytes(), &serr); err != nil {
		t.Fatalf("S3 relay: invalid XML error %q: %v", w.Body.String(), err)
	}
	if serr.Message != relayErr.Error() {
		t.Fatalf("S3 relay: expected message %q, got %q", relayErr, serr.Message)
	}

	// Reuse the same cached reverse proxy without the per-request override.
	// Its original handler must still return the native bare status.
	r = httptest.NewRequest(http.MethodGet, "/v1/health", http.NoBody)
	w = httptest.NewRecorder()
	p.reverseRequest(w, r, si.ID(), rawURL)

	if w.Code != http.StatusBadGateway {
		t.Fatalf("native relay: expected status %d, got %d", http.StatusBadGateway, w.Code)
	}
	if w.Body.Len() != 0 {
		t.Fatalf("native relay: expected empty body, got %q", w.Body.String())
	}
	if ctype := w.Header().Get(cos.HdrContentType); ctype != "" {
		t.Fatalf("native relay: expected no content type, got %q", ctype)
	}
}
