// Package api_test contains tests for the public Go API.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package api_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"
)

type contextCancelHandler struct {
	canceled chan struct{}
	release  chan struct{}
	calls    atomic.Int64
}

func (h *contextCancelHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Length", "1")
	w.Header().Set(apc.HdrObjVersion, "1")
	w.WriteHeader(http.StatusOK)
	if h.calls.Add(1) == 1 {
		_, _ = w.Write([]byte{'x'})
		return
	}
	w.(http.Flusher).Flush()
	select {
	case <-r.Context().Done():
		close(h.canceled)
	case <-h.release:
	}
}

func TestGetObjectReaderContextCancellation(t *testing.T) {
	h := &contextCancelHandler{
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
	}
	srv := httptest.NewServer(h)
	defer srv.Close()
	defer close(h.release)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := cmn.NewClient(cmn.TransportArgs{})
	defer client.CloseIdleConnections()
	bp := api.BaseParams{Client: client, URL: srv.URL}
	bck := cmn.Bck{Name: "bucket", Provider: apc.AIS}
	warmup, size, err := api.GetObjectReader(bp, bck, "object", nil)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, size == 1, "expected GET size 1, got %d", size)
	_, err = io.Copy(io.Discard, warmup)
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, warmup.Close())

	r, oah, err := api.GetObjectReaderWithAttrs(bp, bck, "object", &api.GetArgs{Context: ctx})
	tassert.CheckFatal(t, err)
	defer r.Close()
	tassert.Fatalf(t, oah.Size() == 1, "expected GET size 1, got %d", oah.Size())
	tassert.Fatalf(t, oah.RespHeader().Get("Content-Length") == "1", "GET response headers not returned")
	oa := oah.Attrs()
	tassert.Fatalf(t, oa.Version() == "1", "expected GET version 1, got %q", oa.Version())

	cancel()
	select {
	case <-h.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("GET request context was not canceled")
	}
}
