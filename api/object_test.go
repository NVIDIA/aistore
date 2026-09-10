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
	warmup, _, err := api.GetObjectReader(bp, bck, "object", nil)
	tassert.CheckFatal(t, err)
	_, err = io.Copy(io.Discard, warmup)
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, warmup.Close())

	r, _, err := api.GetObjectReader(bp, bck, "object", &api.GetArgs{Context: ctx})
	tassert.CheckFatal(t, err)
	defer r.Close()

	cancel()
	select {
	case <-h.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("GET request context was not canceled")
	}
}
