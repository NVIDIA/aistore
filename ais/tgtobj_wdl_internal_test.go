// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
)

// GET write deadline (wdl): no-progress timeout for clients that stop reading

const (
	wdlTestTout = 250 * time.Millisecond // => wdlMinChunkSize; implied min drain rate 4MiB/s
	wdlTestSize = 32 * cos.MiB           // well above loopback socket buffering
	wdlTestWait = 30 * time.Second       // test-level safety net
)

type wdlResult struct {
	err     error
	written int64
	elapsed time.Duration
}

func TestWdlChunkSize(t *testing.T) {
	tests := []struct {
		tout time.Duration
		want int64
	}{
		{0, wdlMinChunkSize},
		{time.Minute, wdlMinChunkSize}, // 960KiB => min (also: config-validated minimum)
		{5 * time.Minute, 300 * wdlMinRate},
		{68 * time.Minute, 68 * 60 * wdlMinRate},
		{2 * time.Hour, wdlMaxChunkSize},
		{100 * 24 * time.Hour, wdlMaxChunkSize},
	}
	for _, tc := range tests {
		if got := wdlChunkSize(tc.tout); got != tc.want {
			t.Errorf("wdlChunkSize(%v) = %d, want %d", tc.tout, got, tc.want)
		}
	}
}

func TestWdlInit(t *testing.T) {
	defer cmn.Rom.Set(&cmn.GCO.Get().ClusterConfig)

	tests := []struct {
		name     string
		sendfile time.Duration
		features feat.Flags
		wrap     bool // writer that does not support deadlines
		enabled  bool
	}{
		{"enabled", 5 * time.Minute, 0, false, true},
		{"opt-out", 5 * time.Minute, feat.DisableGetWriteDeadline, false, false},
		{"zero-timeout", 0, 0, false, false},
		{"not-supported", 5 * time.Minute, 0, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := cmn.GCO.Get().ClusterConfig
			cfg.Timeout.SendFile = cos.Duration(tc.sendfile)
			cfg.Features = tc.features
			cmn.Rom.Set(&cfg)

			ch := make(chan *getOI, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.wrap {
					w = struct{ http.ResponseWriter }{w} // hides SetWriteDeadline and Unwrap
				}
				goi := &getOI{w: w}
				goi.initWdl()
				ch <- goi
			}))
			defer srv.Close()

			resp, err := http.Get(srv.URL)
			tassert.CheckFatal(t, err)
			resp.Body.Close()

			goi := <-ch
			if enabled := goi.wctrl != nil; enabled != tc.enabled {
				t.Fatalf("enabled=%t, want %t", enabled, tc.enabled)
			}
			if tc.enabled && goi.wtout != tc.sendfile {
				t.Fatalf("wtout=%v, want %v", goi.wtout, tc.sendfile)
			}
		})
	}
}

func TestWdlSendfile(t *testing.T) { wdlRun(t, true) }
func TestWdlBuffered(t *testing.T) { wdlRun(t, false) }

func wdlRun(t *testing.T, sendfile bool) {
	t.Run("fast", func(t *testing.T) {
		url, ch := wdlServer(t, wdlTestSize, sendfile)
		resp, err := http.Get(url)
		tassert.CheckFatal(t, err)
		_, err = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		tassert.CheckFatal(t, err)

		r := wdlWait(t, ch)
		tassert.CheckFatal(t, r.err)
		tassert.Errorf(t, r.written == wdlTestSize, "written %d != %d", r.written, wdlTestSize)
	})

	// reading steadily at ~10x the implied minimum rate, for longer than the timeout:
	// must complete (deadline re-armed per chunk/write)
	t.Run("slow-progressing", func(t *testing.T) {
		tools.CheckSkip(t, &tools.SkipTestArgs{Long: true}) // runs past the timeout by design
		url, ch := wdlServer(t, wdlTestSize, sendfile)
		resp, err := http.Get(url)
		tassert.CheckFatal(t, err)
		buf := make([]byte, cos.MiB)
		for {
			_, err = io.ReadFull(resp.Body, buf)
			if err != nil {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		resp.Body.Close()

		r := wdlWait(t, ch)
		tassert.CheckFatal(t, r.err)
		tassert.Errorf(t, r.written == wdlTestSize, "written %d != %d", r.written, wdlTestSize)
		tassert.Errorf(t, r.elapsed > wdlTestTout, "elapsed %v <= timeout %v: test did not exercise re-arming",
			r.elapsed, wdlTestTout)
	})

	// connected but not reading: must time out, and be classified benign (see _txerr)
	t.Run("stalled", func(t *testing.T) {
		url, ch := wdlServer(t, wdlTestSize, sendfile)
		resp, err := http.Get(url)
		tassert.CheckFatal(t, err)
		defer resp.Body.Close()
		_, err = io.CopyN(io.Discard, resp.Body, cos.MiB)
		tassert.CheckFatal(t, err)

		r := wdlWait(t, ch) // client stays connected
		if r.err == nil {
			t.Fatalf("expected write deadline exceeded, got nil (written %d)", r.written)
		}
		tassert.Errorf(t, cos.IsErrRetriableConn(r.err), "expected timeout (benign) error, got %v", r.err)
		tassert.Errorf(t, r.written < wdlTestSize, "written %d, expected partial", r.written)
	})
}

// net/http must clear the write deadline after each request: a handler that sets
// no deadline must not inherit an expired one on a keep-alive connection
func TestWdlKeepAlive(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true}) // waits for the deadline to expire
	const size = 64 * cos.KiB
	url, ch := wdlServer(t, size, true)

	client := &http.Client{Transport: &http.Transport{MaxConnsPerHost: 1}}
	defer client.CloseIdleConnections()

	get := func(path string) (reused bool) {
		trace := &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) { reused = i.Reused }}
		req, err := http.NewRequest(http.MethodGet, url+path, http.NoBody)
		tassert.CheckFatal(t, err)
		resp, err := client.Do(req.WithContext(httptrace.WithClientTrace(req.Context(), trace)))
		tassert.CheckFatal(t, err)
		n, err := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, n == size, "%s: received %d, want %d", path, n, size)
		return reused
	}

	get("/")
	r := wdlWait(t, ch)
	tassert.CheckFatal(t, r.err)

	time.Sleep(2 * wdlTestTout) // let the deadline armed above expire

	reused := get("/plain")
	tassert.Fatalf(t, reused, "expected keep-alive connection reuse")
}

//
// helpers
//

// serve `size` zero bytes from a (sparse) file via either sendfile or buffered transmit,
// with write deadline enabled; "/plain" serves the same w/o deadline
func wdlServer(t *testing.T, size int64, sendfile bool) (string, chan wdlResult) {
	fqn := filepath.Join(t.TempDir(), "obj")
	f, err := os.Create(fqn)
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, f.Truncate(size))
	tassert.CheckFatal(t, f.Close())

	ch := make(chan wdlResult, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fh, err := os.Open(fqn)
		if err != nil {
			ch <- wdlResult{err: err}
			return
		}
		defer fh.Close()
		w.Header().Set(cos.HdrContentLength, strconv.FormatInt(size, 10))

		if r.URL.Path == "/plain" {
			_, _ = cos.CopyBuffer(w, fh, make([]byte, 32*cos.KiB))
			return
		}

		var (
			res     wdlResult
			goi     = &getOI{w: w, wctrl: http.NewResponseController(w), wtout: wdlTestTout}
			started = mono.NanoTime()
		)
		if sendfile {
			res.written, res.err = goi._sendfileWdl(&io.LimitedReader{R: fh, N: size})
		} else {
			wdl := &wdlWriter{w: w, rc: goi.wctrl, tout: goi.wtout}
			res.written, res.err = cos.CopyBuffer(wdl, fh, make([]byte, 32*cos.KiB))
		}
		res.elapsed = mono.Since(started)
		ch <- res
	}))
	t.Cleanup(srv.Close)
	return srv.URL, ch
}

func wdlWait(t *testing.T, ch chan wdlResult) wdlResult {
	select {
	case r := <-ch:
		return r
	case <-time.After(wdlTestWait):
		t.Fatalf("handler did not return within %v", wdlTestWait)
		return wdlResult{}
	}
}
