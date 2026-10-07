// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	wdlTestTout = 250 * time.Millisecond // => cmn.XferMinRenew (1MiB); minimum transfer rate 4MiB/s
	wdlTestSize = 32 * cos.MiB           // well above loopback socket buffering
	wdlTestWait = 30 * time.Second       // test-level safety net
)

type wdlResult struct {
	err       error
	written   int64
	elapsed   time.Duration
	deadlines int
}

type wdlResponseWriter struct {
	http.ResponseWriter
	deadlines int
}

func (w *wdlResponseWriter) SetWriteDeadline(t time.Time) error {
	w.deadlines++
	return http.NewResponseController(w.ResponseWriter).SetWriteDeadline(t)
}

func (w *wdlResponseWriter) ReadFrom(r io.Reader) (int64, error) {
	return cos.CopySendfile(w.ResponseWriter, r)
}

type wdlCountingWriter struct {
	http.ResponseWriter
	writes, deadlines int
}

func (w *wdlCountingWriter) Write(p []byte) (int, error) {
	w.writes++
	return len(p), nil
}

func (w *wdlCountingWriter) SetWriteDeadline(time.Time) error {
	w.deadlines++
	return nil
}

func TestWdlChunkSize(t *testing.T) {
	tests := []struct {
		tout time.Duration
		want int64
	}{
		{0, cmn.XferMinRenew},
		{10 * time.Second, cmn.XferMinRenew},          // 640KiB => min (below config-validated minimum)
		{time.Minute, 60 * cmn.XferMinRate},           // 3.75MiB (config-validated minimum)
		{5 * time.Minute, 300 * cmn.XferMinRate},      // 18.75MiB (default)
		{17 * time.Minute, 17 * 60 * cmn.XferMinRate}, // 63.75MiB (last below max)
		{18 * time.Minute, cmn.XferMaxRenew},
		{2 * time.Hour, cmn.XferMaxRenew},
		{100 * 24 * time.Hour, cmn.XferMaxRenew},
	}
	for _, tc := range tests {
		if got := cmn.XferRenewSize(tc.tout); got != tc.want {
			t.Errorf("cmn.XferRenewSize(%v) = %d, want %d", tc.tout, got, tc.want)
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
		{"opt-out", 5 * time.Minute, feat.DisableGetDeadline, false, false},
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

func TestWdlBufferedSizes(t *testing.T) {
	const (
		bufSize = 128 * cos.KiB
		tout    = 5 * time.Minute
	)
	renewSize := cmn.XferRenewSize(tout)
	payload := make([]byte, renewSize+1)
	buf := make([]byte, bufSize)
	tests := []struct {
		name     string
		size     int64
		unknown  bool
		disabled bool
		renew    bool
	}{
		{name: "empty"},
		{name: "below-buffer", size: bufSize - 1},
		{name: "one-buffer", size: bufSize},
		{name: "above-buffer", size: bufSize + 1},
		{name: "multiple-buffers", size: 512 * cos.KiB},
		{name: "below-chunk", size: renewSize - 1},
		{name: "one-chunk", size: renewSize},
		{name: "above-chunk", size: renewSize + 1, renew: true},
		{name: "unknown-small", size: 512 * cos.KiB, unknown: true},
		{name: "unknown-chunk", size: renewSize, unknown: true, renew: true},
		{name: "disabled", size: renewSize + 1, disabled: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := &wdlCountingWriter{ResponseWriter: httptest.NewRecorder()}
			goi := &getOI{w: w, wtout: tout}
			if !tc.disabled {
				goi.wctrl = w
				tassert.CheckFatal(t, w.SetWriteDeadline(time.Now().Add(tout)))
			}
			size := tc.size
			if tc.unknown {
				size = -1
			}
			n, err := goi._copyWdl(bytes.NewReader(payload[:tc.size]), buf, size)
			tassert.CheckFatal(t, err)
			tassert.Errorf(t, n == tc.size, "written %d, wanted %d", n, tc.size)
			tassert.Errorf(t, w.writes == int((tc.size+bufSize-1)/bufSize), "unexpected write count: %d", w.writes)
			wantDeadlines := 1
			if tc.disabled {
				wantDeadlines = 0
			} else if tc.renew {
				wantDeadlines++
			}
			tassert.Errorf(t, w.deadlines == wantDeadlines, "set %d deadlines, wanted %d", w.deadlines, wantDeadlines)
		})
	}
}

// buffered: non-divisible writes - one renewal per renewal boundary crossed,
// overshoot carries over (renewal k at cumulative k*renewSize)
func TestWdlWriterRenewal(t *testing.T) {
	const renewSize = 64 * cos.KiB
	tests := []struct {
		name      string
		writes    []int
		renewals  int
		remainder int64
	}{
		{"below", []int{renewSize - 1}, 0, renewSize - 1},
		{"divisible", []int{renewSize, renewSize}, 2, 0},
		{"three-quarter-steps", []int{48 * cos.KiB, 48 * cos.KiB, 48 * cos.KiB, 48 * cos.KiB}, 3, 0}, // reset-to-zero: 2
		{"overshoot", []int{48 * cos.KiB, 48 * cos.KiB}, 1, 32 * cos.KiB},
		{"multi-renewal", []int{renewSize*2 + renewSize/2}, 1, renewSize / 2}, // one write, one renewal
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := &wdlCountingWriter{ResponseWriter: httptest.NewRecorder()}
			d := &wdlWriter{w: w, rc: w, tout: time.Minute, renewSize: renewSize}
			for _, n := range tc.writes {
				if _, err := d.Write(make([]byte, n)); err != nil {
					t.Fatal(err)
				}
			}
			if w.deadlines != tc.renewals || d.n != tc.remainder {
				t.Fatalf("renewals %d, remainder %d; want %d, %d", w.deadlines, d.n, tc.renewals, tc.remainder)
			}
		})
	}
}

func TestWdlSendfile(t *testing.T) { wdlRun(t, true) }
func TestWdlBuffered(t *testing.T) { wdlRun(t, false) }

func wdlRun(t *testing.T, sendfile bool) {
	t.Run("small", func(t *testing.T) {
		const size = 64 * cos.KiB
		url, ch := wdlServer(t, size, sendfile)
		resp, err := http.Get(url)
		tassert.CheckFatal(t, err)
		n, err := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, n == size, "received %d, wanted %d", n, size)
		r := wdlWait(t, ch)
		tassert.CheckFatal(t, r.err)
		tassert.Errorf(t, r.deadlines == 1, "small response set %d deadlines, wanted initial deadline only", r.deadlines)
	})

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

	// reading steadily at ~10x the minimum transfer rate, for longer than the window:
	// must complete (deadline renewed per renewal size)
	t.Run("slow-progressing", func(t *testing.T) {
		tools.CheckSkip(t, &tools.SkipTestArgs{Long: true}) // runs past the timeout by design
		url, ch := wdlServer(t, wdlTestSize, sendfile)
		resp, err := http.Get(url)
		tassert.CheckFatal(t, err)
		buf := make([]byte, cos.MiB)
		var received int64
		for {
			var n int
			n, err = io.ReadFull(resp.Body, buf)
			received += int64(n)
			if err != nil {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		resp.Body.Close()
		tassert.Errorf(t, err == io.EOF, "client read ended with %v, wanted EOF", err)
		tassert.Errorf(t, received == wdlTestSize, "received %d, wanted %d", received, wdlTestSize)

		r := wdlWait(t, ch)
		tassert.CheckFatal(t, r.err)
		tassert.Errorf(t, r.deadlines > 1, "long response never renewed its initial deadline")
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
		tassert.Errorf(t, cos.IsErrNetTimeoutConn(r.err), "expected write timeout, got %v", r.err)
		tassert.Errorf(t, errors.Is(r.err, os.ErrDeadlineExceeded), // => cmn.ErrSlowReadingClient (see _txerr)
			"expected os.ErrDeadlineExceeded, got %v", r.err)
		tassert.Errorf(t, r.written < wdlTestSize, "written %d, expected partial", r.written)
	})
}

// Even a one-buffer response can block on its first write. Keep the initial
// deadline installed, including when wrapping and splitting are skipped.
func TestWdlFirstWrite(t *testing.T) {
	const size = 128 * cos.KiB
	for _, sendfile := range []bool{false, true} {
		name := "buffered"
		if sendfile {
			name = "sendfile"
		}
		t.Run(name, func(t *testing.T) {
			url, ch := wdlServer(t, size, sendfile, 4*cos.KiB)
			conn, err := net.DialTimeout("tcp", strings.TrimPrefix(url, "http://"), wdlTestWait)
			tassert.CheckFatal(t, err)
			defer conn.Close()
			tassert.CheckFatal(t, conn.(*net.TCPConn).SetReadBuffer(4*cos.KiB))
			tassert.CheckFatal(t, conn.SetDeadline(time.Now().Add(wdlTestWait)))
			_, err = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
			tassert.CheckFatal(t, err)
			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			tassert.CheckFatal(t, err)
			// Do not read or close the body until the handler reports its timeout.
			r := wdlWait(t, ch)
			conn.Close()
			resp.Body.Close()
			tassert.Errorf(t, cos.IsErrNetTimeoutConn(r.err), "first write did not time out: %v", r.err)
			tassert.Errorf(t, r.written < size, "first write completed: %d", r.written)
			tassert.Errorf(t, r.deadlines == 1, "set %d deadlines, wanted initial deadline only", r.deadlines)
		})
	}
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
func wdlServer(t *testing.T, size int64, sendfile bool, socketBuffer ...int) (string, chan wdlResult) {
	fqn := filepath.Join(t.TempDir(), "obj")
	f, err := os.Create(fqn)
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, f.Truncate(size))
	tassert.CheckFatal(t, f.Close())

	ch := make(chan wdlResult, 1)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			cw      = &wdlResponseWriter{ResponseWriter: w}
			goi     = &getOI{w: cw, wctrl: http.NewResponseController(cw), wtout: wdlTestTout}
			started = mono.NanoTime()
		)
		goi.ltime = started
		res.err = goi.wctrl.SetWriteDeadline(time.Now().Add(goi.wtout))
		if res.err == nil {
			if sendfile {
				res.written, res.err = goi._sendfileWdl(&io.LimitedReader{R: fh, N: size})
			} else {
				res.written, res.err = goi._copyWdl(fh, make([]byte, min(size, 128*cos.KiB)), size)
			}
		}
		res.deadlines = cw.deadlines
		res.elapsed = mono.Since(started)
		ch <- res
	}))
	if len(socketBuffer) > 0 {
		srv.Config.ConnState = func(c net.Conn, state http.ConnState) {
			if state == http.StateNew {
				_ = c.(*net.TCPConn).SetWriteBuffer(socketBuffer[0])
			}
		}
	}
	srv.Start()
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
