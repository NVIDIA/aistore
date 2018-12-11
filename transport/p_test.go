// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package transport_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/transport"
	"github.com/NVIDIA/dfcpub/tutils"
)

var buf1 []byte

func receive10G(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
	cmn.Assert(err == nil)
	written, _ := io.CopyBuffer(ioutil.Discard, objReader, buf1)
	cmn.Assert(written == hdr.Dsize)
}

func Test_OneStream10G(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	network := "np"
	mux := http.NewServeMux()
	trname := "10G"

	transport.SetMux(network, mux)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	path, err := transport.Register(network, trname, receive10G)
	tutils.CheckFatal(err, t)

	httpclient := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

	url := ts.URL + path
	stream := transport.NewStream(httpclient, url, &transport.Extra{Burst: 2})

	slab := Mem2.SelectSlab2(cmn.MiB)
	buf1 = slab.Alloc()

	random := newRand(time.Now().UnixNano())
	slab, _ = Mem2.GetSlab2(cmn.KiB * 32)
	size, prevsize, num, numhdr := int64(0), int64(0), 0, 0

	for size < cmn.GiB*10 {
		hdr := genStaticHeader()
		if num%3 == 0 { // every so often send header-only
			hdr.Dsize = 0
			stream.Send(hdr, nil, nil)
			numhdr++
		} else {
			reader := newRandReader(random, hdr, slab)
			stream.Send(hdr, reader, nil)
		}
		num++
		size += hdr.Dsize
		if size-prevsize >= cmn.GiB {
			tutils.Logf("%s: %d GiB\n", stream, size/cmn.GiB)
			prevsize = size
			stats := stream.GetStats()
			tutils.Logf("send$ %s: idle=%.2f%%\n", stream, stats.IdlePct)
		}
	}
	stream.Fin()
	stats := stream.GetStats()

	fmt.Printf("send$ %s: offset=%d, num=%d(%d/%d), idle=%.2f%%\n", stream, stats.Offset, stats.Num, num, numhdr, stats.IdlePct)

	printNetworkStats(t, network)
}

func Test_DryRunTB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	stream := transport.NewStream(nil, "dummy/null", &transport.Extra{DryRun: true})

	random := newRand(time.Now().UnixNano())
	slab, _ := Mem2.GetSlab2(cmn.KiB * 32)
	size, num, prevsize := int64(0), 0, int64(0)
	hdr := genStaticHeader()

	for size < cmn.TiB {
		reader := newRandReader(random, hdr, slab)
		stream.Send(hdr, reader, nil)
		num++
		size += hdr.Dsize
		if size-prevsize >= cmn.GiB*100 {
			prevsize = size
			stats := stream.GetStats()
			tutils.Logf("[dry]: %d GiB, idle=%.2f%%\n", size/cmn.GiB, stats.IdlePct)
		}
	}
	go stream.Fin()
	time.Sleep(time.Second * 3)
	stats := stream.GetStats()

	fmt.Printf("[dry]: offset=%d, num=%d(%d), idle=%.2f%%\n", stats.Offset, stats.Num, num, stats.IdlePct)
}

func Test_CompletionCount(t *testing.T) {
	var (
		numSent, numCompleted, numReceived int64
		network                            = "n2"
		mux                                = http.NewServeMux()
	)

	receive := func(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
		cmn.Assert(err == nil)
		written, _ := io.CopyBuffer(ioutil.Discard, objReader, buf1)
		cmn.Assert(written == hdr.Dsize)
		atomic.AddInt64(&numReceived, 1)
	}
	callback := func(hdr transport.Header, reader io.ReadCloser, err error) {
		atomic.AddInt64(&numCompleted, 1)
	}

	transport.SetMux(network, mux)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	path, err := transport.Register(network, "cmpl-cnt", receive)
	if err != nil {
		t.Fatal(err)
	}
	httpclient := &http.Client{Transport: &http.Transport{}}
	url := ts.URL + path
	stream := transport.NewStream(httpclient, url, &transport.Extra{Burst: 256}) // provide for sizeable queue at any point
	random := newRand(time.Now().UnixNano())
	rem := int64(0)
	for idx := 0; idx < 10000; idx++ {
		if idx%7 == 0 {
			hdr := genStaticHeader()
			hdr.Dsize = 0
			hdr.Opaque = []byte(strconv.FormatInt(104729*int64(idx), 10))
			stream.Send(hdr, nil, callback)
			rem = random.Int63() % 13
		} else {
			hdr, rr := makeRandReader()
			stream.Send(hdr, rr, callback)
		}
		numSent++
		if numSent > 5000 && rem == 3 {
			stream.Stop()
			break
		}
	}
	// collect all pending completions untill timeout
	started := time.Now()
	for atomic.LoadInt64(&numCompleted) < numSent {
		time.Sleep(time.Millisecond * 10)
		if time.Since(started) > time.Second*10 {
			break
		}
	}
	if numSent == atomic.LoadInt64(&numCompleted) {
		tutils.Logf("sent %d = %d completed, %d received\n", numSent, atomic.LoadInt64(&numCompleted), atomic.LoadInt64(&numReceived))
	} else {
		t.Fatalf("sent %d != %d completed\n", numSent, atomic.LoadInt64(&numCompleted))
	}
}
