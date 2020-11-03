// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

var cpbuf = make([]byte, 32*cmn.KiB)

func receive10G(w http.ResponseWriter, hdr transport.ObjHdr, objReader io.Reader, err error) {
	cmn.AssertNoErr(err)
	written, _ := io.CopyBuffer(ioutil.Discard, objReader, cpbuf)
	cmn.Assert(written == hdr.ObjAttrs.Size)
}

func Test_CompressedOne(t *testing.T) {
	trname := "cmpr-one"
	config := cmn.GCO.BeginUpdate()
	config.Compression.BlockMaxSize = 256 * cmn.KiB
	cmn.GCO.CommitUpdate(config)
	if err := config.Compression.Validate(config); err != nil {
		tassert.CheckFatal(t, err)
	}

	ts := httptest.NewServer(objmux)
	defer ts.Close()

	err := transport.HandleObjStream(trname, receive10G, memsys.DefaultPageMM() /* optionally, specify memsys*/)
	tassert.CheckFatal(t, err)
	defer transport.Unhandle(trname)

	httpclient := transport.NewIntraDataClient()
	url := ts.URL + transport.ObjURLPath(trname)
	err = os.Setenv("AIS_STREAM_BURST_NUM", "2")
	tassert.CheckFatal(t, err)
	defer os.Unsetenv("AIS_STREAM_BURST_NUM")
	stream := transport.NewObjStream(httpclient, url, &transport.Extra{Compression: cmn.CompressAlways})

	slab, _ := MMSA.GetSlab(memsys.MaxPageSlabSize)
	random := newRand(mono.NanoTime())
	buf := slab.Alloc()
	_, _ = random.Read(buf)
	hdr := genStaticHeader()
	size, prevsize, num, numhdr, numGs := int64(0), int64(0), 0, 0, int64(16)
	if testing.Short() {
		numGs = 2
	}
	for size < cmn.GiB*numGs {
		if num%7 == 0 { // header-only
			hdr.ObjAttrs.Size = 0
			stream.Send(&transport.Obj{Hdr: hdr})
			numhdr++
		} else {
			var reader io.ReadCloser
			if num%3 == 0 {
				hdr.ObjAttrs.Size = int64(random.Intn(100) + 1)
				reader = ioutil.NopCloser(&io.LimitedReader{R: random, N: hdr.ObjAttrs.Size}) // fully random to hinder compression
			} else {
				hdr.ObjAttrs.Size = int64(random.Intn(cmn.GiB) + 1)
				reader = &randReader{buf: buf, hdr: hdr, clone: true}
			}
			stream.Send(&transport.Obj{Hdr: hdr, Reader: reader})
		}
		num++
		size += hdr.ObjAttrs.Size
		if size-prevsize >= cmn.GiB*4 {
			stats := stream.GetStats()
			tutils.Logf("%s: %d GiB compression-ratio=%.2f\n", stream, size/cmn.GiB, stats.CompressionRatio())
			prevsize = size
		}
	}
	stream.Fin()
	stats := stream.GetStats()

	slab.Free(buf)

	fmt.Printf("send$ %s: offset=%d, num=%d(%d/%d), compression-ratio=%.2f\n",
		stream, stats.Offset.Load(), stats.Num.Load(), num, numhdr, stats.CompressionRatio())

	printNetworkStats(t)
}

func Test_DryRun(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	err := os.Setenv("AIS_STREAM_DRY_RUN", "true")
	defer os.Unsetenv("AIS_STREAM_DRY_RUN")
	tassert.CheckFatal(t, err)
	stream := transport.NewObjStream(nil, "dummy/null", nil)

	random := newRand(mono.NanoTime())
	slab, _ := MMSA.GetSlab(cmn.KiB * 32)
	size, num, prevsize := int64(0), 0, int64(0)
	hdr := genStaticHeader()

	for size < cmn.TiB/4 {
		reader := newRandReader(random, hdr, slab)
		stream.Send(&transport.Obj{Hdr: hdr, Reader: reader})
		num++
		size += hdr.ObjAttrs.Size
		if size-prevsize >= cmn.GiB*100 {
			prevsize = size
			tutils.Logf("[dry]: %d GiB\n", size/cmn.GiB)
		}
	}
	stream.Fin()
	stats := stream.GetStats()

	fmt.Printf("[dry]: offset=%d, num=%d(%d)\n", stats.Offset.Load(), stats.Num.Load(), num)
}

func Test_CompletionCount(t *testing.T) {
	var (
		numSent                   int64
		numCompleted, numReceived atomic.Int64
	)

	receive := func(w http.ResponseWriter, hdr transport.ObjHdr, objReader io.Reader, err error) {
		cmn.Assert(err == nil)
		written, _ := io.CopyBuffer(ioutil.Discard, objReader, cpbuf)
		cmn.Assert(written == hdr.ObjAttrs.Size)
		numReceived.Inc()
	}
	callback := func(_ transport.ObjHdr, _ io.ReadCloser, _ unsafe.Pointer, _ error) {
		numCompleted.Inc()
	}

	ts := httptest.NewServer(objmux)
	defer ts.Close()

	trname := "cmpl-cnt"
	err := transport.HandleObjStream(trname, receive)
	tassert.CheckFatal(t, err)
	defer transport.Unhandle(trname)
	httpclient := transport.NewIntraDataClient()
	url := ts.URL + transport.ObjURLPath(trname)
	err = os.Setenv("AIS_STREAM_BURST_NUM", "256")
	tassert.CheckFatal(t, err)
	defer os.Unsetenv("AIS_STREAM_BURST_NUM")
	stream := transport.NewObjStream(httpclient, url, nil) // provide for sizeable queue at any point
	random := newRand(mono.NanoTime())
	rem := int64(0)
	for idx := 0; idx < 10000; idx++ {
		if idx%7 == 0 {
			hdr := genStaticHeader()
			hdr.ObjAttrs.Size = 0
			hdr.Opaque = []byte(strconv.FormatInt(104729*int64(idx), 10))
			stream.Send(&transport.Obj{Hdr: hdr, Callback: callback})
			rem = random.Int63() % 13
		} else {
			hdr, rr := makeRandReader()
			stream.Send(&transport.Obj{Hdr: hdr, Reader: rr, Callback: callback})
		}
		numSent++
		if numSent > 5000 && rem == 3 {
			stream.Stop()
			break
		}
	}
	// collect all pending completions until timeout
	started := time.Now()
	for numCompleted.Load() < numSent {
		time.Sleep(time.Millisecond * 10)
		if time.Since(started) > time.Second*10 {
			break
		}
	}
	if numSent == numCompleted.Load() {
		tutils.Logf("sent %d = %d completed, %d received\n", numSent, numCompleted.Load(), numReceived.Load())
	} else {
		t.Fatalf("sent %d != %d completed\n", numSent, numCompleted.Load())
	}
}
