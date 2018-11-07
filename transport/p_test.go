/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package transport_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/transport"
	"github.com/NVIDIA/dfcpub/tutils"
)

var buf1 []byte

func receive10G(w http.ResponseWriter, hdr transport.Header, objReader io.Reader) {
	written, _ := io.CopyBuffer(ioutil.Discard, objReader, buf1)
	cmn.Assert(written == hdr.Dsize)
}

func Test_OneStream10G(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	mux := http.NewServeMux()

	transport.SetMux("np", mux)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	path, err := transport.Register("np", "10G", receive10G)
	tutils.CheckFatal(err, t)

	httpclient := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

	url := ts.URL + path
	stream := transport.NewStream(httpclient, url, &transport.Extra{Burst: 2})
	trname, sessid := stream.ID()

	slab := Mem2.SelectSlab2(cmn.MiB)
	buf1 = slab.Alloc()

	random := newRand(time.Now().UnixNano())
	slab, _ = Mem2.GetSlab2(cmn.KiB * 32)
	size, num, prevsize := int64(0), 0, int64(0)
	hdr := genStaticHeader()

	for size < cmn.GiB*10 {
		reader := newRandReader(random, hdr, slab)
		stream.Send(hdr, reader, nil)
		num++
		size += hdr.Dsize
		if size-prevsize >= cmn.GiB {
			tutils.Logf("[10G]: %d GiB\n", size/cmn.GiB)
			prevsize = size
			stats := stream.GetStats()
			tutils.Logf("send$ %s[%d]: idle=%.2f%%\n", trname, sessid, stats.IdlePct)
		}
	}
	stream.Fin()
	stats := stream.GetStats()

	fmt.Printf("send$ %s[%d]: offset=%d, num=%d(%d), idle=%.2f%%\n",
		trname, sessid, stats.Offset, stats.Num, num, stats.IdlePct)

	printNetworkStats(t, "np")
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
