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
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/golang/mux"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

type (
	sowner     struct{}
	slisteners struct{}
)

var (
	smap      cluster.Smap
	listeners slisteners
)

func (sowner *sowner) Get() *cluster.Smap               { return &smap }
func (sowner *sowner) Listeners() cluster.SmapListeners { return &listeners }
func (listeners *slisteners) Reg(sl cluster.Slistener)  {}
func (listeners *slisteners) Unreg(cluster.Slistener)   {}

func Test_Bundle(t *testing.T) {
	tests := []struct {
		name string
		nvs  cmn.SimpleKVs
	}{
		{
			name: "not-compressed",
			nvs: cmn.SimpleKVs{
				"compression": cmn.CompressNever,
			},
		},
		{
			name: "compress-block-1M",
			nvs: cmn.SimpleKVs{
				"compression": cmn.CompressAlways,
				"block":       "1MiB",
			},
		},
		{
			name: "compress-block-256K",
			nvs: cmn.SimpleKVs{
				"compression": cmn.CompressAlways,
				"block":       "256KiB",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testBundle(t, test.nvs)
			time.Sleep(time.Second)
		})
	}
}

func testBundle(t *testing.T, nvs cmn.SimpleKVs) {
	var (
		numCompleted atomic.Int64
		MMSA         = tutils.MMSA
		network      = cmn.NetworkIntraData
		trname       = "bundle"
		tss          = make([]*httptest.Server, 0, 32)
	)
	mux := mux.NewServeMux()

	smap.Tmap = make(cluster.NodeMap, 100)
	for i := 0; i < 10; i++ {
		ts := httptest.NewServer(mux)
		tss = append(tss, ts)
		addTarget(&smap, ts, i)
	}
	defer func() {
		for _, ts := range tss {
			ts.Close()
		}
	}()
	smap.Version = 1

	transport.SetMux(network, mux)

	slab, _ := MMSA.GetSlab(32 * cmn.KiB)
	rbuf := slab.Alloc()
	defer slab.Free(rbuf)
	receive := func(w http.ResponseWriter, hdr transport.ObjHdr, objReader io.Reader, err error) {
		tassert.CheckFatal(t, err)
		written, _ := io.CopyBuffer(ioutil.Discard, objReader, rbuf)
		cmn.Assert(written == hdr.ObjAttrs.Size)
	}
	callback := func(_ transport.ObjHdr, _ io.ReadCloser, _ unsafe.Pointer, _ error) {
		numCompleted.Inc()
	}

	_, err := transport.Register(network, trname, receive) // DirectURL = /v1/transport/10G
	tassert.CheckFatal(t, err)

	var (
		httpclient     = transport.NewIntraDataClient()
		sowner         = &sowner{}
		lsnode         = cluster.Snode{DaemonID: "local"}
		random         = newRand(mono.NanoTime())
		wbuf           = slab.Alloc()
		extra          = &transport.Extra{Compression: nvs["compression"], MMSA: MMSA}
		size, prevsize int64
		multiplier     = int(random.Int63()%13) + 4
		num            int
	)
	if nvs["compression"] != cmn.CompressNever {
		v, _ := cmn.S2B(nvs["block"])
		cmn.Assert(v == cmn.MiB || v == cmn.KiB*256 || v == cmn.KiB*64)
		config := cmn.GCO.BeginUpdate()
		config.Compression.BlockMaxSize = int(v)
		cmn.GCO.CommitUpdate(config)
		if err := config.Compression.Validate(config); err != nil {
			tassert.CheckFatal(t, err)
		}
	}
	_, _ = random.Read(wbuf)
	sb := bundle.NewStreams(sowner, &lsnode, httpclient,
		bundle.Args{Network: network, Trname: trname, Multiplier: multiplier, Extra: extra})
	var numGs int64 = 10
	if testing.Short() {
		numGs = 1
	}
	for size < cmn.GiB*numGs {
		var err error
		hdr := genRandomHeader(random)
		if num%7 == 0 {
			hdr.ObjAttrs.Size = 0
			err = sb.Send(transport.Obj{Hdr: hdr, Callback: callback}, nil)
		} else {
			reader := &randReader{buf: wbuf, hdr: hdr, slab: slab, clone: true} // FIXME: multiplier reopen
			err = sb.Send(transport.Obj{Hdr: hdr, Callback: callback}, reader)
		}
		if err != nil {
			t.Fatalf("%s: exiting with err [%v]\n", sb, err)
		}
		num++
		size += hdr.ObjAttrs.Size
		if size-prevsize >= cmn.GiB {
			tutils.Logf("%s: %d GiB\n", sb, size/cmn.GiB)
			prevsize = size
		}
	}
	sb.Close(true /* gracefully */)
	stats := sb.GetStats()

	slab.Free(wbuf)

	if nvs["compression"] != cmn.CompressNever {
		for id, tstat := range stats {
			fmt.Printf("send$ %s/%s: offset=%d, num=%d(%d), compression-ratio=%.2f\n",
				id, trname, tstat.Offset.Load(), tstat.Num.Load(), num, tstat.CompressionRatio())
		}
	} else {
		for id, tstat := range stats {
			fmt.Printf("send$ %s/%s: offset=%d, num=%d(%d)\n",
				id, trname, tstat.Offset.Load(), tstat.Num.Load(), num)
		}
	}
	fmt.Printf("send$: num-sent=%d, num-completed=%d\n", num, numCompleted.Load())
}

func addTarget(smap *cluster.Smap, ts *httptest.Server, i int) {
	netinfo := cluster.NetInfo{DirectURL: ts.URL}
	tid := "t_" + strconv.FormatInt(int64(i), 10)
	smap.Tmap[tid] = &cluster.Snode{PublicNet: netinfo, IntraControlNet: netinfo, IntraDataNet: netinfo}
}
