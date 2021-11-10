// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport_test

import (
	"fmt"
	"io"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
)

type (
	sowner     struct{}
	slisteners struct{}
)

var (
	smap      cluster.Smap
	listeners slisteners
)

func (*sowner) Get() *cluster.Smap               { return &smap }
func (*sowner) Listeners() cluster.SmapListeners { return &listeners }

func (*slisteners) Reg(cluster.Slistener)   {}
func (*slisteners) Unreg(cluster.Slistener) {}

func Test_Bundle(t *testing.T) {
	tests := []struct {
		name string
		nvs  cos.SimpleKVs
	}{
		{
			name: "not-compressed",
			nvs: cos.SimpleKVs{
				"compression": cmn.CompressNever,
			},
		},
		{
			name: "not-compressed-unsized",
			nvs: cos.SimpleKVs{
				"compression": cmn.CompressNever,
				"unsized":     "yes",
			},
		},
		{
			name: "compress-block-1M",
			nvs: cos.SimpleKVs{
				"compression": cmn.CompressAlways,
				"block":       "1MiB",
			},
		},
		{
			name: "compress-block-256K",
			nvs: cos.SimpleKVs{
				"compression": cmn.CompressAlways,
				"block":       "256KiB",
			},
		},
		{
			name: "compress-block-256K-unsized",
			nvs: cos.SimpleKVs{
				"compression": cmn.CompressAlways,
				"block":       "256KiB",
				"unsized":     "yes",
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

func testBundle(t *testing.T, nvs cos.SimpleKVs) {
	var (
		numCompleted atomic.Int64
		MMSA         = tutils.MMSA()
		network      = cmn.NetworkIntraData
		trname       = "bundle" + nvs["block"]
		tss          = make([]*httptest.Server, 0, 32)
	)
	smap.Tmap = make(cluster.NodeMap, 100)
	for i := 0; i < 10; i++ {
		ts := httptest.NewServer(objmux)
		tss = append(tss, ts)
		addTarget(&smap, ts, i)
	}
	defer func() {
		for _, ts := range tss {
			ts.Close()
		}
	}()
	smap.Version = 1

	receive := func(hdr transport.ObjHdr, objReader io.Reader, err error) {
		if err != nil && !cos.IsEOF(err) {
			tassert.CheckFatal(t, err)
		}
		written, _ := io.Copy(io.Discard, objReader)
		cos.Assert(written == hdr.ObjAttrs.Size || hdr.IsUnsized())
	}
	callback := func(_ transport.ObjHdr, _ io.ReadCloser, _ interface{}, _ error) {
		numCompleted.Inc()
	}

	err := transport.HandleObjStream(trname, receive) // DirectURL = /v1/transport/10G
	tassert.CheckFatal(t, err)
	defer transport.Unhandle(trname)

	var (
		httpclient     = transport.NewIntraDataClient()
		sowner         = &sowner{}
		lsnode         = cluster.Snode{DaemonID: "local"}
		random         = newRand(mono.NanoTime())
		wbuf, slab     = MMSA.Alloc()
		extra          = &transport.Extra{Compression: nvs["compression"], MMSA: MMSA}
		size, prevsize int64
		multiplier     = int(random.Int63()%13) + 4
		num            int
		usePDU         bool
	)
	if nvs["compression"] != cmn.CompressNever {
		v, _ := cos.S2B(nvs["block"])
		cos.Assert(v == cos.MiB || v == cos.KiB*256 || v == cos.KiB*64)
		config := cmn.GCO.BeginUpdate()
		config.Compression.BlockMaxSize = int(v)
		cmn.GCO.CommitUpdate(config)
		if err := config.Compression.Validate(); err != nil {
			tassert.CheckFatal(t, err)
		}
	}
	if _, usePDU = nvs["unsized"]; usePDU {
		extra.SizePDU = transport.DefaultSizePDU
	}
	_, _ = random.Read(wbuf)
	sb := bundle.NewStreams(sowner, &lsnode, httpclient,
		bundle.Args{Net: network, Trname: trname, Multiplier: multiplier, Extra: extra})
	var numGs int64 = 6
	if testing.Short() {
		numGs = 1
	}
	for size < cos.GiB*numGs {
		var err error
		hdr := genRandomHeader(random, usePDU)
		objSize := hdr.ObjAttrs.Size
		if num%7 == 0 {
			objSize, hdr.ObjAttrs.Size = 0, 0
			err = sb.Send(&transport.Obj{Hdr: hdr, Callback: callback}, nil)
		} else {
			reader := &randReader{buf: wbuf, hdr: hdr, slab: slab, clone: true} // FIXME: multiplier reopen
			if hdr.IsUnsized() {
				reader.offEOF = int64(random.Int31()>>1) + 1
				objSize = reader.offEOF
			}
			err = sb.Send(&transport.Obj{Hdr: hdr, Callback: callback}, reader)
		}
		if err != nil {
			t.Fatalf("%s: exiting with err [%v]\n", sb, err)
		}
		num++
		size += objSize
		if size-prevsize >= cos.GiB {
			tlog.Logf("%s: %d GiB\n", sb, size/cos.GiB)
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
