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

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/transport"
	"github.com/NVIDIA/dfcpub/tutils"
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
func (listeners *slisteners) Reg(cluster.Slistener)     {}
func (listeners *slisteners) Unreg(cluster.Slistener)   {}

func Test_Bundle(t *testing.T) {
	var (
		numCompleted int64
		Mem2         = tutils.Mem2
		network      = cmn.NetworkIntraData
		trname       = "bundle"
	)
	mux := http.NewServeMux()

	smap.Tmap = make(cluster.NodeMap, 100)
	for i := 0; i < 10; i++ {
		ts := httptest.NewServer(mux)
		defer ts.Close()
		addTarget(&smap, ts, i)
	}
	smap.Version = 1

	transport.SetMux(network, mux)

	slab := Mem2.SelectSlab2(32 * cmn.KiB)
	buf := slab.Alloc()
	defer slab.Free(buf)
	receive := func(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
		cmn.Assert(err == nil)
		written, _ := io.CopyBuffer(ioutil.Discard, objReader, buf)
		cmn.Assert(written == hdr.Dsize)
	}
	callback := func(hdr transport.Header, reader io.ReadCloser, err error) {
		atomic.AddInt64(&numCompleted, 1)
	}

	_, err := transport.Register(network, trname, receive) // DirectURL = /v1/transport/10G
	tutils.CheckFatal(err, t)

	httpclient := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

	sowner := &sowner{}
	lsnode := cluster.Snode{DaemonID: "local"}

	random := newRand(time.Now().UnixNano())
	multiplier := int(random.Int63()%13) + 4
	sb := cluster.NewStreamBundle(sowner, &lsnode, httpclient, network, trname, nil, cluster.Targets, multiplier)

	size, num, prevsize := int64(0), 0, int64(0)

	for size < cmn.GiB*10 {
		var err error
		hdr := genRandomHeader(random)
		if num%7 == 0 {
			hdr.Dsize = 0
			err = sb.SendV(hdr, nil, callback)
		} else {
			reader := newRandReader(random, hdr, slab)
			err = sb.SendV(hdr, reader, callback)
		}
		if err != nil {
			t.Fatalf("%s: exiting with err [%v]\n", sb, err)
		}
		num++
		size += hdr.Dsize
		if size-prevsize >= cmn.GiB {
			tutils.Logf("%s: %d GiB\n", sb, size/cmn.GiB)
			prevsize = size
		}
	}
	sb.Close(true /* gracefully */)
	stats := sb.GetStats()

	for id, tstat := range stats {
		fmt.Printf("send$ %s/%s: offset=%d, num=%d(%d), idle=%.2f%%\n", id, trname, tstat.Offset, tstat.Num, num, tstat.IdlePct)
	}
	// printNetworkStats(t, network)

	fmt.Printf("send$: num-sent=%d, num-completed=%d\n", num, atomic.LoadInt64(&numCompleted))

	glog.Flush()
}

func addTarget(smap *cluster.Smap, ts *httptest.Server, i int) {
	netinfo := cluster.NetInfo{DirectURL: ts.URL}
	tid := "t_" + strconv.FormatInt(int64(i), 10)
	smap.Tmap[tid] = &cluster.Snode{PublicNet: netinfo, IntraControlNet: netinfo, IntraDataNet: netinfo}
}
