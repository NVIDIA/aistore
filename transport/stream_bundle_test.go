// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package transport_test

import (
	"bytes"
	cryptorand "crypto/rand"
	"errors"
	"io"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
)

type (
	sowner           struct{}
	slisteners       struct{}
	failingOpenState struct {
		opens  atomic.Int64
		closes atomic.Int64
	}
	failingOpenReader struct {
		*bytes.Reader
		state *failingOpenState
	}
)

var errTestReopen = errors.New("reopen failed")

func (r *failingOpenReader) Open() (cos.ReadOpenCloser, error) {
	if r.state.opens.Inc() == 2 {
		return nil, errTestReopen
	}
	return &failingOpenReader{Reader: bytes.NewReader([]byte{1}), state: r.state}, nil
}

func (r *failingOpenReader) Close() error {
	r.state.closes.Inc()
	return nil
}

var (
	smap      meta.Smap
	listeners slisteners
)

func (*sowner) Get() *meta.Smap               { return &smap }
func (*sowner) Listeners() meta.SmapListeners { return &listeners }

func (*slisteners) Reg(meta.Slistener)   {}
func (*slisteners) Unreg(meta.Slistener) {}

func TestBundle(t *testing.T) {
	tests := []struct {
		name string
		nvs  cos.StrKVs
	}{
		{
			name: "not-compressed",
			nvs: cos.StrKVs{
				"compression": apc.CompressNever,
			},
		},
		{
			name: "not-compressed-unsized",
			nvs: cos.StrKVs{
				"compression": apc.CompressNever,
				"unsized":     "yes",
			},
		},
	}
	if !testing.Short() {
		testsLong := []struct {
			name string
			nvs  cos.StrKVs
		}{
			{
				name: "compress-block-1M",
				nvs: cos.StrKVs{
					"compression": apc.CompressAlways,
					"block":       "1MiB",
				},
			},
			{
				name: "compress-block-256K",
				nvs: cos.StrKVs{
					"compression": apc.CompressAlways,
					"block":       "256KiB",
				},
			},
			{
				name: "compress-block-256K-unsized",
				nvs: cos.StrKVs{
					"compression": apc.CompressAlways,
					"block":       "256KiB",
					"unsized":     "yes",
				},
			},
		}
		tests = append(tests, testsLong...)
	}

	tMock := mock.NewTarget(nil)
	tMock.SO = &sowner{}
	core.T = tMock

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testBundle(t, test.nvs)
			time.Sleep(time.Second)
		})
	}
}

func TestBundleReopenFailure(t *testing.T) {
	var (
		state  failingOpenState
		tss    = make([]*httptest.Server, 0, 4)
		cmplCh = make(chan error, 2)
	)

	tMock := mock.NewTarget(nil)
	tMock.SO = &sowner{}
	core.T = tMock
	lsnode := tMock.Snode()

	smap.Tmap = make(meta.NodeMap, 5)
	smap.Tmap[lsnode.ID()] = lsnode
	for i := range 4 {
		ts := httptest.NewServer(objmux)
		tss = append(tss, ts)
		addTarget(&smap, ts, i)
	}
	smap.Version++
	t.Cleanup(func() {
		for _, ts := range tss {
			ts.Close()
		}
	})

	const trname = "bundle-reopen-failure"
	receive := func(_ *transport.ObjHdr, objReader io.Reader, err error) error {
		if err != nil && !cos.IsOkEOF(err) {
			return err
		}
		_, err = io.Copy(io.Discard, objReader)
		return err
	}
	tassert.CheckFatal(t, transport.Handle(trname, receive))
	t.Cleanup(func() { _ = transport.Unhandle(trname) })

	extra := &transport.Extra{
		Config: cmn.GCO.Get(),
		Parent: &transport.Parent{SentCB: func(_ *transport.ObjHdr, _ io.ReadCloser, _ any, err error) {
			cmplCh <- err
		}},
	}
	sb := bundle.New(transport.NewIntraDataClient(), bundle.Args{Trname: trname, Extra: extra})
	closed := false
	t.Cleanup(func() {
		if !closed {
			sb.Close(false /*gracefully*/)
		}
	})

	reader := &failingOpenReader{Reader: bytes.NewReader([]byte{1}), state: &state}
	obj := &transport.Obj{Hdr: transport.ObjHdr{ObjAttrs: cmn.ObjAttrs{Size: 1}}}
	if err := sb.Send(obj, reader); !errors.Is(err, errTestReopen) {
		t.Fatalf("expected reopen failure, got %v", err)
	}
	select {
	case err := <-cmplCh:
		if !errors.Is(err, errTestReopen) {
			t.Errorf("completion error %v, expected reopen failure", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for failed send completion")
	}
	if got := state.opens.Load(); got != 2 {
		t.Errorf("open count %d, expected 2", got)
	}
	if got := state.closes.Load(); got != 2 {
		t.Errorf("close count %d, expected 2", got)
	}

	// The failed object must not prevent subsequent use of the bundle.
	if err := sb.Send(&transport.Obj{}, nil); err != nil {
		t.Fatalf("subsequent send failed: %v", err)
	}
	select {
	case err := <-cmplCh:
		if err != nil {
			t.Errorf("subsequent completion failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for subsequent send completion")
	}

	sb.Close(true /*gracefully*/)
	closed = true
}

func testBundle(t *testing.T, nvs cos.StrKVs) {
	var (
		numCompleted atomic.Int64
		mmsa         = memsys.NewMMSA("bundle.test", false)
		network      = cmn.NetIntraData
		trname       = "bundle" + nvs["block"]
		tss          = make([]*httptest.Server, 0, 32)
	)

	// init local target
	tMock := mock.NewTarget(nil)
	tMock.SO = &sowner{}
	core.T = tMock
	lsnode := tMock.Snode()

	// add target nodes
	smap.Tmap = make(meta.NodeMap, 100)
	smap.Tmap[lsnode.ID()] = lsnode
	for i := range 10 {
		ts := httptest.NewServer(objmux)
		tss = append(tss, ts)
		addTarget(&smap, ts, i)
	}
	defer func() {
		for _, ts := range tss {
			ts.Close()
		}
		mmsa.Terminate(false)
	}()
	smap.Version = 1

	receive := func(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
		if err != nil && !cos.IsOkEOF(err) {
			return err
		}
		written, _ := io.Copy(io.Discard, objReader)
		cos.Assert(written == hdr.ObjAttrs.Size || hdr.IsUnsized())
		return nil
	}
	callback := func(*transport.ObjHdr, io.ReadCloser, any, error) {
		numCompleted.Inc()
	}

	err := transport.Handle(trname, receive) // URL = /v1/transport/10G
	tassert.CheckFatal(t, err)
	defer transport.Unhandle(trname)

	var (
		config         = cmn.GCO.Get()
		httpclient     = transport.NewIntraDataClient()
		random         = newRand(mono.NanoTime())
		wbuf, slab     = mmsa.Alloc()
		extra          = &transport.Extra{Compression: nvs["compression"]}
		size, prevsize int64
		multiplier     = int(random.Int64()%13) + 4
		num            int
		usePDU         bool
	)
	if nvs["compression"] != apc.CompressNever {
		v, _ := cos.ParseSize(nvs["block"], cos.UnitsIEC)
		cos.Assert(v == cos.MiB*4 || v == cos.MiB || v == cos.KiB*256 || v == cos.KiB*64)
		config = cmn.GCO.BeginUpdate()
		config.Transport.LZ4BlockMaxSize = cos.SizeIEC(v)
		cmn.GCO.CommitUpdate(config)
		if err := config.Transport.Validate(); err != nil {
			tassert.CheckFatal(t, err)
		}
	}
	if _, usePDU = nvs["unsized"]; usePDU {
		extra.SizePDU = memsys.DefaultBufSize
	}
	extra.Config = config
	extra.SbundleMult = multiplier
	_, _ = cryptorand.Read(wbuf)
	sb := bundle.New(httpclient,
		bundle.Args{Net: network, Trname: trname, Extra: extra})
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
			err = sb.Send(&transport.Obj{Hdr: hdr, SentCB: callback}, nil)
		} else {
			reader := &randReader{buf: wbuf, hdr: hdr, slab: slab, clone: true} // FIXME: multiplier reopen
			if hdr.IsUnsized() {
				reader.offEOF = int64(random.Int32()>>1) + 1
				objSize = reader.offEOF
			}
			err = sb.Send(&transport.Obj{Hdr: hdr, SentCB: callback}, reader)
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

	slab.Free(wbuf)

	tlog.Logf("send$: num-sent=%d, num-completed=%d\n", num, numCompleted.Load())
}

func addTarget(smap *meta.Smap, ts *httptest.Server, i int) {
	netinfo := meta.NetInfo{URL: ts.URL}
	tid := "t_" + strconv.FormatInt(int64(i), 10)
	smap.Tmap[tid] = &meta.Snode{PubNet: netinfo, ControlNet: netinfo, DataNet: netinfo}
}
