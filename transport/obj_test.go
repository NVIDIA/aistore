// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package transport_test

// How to run:
//
// 1) run all unit tests
// go test -v
//
// 2) run tests matching "Multi" with debug enabled:
// go test -v -run=Multi -tags=debug

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/golang/mux"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/transport"
)

const (
	lorem = `Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut
labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.`
	duis = `Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.
Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.`
	et = `Et harum quidem rerum facilis est et expedita distinctio. Nam libero tempore, cum soluta nobis est
eligendi optio, cumque nihil impedit, quo minus id, quod maxime placeat, facere possimus, omnis voluptas assumenda est, omnis dolor repellendus.`
	temporibus = `Temporibus autem quibusdam et aut officiis debitis aut rerum necessitatibus saepe eveniet,
ut et voluptates repudiandae sint et molestiae non-recusandae.`
	text = lorem + duis + et + temporibus
)

type dummyStatsTracker struct{}

// interface guard
var _ cos.StatsUpdater = (*dummyStatsTracker)(nil)

func (*dummyStatsTracker) Add(string, int64)                                         {}
func (*dummyStatsTracker) Inc(string)                                                {}
func (*dummyStatsTracker) Get(string) int64                                          { return 0 }
func (*dummyStatsTracker) AddWith(...cos.NamedVal64)                                 {}
func (*dummyStatsTracker) IncWith(string, map[string]string)                         {}
func (*dummyStatsTracker) ClrFlag(string, cos.NodeStateFlags)                        {}
func (*dummyStatsTracker) SetFlag(string, cos.NodeStateFlags)                        {}
func (*dummyStatsTracker) SetClrFlag(string, cos.NodeStateFlags, cos.NodeStateFlags) {}

var (
	objmux   *mux.ServeMux
	duration time.Duration // test duration
)

func TestMain(t *testing.M) {
	var (
		d   string
		err error
	)
	flag.StringVar(&d, "duration", "30s", "test duration")
	flag.Parse()
	if duration, err = time.ParseDuration(d); err != nil {
		cos.Exitf("Invalid duration %q", d)
	}

	config := cmn.GCO.BeginUpdate()
	config.Transport.MaxHeaderSize = memsys.PageSize
	config.Transport.IdleTeardown = cos.Duration(time.Second)
	config.Transport.QuiesceTime = cos.Duration(10 * time.Second)
	config.Log.Level = "3"
	cmn.GCO.CommitUpdate(config)
	sc := transport.Init(&dummyStatsTracker{})
	go sc.Run()

	tMock := mock.NewTarget(nil)
	tMock.SO = &sowner{}
	core.T = tMock

	objmux = mux.NewServeMux(false /*enableTracing*/)
	path := transport.ObjURLPath("")
	objmux.HandleFunc(path, transport.RxAnyStream)
	objmux.HandleFunc(path+"/", transport.RxAnyStream)

	os.Exit(t.Run())
}

func Example_headers() {
	f := func(_ http.ResponseWriter, r *http.Request) {
		body, err := cos.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		if len(body) == 0 {
			return
		}
		var (
			hdr       transport.ObjHdr
			hlen, off int
		)
		for {
			hlen = int(binary.BigEndian.Uint64(body[off:]))
			off += 16 // hlen and hlen-checksum
			hdr = transport.ExtObjHeader(body[off:], hlen)

			if transport.ReservedOpcode(hdr.Opcode) {
				break
			}

			fmt.Printf("Bck:%s ObjName:%s SID:%s Opaque:%v ObjAttrs:{%s} (%d)\n",
				hdr.Bck.String(), hdr.ObjName, hdr.SID, hdr.Opaque, hdr.ObjAttrs.String(), hlen)
			off += hlen + int(hdr.ObjAttrs.Size)
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(f))
	defer ts.Close()

	httpclient := transport.NewIntraDataClient()
	stream := transport.NewObjStream(httpclient, ts.URL, cos.GenTie(), nil)

	sendText(stream, lorem, duis)
	stream.Fin()

	// Output:
	// Bck:s3://@uuid#namespace/abc ObjName:X SID: Opaque:[] ObjAttrs:{231B, v"1", xxhash2[h1], map[]} (72)
	// Bck:ais://abracadabra ObjName:p/q/s SID: Opaque:[49 50 51] ObjAttrs:{213B, v"222222222222222222222222", xxhash2[h2], map[xx:11 yy:22]} (113)
}

func sendText(stream *transport.Stream, txt1, txt2 string) {
	var wg sync.WaitGroup
	cb := func(*transport.ObjHdr, io.ReadCloser, any, error) {
		wg.Done()
	}
	sgl1 := memsys.PageMM().NewSGL(0)
	sgl1.Write([]byte(txt1))
	hdr := transport.ObjHdr{
		Bck: cmn.Bck{
			Name:     "abc",
			Provider: apc.AWS,
			Ns:       cmn.Ns{UUID: "uuid", Name: "namespace"},
		},
		ObjName: "X",
		ObjAttrs: cmn.ObjAttrs{
			Size:  sgl1.Size(),
			Atime: 663346294,
			Cksum: cos.NewCksum(cos.ChecksumCesXxh, "h1"),
		},
		Opaque: nil,
	}
	hdr.ObjAttrs.SetVersion("1")
	wg.Add(1)
	stream.Send(&transport.Obj{Hdr: hdr, Reader: sgl1, SentCB: cb})
	wg.Wait()

	sgl2 := memsys.PageMM().NewSGL(0)
	sgl2.Write([]byte(txt2))
	hdr = transport.ObjHdr{
		Bck: cmn.Bck{
			Name:     "abracadabra",
			Provider: apc.AIS,
			Ns:       cmn.NsGlobal,
		},
		ObjName: "p/q/s",
		ObjAttrs: cmn.ObjAttrs{
			Size:  sgl2.Size(),
			Atime: 663346294,
			Cksum: cos.NewCksum(cos.ChecksumCesXxh, "h2"),
		},
		Opaque: []byte{'1', '2', '3'},
	}
	hdr.ObjAttrs.SetVersion("222222222222222222222222")
	hdr.ObjAttrs.SetCustomMD(cos.StrKVs{"xx": "11", "yy": "22"})
	wg.Add(1)
	stream.Send(&transport.Obj{Hdr: hdr, Reader: sgl2, SentCB: cb})
	wg.Wait()
}

func Example_obj() {
	receive := func(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
		cos.Assert(err == nil)
		object, err := cos.ReadAll(objReader)
		if err != nil {
			panic(err)
		}
		if int64(len(object)) != hdr.ObjAttrs.Size {
			panic(fmt.Sprintf("size %d != %d", len(object), hdr.ObjAttrs.Size))
		}
		fmt.Printf("%s...\n", string(object[:16]))
		return nil
	}
	ts := httptest.NewServer(objmux)
	defer ts.Close()
	trname := "dummy-obj"
	err := transport.Handle(trname, receive)
	if err != nil {
		fmt.Println(err)
		return
	}
	httpclient := transport.NewIntraDataClient()
	stream := transport.NewObjStream(httpclient, ts.URL+transport.ObjURLPath(trname), cos.GenTie(), nil)
	sendText(stream, lorem, duis)
	sendText(stream, et, temporibus)
	stream.Fin()

	// Output:
	// Lorem ipsum dolo...
	// Duis aute irure ...
	// Et harum quidem ...
	// Temporibus autem...
}

// test random streaming
func TestOneStream(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	ts := httptest.NewServer(objmux)
	defer ts.Close()

	streamWriteUntil(t, 55, nil, ts, false /*compress*/, true /*PDU*/)
}

func TestMultiStream(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	tlog.Logf("Duration %v\n", duration)
	ts := httptest.NewServer(objmux)
	defer ts.Close()

	wg := &sync.WaitGroup{}
	for i := range 16 {
		wg.Add(1)
		go streamWriteUntil(t, i, wg, ts, false /*compress*/, false /*PDU*/)
	}
	wg.Wait()
	tlog.Logln("Multi-stream test completed successfully")
}

func TestMultipleNetworks(t *testing.T) {
	totalRecv, recvFunc := makeRecvFunc(t)

	streams := make([]*transport.Stream, 0, 10)
	for idx := range 10 {
		ts := httptest.NewServer(objmux)
		defer ts.Close()
		trname := "endpoint" + strconv.Itoa(idx)
		err := transport.Handle(trname, recvFunc)
		tassert.CheckFatal(t, err)
		defer transport.Unhandle(trname)

		httpclient := transport.NewIntraDataClient()
		url := ts.URL + transport.ObjURLPath(trname)
		streams = append(streams, transport.NewObjStream(httpclient, url, cos.GenTie(), nil))
	}

	totalSend := int64(0)
	random := newRand(mono.NanoTime())
	for _, stream := range streams {
		hdr, reader := makeRandReader(random, false)
		totalSend += hdr.ObjAttrs.Size
		stream.Send(&transport.Obj{Hdr: hdr, Reader: reader})
	}

	for _, stream := range streams {
		stream.Fin()
	}
	time.Sleep(3 * time.Second) // FIN has been sent but not necessarily received

	if *totalRecv != totalSend {
		t.Fatalf("total received bytes %d is different from expected: %d", *totalRecv, totalSend)
	}
}

func TestSendCallback(t *testing.T) {
	objectCnt := 10000
	if testing.Short() {
		objectCnt = 1000
	}

	ts := httptest.NewServer(objmux)
	defer ts.Close()

	totalRecv, recvFunc := makeRecvFunc(t)
	trname := "callback"
	err := transport.Handle(trname, recvFunc)
	tassert.CheckFatal(t, err)
	defer transport.Unhandle(trname)
	httpclient := transport.NewIntraDataClient()
	url := ts.URL + transport.ObjURLPath(trname)
	stream := transport.NewObjStream(httpclient, url, cos.GenTie(), nil)

	var (
		totalSend int64
		mu        sync.Mutex
		posted    = make([]*randReader, objectCnt)
	)
	random := newRand(mono.NanoTime())
	for idx := range posted {
		hdr, rr := makeRandReader(random, false)
		mu.Lock()
		posted[idx] = rr
		mu.Unlock()
		rrc := &randReaderCtx{t, rr, posted, &mu, idx}
		totalSend += hdr.ObjAttrs.Size
		stream.Send(&transport.Obj{Hdr: hdr, Reader: rr, SentCB: rrc.sentCallback})
	}
	stream.Fin()

	for idx := range posted {
		if posted[idx] != nil {
			t.Errorf("sent-callback %d never fired", idx)
		}
	}
	if *totalRecv != totalSend {
		t.Fatalf("total received bytes %d is different from expected: %d", *totalRecv, totalSend)
	}
}

func _ptrstr(s string) *string { return &s }

func TestObjAttrs(t *testing.T) {
	testAttrs := []cmn.ObjAttrs{
		{
			Size:  1024,
			Atime: 1024,
			Cksum: cos.NewCksum("", ""),
			Ver:   _ptrstr("102.44"),
		},
		{
			Size:  1024,
			Atime: math.MaxInt64,
			Cksum: cos.NewCksum(cos.ChecksumCesXxh, "120421"),
			Ver:   _ptrstr("102.44"),
		},
		{
			Size:  0,
			Atime: 0,
			Cksum: cos.NewCksum(cos.ChecksumNone, "120421"),
			Ver:   nil, // NOTE: "" becomes nil via ObjAttrs.SetVersion()
		},
	}

	ts := httptest.NewServer(objmux)
	defer ts.Close()

	var receivedCount atomic.Int64
	recvFunc := func(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
		cos.Assert(err == nil)
		idx := hdr.Opaque[0]
		cos.AssertMsg(hdr.Bck.IsAIS(), "expecting ais bucket")
		cos.Assertf(reflect.DeepEqual(testAttrs[idx], hdr.ObjAttrs),
			"attrs are not equal: %v; %v;", testAttrs[idx], hdr.ObjAttrs)

		written, err := io.Copy(io.Discard, objReader)
		cos.Assert(err == nil)
		cos.Assertf(written == hdr.ObjAttrs.Size, "written: %d, expected: %d", written, hdr.ObjAttrs.Size)

		receivedCount.Inc()
		return nil
	}
	trname := "objattrs"
	err := transport.Handle(trname, recvFunc)
	tassert.CheckFatal(t, err)
	defer transport.Unhandle(trname)
	httpclient := transport.NewIntraDataClient()
	url := ts.URL + transport.ObjURLPath(trname)
	stream := transport.NewObjStream(httpclient, url, cos.GenTie(), nil)

	random := newRand(mono.NanoTime())
	for idx, attrs := range testAttrs {
		var (
			reader io.ReadCloser
			hdr    = transport.ObjHdr{
				Bck: cmn.Bck{
					Provider: apc.AIS,
				},
				ObjAttrs: attrs,
				Opaque:   []byte{byte(idx)},
			}
		)
		slab, err := memsys.PageMM().GetSlab(memsys.PageSize)
		if err != nil {
			t.Fatal(err)
		}
		if hdr.ObjAttrs.Size > 0 {
			reader = newRandReader(random, hdr, slab)
		}
		if err := stream.Send(&transport.Obj{Hdr: hdr, Reader: reader}); err != nil {
			t.Fatal(err)
		}
	}
	stream.Fin()
	if receivedCount.Load() != int64(len(testAttrs)) {
		t.Fatalf("invalid received count: %d, expected: %d", receivedCount.Load(), len(testAttrs))
	}
}

func receive10G(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
	cos.Assert(err == nil || cos.IsAnyEOF(err))
	written, _ := io.Copy(io.Discard, objReader)
	cos.Assert(written == hdr.ObjAttrs.Size)
	return nil
}

func TestCompressedOne(t *testing.T) {
	trname := "cmpr-one"
	config := cmn.GCO.BeginUpdate()
	config.Transport.LZ4BlockMaxSize = 256 * cos.KiB
	config.Transport.IdleTeardown = cos.Duration(time.Second)
	config.Transport.QuiesceTime = cos.Duration(8 * time.Second)
	cmn.GCO.CommitUpdate(config)
	if err := config.Transport.Validate(); err != nil {
		tassert.CheckFatal(t, err)
	}

	ts := httptest.NewServer(objmux)
	defer ts.Close()

	err := transport.Handle(trname, receive10G)
	tassert.CheckFatal(t, err)
	defer transport.Unhandle(trname)

	httpclient := transport.NewIntraDataClient()
	url := ts.URL + transport.ObjURLPath(trname)
	t.Setenv("AIS_STREAM_BURST_NUM", "2")
	stream := transport.NewObjStream(httpclient, url, cos.GenTie(), &transport.Extra{Compression: apc.CompressAlways})

	slab, _ := memsys.PageMM().GetSlab(memsys.MaxPageSlabSize)
	random := newRand(mono.NanoTime())
	buf := slab.Alloc()
	_, _ = cryptorand.Read(buf)
	hdr := genStaticHeader(random)
	size, prevsize, num, numhdr, numGs := int64(0), int64(0), 0, 0, int64(16)
	if testing.Short() {
		numGs = 2
	}
	for size < cos.GiB*numGs {
		if num%7 == 0 { // header-only
			hdr.ObjAttrs.Size = 0
			stream.Send(&transport.Obj{Hdr: hdr})
			numhdr++
		} else {
			var reader io.ReadCloser
			if num%3 == 0 {
				hdr.ObjAttrs.Size = int64(random.IntN(100) + 1)
				// fully random to prevent compression
				reader = io.NopCloser(&io.LimitedReader{R: cryptorand.Reader, N: hdr.ObjAttrs.Size})
			} else {
				hdr.ObjAttrs.Size = int64(random.IntN(cos.GiB) + 1)
				reader = &randReader{buf: buf, hdr: hdr, clone: true}
			}
			stream.Send(&transport.Obj{Hdr: hdr, Reader: reader})
		}
		num++
		size += hdr.ObjAttrs.Size
		if size-prevsize >= cos.GiB*4 {
			tlog.Logf("%s: %d GiB\n", stream, size/cos.GiB)
			prevsize = size
		}
	}
	stream.Fin()

	slab.Free(buf)
	tlog.Logf("Compressed stream test: sent %d objects (%d header-only) totaling %d GiB\n", num, numhdr, size/cos.GiB)
}

// TODO: Skip unmaintained dry-run test to reduce test runtime (revisit)
func TestDryRun(t *testing.T) {
	t.Skipf("skipping %s", t.Name())
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	t.Setenv("AIS_STREAM_DRY_RUN", "true")

	stream := transport.NewObjStream(nil, "dummy/null", cos.GenTie(), nil)

	random := newRand(mono.NanoTime())
	sgl := memsys.PageMM().NewSGL(cos.MiB)
	defer sgl.Free()
	buf, slab := memsys.PageMM().AllocSize(cos.KiB * 128)
	defer slab.Free(buf)
	for sgl.Len() < cos.MiB {
		cryptorand.Read(buf)
		sgl.Write(buf)
	}

	size, num, prevsize := int64(0), 0, int64(0)
	hdr := genStaticHeader(random)
	total := int64(cos.TiB)
	if testing.Short() {
		total = cos.TiB / 4
	}

	for size < total {
		hdr.ObjAttrs.Size = cos.KiB * 128
		for i := range cos.MiB / hdr.ObjAttrs.Size {
			reader := memsys.NewReader(sgl)
			reader.Seek(i*hdr.ObjAttrs.Size, io.SeekStart)

			stream.Send(&transport.Obj{Hdr: hdr, Reader: reader})
			num++
			size += hdr.ObjAttrs.Size
			if size-prevsize >= cos.GiB*100 {
				prevsize = size
				fmt.Printf("[dry]: %d GiB\n", size/cos.GiB)
			}
		}
	}
	stream.Fin()
	tlog.Logf("[dry]: sent %d objects totaling %d GiB\n", num, size/cos.GiB)
}

func TestCompletionCount(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	var (
		numSent                   int64
		numCompleted, numReceived atomic.Int64
	)

	receive := func(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
		cos.Assert(err == nil)
		written, _ := io.Copy(io.Discard, objReader)
		cos.Assert(written == hdr.ObjAttrs.Size)
		numReceived.Inc()
		return nil
	}
	callback := func(_ *transport.ObjHdr, _ io.ReadCloser, _ any, _ error) {
		numCompleted.Inc()
	}

	ts := httptest.NewServer(objmux)
	defer ts.Close()

	trname := "cmpl-cnt"
	err := transport.Handle(trname, receive)
	tassert.CheckFatal(t, err)
	defer transport.Unhandle(trname)
	httpclient := transport.NewIntraDataClient()
	url := ts.URL + transport.ObjURLPath(trname)
	t.Setenv("AIS_STREAM_BURST_NUM", "256")
	stream := transport.NewObjStream(httpclient, url, cos.GenTie(), nil) // provide for sizeable queue at any point
	random := newRand(mono.NanoTime())
	rem := int64(0)
	for idx := range 10000 {
		if idx%7 == 0 {
			hdr := genStaticHeader(random)
			hdr.ObjAttrs.Size = 0
			hdr.Opaque = []byte(strconv.FormatInt(104729*int64(idx), 10))
			stream.Send(&transport.Obj{Hdr: hdr, SentCB: callback})
			rem = random.Int64() % 13
		} else {
			hdr, rr := makeRandReader(random, false)
			stream.Send(&transport.Obj{Hdr: hdr, Reader: rr, SentCB: callback})
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
		tlog.Logf("sent %d = %d completed, %d received\n", numSent, numCompleted.Load(), numReceived.Load())
	} else {
		t.Fatalf("sent %d != %d completed\n", numSent, numCompleted.Load())
	}
}

//
// test helpers
//

func streamWriteUntil(t *testing.T, ii int, wg *sync.WaitGroup, ts *httptest.Server, compress, usePDU bool) {
	if wg != nil {
		defer wg.Done()
	}
	totalRecv, recvFunc := makeRecvFunc(t)
	trname := fmt.Sprintf("rand-rx-%d", ii)
	err := transport.Handle(trname, recvFunc)
	tassert.CheckFatal(t, err)
	defer transport.Unhandle(trname)

	if compress {
		config := cmn.GCO.BeginUpdate()
		config.Transport.LZ4BlockMaxSize = cos.KiB * 256
		cmn.GCO.CommitUpdate(config)
		if err := config.Transport.Validate(); err != nil {
			tassert.CheckFatal(t, err)
		}
	}

	httpclient := transport.NewIntraDataClient()
	url := ts.URL + transport.ObjURLPath(trname)
	var extra *transport.Extra
	if compress || usePDU {
		extra = &transport.Extra{}
		if compress {
			extra.Compression = apc.CompressAlways
		}
		if usePDU {
			extra.SizePDU = memsys.DefaultBufSize
		}
	}
	stream := transport.NewObjStream(httpclient, url, cos.GenTie(), extra)
	trname, sessID := stream.ID()
	now := time.Now()

	random := newRand(mono.NanoTime())
	size, num, prevsize := int64(0), 0, int64(0)
	runFor := duration
	if testing.Short() {
		runFor = 10 * time.Second
	}
	var randReader *randReader
	for time.Since(now) < runFor {
		obj := transport.AllocSend()
		obj.Hdr, randReader = makeRandReader(random, usePDU)
		obj.Reader = randReader
		stream.Send(obj)
		num++
		if obj.IsUnsized() {
			size += randReader.offEOF
		} else {
			size += obj.Hdr.ObjAttrs.Size
		}
		if size-prevsize >= cos.GiB*4 {
			tlog.Logf("%s: %d GiB\n", stream, size/cos.GiB)
			prevsize = size
			if random.Int64()%7 == 0 {
				time.Sleep(time.Second * 2) // simulate occasional timeout
			}
		}
	}
	stream.Fin()
	reason, termErr := stream.TermInfo()
	tassert.Errorf(t, reason != "", "expecting reason for termination")
	tlog.Logf("stream[%s/%d]: sent %d objects (%d GiB), terminated(%q, %v)\n",
		trname, sessID, num, size/cos.GiB, reason, termErr)

	if *totalRecv != size {
		t.Errorf("total received bytes %d is different from expected: %d", *totalRecv, size)
		return
	}
}

func makeRecvFunc(t *testing.T) (*int64, transport.RecvObj) {
	totalReceived := new(int64)
	return totalReceived, func(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
		cos.Assert(err == nil || cos.IsAnyEOF(err))
		written, err := io.Copy(io.Discard, objReader)
		if err != nil && !cos.IsOkEOF(err) {
			tassert.CheckFatal(t, err)
		}
		if written != hdr.ObjAttrs.Size && !hdr.IsUnsized() {
			t.Fatalf("size %d != %d", written, hdr.ObjAttrs.Size)
		}
		*totalReceived += written
		return nil
	}
}

func newRand(seed int64) *rand.Rand {
	src := cos.NewRandSource(uint64(seed))
	random := rand.New(src)
	return random
}

func genStaticHeader(random *rand.Rand) (hdr transport.ObjHdr) {
	hdr.Bck = cmn.Bck{
		Name:     "a",
		Provider: apc.AIS,
	}
	hdr.ObjName = strconv.FormatInt(random.Int64(), 10)
	hdr.Opaque = []byte("123456789abcdef")
	hdr.ObjAttrs.Size = cos.GiB
	hdr.ObjAttrs.SetCustomKey(strconv.FormatInt(random.Int64(), 10), "d")
	hdr.ObjAttrs.SetCustomKey("e", "")
	hdr.ObjAttrs.SetCksum(cos.ChecksumCesXxh, "xxhash")
	return
}

func genRandomHeader(random *rand.Rand, usePDU bool) (hdr transport.ObjHdr) {
	x := random.Int64()
	hdr.Bck.Name = strconv.FormatInt(x, 10)
	hdr.Bck.Provider = apc.AIS
	hdr.ObjName = path.Join(hdr.Bck.Name, strconv.FormatInt(math.MaxInt64-x, 10))

	pos := x % int64(len(text))
	hdr.Opaque = []byte(text[int(pos):])

	// test a variety of payload sizes
	y := x & 3
	s := strconv.FormatInt(x, 10)
	switch y {
	case 0:
		hdr.ObjAttrs.Size = (x & 0xffffff) + 1
		hdr.ObjAttrs.SetVersion(s)
		hdr.ObjAttrs.SetCksum(cos.ChecksumNone, "")
	case 1:
		if usePDU {
			hdr.ObjAttrs.Size = transport.SizeUnknown
		} else {
			hdr.ObjAttrs.Size = (x & 0xfffff) + 1
		}
		hdr.ObjAttrs.SetCustomKey(strconv.FormatInt(random.Int64(), 10), s)
		hdr.ObjAttrs.SetCustomKey(s, "")
		hdr.ObjAttrs.SetCksum(cos.ChecksumMD5, "md5")
	case 2:
		hdr.ObjAttrs.Size = (x & 0xffff) + 1
		hdr.ObjAttrs.SetCksum(cos.ChecksumCesXxh, "xxhash")
		for range int(x & 0x1f) {
			hdr.ObjAttrs.SetCustomKey(strconv.FormatInt(random.Int64(), 10), s)
		}
	default:
		hdr.ObjAttrs.Size = 0
		hdr.ObjAttrs.SetVersion(s)
		hdr.ObjAttrs.SetCustomKey(s, "")
		hdr.ObjAttrs.SetCksum(cos.ChecksumNone, "")
	}
	return hdr
}

////////////////
// randReader //
////////////////

type randReader struct {
	buf    []byte
	hdr    transport.ObjHdr
	slab   *memsys.Slab
	off    int64
	random *rand.Rand
	offEOF int64 // when size is unknown
	clone  bool
}

//nolint:gocritic // can do (hdr) hugeParam
func newRandReader(random *rand.Rand, hdr transport.ObjHdr, slab *memsys.Slab) *randReader {
	buf := slab.Alloc()
	_, err := cryptorand.Read(buf)
	if err != nil {
		panic("Failed read rand: " + err.Error())
	}
	r := &randReader{buf: buf, hdr: hdr, slab: slab, random: random}
	if hdr.IsUnsized() {
		r.offEOF = int64(random.Int32()>>1) + 1
	}
	return r
}

func makeRandReader(random *rand.Rand, usePDU bool) (transport.ObjHdr, *randReader) {
	hdr := genRandomHeader(random, usePDU)
	if hdr.ObjSize() == 0 {
		return hdr, nil
	}
	slab, err := memsys.PageMM().GetSlab(memsys.DefaultBufSize)
	if err != nil {
		panic("Failed getting slab: " + err.Error())
	}
	return hdr, newRandReader(random, hdr, slab)
}

func (r *randReader) Read(p []byte) (n int, err error) {
	var objSize int64
	if r.hdr.IsUnsized() {
		objSize = r.offEOF
	} else {
		objSize = r.hdr.ObjAttrs.Size
	}
	for loff := 0; ; {
		rem := objSize - r.off
		if rem == 0 {
			return n, io.EOF
		}
		l64 := min(rem, int64(len(p)-n))
		if l64 == 0 {
			return
		}
		nr := copy(p[n:n+int(l64)], r.buf[loff:])
		n += nr
		loff += 16
		if loff >= len(r.buf)-16 {
			loff = 0
		}
		r.off += int64(nr)
	}
}

func (r *randReader) Open() (cos.ReadOpenCloser, error) {
	buf := r.slab.Alloc()
	copy(buf, r.buf)
	r2 := randReader{buf: buf, hdr: r.hdr, slab: r.slab, offEOF: r.offEOF}
	return &r2, nil
}

func (r *randReader) Close() error {
	if r != nil && !r.clone {
		r.slab.Free(r.buf)
	}
	return nil
}

type randReaderCtx struct {
	t      *testing.T
	rr     *randReader
	posted []*randReader // => stream
	mu     *sync.Mutex
	idx    int
}

func (rrc *randReaderCtx) sentCallback(hdr *transport.ObjHdr, _ io.ReadCloser, _ any, err error) {
	if err != nil {
		rrc.t.Errorf("sent-callback %d(%s) returned an error: %v", rrc.idx, hdr.Cname(), err)
	}
	rr := rrc.rr
	if rr != nil {
		rr.slab.Free(rr.buf)
	}
	rrc.mu.Lock()
	rrc.posted[rrc.idx] = nil
	if rrc.idx > 0 && rrc.posted[rrc.idx-1] != nil {
		rrc.t.Errorf("sent-callback %d(%s) fired out of order", rrc.idx, hdr.Cname())
	}
	rrc.posted[rrc.idx] = nil
	rrc.mu.Unlock()
}
