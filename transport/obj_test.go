// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport_test

// How to run:
//
// 1) run all tests while redirecting glog to STDERR:
// go test -v -logtostderr=true
//
// 2) run tests matching "Multi" with debug enabled and glog level=1 (non-verbose):
// AIS_DEBUG=transport=1 go test -v -run=Multi -tags=debug
//
// 3) run tests matching "Multi" with debug enabled, glog level=4 (super-verbose) and glog redirect:
// AIS_DEBUG=transport=4 go test -v -run=Multi -tags=debug -logtostderr=true

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/golang/mux"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
	"github.com/NVIDIA/aistore/memsys"
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

var (
	objmux   *mux.ServeMux
	msgmux   *mux.ServeMux
	MMSA     *memsys.MMSA
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
		cmn.Exitf("Invalid duration %q", d)
	}
	MMSA = memsys.DefaultPageMM()

	sc := transport.Init()
	go sc.Run()

	objmux = mux.NewServeMux()
	path := transport.ObjURLPath("")
	objmux.HandleFunc(path, transport.RxAnyStream)
	objmux.HandleFunc(path+"/", transport.RxAnyStream)

	msgmux = mux.NewServeMux()
	path = transport.MsgURLPath("")
	msgmux.HandleFunc(path, transport.RxAnyStream)
	msgmux.HandleFunc(path+"/", transport.RxAnyStream)

	os.Exit(t.Run())
}

func Example_headers() {
	f := func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
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
			if !hdr.IsLast() {
				fmt.Printf("%+v (%d)\n", hdr, hlen)
				off += hlen + int(hdr.ObjAttrs.Size)
			} else {
				break
			}
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(f))
	defer ts.Close()

	httpclient := transport.NewIntraDataClient()
	stream := transport.NewObjStream(httpclient, ts.URL, nil)

	sendText(stream, lorem, duis)
	stream.Fin()

	// Output:
	// {Bck:aws://@uuid#namespace/abc ObjName:X ObjAttrs:{Atime:663346294 Size:231 CksumType:xxhash CksumValue:hash Version:2} Opaque:[]} (119)
	// {Bck:ais://abracadabra ObjName:p/q/s ObjAttrs:{Atime:663346294 Size:213 CksumType:xxhash CksumValue:hash Version:2} Opaque:[49 50 51]} (121)
}

func sendText(stream *transport.Stream, txt1, txt2 string) {
	var wg sync.WaitGroup
	cb := func(transport.ObjHdr, io.ReadCloser, unsafe.Pointer, error) {
		wg.Done()
	}
	sgl1 := MMSA.NewSGL(0)
	sgl1.Write([]byte(txt1))
	hdr := transport.ObjHdr{
		Bck: cmn.Bck{
			Name:     "abc",
			Provider: cmn.ProviderAmazon,
			Ns:       cmn.Ns{UUID: "uuid", Name: "namespace"},
		},
		ObjName: "X",
		ObjAttrs: transport.ObjectAttrs{
			Size:       sgl1.Size(),
			Atime:      663346294,
			CksumType:  cmn.ChecksumXXHash,
			CksumValue: "hash",
			Version:    "2",
		},
		Opaque: nil,
	}
	wg.Add(1)
	stream.Send(&transport.Obj{Hdr: hdr, Reader: sgl1, Callback: cb})
	wg.Wait()

	sgl2 := MMSA.NewSGL(0)
	sgl2.Write([]byte(txt2))
	hdr = transport.ObjHdr{
		Bck: cmn.Bck{
			Name:     "abracadabra",
			Provider: cmn.ProviderAIS,
			Ns:       cmn.NsGlobal,
		},
		ObjName: "p/q/s",
		ObjAttrs: transport.ObjectAttrs{
			Size:       sgl2.Size(),
			Atime:      663346294,
			CksumType:  cmn.ChecksumXXHash,
			CksumValue: "hash",
			Version:    "2",
		},
		Opaque: []byte{'1', '2', '3'},
	}
	wg.Add(1)
	stream.Send(&transport.Obj{Hdr: hdr, Reader: sgl2, Callback: cb})
	wg.Wait()
}

func Example_obj() {
	receive := func(w http.ResponseWriter, hdr transport.ObjHdr, objReader io.Reader, err error) {
		cmn.Assert(err == nil)
		object, err := ioutil.ReadAll(objReader)
		if err != nil {
			panic(err)
		}
		if int64(len(object)) != hdr.ObjAttrs.Size {
			panic(fmt.Sprintf("size %d != %d", len(object), hdr.ObjAttrs.Size))
		}
		fmt.Printf("%s...\n", string(object[:16]))
	}
	ts := httptest.NewServer(objmux)
	defer ts.Close()
	trname := "dummy-obj"
	err := transport.HandleObjStream(trname, receive)
	if err != nil {
		fmt.Println(err)
		return
	}
	httpclient := transport.NewIntraDataClient()
	stream := transport.NewObjStream(httpclient, ts.URL+transport.ObjURLPath(trname), nil)
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
func Test_OneStream(t *testing.T) {
	ts := httptest.NewServer(objmux)
	defer ts.Close()

	streamWriteUntil(t, 55, nil, ts, nil, nil, false /*compress*/, true /*PDU*/)
	printNetworkStats(t)
}

func Test_MultiStream(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	tutils.Logf("Duration %v\n", duration)
	ts := httptest.NewServer(objmux)
	defer ts.Close()

	wg := &sync.WaitGroup{}
	netstats := make(map[string]transport.EndpointStats)
	lock := &sync.Mutex{}
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go streamWriteUntil(t, i, wg, ts, netstats, lock, false /*compress*/, false /*PDU*/)
	}
	wg.Wait()
	compareNetworkStats(t, netstats)
}

func printNetworkStats(t *testing.T) {
	netstats, err := transport.GetStats()
	tassert.CheckFatal(t, err)
	for trname, eps := range netstats {
		for uid, stats := range eps { // EndpointStats by session ID
			xx, sessID := transport.UID2SessID(uid)
			fmt.Printf("recv$ %s[%d:%d]: offset=%d, num=%d\n",
				trname, xx, sessID, stats.Offset.Load(), stats.Num.Load())
		}
	}
}

func compareNetworkStats(t *testing.T, netstats1 map[string]transport.EndpointStats) {
	netstats2, err := transport.GetStats()
	tassert.CheckFatal(t, err)
	for trname, eps2 := range netstats2 {
		eps1, ok := netstats1[trname]
		for uid, stats2 := range eps2 { // EndpointStats by session ID
			xx, sessID := transport.UID2SessID(uid)
			fmt.Printf("recv$ %s[%d:%d]: offset=%d, num=%d\n", trname, xx, sessID,
				stats2.Offset.Load(), stats2.Num.Load())
			if ok {
				stats1, ok := eps1[sessID]
				if ok {
					fmt.Printf("send$ %s[%d]: offset=%d, num=%d\n",
						trname, sessID, stats1.Offset.Load(), stats1.Num.Load())
				} else {
					fmt.Printf("send$ %s[%d]: -- not present --\n", trname, sessID)
				}
			} else {
				fmt.Printf("send$ %s[%d]: -- not present --\n", trname, sessID)
			}
		}
	}
}

func Test_MultipleNetworks(t *testing.T) {
	totalRecv, recvFunc := makeRecvFunc(t)

	streams := make([]*transport.Stream, 0, 10)
	for idx := 0; idx < 10; idx++ {
		ts := httptest.NewServer(objmux)
		defer ts.Close()
		trname := "endpoint" + strconv.Itoa(idx)
		err := transport.HandleObjStream(trname, recvFunc)
		tassert.CheckFatal(t, err)
		defer transport.Unhandle(trname)

		httpclient := transport.NewIntraDataClient()
		url := ts.URL + transport.ObjURLPath(trname)
		streams = append(streams, transport.NewObjStream(httpclient, url, nil))
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
	time.Sleep(time.Second) // FIN has been sent but not necessarily received

	if *totalRecv != totalSend {
		t.Fatalf("total received bytes %d is different from expected: %d", *totalRecv, totalSend)
	}
}

func Test_OnSendCallback(t *testing.T) {
	objectCnt := 10000
	if testing.Short() {
		objectCnt = 1000
	}

	ts := httptest.NewServer(objmux)
	defer ts.Close()

	totalRecv, recvFunc := makeRecvFunc(t)
	trname := "callback"
	err := transport.HandleObjStream(trname, recvFunc)
	tassert.CheckFatal(t, err)
	defer transport.Unhandle(trname)
	httpclient := transport.NewIntraDataClient()
	url := ts.URL + transport.ObjURLPath(trname)
	stream := transport.NewObjStream(httpclient, url, nil)

	var (
		totalSend int64
		mu        sync.Mutex
		posted    = make([]*randReader, objectCnt)
	)
	random := newRand(mono.NanoTime())
	for idx := 0; idx < len(posted); idx++ {
		hdr, rr := makeRandReader(random, false)
		mu.Lock()
		posted[idx] = rr
		mu.Unlock()
		rrc := &randReaderCtx{t, rr, posted, &mu, idx}
		totalSend += hdr.ObjAttrs.Size
		stream.Send(&transport.Obj{Hdr: hdr, Reader: rr, Callback: rrc.sentCallback})
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

func Test_ObjAttrs(t *testing.T) {
	testAttrs := []transport.ObjectAttrs{
		{
			Size:       1024,
			Atime:      1024,
			CksumType:  "",
			CksumValue: "cheksum",
			Version:    "102.44",
		},
		{
			Size:       1024,
			Atime:      math.MaxInt64,
			CksumType:  cmn.ChecksumXXHash,
			CksumValue: "120421",
			Version:    "102.44",
		},
		{
			Size:       0,
			Atime:      0,
			CksumType:  "",
			CksumValue: "102412",
			Version:    "",
		},
	}

	ts := httptest.NewServer(objmux)
	defer ts.Close()

	var receivedCount atomic.Int64
	recvFunc := func(w http.ResponseWriter, hdr transport.ObjHdr, objReader io.Reader, err error) {
		cmn.Assert(err == nil)

		idx := hdr.Opaque[0]
		cmn.AssertMsg(hdr.Bck.IsAIS(), "expecting ais bucket")
		cmn.Assertf(reflect.DeepEqual(testAttrs[idx], hdr.ObjAttrs),
			"attrs are not equal: %v; %v;", testAttrs[idx], hdr.ObjAttrs)

		written, err := io.Copy(ioutil.Discard, objReader)
		cmn.Assert(err == nil)
		cmn.Assertf(written == hdr.ObjAttrs.Size, "written: %d, expected: %d", written, hdr.ObjAttrs.Size)

		receivedCount.Inc()
	}
	trname := "objattrs"
	err := transport.HandleObjStream(trname, recvFunc)
	tassert.CheckFatal(t, err)
	defer transport.Unhandle(trname)
	httpclient := transport.NewIntraDataClient()
	url := ts.URL + transport.ObjURLPath(trname)
	stream := transport.NewObjStream(httpclient, url, nil)

	random := newRand(mono.NanoTime())
	for idx, attrs := range testAttrs {
		var (
			reader io.ReadCloser
			hdr    = transport.ObjHdr{
				Bck: cmn.Bck{
					Provider: cmn.ProviderAIS,
				},
				ObjAttrs: attrs,
				Opaque:   []byte{byte(idx)},
			}
		)
		slab, err := MMSA.GetSlab(memsys.PageSize)
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

func receive10G(w http.ResponseWriter, hdr transport.ObjHdr, objReader io.Reader, err error) {
	cmn.AssertNoErr(err)
	written, _ := io.Copy(ioutil.Discard, objReader)
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
	sgl := MMSA.NewSGL(cmn.MiB)
	defer sgl.Free()
	buf, slab := MMSA.Alloc(cmn.KiB * 128)
	defer slab.Free(buf)
	for sgl.Len() < cmn.MiB {
		random.Read(buf)
		sgl.Write(buf)
	}

	size, num, prevsize := int64(0), 0, int64(0)
	hdr := genStaticHeader()
	total := int64(cmn.TiB)
	if testing.Short() {
		total = cmn.TiB / 4
	}

	for size < total {
		hdr.ObjAttrs.Size = cmn.KiB * 128
		for i := int64(0); i < cmn.MiB/hdr.ObjAttrs.Size; i++ {
			reader := memsys.NewReader(sgl)
			reader.Seek(i*hdr.ObjAttrs.Size, io.SeekStart)

			obj := transport.AllocSend()
			*obj = transport.Obj{Hdr: hdr, Reader: reader}
			obj.CmplPtr = unsafe.Pointer(obj) // TODO -- FIXME
			stream.Send(obj)
			num++
			size += hdr.ObjAttrs.Size
			if size-prevsize >= cmn.GiB*100 {
				prevsize = size
				tutils.Logf("[dry]: %d GiB\n", size/cmn.GiB)
			}
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
		written, _ := io.Copy(ioutil.Discard, objReader)
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
			hdr, rr := makeRandReader(random, false)
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

//
// test helpers
//

func streamWriteUntil(t *testing.T, ii int, wg *sync.WaitGroup, ts *httptest.Server,
	netstats map[string]transport.EndpointStats, lock sync.Locker, compress, usePDU bool) {
	if wg != nil {
		defer wg.Done()
	}
	totalRecv, recvFunc := makeRecvFunc(t)
	trname := fmt.Sprintf("rand-rx-%d", ii)
	err := transport.HandleObjStream(trname, recvFunc)
	tassert.CheckFatal(t, err)
	defer transport.Unhandle(trname)

	if compress {
		config := cmn.GCO.BeginUpdate()
		config.Compression.BlockMaxSize = cmn.KiB * 256
		cmn.GCO.CommitUpdate(config)
		if err := config.Compression.Validate(config); err != nil {
			tassert.CheckFatal(t, err)
		}
	}

	httpclient := transport.NewIntraDataClient()
	url := ts.URL + transport.ObjURLPath(trname)
	var extra *transport.Extra
	if compress || usePDU {
		extra = &transport.Extra{}
		if compress {
			extra.Compression = cmn.CompressAlways
		}
		if usePDU {
			extra.SizePDU = transport.DefaultSizePDU
		}
	}
	stream := transport.NewObjStream(httpclient, url, extra)
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
		if size-prevsize >= cmn.GiB*4 {
			tutils.Logf("%s: %d GiB\n", stream, size/cmn.GiB)
			prevsize = size
			if random.Int63()%7 == 0 {
				time.Sleep(time.Second * 2) // simulate occasional timeout
			}
		}
	}
	stream.Fin()
	stats := stream.GetStats()
	if netstats == nil {
		termReason, termErr := stream.TermInfo()
		fmt.Printf("send$ %s[%d]: offset=%d, num=%d(%d), term(%s, %v)\n",
			trname, sessID, stats.Offset.Load(), stats.Num.Load(), num, termReason, termErr)
	} else {
		lock.Lock()
		eps := make(transport.EndpointStats)
		eps[uint64(sessID)] = &stats
		netstats[trname] = eps
		lock.Unlock()
	}

	if *totalRecv != size {
		t.Errorf("total received bytes %d is different from expected: %d", *totalRecv, size)
		return
	}
}

func makeRecvFunc(t *testing.T) (*int64, transport.ReceiveObj) {
	totalReceived := new(int64)
	return totalReceived, func(w http.ResponseWriter, hdr transport.ObjHdr, objReader io.Reader, err error) {
		cmn.Assert(err == nil)
		written, err := io.Copy(ioutil.Discard, objReader)
		if err != nil && !errors.Is(err, io.EOF) {
			tassert.CheckFatal(t, err)
		}
		if written != hdr.ObjAttrs.Size && !hdr.IsUnsized() {
			t.Fatalf("size %d != %d", written, hdr.ObjAttrs.Size)
		}
		*totalReceived += written
	}
}

func newRand(seed int64) *rand.Rand {
	src := rand.NewSource(seed)
	random := rand.New(src)
	return random
}

func genStaticHeader() (hdr transport.ObjHdr) {
	hdr.Bck = cmn.Bck{
		Name:     "a",
		Provider: cmn.ProviderAIS,
	}
	hdr.ObjName = "b"
	hdr.Opaque = []byte("c")
	hdr.ObjAttrs.Size = cmn.GiB
	return
}

func genRandomHeader(random *rand.Rand, usePDU bool) (hdr transport.ObjHdr) {
	x := random.Int63()
	hdr.Bck.Name = strconv.FormatInt(x, 10)
	hdr.ObjName = path.Join(hdr.Bck.Name, strconv.FormatInt(math.MaxInt64-x, 10))

	pos := x % int64(len(text))
	hdr.Opaque = []byte(text[int(pos):])

	// test a variety of payload sizes
	y := x & 3
	switch y {
	case 0:
		hdr.ObjAttrs.Size = (x & 0xffffff) + 1
	case 1:
		if usePDU {
			hdr.ObjAttrs.Size = transport.SizeUnknown
		} else {
			hdr.ObjAttrs.Size = (x & 0xfffff) + 1
		}
	case 2:
		hdr.ObjAttrs.Size = (x & 0xffff) + 1
	default:
		hdr.ObjAttrs.Size = 0
	}
	return
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

func newRandReader(random *rand.Rand, hdr transport.ObjHdr, slab *memsys.Slab) *randReader {
	buf := slab.Alloc()
	_, err := random.Read(buf)
	if err != nil {
		panic("Failed read rand: " + err.Error())
	}
	r := &randReader{buf: buf, hdr: hdr, slab: slab, random: random}
	if hdr.IsUnsized() {
		r.offEOF = int64(random.Int31()>>1) + 1
	}
	return r
}

func makeRandReader(random *rand.Rand, usePDU bool) (transport.ObjHdr, *randReader) {
	hdr := genRandomHeader(random, usePDU)
	if hdr.ObjSize() == 0 {
		return hdr, nil
	}
	slab, err := MMSA.GetSlab(memsys.DefaultBufSize)
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
		l64 := cmn.MinI64(rem, int64(len(p)-n))
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

func (r *randReader) Open() (io.ReadCloser, error) {
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

func (rrc *randReaderCtx) sentCallback(hdr transport.ObjHdr, reader io.ReadCloser, _ unsafe.Pointer, err error) {
	if err != nil {
		rrc.t.Errorf("sent-callback %d(%s/%s) returned an error: %v", rrc.idx, hdr.Bck, hdr.ObjName, err)
	}
	rr := rrc.rr
	if rr != nil {
		rr.slab.Free(rr.buf)
	}
	rrc.mu.Lock()
	rrc.posted[rrc.idx] = nil
	if rrc.idx > 0 && rrc.posted[rrc.idx-1] != nil {
		rrc.t.Errorf("sent-callback %d(%s/%s) fired out of order", rrc.idx, hdr.Bck, hdr.ObjName)
	}
	rrc.posted[rrc.idx] = nil
	rrc.mu.Unlock()
}
