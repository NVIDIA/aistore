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
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
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

	if mono.NanoTime()%2 == 1 {
		streamWriteUntil(t, 99, nil, ts, nil, nil, true /*compress*/)
	} else {
		streamWriteUntil(t, 55, nil, ts, nil, nil, false)
	}
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
		go streamWriteUntil(t, i, wg, ts, netstats, lock, false)
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
	for _, stream := range streams {
		hdr, reader := makeRandReader()
		stream.Send(&transport.Obj{Hdr: hdr, Reader: reader})
		totalSend += hdr.ObjAttrs.Size
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
	for idx := 0; idx < len(posted); idx++ {
		hdr, rr := makeRandReader()
		mu.Lock()
		posted[idx] = rr
		mu.Unlock()
		rrc := &randReaderCtx{t, rr, posted, &mu, idx}
		stream.Send(&transport.Obj{Hdr: hdr, Reader: rr, Callback: rrc.sentCallback})
		totalSend += hdr.ObjAttrs.Size
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

//
// test helpers
//

func streamWriteUntil(t *testing.T, ii int, wg *sync.WaitGroup, ts *httptest.Server,
	netstats map[string]transport.EndpointStats, lock sync.Locker, compress bool) {
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
	if compress {
		extra = &transport.Extra{Compression: cmn.CompressAlways}
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
	for time.Since(now) < runFor {
		hdr, reader := makeRandReader()
		stream.Send(&transport.Obj{Hdr: hdr, Reader: reader})
		num++
		size += hdr.ObjAttrs.Size
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
		slab, _ := MMSA.GetSlab(32 * cmn.KiB)
		buf := slab.Alloc()
		written, err := io.CopyBuffer(ioutil.Discard, objReader, buf)
		if err != io.EOF {
			tassert.CheckFatal(t, err)
		}
		if written != hdr.ObjAttrs.Size {
			t.Fatalf("size %d != %d", written, hdr.ObjAttrs.Size)
		}
		*totalReceived += written
		slab.Free(buf)
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

func genRandomHeader(random *rand.Rand) (hdr transport.ObjHdr) {
	x := random.Int63()
	hdr.Bck.Name = strconv.FormatInt(x, 10)
	hdr.ObjName = path.Join(hdr.Bck.Name, strconv.FormatInt(math.MaxInt64-x, 10))
	pos := x % int64(len(text))
	hdr.Opaque = []byte(text[int(pos):])
	y := x & 3
	switch y {
	case 0:
		hdr.ObjAttrs.Size = (x & 0xffffff) + 1
	case 1:
		hdr.ObjAttrs.Size = (x & 0xfffff) + 1
	case 2:
		hdr.ObjAttrs.Size = (x & 0xffff) + 1
	default:
		hdr.ObjAttrs.Size = (x & 0xfff) + 1
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
	clone  bool
}

func newRandReader(random *rand.Rand, hdr transport.ObjHdr, slab *memsys.Slab) *randReader {
	buf := slab.Alloc()
	_, err := random.Read(buf)
	if err != nil {
		panic("Failed read rand: " + err.Error())
	}
	return &randReader{buf: buf, hdr: hdr, slab: slab, random: random}
}

func makeRandReader() (transport.ObjHdr, *randReader) {
	slab, err := MMSA.GetSlab(32 * cmn.KiB)
	if err != nil {
		panic("Failed getting slab: " + err.Error())
	}
	random := newRand(mono.NanoTime())
	hdr := genRandomHeader(random)
	reader := newRandReader(random, hdr, slab)
	return hdr, reader
}

func (r *randReader) Read(p []byte) (n int, err error) {
	for {
		rem := r.hdr.ObjAttrs.Size - r.off
		if rem == 0 {
			return n, io.EOF
		}
		l64 := cmn.MinI64(rem, int64(len(p)-n))
		if l64 == 0 {
			return
		}
		nr := copy(p[n:n+int(l64)], r.buf)
		if false && nr > 0 { // to fully randomize
			r.random.Read(p[n : n+nr])
		}
		n += nr
		r.off += int64(nr)
	}
}

func (r *randReader) Open() (io.ReadCloser, error) {
	buf := r.slab.Alloc()
	copy(buf, r.buf)
	r2 := randReader{buf: buf, hdr: r.hdr, slab: r.slab}
	return &r2, nil
}

func (r *randReader) Close() error {
	if !r.clone {
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
	rr.slab.Free(rr.buf)
	rrc.mu.Lock()
	rrc.posted[rrc.idx] = nil
	if rrc.idx > 0 && rrc.posted[rrc.idx-1] != nil {
		rrc.t.Errorf("sent-callback %d(%s/%s) fired out of order", rrc.idx, hdr.Bck, hdr.ObjName)
	}
	rrc.posted[rrc.idx] = nil
	rrc.mu.Unlock()
}
