// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport_test

// How to run:
//
// 1) run all tests while redirecting errors to standard error:
// go test -v -logtostderr=true
//
// 2) run a given test (name matching "Multi") with debug enabled:
// AIS_DEBUG=transport=1 go test -v -run=Multi
//
// 3) same, with no debug and for 2 minutes instead of (the default) 30s
// go test -v -run=Multi -duration 2m
//
// 4) same as above non-verbose
// go test -run=Multi -duration 2m
//

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
	text1 = `Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut
labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.`
	text2 = `Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.
Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.`
	text3 = `Et harum quidem rerum facilis est et expedita distinctio. Nam libero tempore, cum soluta nobis est
eligendi optio, cumque nihil impedit, quo minus id, quod maxime placeat, facere possimus, omnis voluptas assumenda est, omnis dolor repellendus.`
	text4 = `Temporibus autem quibusdam et aut officiis debitis aut rerum necessitatibus saepe eveniet,
ut et voluptates repudiandae sint et molestiae non-recusandae.`
	text = text1 + text2 + text3 + text4
)

var (
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
	sc.SetRunName("stream-collector")
	go sc.Run()

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
			hdr       transport.Header
			hlen, off int
		)
		for {
			hlen = int(binary.BigEndian.Uint64(body[off:]))
			off += 16 // hlen and hlen-checksum
			hdr = transport.ExtHeader(body[off:], hlen)
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
	stream := transport.NewStream(httpclient, ts.URL, nil)

	sendText(stream, text1, text2)
	stream.Fin()

	// Output:
	// {Bck:aws://@uuid#namespace/abc ObjName:X ObjAttrs:{Atime:663346294 Size:231 CksumType:xxhash CksumValue:hash Version:2} Opaque:[]} (119)
	// {Bck:ais://abracadabra ObjName:p/q/s ObjAttrs:{Atime:663346294 Size:213 CksumType:xxhash CksumValue:hash Version:2} Opaque:[49 50 51]} (121)
}

func sendText(stream *transport.Stream, txt1, txt2 string) {
	var wg sync.WaitGroup
	cb := func(transport.Header, io.ReadCloser, unsafe.Pointer, error) {
		wg.Done()
	}
	sgl1 := MMSA.NewSGL(0)
	sgl1.Write([]byte(txt1))
	hdr := transport.Header{
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
	stream.Send(transport.Obj{Hdr: hdr, Reader: sgl1, Callback: cb})
	wg.Wait()

	sgl2 := MMSA.NewSGL(0)
	sgl2.Write([]byte(txt2))
	hdr = transport.Header{
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
	stream.Send(transport.Obj{Hdr: hdr, Reader: sgl2, Callback: cb})
	wg.Wait()
}

func Example_mux() {
	receive := func(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
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
	mux := mux.NewServeMux()

	transport.SetMux("n1", mux)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	path, err := transport.Register("n1", "dummy-rx", receive)
	if err != nil {
		fmt.Println(err)
		return
	}
	httpclient := transport.NewIntraDataClient()
	url := ts.URL + path
	stream := transport.NewStream(httpclient, url, nil)

	sendText(stream, text1, text2)
	sendText(stream, text3, text4)
	stream.Fin()

	// Output:
	// Lorem ipsum dolo...
	// Duis aute irure ...
	// Et harum quidem ...
	// Temporibus autem...
}

// test random streaming
func Test_OneStream(t *testing.T) {
	mux := mux.NewServeMux()

	transport.SetMux("n1", mux)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	if mono.NanoTime()%2 == 1 {
		streamWriteUntil(t, 99, nil, ts, nil, nil, true /*compress*/)
	} else {
		streamWriteUntil(t, 55, nil, ts, nil, nil, false)
	}
	printNetworkStats(t, "n1")
}

func Test_MultiStream(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	tutils.Logf("Duration %v\n", duration)
	mux := mux.NewServeMux()
	transport.SetMux("n1", mux)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	wg := &sync.WaitGroup{}
	netstats := make(map[string]transport.EndpointStats)
	lock := &sync.Mutex{}
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go streamWriteUntil(t, i, wg, ts, netstats, lock, false)
	}
	wg.Wait()
	compareNetworkStats(t, "n1", netstats)
}

func printNetworkStats(t *testing.T, network string) {
	netstats, err := transport.GetNetworkStats(network)
	tassert.CheckFatal(t, err)
	for trname, eps := range netstats {
		for uid, stats := range eps { // EndpointStats by session ID
			xx, sessID := transport.UID2SessID(uid)
			fmt.Printf("recv$ %s[%d:%d]: offset=%d, num=%d\n",
				trname, xx, sessID, stats.Offset.Load(), stats.Num.Load())
		}
	}
}

func compareNetworkStats(t *testing.T, network string, netstats1 map[string]transport.EndpointStats) {
	netstats2, err := transport.GetNetworkStats(network)
	tassert.CheckFatal(t, err)
	for trname, eps2 := range netstats2 {
		eps1, ok := netstats1[trname]
		for uid, stats2 := range eps2 { // EndpointStats by session ID
			xx, sessID := transport.UID2SessID(uid)
			fmt.Printf("recv$ %s[%d:%d]: offset=%d, num=%d\n", trname, xx, sessID, stats2.Offset.Load(), stats2.Num.Load())
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
		network := fmt.Sprintf("network-%d", idx)
		mux := mux.NewServeMux()
		transport.SetMux(network, mux)
		ts := httptest.NewServer(mux)
		defer ts.Close()
		path, err := transport.Register(network, "endpoint", recvFunc)
		tassert.CheckFatal(t, err)

		httpclient := transport.NewIntraDataClient()
		url := ts.URL + path
		streams = append(streams, transport.NewStream(httpclient, url, nil))
	}

	totalSend := int64(0)
	for _, stream := range streams {
		hdr, reader := makeRandReader()
		stream.Send(transport.Obj{Hdr: hdr, Reader: reader})
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
	var (
		objectCnt = 10000
		mux       = mux.NewServeMux()
	)

	if testing.Short() {
		objectCnt = 1000
	}

	transport.SetMux("n1", mux)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	totalRecv, recvFunc := makeRecvFunc(t)
	path, err := transport.Register("n1", "callback", recvFunc)
	if err != nil {
		t.Fatal(err)
	}
	httpclient := transport.NewIntraDataClient()
	url := ts.URL + path
	stream := transport.NewStream(httpclient, url, nil)

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
		stream.Send(transport.Obj{Hdr: hdr, Reader: rr, Callback: rrc.sentCallback})
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

	mux := mux.NewServeMux()
	transport.SetMux("n1", mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	var receivedCount atomic.Int64
	recvFunc := func(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
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
	path, err := transport.Register("n1", "callback", recvFunc)
	if err != nil {
		t.Fatal(err)
	}
	httpclient := transport.NewIntraDataClient()
	url := ts.URL + path
	stream := transport.NewStream(httpclient, url, nil)

	random := newRand(mono.NanoTime())
	for idx, attrs := range testAttrs {
		hdr := transport.Header{
			Bck: cmn.Bck{
				Provider: cmn.ProviderAIS,
			},
			ObjAttrs: attrs,
			Opaque:   []byte{byte(idx)},
		}
		slab, err := MMSA.GetSlab(memsys.PageSize)
		if err != nil {
			t.Fatal(err)
		}
		reader := newRandReader(random, hdr, slab)
		if err := stream.Send(transport.Obj{Hdr: hdr, Reader: reader}); err != nil {
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
	path, err := transport.Register("n1", fmt.Sprintf("rand-rx-%d", ii), recvFunc)
	tassert.CheckFatal(t, err)

	if compress {
		config := cmn.GCO.BeginUpdate()
		config.Compression.BlockMaxSize = cmn.KiB * 256
		cmn.GCO.CommitUpdate(config)
		if err := config.Compression.Validate(config); err != nil {
			tassert.CheckFatal(t, err)
		}
	}

	httpclient := transport.NewIntraDataClient()
	url := ts.URL + path
	var extra *transport.Extra
	if compress {
		extra = &transport.Extra{Compression: cmn.CompressAlways}
	}
	stream := transport.NewStream(httpclient, url, extra)
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
		stream.Send(transport.Obj{Hdr: hdr, Reader: reader})
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

func makeRecvFunc(t *testing.T) (*int64, transport.Receive) {
	totalReceived := new(int64)
	return totalReceived, func(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
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

func genStaticHeader() (hdr transport.Header) {
	hdr.Bck = cmn.Bck{
		Name:     "a",
		Provider: cmn.ProviderAIS,
	}
	hdr.ObjName = "b"
	hdr.Opaque = []byte("c")
	hdr.ObjAttrs.Size = cmn.GiB
	return
}

func genRandomHeader(random *rand.Rand) (hdr transport.Header) {
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
	hdr    transport.Header
	slab   *memsys.Slab
	off    int64
	random *rand.Rand
	clone  bool
}

func newRandReader(random *rand.Rand, hdr transport.Header, slab *memsys.Slab) *randReader {
	buf := slab.Alloc()
	_, err := random.Read(buf)
	if err != nil {
		panic("Failed read rand: " + err.Error())
	}
	return &randReader{buf: buf, hdr: hdr, slab: slab, random: random}
}

func makeRandReader() (transport.Header, *randReader) {
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

func (rrc *randReaderCtx) sentCallback(hdr transport.Header, reader io.ReadCloser, _ unsafe.Pointer, err error) {
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
