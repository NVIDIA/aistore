// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package transport_test

// How to run:
//
// 1) run all tests while redirecting errors to standard error:
// go test -v -logtostderr=true
//
// 2) run a given test (name matching "Multi") with debug enabled:
// DFC_STREAM_DEBUG=1 go test -v -run=Multi
//
// 3) same, with no debug and for 2 minutes instead of (the default) 30s
// go test -v -run=Multi -duration 2m
//
// 4) same as above non-verbose
// go test -run=Multi -duration 2m
//

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/transport"
	"github.com/NVIDIA/dfcpub/tutils"
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
	Mem2     *memsys.Mem2
	duration time.Duration // test duration
)

func init() {
	var (
		d   string
		err error
	)
	flag.StringVar(&d, "duration", "30s", "test duration")
	flag.Parse()
	if duration, err = time.ParseDuration(d); err != nil {
		fmt.Printf("Invalid duration '%s'\n", d)
		os.Exit(1)
	}
	Mem2 = memsys.Init()
}

func Example_Headers() {
	f := func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		var (
			hdr       transport.Header
			hlen, off int
		)
		for {
			hlen = int(binary.BigEndian.Uint64(body[off:]))
			off += 16 // hlen and hlen-checksum
			hdr, _ = transport.ExtHeader(body[off:], hlen)
			if !hdr.IsLast() {
				fmt.Printf("%+v (%d)\n", hdr, hlen)
				off += hlen + int(hdr.Dsize)
			} else {
				break
			}
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(f))
	defer ts.Close()

	httpclient := &http.Client{Transport: &http.Transport{}}

	stream := transport.NewStream(httpclient, ts.URL, nil)

	sendText(stream, text1, text2)
	stream.Fin()
	// Output:
	// {Bucket:abc Objname:X Opaque:[] Dsize:231} (44)
	// {Bucket:abracadabra Objname:p/q/s Opaque:[49 50 51] Dsize:213} (59)
}

func sendText(stream *transport.Stream, txt1, txt2 string) {
	sgl1 := Mem2.NewSGL(0)
	sgl1.Write([]byte(txt1))
	stream.Send(transport.Header{"abc", "X", nil, sgl1.Size()}, sgl1, nil)

	sgl2 := Mem2.NewSGL(0)
	sgl2.Write([]byte(txt2))
	stream.Send(transport.Header{"abracadabra", "p/q/s", []byte{'1', '2', '3'}, sgl2.Size()}, sgl2, nil)
}

func Example_Mux() {
	receive := func(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
		cmn.Assert(err == nil)
		object, err := ioutil.ReadAll(objReader)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if int64(len(object)) != hdr.Dsize {
			panic(fmt.Sprintf("size %d != %d", len(object), hdr.Dsize))
		}
		// fmt.Printf("%s...\n", string(object[:16])) // FIXME
	}
	mux := http.NewServeMux()

	transport.SetMux("n1", mux)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	path, err := transport.Register("n1", "dummy-rx", receive)
	if err != nil {
		fmt.Println(err)
		return
	}
	httpclient := &http.Client{Transport: &http.Transport{}}
	url := ts.URL + path
	stream := transport.NewStream(httpclient, url, nil)

	time.Sleep(time.Second * 2)

	sendText(stream, text1, text2)

	time.Sleep(time.Second * 2)

	sendText(stream, text3, text4)
	stream.Fin()

	// Lorem ipsum dolo...
	// Duis aute irure ...
	// Et harum quidem ...
	// Temporibus autem...

	// Output:
}

// test random streaming
func Test_OneStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	mux := http.NewServeMux()

	transport.SetMux("n1", mux)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	streamWriteUntil(t, 99, nil, ts, nil, nil)
	printNetworkStats(t, "n1")
}

func Test_CancelStream(t *testing.T) {
	mux := http.NewServeMux()
	network := "nc"
	transport.SetMux(network, mux)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	recvFunc := func(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
		if err != nil {
			return // stream probably canceled nothing to do
		}
		slab := Mem2.SelectSlab2(cmn.GiB)
		buf := slab.Alloc()
		written, err := io.CopyBuffer(ioutil.Discard, objReader, buf)
		if err != nil && err != io.EOF {
			tutils.Logf("err %v", err)
		} else if err == io.EOF && written != hdr.Dsize {
			t.Fatalf("size %d != %d", written, hdr.Dsize)
		}
		slab.Free(buf)
	}
	trname := "cancel-rx-88"
	path, err := transport.Register(network, trname, recvFunc)
	tutils.CheckFatal(err, t)

	httpclient := &http.Client{Transport: &http.Transport{}}
	url := ts.URL + path
	ctx, cancel := context.WithCancel(context.Background())
	stream := transport.NewStream(httpclient, url, &transport.Extra{Burst: 1, Ctx: ctx})
	now := time.Now()

	random := newRand(time.Now().UnixNano())
	size, num, prevsize := int64(0), 0, int64(0)
	for time.Since(now) < duration {
		hdr := genStaticHeader()
		slab := Mem2.SelectSlab2(hdr.Dsize)
		reader := newRandReader(random, hdr, slab)
		stream.Send(hdr, reader, nil)
		num++
		size += hdr.Dsize
		if size-prevsize >= cmn.GiB {
			tutils.Logf("%s: %d GiB\n", stream, size/cmn.GiB)
			prevsize = size
			if num > 10 && random.Int63()%3 == 0 {
				cancel()
				tutils.Logln("Canceling...")
				break
			}
		}
	}
	time.Sleep(time.Second)
	termReason, termErr := stream.TermInfo()
	stats := stream.GetStats()
	fmt.Printf("send$ %s: offset=%d, num=%d(%d), idle=%.2f%%, term(%s, %v)\n",
		stream, stats.Offset, stats.Num, num, stats.IdlePct, termReason, termErr)
	stream.Fin() // vs. stream being term-ed
}

func Test_MultiStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	tutils.Logf("Duration %v\n", duration)
	mux := http.NewServeMux()
	transport.SetMux("n1", mux)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	wg := &sync.WaitGroup{}
	netstats := make(map[string]transport.EndpointStats)
	lock := &sync.Mutex{}
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go streamWriteUntil(t, i, wg, ts, netstats, lock)
	}
	wg.Wait()
	compareNetworkStats(t, "n1", netstats)
}

func printNetworkStats(t *testing.T, network string) {
	netstats, err := transport.GetNetworkStats(network)
	tutils.CheckFatal(err, t)
	for trname, eps := range netstats {
		for sessid, stats := range eps { // EndpointStats by session ID
			fmt.Printf("recv$ %s[%d]: offset=%d, num=%d\n", trname, sessid, stats.Offset, stats.Num)
		}
	}
}

func compareNetworkStats(t *testing.T, network string, netstats1 map[string]transport.EndpointStats) {
	netstats2, err := transport.GetNetworkStats(network)
	tutils.CheckFatal(err, t)
	for trname, eps2 := range netstats2 {
		eps1, ok := netstats1[trname]
		for sessid, stats2 := range eps2 { // EndpointStats by session ID
			fmt.Printf("recv$ %s[%d]: offset=%d, num=%d\n", trname, sessid, stats2.Offset, stats2.Num)
			if ok {
				stats1, ok := eps1[sessid]
				if ok {
					fmt.Printf("send$ %s[%d]: offset=%d, num=%d, idle=%.2f%%\n",
						trname, sessid, stats1.Offset, stats1.Num, stats1.IdlePct)
				} else {
					fmt.Printf("send$ %s[%d]: -- not present --\n", trname, sessid)
				}
			} else {
				fmt.Printf("send$ %s[%d]: -- not present --\n", trname, sessid)
			}
		}
	}
}

func Test_MultipleNetworks(t *testing.T) {
	totalRecv, recvFunc := makeRecvFunc(t)

	var streams []*transport.Stream
	for idx := 0; idx < 10; idx++ {
		network := fmt.Sprintf("network-%d", idx)
		mux := http.NewServeMux()
		transport.SetMux(network, mux)
		ts := httptest.NewServer(mux)
		defer ts.Close()
		path, err := transport.Register(network, "endpoint", recvFunc)
		tutils.CheckFatal(err, t)

		httpclient := &http.Client{Transport: &http.Transport{}}
		url := ts.URL + path
		streams = append(streams, transport.NewStream(httpclient, url, nil))
	}

	totalSend := int64(0)
	for _, stream := range streams {
		hdr, reader := makeRandReader()
		stream.Send(hdr, reader, nil)
		totalSend += hdr.Dsize
	}

	for _, stream := range streams {
		stream.Fin()
	}

	if *totalRecv != totalSend {
		t.Fatalf("total received bytes %d is different from expected: %d", *totalRecv, totalSend)
	}
}

func Test_OnSendCallback(t *testing.T) {
	mux := http.NewServeMux()

	transport.SetMux("n1", mux)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	totalRecv, recvFunc := makeRecvFunc(t)
	path, err := transport.Register("n1", "callback", recvFunc)
	if err != nil {
		t.Fatal(err)
	}
	httpclient := &http.Client{Transport: &http.Transport{}}
	url := ts.URL + path
	stream := transport.NewStream(httpclient, url, nil)

	var (
		totalSend int64
		mu        sync.Mutex
		posted    []*randReader = make([]*randReader, 10000) // 10K objects
	)
	for idx := 0; idx < len(posted); idx++ {
		hdr, rr := makeRandReader()
		mu.Lock()
		posted[idx] = rr
		mu.Unlock()
		rrc := &randReaderCtx{t, rr, posted, &mu, idx}
		stream.Send(hdr, rr, rrc.sentCallback)
		totalSend += hdr.Dsize
	}
	stream.Fin()

	// mu.Lock()  - no need to crit-sect as the Fin is done
	for idx := range posted {
		if posted[idx] != nil {
			t.Errorf("sent-callback %d never fired", idx)
		}
	}
	if *totalRecv != totalSend {
		t.Fatalf("total received bytes %d is different from expected: %d", *totalRecv, totalSend)
	}
}

//
// test helpers
//

func streamWriteUntil(t *testing.T, ii int, wg *sync.WaitGroup, ts *httptest.Server, netstats map[string]transport.EndpointStats, lock *sync.Mutex) {
	if wg != nil {
		defer wg.Done()
	}
	totalRecv, recvFunc := makeRecvFunc(t)
	path, err := transport.Register("n1", fmt.Sprintf("rand-rx-%d", ii), recvFunc)
	tutils.CheckFatal(err, t)

	// NOTE: prior to running traffic, give it some time for all other goroutines
	//       to perform registration (see README for details)
	time.Sleep(time.Second)

	httpclient := &http.Client{Transport: &http.Transport{}}
	url := ts.URL + path
	stream := transport.NewStream(httpclient, url, nil)
	trname, sessid := stream.ID()
	now := time.Now()

	random := newRand(time.Now().UnixNano())
	size, num, prevsize := int64(0), 0, int64(0)
	for time.Since(now) < duration {
		hdr := genRandomHeader(random)
		slab := Mem2.SelectSlab2(hdr.Dsize)
		reader := newRandReader(random, hdr, slab)
		stream.Send(hdr, reader, nil)
		num++
		size += hdr.Dsize
		if size-prevsize >= cmn.GiB {
			tutils.Logf("[%2d]: %d GiB\n", ii, size/cmn.GiB)
			prevsize = size
			if random.Int63()%7 == 0 {
				time.Sleep(time.Second * 2) // simulate occasional timeout
			}
			if random.Int63()%5 == 0 {
				stats := stream.GetStats()
				tutils.Logf("send$ %s[%d]: idle=%.2f%%\n", trname, sessid, stats.IdlePct)
			}
		}
	}
	stream.Fin()
	stats := stream.GetStats()
	if netstats == nil {
		termReason, termErr := stream.TermInfo()
		fmt.Printf("send$ %s[%d]: offset=%d, num=%d(%d), idle=%.2f%%, term(%s, %v)\n",
			trname, sessid, stats.Offset, stats.Num, num, stats.IdlePct, termReason, termErr)
	} else {
		lock.Lock()
		eps := make(transport.EndpointStats)
		eps[sessid] = &stats
		netstats[trname] = eps
		lock.Unlock()
	}

	if *totalRecv != size {
		t.Fatalf("total received bytes %d is different from expected: %d", *totalRecv, size)
	}
}

func makeRecvFunc(t *testing.T) (*int64, transport.Receive) {
	totalReceived := new(int64)
	return totalReceived, func(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
		cmn.Assert(err == nil)
		slab := Mem2.SelectSlab2(32 * cmn.KiB)
		buf := slab.Alloc()
		written, err := io.CopyBuffer(ioutil.Discard, objReader, buf)
		if err != io.EOF {
			tutils.CheckFatal(err, t)
		}
		if written != hdr.Dsize {
			t.Fatalf("size %d != %d", written, hdr.Dsize)
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
	hdr.Bucket = "a"
	hdr.Objname = "b"
	hdr.Opaque = []byte("c")
	hdr.Dsize = cmn.GiB
	return
}

func genRandomHeader(random *rand.Rand) (hdr transport.Header) {
	x := random.Int63()
	hdr.Bucket = strconv.FormatInt(x, 10)
	hdr.Objname = path.Join(hdr.Bucket, strconv.FormatInt(cmn.MaxInt64-x, 10))
	pos := x % int64(len(text))
	hdr.Opaque = []byte(text[int(pos):])
	y := x & 3
	switch y {
	case 0:
		hdr.Dsize = (x & 0xffffff) + 1
	case 1:
		hdr.Dsize = (x & 0xfffff) + 1
	case 2:
		hdr.Dsize = (x & 0xffff) + 1
	default:
		hdr.Dsize = (x & 0xfff) + 1
	}
	return
}

//===========================================================================
//
// randReader
//
//===========================================================================

type randReader struct {
	buf    []byte
	hdr    transport.Header
	slab   *memsys.Slab2
	off    int64
	random *rand.Rand
}

func newRandReader(random *rand.Rand, hdr transport.Header, slab *memsys.Slab2) *randReader {
	buf := slab.Alloc()
	_, err := random.Read(buf)
	if err != nil {
		panic("Failed read rand: " + err.Error())
	}
	return &randReader{buf: buf, hdr: hdr, slab: slab, random: random}
}

func makeRandReader() (transport.Header, *randReader) {
	slab := Mem2.SelectSlab2(32 * cmn.KiB)
	random := newRand(time.Now().UnixNano())
	hdr := genRandomHeader(random)
	reader := newRandReader(random, hdr, slab)
	return hdr, reader
}

func (r *randReader) Read(p []byte) (n int, err error) {
	for {
		rem := r.hdr.Dsize - r.off
		if rem == 0 {
			return n, io.EOF
		}
		l64 := cmn.MinI64(rem, int64(len(p)-n))
		if l64 == 0 {
			return
		}
		nr := copy(p[n:n+int(l64)], r.buf)
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
	r.slab.Free(r.buf)
	return nil
}

type randReaderCtx struct {
	t      *testing.T
	rr     *randReader
	posted []*randReader // => stream
	mu     *sync.Mutex
	idx    int
}

func (rrc *randReaderCtx) sentCallback(hdr transport.Header, reader io.ReadCloser, err error) {
	if err != nil {
		rrc.t.Errorf("sent-callback %d(%s/%s) returned an error: %v", rrc.idx, hdr.Bucket, hdr.Objname, err)
	}
	rr := rrc.rr
	rr.slab.Free(rr.buf)
	rrc.mu.Lock()
	rrc.posted[rrc.idx] = nil
	if rrc.idx > 0 && rrc.posted[rrc.idx-1] != nil {
		rrc.t.Errorf("sent-callback %d(%s/%s) fired out of order", rrc.idx, hdr.Bucket, hdr.Objname)
	}
	rrc.posted[rrc.idx] = nil
	rrc.mu.Unlock()
}
