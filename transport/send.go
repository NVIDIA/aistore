/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
//
// Package transport provides streaming object-based transport over http for
// massive intra-DFC or DFC-to-DFC data transfers.
//
// See README for details and usage examples.
//
package transport

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/xoshiro256"
)

// transport defaults
const (
	MaxHeaderSize  = 1024
	lastMarker     = cmn.MaxInt64
	defaultIdleOut = time.Second
	sizeofI64      = int(unsafe.Sizeof(uint64(0)))
	burstNum       = 64 // default max num objects that can be posted for sending without any back-pressure
)

// stream TCP/HTTP lifecycle: expired => posted => activated ( => expired) transitions
const (
	expired = iota + 1
	posted
	activated
)

// API: types
type (
	Stream struct {
		// user-defined & queryable
		client          *http.Client // http client this send-stream will use
		toURL, trname   string       // http endpoint
		sessid          int64        // stream session ID
		stats           Stats        // stream stats
		Numcur, Sizecur int64        // gets reset to zero upon each timeout
		// internals
		lid      string        // log prefix
		workCh   chan obj      // next object to stream
		lastCh   chan struct{} // end of stream
		stopCh   chan struct{} // stop/abort stream
		postCh   chan struct{} // expired => posted transition: new HTTP/TCP session
		callback SendCallback  // to free SGLs, close files, etc.
		time     struct {
			start   int64         // to support idle(%)
			idleOut time.Duration // inter-object timeout: when triggers, causes recycling of the underlying http request
			idle    *time.Timer
		}
		lifecycle int64 // see state enum above
		wg        sync.WaitGroup
		sendoff   sendoff
		maxheader []byte // max header buffer
		header    []byte // object header - slice of the maxheader with bucket/objname, etc. fields
	}
	// advanced usage: additional stream control
	Extra struct {
		IdleTimeout time.Duration   // stream idle timeout: causes PUT to terminate (and renew on the next obj send)
		Ctx         context.Context // presumably, result of context.WithCancel(context.Background()) by the caller
		Callback    SendCallback    // typical usage: to free SGLs, close files, etc.
		Burst       int             // max num objects that can be posted for sending without any back-pressure
		DryRun      bool            // dry run: short-circuit the stream on the send side
	}
	// stream stats
	Stats struct {
		Num     int64   // number of transferred objects
		Size    int64   // transferred size, in bytes
		Offset  int64   // stream offset, in bytes
		IdleDur int64   // the time stream was idle since the previous GetStats call
		TotlDur int64   // total time since --/---/---
		IdlePct float64 // idle time % since --/---/--
	}
	EndpointStats map[int64]*Stats // all stats for a given http endpoint defined by a tuple (network, trname) by session ID
	// object header
	Header struct {
		Bucket, Objname string // uname at the destination
		Opaque          []byte // custom control (optional)
		Dsize           int64  // size of the object (size=0 (unknown) not supported yet)
	}
	// object-sent callback that has the following signature can optionally be defined on a:
	// a) per-stream basis (via NewStream constructor - see Extra struct above)
	// b) for a given object that is being sent (for instance, to support a call-per-batch semantics)
	// Naturally, object callback "overrides" the per-stream one: when object callback is defined
	// (i.e., non-nil), the stream callback is ignored/skipped.
	// NOTE:
	// This callback's latency adds to the latency of the entire stream operation on the send side.
	// It is critically important, therefore, that user implementations do not take extra locks,
	// do not execute system calls and, generally, return as soon as possible.
	SendCallback func(Header, io.ReadCloser, error)
)

// internal
type (
	obj struct {
		hdr      Header
		reader   io.ReadCloser // reader, to read the object, and close when done
		callback SendCallback  // callback fired when send has finished either successfully or with error
	}
	sendoff struct {
		obj obj
		// in progress
		off int64
		dod int64
	}
)

//
// API: methods
//
func NewStream(client *http.Client, toURL string, extra *Extra) (s *Stream) {
	var dryrun bool
	u, err := url.Parse(toURL)
	if err != nil {
		glog.Errorf("Failed to parse %s: %v", toURL, err)
		return
	}
	s = &Stream{client: client, toURL: toURL}

	s.time.idleOut = defaultIdleOut
	if extra != nil {
		s.callback = extra.Callback
		if extra.IdleTimeout > 0 {
			s.time.idleOut = extra.IdleTimeout
		}
		dryrun = extra.DryRun
		cmn.Assert(dryrun || client != nil)
	}
	if tm := time.Now().UnixNano(); tm&0xffff != 0 {
		s.sessid = tm & 0xffff
	} else { // enforce non-zero
		s.sessid = tm
	}
	s.trname = path.Base(u.Path)
	s.lid = fmt.Sprintf("%s[%d]:", s.trname, s.sessid)

	// burst size: the number of objects the caller is permitted to post for sending
	// without experiencing any sort of back-pressure
	burst := burstNum
	if extra != nil && extra.Burst > 0 {
		burst = extra.Burst
	}
	if a := os.Getenv("DFC_STREAM_BURST_NUM"); a != "" {
		if burst64, err := strconv.ParseInt(a, 10, 0); err != nil {
			glog.Errorf("%s error parsing burst env '%s': %v", s.lid, a, err)
			burst = burstNum
		} else {
			burst = int(burst64)
		}
	}
	s.workCh = make(chan obj, burst)

	s.lastCh = make(chan struct{}, 1)
	s.stopCh = make(chan struct{}, 1)
	s.postCh = make(chan struct{}, 1)
	s.maxheader = make([]byte, MaxHeaderSize)
	s.time.idle = time.NewTimer(s.time.idleOut)
	s.time.idle.Stop()
	atomic.StoreInt64(&s.lifecycle, expired) // initiate HTTP/TCP session upon arrival of the very first object and *not* earlier

	var ctx context.Context
	if extra != nil && extra.Ctx != nil {
		ctx = extra.Ctx
	} else {
		ctx, _ = context.WithCancel(context.Background())
	}
	atomic.StoreInt64(&s.time.start, time.Now().UnixNano())
	s.wg.Add(1)
	go s.doTx(ctx, dryrun)
	return
}

func (s *Stream) Send(hdr Header, reader io.ReadCloser, callback SendCallback) {
	if bool(glog.V(4)) || debug {
		glog.Infof("%s send %s/%s(%d)", s.lid, hdr.Bucket, hdr.Objname, hdr.Dsize)
	}
	s.time.idle.Stop()
	if atomic.CompareAndSwapInt64(&s.lifecycle, expired, posted) {
		if bool(glog.V(4)) || debug {
			glog.Infof("%s expired => posted", s.lid)
		}
		s.postCh <- struct{}{}
	}
	s.workCh <- obj{hdr, reader, callback}
}

func (s *Stream) Fin() {
	defer func() {
		if r := recover(); r != nil { // case in point: stream cancel() mistakenly followed by Fin()
			glog.Errorln("recovered", r)
		}
	}()
	s.workCh <- obj{hdr: Header{Dsize: lastMarker}}
	if atomic.CompareAndSwapInt64(&s.lifecycle, expired, posted) {
		glog.Infof("%s (expired => posted) to handle Fin", s.lid)
		s.postCh <- struct{}{}
	}
	s.wg.Wait() // synchronous termination
}

func (s *Stream) Stop()               { s.stopCh <- struct{}{} }
func (s *Stream) ID() (string, int64) { return s.trname, s.sessid }
func (s *Stream) URL() string         { return s.toURL }

func (s *Stream) GetStats() (stats Stats) {
	// byte-num transfer stats
	stats.Num = atomic.LoadInt64(&s.stats.Num)
	stats.Offset = atomic.LoadInt64(&s.stats.Offset)
	stats.Size = atomic.LoadInt64(&s.stats.Size)
	// idle(%)
	now := time.Now().UnixNano()
	stats.TotlDur = now - atomic.LoadInt64(&s.time.start)
	stats.IdlePct = float64(atomic.LoadInt64(&s.stats.IdleDur)) * 100 / float64(stats.TotlDur)
	stats.IdlePct = cmn.MinF64(100, stats.IdlePct) // GetStats is async vis-à-vis IdleDur += deltas
	atomic.StoreInt64(&s.time.start, now)
	atomic.StoreInt64(&s.stats.IdleDur, 0)
	return
}

func (hdr *Header) IsLast() bool { return hdr.Dsize == lastMarker }

//
// internal methods
//
// main loop
func (s *Stream) doTx(ctx context.Context, dryrun bool) {
	for {
		if atomic.CompareAndSwapInt64(&s.lifecycle, posted, activated) {
			if bool(glog.V(4)) || debug {
				glog.Infof("%s posted => activated", s.lid)
			}
			if dryrun {
				s.dryrun()
			} else if err := s.doRequest(ctx); err != nil {
				break
			}
		}
		if !s.isNextReq(ctx) {
			break
		}
	}
	s.time.idle.Stop()
	close(s.workCh)
	close(s.lastCh)
	close(s.stopCh)
	s.wg.Done()
}

func (s *Stream) isNextReq(ctx context.Context) (next bool) {
	beg := time.Now()
	defer s.addIdle(beg)
	for {
		select {
		case <-ctx.Done():
			glog.Infof("%s %v", s.lid, ctx.Err())
			return
		case <-s.lastCh:
			if bool(glog.V(4)) || debug {
				glog.Infof("%s end-of-stream", s.lid)
			}
			return
		case <-s.stopCh:
			glog.Infof("%s stopped", s.lid)
			return
		case <-s.postCh:
			if v := atomic.LoadInt64(&s.lifecycle); v != expired {
				if debug {
					cmn.Assert(v == posted)
				}
				next = true // initiate new HTTP/TCP session
				return
			}
		}
	}
}

func (s *Stream) doRequest(ctx context.Context) (err error) {
	var (
		request  *http.Request
		response *http.Response
	)
	if request, err = http.NewRequest(http.MethodPut, s.toURL, s); err != nil {
		return
	}
	request = request.WithContext(ctx)
	s.Numcur, s.Sizecur = 0, 0
	if bool(glog.V(4)) || debug {
		glog.Infof("%s Do", s.lid)
	}
	response, err = s.client.Do(request)
	if err == nil {
		if bool(glog.V(4)) || debug {
			glog.Infof("%s Done", s.lid)
		}
	} else {
		glog.Errorf("%s Done, err %v", s.lid, err)
		return
	}
	ioutil.ReadAll(response.Body)
	response.Body.Close()
	return
}

// as io.Reader
func (s *Stream) Read(b []byte) (n int, err error) {
	// current object
	if s.sendoff.obj.reader != nil {
		if s.sendoff.dod != 0 {
			return s.sendData(b)
		}
		return s.sendHdr(b)
	}
	beg := time.Now()
	defer s.addIdle(beg)
	select {
	// next object
	case s.sendoff.obj = <-s.workCh:
		s.time.idle.Stop()
		l := s.insHeader(s.sendoff.obj.hdr)
		s.header = s.maxheader[:l]
		return s.sendHdr(b)
	// control stream lifesycle at object boundaries
	case <-s.time.idle.C:
		err = io.EOF
		num := atomic.LoadInt64(&s.stats.Num)
		glog.Warningf("%s timed out (%d/%d)", s.lid, s.Numcur, num)
		if atomic.CompareAndSwapInt64(&s.lifecycle, activated, expired) {
			glog.Infof("%s activated => expired", s.lid)
		}
		return
	case <-s.stopCh:
		num := atomic.LoadInt64(&s.stats.Num)
		glog.Infof("%s stopped (%d/%d)", s.lid, s.Numcur, num)
		s.stopCh <- struct{}{}
		err = io.EOF
		return
	}
}

func (s *Stream) sendHdr(b []byte) (n int, err error) {
	n = copy(b, s.header[s.sendoff.off:])
	s.sendoff.off += int64(n)
	if s.sendoff.off >= int64(len(s.header)) {
		if debug {
			cmn.Assert(s.sendoff.off == int64(len(s.header)))
		}
		atomic.AddInt64(&s.stats.Offset, s.sendoff.off)
		if bool(glog.V(4)) || debug {
			num := atomic.LoadInt64(&s.stats.Num)
			glog.Infof("%s hlen=%d (%d/%d)", s.lid, s.sendoff.off, s.Numcur, num)
		}
		s.sendoff.dod = s.sendoff.off
		s.sendoff.off = 0

		obj := &s.sendoff.obj
		if !obj.hdr.IsLast() {
			if bool(glog.V(4)) || debug {
				glog.Infof("%s sent header %s/%s(%d)", s.lid, obj.hdr.Bucket, obj.hdr.Objname, obj.hdr.Dsize)
			}
		} else {
			if bool(glog.V(4)) || debug {
				glog.Infof("%s sent last", s.lid)
			}
			err = io.EOF
			s.lastCh <- struct{}{}
		}
	}
	return
}

func (s *Stream) sendData(b []byte) (n int, err error) {
	n, err = s.sendoff.obj.reader.Read(b)
	s.sendoff.off += int64(n) // (avg send transfer size tbd)
	if err != nil {
		if err == io.EOF {
			err = nil
		}
		s.eoObj(err)
	} else if s.sendoff.off >= s.sendoff.obj.hdr.Dsize {
		s.eoObj(err)
	}
	return
}

func (s *Stream) eoObj(err error) {
	var obj = s.sendoff.obj
	s.Numcur++
	s.Sizecur += s.sendoff.off
	atomic.AddInt64(&s.stats.Num, 1)
	atomic.AddInt64(&s.stats.Offset, s.sendoff.off)
	atomic.AddInt64(&s.stats.Size, s.sendoff.off)
	if s.sendoff.off != obj.hdr.Dsize {
		if debug {
			errstr := fmt.Sprintf("%s: hdr: %v; offset %d != %d size", s.lid, string(s.header), s.sendoff.off, obj.hdr.Dsize)
			cmn.Assert(false, errstr)
		} else {
			glog.Errorf("%s offset %d != %d size", s.lid, s.sendoff.off, obj.hdr.Dsize)
		}
	} else {
		if bool(glog.V(4)) || debug {
			glog.Infof("%s sent size=%d (%d/%d)", s.lid, obj.hdr.Dsize, s.Numcur, s.stats.Num)
		}
	}

	if closeErr := obj.reader.Close(); closeErr != nil {
		if debug {
			cmn.Assert(false, fmt.Sprintf("%s: hdr: %v; failed to close reader %v", s.lid, string(s.header), closeErr))
		} else {
			glog.Errorf("%s: failed to close reader %v", s.lid, closeErr)
		}

		if err == nil {
			err = closeErr
		}
	}
	s.time.idle.Reset(s.time.idleOut)
	if obj.callback != nil {
		obj.callback(obj.hdr, obj.reader, err)
	} else if s.callback != nil {
		s.callback(obj.hdr, obj.reader, err)
	}
	s.sendoff = sendoff{}
}

//
// stream helpers
//
func (s *Stream) insHeader(hdr Header) (l int) {
	if debug {
		cmn.Assert(len(hdr.Bucket)+len(hdr.Objname)+len(hdr.Opaque) < MaxHeaderSize-12*sizeofI64)
	}
	l = sizeofI64 * 2
	l = insString(l, s.maxheader, hdr.Bucket)
	l = insString(l, s.maxheader, hdr.Objname)
	l = insByte(l, s.maxheader, hdr.Opaque)
	l = insInt64(l, s.maxheader, s.sessid)
	l = insInt64(l, s.maxheader, hdr.Dsize)
	hlen := l - sizeofI64*2
	insInt64(0, s.maxheader, int64(hlen))
	checksum := xoshiro256.Hash(uint64(hlen))
	insUint64(sizeofI64, s.maxheader, checksum)
	return
}

func insString(off int, to []byte, str string) int {
	var l = len(str)
	binary.BigEndian.PutUint64(to[off:], uint64(l))
	off += sizeofI64
	n := copy(to[off:], str)
	if debug {
		cmn.Assert(n == l)
	}
	return off + l
}

func insByte(off int, to []byte, b []byte) int {
	var l = len(b)
	binary.BigEndian.PutUint64(to[off:], uint64(l))
	off += sizeofI64
	n := copy(to[off:], b)
	if debug {
		cmn.Assert(n == l)
	}
	return off + l
}

func insInt64(off int, to []byte, i int64) int {
	binary.BigEndian.PutUint64(to[off:], uint64(i))
	return off + sizeofI64
}

func insUint64(off int, to []byte, i uint64) int {
	binary.BigEndian.PutUint64(to[off:], i)
	return off + sizeofI64
}

// addIdle
func (s *Stream) addIdle(beg time.Time) { atomic.AddInt64(&s.stats.IdleDur, int64(time.Since(beg))) }

//
// dry-run ---------------------------
//
func (s *Stream) dryrun() {
	buf := make([]byte, cmn.KiB*32)
	scloser := ioutil.NopCloser(s)
	it := iterator{trname: s.trname, body: scloser, headerBuf: make([]byte, MaxHeaderSize)}
	for {
		objReader, _, _, err := it.next()
		if objReader != nil {
			written, _ := io.CopyBuffer(ioutil.Discard, objReader, buf)
			cmn.Assert(written == objReader.hdr.Dsize)
			continue
		}
		if err != nil {
			break
		}
	}
}
