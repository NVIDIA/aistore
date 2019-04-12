// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"container/heap"
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

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/xoshiro256"
)

// transport defaults
const (
	maxHeaderSize  = 1024
	lastMarker     = cmn.MaxInt64
	tickMarker     = cmn.MaxInt64 ^ 0xa5a5a5a5
	tickUnit       = time.Second
	defaultIdleOut = time.Second * 2
	sizeofI64      = int(unsafe.Sizeof(uint64(0)))
	burstNum       = 32 // default max num objects that can be posted for sending without any back-pressure
)

// stream TCP/HTTP session: inactive <=> active transitions
const (
	inactive = iota
	active
)

// termination: reasons
const (
	reasonCanceled = "canceled"
	reasonUnknown  = "unknown"
	reasonError    = "error"
	endOfStream    = "end-of-stream"
	reasonStopped  = "stopped"
)

// API: types
type (
	Stream struct {
		// user-defined & queryable
		client          *http.Client // http client this send-stream will use
		toURL, trname   string       // http endpoint
		sessID          int64        // stream session ID
		sessST          int64        // state of the TCP/HTTP session: active (connected) | inactive (disconnected)
		stats           Stats        // stream stats
		Numcur, Sizecur int64        // gets reset to zero upon each timeout
		// internals
		lid      string        // log prefix
		workCh   chan obj      // aka SQ: next object to stream
		cmplCh   chan cmpl     // aka SCQ; note that SQ and SCQ together form a FIFO
		lastCh   cmn.StopCh    // end of stream
		stopCh   cmn.StopCh    // stop/abort stream
		postCh   chan struct{} // to indicate that workCh has work
		callback SendCallback  // to free SGLs, close files, etc.
		time     struct {
			start   int64         // to support idle(%)
			idleOut time.Duration // idle timeout
			posted  int64         // num posted since the last GC do()
			ticks   int           // num 1s ticks until idle timeout
			index   int           // heap stuff
		}
		wg        sync.WaitGroup
		sendoff   sendoff
		maxheader []byte // max header buffer
		header    []byte // object header - slice of the maxheader with bucket/objname, etc. fields
		term      struct {
			barr   int64
			err    error
			reason *string
		}
	}
	// advanced usage: additional stream control
	Extra struct {
		IdleTimeout time.Duration   // stream idle timeout: causes PUT to terminate (and renew on the next obj send)
		Ctx         context.Context // presumably, result of context.WithCancel(context.Background()) by the caller
		Callback    SendCallback    // typical usage: to free SGLs, close files, etc.
		Burst       int             // SQ and CSQ buffer sizes: max num objects and send-completions
		DryRun      bool            // dry run: short-circuit the stream on the send side
	}
	// stream stats
	Stats struct {
		Num     int64   // number of transferred objects
		Size    int64   // transferred size, in bytes
		Offset  int64   // stream offset, in bytes
		IdleDur int64   // the time stream was idle since the previous getStats call
		TotlDur int64   // total time since --/---/---
		IdlePct float64 // idle time % since --/---/--
	}
	EndpointStats map[int64]*Stats // all stats for a given http endpoint defined by a tuple (network, trname) by session ID

	// attributes associated with given object
	ObjectAttrs struct {
		Atime      int64  // access time - nanoseconds since UNIX epoch
		Size       int64  // size of objects in bytes
		CksumType  string // checksum type
		CksumValue string // checksum of the object produced by given checksum type
		Version    string // version of the object
	}

	// object header
	Header struct {
		Bucket, Objname string      // uname at the destination
		IsLocal         bool        // determines if the bucket is local
		Opaque          []byte      // custom control (optional)
		ObjAttrs        ObjectAttrs // attributes/metadata of the sent object
	}
	// object-sent callback that has the following signature can optionally be defined on a:
	// a) per-stream basis (via NewStream constructor - see Extra struct above)
	// b) for a given object that is being sent (for instance, to support a call-per-batch semantics)
	// Naturally, object callback "overrides" the per-stream one: when object callback is defined
	// (i.e., non-nil), the stream callback is ignored/skipped.
	//
	// NOTE: if defined, the callback executes asynchronously as far as the sending part is concerned
	SendCallback func(Header, io.ReadCloser, error)

	StreamCollector struct {
		cmn.Named
	}
)

// internal
type (
	obj struct {
		hdr      Header        // object header
		reader   io.ReadCloser // reader, to read the object, and close when done
		callback SendCallback  // callback fired when sending is done OR when the stream terminates (see term.reason)
		prc      *int64        // optional refcount; if present, SendCallback gets called if and when *prc reaches zero
	}
	sendoff struct {
		obj obj
		// in progress
		off int64
		dod int64
	}
	cmpl struct { // send completions => SCQ
		obj obj
		err error
	}
	nopReadCloser struct{}

	collector struct {
		streams map[string]*Stream
		heap    []*Stream
		ticker  *time.Ticker
		stopCh  cmn.StopCh
		ctrlCh  chan ctrl
	}
	ctrl struct { // add/del channel to/from collector
		s   *Stream
		add bool
	}
)

var (
	nopRC      = &nopReadCloser{}
	background = context.Background()
	sessID     = int64(100) // unique session IDs starting from 101
	sc         = &StreamCollector{}
	gc         *collector // real collector
)

// default HTTP client to be used with streams
func NewDefaultClient() *http.Client { return cmn.NewClient(cmn.ClientArgs{IdleConnsPerHost: 1000}) }

//
// Stream Collector - a singleton object with responsibilities that include:
// 1. control part of the stream lifecycle:
//    - activation (followed by connection establishment and HTTP PUT), and
//    - deactivation (teardown)
// 2. implement per-stream idle timeouts measured in tick units (tickUnit)
//    whereby each stream is effectively provided with its own idle timer
//
// NOTE: requires static initialization
//
func Init() *StreamCollector {
	cmn.Assert(gc == nil)

	// real stream collector
	gc = &collector{
		stopCh:  cmn.NewStopCh(),
		ctrlCh:  make(chan ctrl, 16),
		streams: make(map[string]*Stream, 16),
		heap:    make([]*Stream, 0, 16), // min-heap sorted by stream.time.ticks
	}
	heap.Init(gc)

	return sc
}

func (sc *StreamCollector) Run() (err error) {
	glog.Infof("Starting %s", sc.Getname())
	return gc.run()
}
func (sc *StreamCollector) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", sc.Getname(), err)
	gc.stop()
}

func (gc *collector) run() (err error) {
	gc.ticker = time.NewTicker(tickUnit)
	for {
		select {
		case <-gc.ticker.C:
			gc.do()
		case ctrl, ok := <-gc.ctrlCh:
			if !ok {
				return
			}
			s, add := ctrl.s, ctrl.add
			_, ok = gc.streams[s.lid]
			if add {
				cmn.AssertMsg(!ok, s.lid)
				gc.streams[s.lid] = s
				heap.Push(gc, s)
			} else {
				cmn.AssertMsg(ok, s.lid)
				heap.Remove(gc, s.time.index)
				s.time.ticks = 1
			}
		case <-gc.stopCh.Listen():
			for _, s := range gc.streams {
				s.Stop()
			}
			gc.streams = nil
			return
		}
	}
}

func (gc *collector) stop() {
	gc.stopCh.Close()
}

func (gc *collector) remove(s *Stream) {
	gc.ctrlCh <- ctrl{s, false} // remove and close workCh
}

// as min-heap
func (gc *collector) Len() int { return len(gc.heap) }

func (gc *collector) Less(i, j int) bool {
	si := gc.heap[i]
	sj := gc.heap[j]
	return si.time.ticks < sj.time.ticks
}

func (gc *collector) Swap(i, j int) {
	gc.heap[i], gc.heap[j] = gc.heap[j], gc.heap[i]
	gc.heap[i].time.index = i
	gc.heap[j].time.index = j
}

func (gc *collector) Push(x interface{}) {
	l := len(gc.heap)
	s := x.(*Stream)
	s.time.index = l
	gc.heap = append(gc.heap, s)
	heap.Fix(gc, s.time.index) // reorder the newly added stream right away
}

func (gc *collector) update(s *Stream, ticks int) {
	s.time.ticks = ticks
	cmn.Assert(s.time.ticks >= 0)
	heap.Fix(gc, s.time.index)
}

func (gc *collector) Pop() interface{} {
	old := gc.heap
	n := len(old)
	sl := old[n-1]
	gc.heap = old[0 : n-1]
	return sl
}

// collector's main method
func (gc *collector) do() {
	for _, s := range gc.streams {
		if s.Terminated() {
			s.time.ticks--
			if s.time.ticks <= 0 {
				delete(gc.streams, s.lid)
				close(s.workCh) // delayed close
				if s.term.err == nil {
					s.term.err = fmt.Errorf(reasonUnknown)
				}
				for obj := range s.workCh {
					s.objDone(&obj, s.term.err)
				}
			}
		} else if atomic.LoadInt64(&s.sessST) == active {
			gc.update(s, s.time.ticks-1)
		}
	}
	for _, s := range gc.streams {
		if s.time.ticks > 0 {
			continue
		}
		gc.update(s, int(s.time.idleOut/tickUnit))
		if atomic.SwapInt64(&s.time.posted, 0) > 0 {
			continue
		}
		if len(s.workCh) == 0 && atomic.CompareAndSwapInt64(&s.sessST, active, inactive) {
			s.workCh <- obj{hdr: Header{ObjAttrs: ObjectAttrs{Size: tickMarker}}}
			if glog.FastV(4, glog.SmoduleTransport) {
				glog.Infof("%s: active => inactive", s)
			}
		}
	}
	// at this point the following must be true for each i = range gc.heap:
	// 1. heap[i].index == i
	// 2. heap[i+1].ticks >= heap[i].ticks
}

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
	if s.time.idleOut < tickUnit {
		s.time.idleOut = tickUnit
	}
	s.time.ticks = int(s.time.idleOut / tickUnit)
	s.sessID = atomic.AddInt64(&sessID, 1)
	s.trname = path.Base(u.Path)
	s.lid = fmt.Sprintf("%s[%d]", s.trname, s.sessID)

	// burst size: the number of objects the caller is permitted to post for sending
	// without experiencing any sort of back-pressure
	burst := burstNum
	if extra != nil && extra.Burst > 0 {
		burst = extra.Burst
	}
	if a := os.Getenv("AIS_STREAM_BURST_NUM"); a != "" {
		if burst64, err := strconv.ParseInt(a, 10, 0); err != nil {
			glog.Errorf("%s: error parsing burst env '%s': %v", s, a, err)
			burst = burstNum
		} else {
			burst = int(burst64)
		}
	}
	s.workCh = make(chan obj, burst)  // Send Qeueue or SQ
	s.cmplCh = make(chan cmpl, burst) // Send Completion Queue or SCQ

	s.lastCh = cmn.NewStopCh()
	s.stopCh = cmn.NewStopCh()
	s.postCh = make(chan struct{}, 1)
	s.maxheader = make([]byte, maxHeaderSize) // NOTE: must be large enough to accommodate all max-size Header
	atomic.StoreInt64(&s.sessST, inactive)    // NOTE: initiate HTTP session upon arrival of the first object

	var ctx context.Context
	if extra != nil && extra.Ctx != nil {
		ctx = extra.Ctx
	} else {
		ctx = background
	}

	atomic.StoreInt64(&s.time.start, time.Now().UnixNano())
	s.term.reason = new(string)

	s.wg.Add(2)
	go s.sendLoop(ctx, dryrun) // handle SQ
	go s.cmplLoop()            // handle SCQ

	gc.ctrlCh <- ctrl{s, true /* collect */}
	return
}

// Asynchronously send an object defined by its header and its reader.
// ---------------------------------------------------------------------------------------
//
// The sending pipeline is implemented as a pair (SQ, SCQ) where the former is a send queue
// realized as workCh, and the latter is a send completion queue (cmplCh).
// Together, SQ and SCQ form a FIFO as far as ordering of transmitted objects.
//
// NOTE: header-only objects are supported; when there's no data to send (that is,
// when the header's Dsize field is set to zero), the reader is not required and the
// corresponding argument in Send() can be set to nil.
//
// NOTE: object reader is always closed by the code that handles send completions.
// In the case when SendCallback is provided (i.e., non-nil), the closing is done
// right after calling this callback - see objDone below for details.
//
// NOTE: Optional reference counting is also done by (and in) the objDone, so that the
// SendCallback gets called if and only when the refcount (if provided i.e., non-nil)
// reaches zero.
//
// NOTE: For every transmission of every object there's always an objDone() completion
// (with its refcounting and reader-closing). This holds true in all cases including
// network errors that may cause sudden and instant termination of the underlying
// stream(s).
//
// ---------------------------------------------------------------------------------------
func (s *Stream) Send(hdr Header, reader io.ReadCloser, callback SendCallback, prc ...*int64) (err error) {
	if s.Terminated() {
		err = fmt.Errorf("%s terminated(%s, %v), cannot send [%s/%s(%d)]",
			s, *s.term.reason, s.term.err, hdr.Bucket, hdr.Objname, hdr.ObjAttrs.Size)
		glog.Errorln(err)
		return
	}
	atomic.AddInt64(&s.time.posted, 1)
	if atomic.CompareAndSwapInt64(&s.sessST, inactive, active) {
		s.postCh <- struct{}{}
		if glog.FastV(4, glog.SmoduleTransport) {
			glog.Infof("%s: inactive => active", s)
		}
	}
	// next object => SQ
	if reader == nil {
		cmn.Assert(hdr.IsHeaderOnly())
		reader = nopRC
	}
	obj := obj{hdr: hdr, reader: reader, callback: callback}
	if len(prc) > 0 {
		obj.prc = prc[0]
	}
	s.workCh <- obj
	if glog.FastV(4, glog.SmoduleTransport) {
		glog.Infof("%s: send %s/%s(%d)[sq=%d]", s, hdr.Bucket, hdr.Objname, hdr.ObjAttrs.Size, len(s.workCh))
	}
	return
}

func (s *Stream) Fin() (err error) {
	hdr := Header{ObjAttrs: ObjectAttrs{Size: lastMarker}}
	err = s.Send(hdr, nil, nil)
	s.wg.Wait() // normal (graceful, synchronous) termination
	return
}
func (s *Stream) Stop() {
	s.stopCh.Close()
}
func (s *Stream) URL() string         { return s.toURL }
func (s *Stream) ID() (string, int64) { return s.trname, s.sessID }
func (s *Stream) String() string      { return s.lid }
func (s *Stream) Terminated() bool    { return atomic.LoadInt64(&s.term.barr) != 0 }
func (s *Stream) terminate() {
	atomic.StoreInt64(&s.term.barr, 0xDEADBEEF)
	gc.remove(s)
	s.Stop()
	close(s.cmplCh)
}

func (s *Stream) TermInfo() (string, error) {
	if s.Terminated() && *s.term.reason == "" {
		if s.term.err == nil {
			s.term.err = fmt.Errorf(reasonUnknown)
		}
		*s.term.reason = reasonUnknown
	}
	return *s.term.reason, s.term.err
}

func (s *Stream) GetStats() (stats Stats) {
	// byte-num transfer stats
	stats.Num = atomic.LoadInt64(&s.stats.Num)
	stats.Offset = atomic.LoadInt64(&s.stats.Offset)
	stats.Size = atomic.LoadInt64(&s.stats.Size)
	// idle(%)
	now := time.Now().UnixNano()
	stats.TotlDur = now - atomic.LoadInt64(&s.time.start)
	stats.IdlePct = float64(atomic.LoadInt64(&s.stats.IdleDur)) * 100 / float64(stats.TotlDur)
	stats.IdlePct = cmn.MinF64(100, stats.IdlePct) // getStats is async vis-à-vis IdleDur += deltas
	atomic.StoreInt64(&s.time.start, now)
	atomic.StoreInt64(&s.stats.IdleDur, 0)
	return
}

func (hdr *Header) IsLast() bool       { return hdr.ObjAttrs.Size == lastMarker }
func (hdr *Header) IsIdleTick() bool   { return hdr.ObjAttrs.Size == tickMarker }
func (hdr *Header) IsHeaderOnly() bool { return hdr.ObjAttrs.Size == 0 || hdr.IsLast() }

//
// internal methods including the sending and completing loops below, each running in its own goroutine
//

func (s *Stream) sendLoop(ctx context.Context, dryrun bool) {
	for {
		if atomic.LoadInt64(&s.sessST) == active {
			if dryrun {
				s.dryrun()
			} else if err := s.doRequest(ctx); err != nil {
				*s.term.reason = reasonError
				s.term.err = err
				break
			}
		}
		if !s.isNextReq(ctx) {
			break
		}
	}

	s.terminate()
	s.wg.Done()

	// handle termination that is caused by anything other than Fin()
	if *s.term.reason != endOfStream {
		if *s.term.reason == reasonStopped {
			if glog.FastV(4, glog.SmoduleTransport) {
				glog.Infof("%s: stopped", s)
			}
		} else {
			glog.Errorf("%s: terminating (%s, %v)", s, *s.term.reason, s.term.err)
		}
		// first, wait for the SCQ/cmplCh to empty
		s.wg.Wait()

		// second, handle the last send that was interrupted
		if s.sendoff.obj.reader != nil {
			obj := &s.sendoff.obj
			s.objDone(obj, s.term.err)
		}
		// finally, handle pending SQ
		for obj := range s.workCh {
			s.objDone(&obj, s.term.err)
		}
	}
}

func (s *Stream) cmplLoop() {
	for {
		if cmpl, ok := <-s.cmplCh; ok {
			obj := &cmpl.obj
			cmn.Assert(!obj.hdr.IsLast()) // remove
			s.objDone(obj, cmpl.err)
		} else {
			break
		}
	}
	s.wg.Done()
}

// refcount, invoke Sendcallback, and *always* close the reader
func (s *Stream) objDone(obj *obj, err error) {
	var rc int64
	if obj.prc != nil {
		rc = atomic.AddInt64(obj.prc, -1)
		cmn.Assert(rc >= 0) // remove
	}
	// SCQ completion callback
	if rc == 0 {
		if obj.callback != nil {
			obj.callback(obj.hdr, obj.reader, err)
		} else if s.callback != nil {
			s.callback(obj.hdr, obj.reader, err)
		}
	}
	if obj.reader != nil {
		obj.reader.Close() // NOTE: always closing
	}
}

func (s *Stream) isNextReq(ctx context.Context) (next bool) {
	beg := time.Now()
	defer s.addIdle(beg)
	for {
		select {
		case <-ctx.Done():
			glog.Infof("%s: %v", s, ctx.Err())
			*s.term.reason = reasonCanceled
			return
		case <-s.lastCh.Listen():
			if glog.FastV(4, glog.SmoduleTransport) {
				glog.Infof("%s: end-of-stream", s)
			}
			*s.term.reason = endOfStream
			return
		case <-s.stopCh.Listen():
			glog.Infof("%s: stopped", s)
			*s.term.reason = reasonStopped
			return
		case <-s.postCh:
			atomic.StoreInt64(&s.sessST, active)
			next = true // initiate new HTTP/TCP session
			if glog.FastV(4, glog.SmoduleTransport) {
				glog.Infof("%s: active <- posted", s)
			}
			return
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
	if ctx != background {
		request = request.WithContext(ctx)
	}
	s.Numcur, s.Sizecur = 0, 0
	if glog.FastV(4, glog.SmoduleTransport) {
		glog.Infof("%s: Do", s)
	}
	response, err = s.client.Do(request)
	if err == nil {
		if glog.FastV(4, glog.SmoduleTransport) {
			glog.Infof("%s: Done", s)
		}
	} else {
		glog.Errorf("%s: Error [%v]", s, err)
		return
	}
	ioutil.ReadAll(response.Body)
	response.Body.Close()
	return
}

// as io.Reader
func (s *Stream) Read(b []byte) (n int, err error) {
	obj := &s.sendoff.obj
	if obj.reader != nil { // have object
		if s.sendoff.dod != 0 { // fast path
			if !obj.hdr.IsHeaderOnly() {
				return s.sendData(b)
			}
			if !obj.hdr.IsLast() {
				s.eoObj(nil)
			} else {
				err = io.EOF
				return
			}
		} else {
			return s.sendHdr(b)
		}
	}
repeat:
	select { // ignoring idle time spent here to optimize-out addIdle(time.Now()) overhead
	case s.sendoff.obj = <-s.workCh: // next object OR idle tick
		if s.sendoff.obj.hdr.IsIdleTick() {
			if len(s.workCh) > 0 {
				goto repeat
			}
			return s.deactivate()
		}
		l := s.insHeader(s.sendoff.obj.hdr)
		s.header = s.maxheader[:l]
		return s.sendHdr(b)
	case <-s.stopCh.Listen():
		num := atomic.LoadInt64(&s.stats.Num)
		glog.Infof("%s: stopped (%d/%d)", s, s.Numcur, num)
		err = io.EOF
		return
	}
}

func (s *Stream) deactivate() (n int, err error) {
	err = io.EOF
	if glog.FastV(4, glog.SmoduleTransport) {
		num := atomic.LoadInt64(&s.stats.Num)
		glog.Infof("%s: connection teardown (%d/%d)", s, s.Numcur, num)
	}
	return
}

func (s *Stream) sendHdr(b []byte) (n int, err error) {
	n = copy(b, s.header[s.sendoff.off:])
	s.sendoff.off += int64(n)
	if s.sendoff.off >= int64(len(s.header)) {
		cmn.Assert(s.sendoff.off == int64(len(s.header)))
		atomic.AddInt64(&s.stats.Offset, s.sendoff.off)
		if glog.FastV(4, glog.SmoduleTransport) {
			num := atomic.LoadInt64(&s.stats.Num)
			glog.Infof("%s: hlen=%d (%d/%d)", s, s.sendoff.off, s.Numcur, num)
		}
		s.sendoff.dod = s.sendoff.off
		s.sendoff.off = 0
		if s.sendoff.obj.hdr.IsLast() {
			if glog.FastV(4, glog.SmoduleTransport) {
				glog.Infof("%s: sent last", s)
			}
			err = io.EOF
			s.lastCh.Close()
		}
	} else if glog.FastV(4, glog.SmoduleTransport) {
		glog.Infof("%s: split header: copied %d < %d hlen", s, s.sendoff.off, len(s.header))
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
	} else if s.sendoff.off >= s.sendoff.obj.hdr.ObjAttrs.Size {
		s.eoObj(err)
	}
	return
}

//
// end-of-object: updates stats, reset idle timeout, and post completion
// NOTE: reader.Close() is done by the completion handling code objDone
//
func (s *Stream) eoObj(err error) {
	obj := &s.sendoff.obj

	s.Sizecur += s.sendoff.off
	atomic.AddInt64(&s.stats.Offset, s.sendoff.off)
	atomic.AddInt64(&s.stats.Size, s.sendoff.off)

	if err != nil {
		goto exit
	}
	if s.sendoff.off != obj.hdr.ObjAttrs.Size {
		err = fmt.Errorf("%s: obj %s/%s offset %d != %d size",
			s, s.sendoff.obj.hdr.Bucket, s.sendoff.obj.hdr.Objname, s.sendoff.off, obj.hdr.ObjAttrs.Size)
		goto exit
	}
	s.Numcur++
	atomic.AddInt64(&s.stats.Num, 1)

	if glog.FastV(4, glog.SmoduleTransport) {
		glog.Infof("%s: sent size=%d (%d/%d): %s", s, obj.hdr.ObjAttrs.Size, s.Numcur, s.stats.Num, obj.hdr.Objname)
	}
exit:
	if err != nil {
		glog.Errorln(err)
	}

	// next completion => SCQ
	s.cmplCh <- cmpl{s.sendoff.obj, err}

	s.sendoff = sendoff{}
}

//
// stream helpers
//
func (s *Stream) insHeader(hdr Header) (l int) {
	l = sizeofI64 * 2
	l = insString(l, s.maxheader, hdr.Bucket)
	l = insString(l, s.maxheader, hdr.Objname)
	l = insBool(l, s.maxheader, hdr.IsLocal)
	l = insByte(l, s.maxheader, hdr.Opaque)
	l = insAttrs(l, s.maxheader, hdr.ObjAttrs)
	l = insInt64(l, s.maxheader, s.sessID)
	hlen := l - sizeofI64*2
	insInt64(0, s.maxheader, int64(hlen))
	checksum := xoshiro256.Hash(uint64(hlen))
	insUint64(sizeofI64, s.maxheader, checksum)
	return
}

func insString(off int, to []byte, str string) int {
	return insByte(off, to, []byte(str))
}

func insBool(off int, to []byte, b bool) int {
	bt := byte(0)
	if b {
		bt = byte(1)
	}
	return insByte(off, to, []byte{bt})
}

func insByte(off int, to []byte, b []byte) int {
	var l = len(b)
	binary.BigEndian.PutUint64(to[off:], uint64(l))
	off += sizeofI64
	n := copy(to[off:], b)
	cmn.Assert(n == l)
	return off + l
}

func insInt64(off int, to []byte, i int64) int {
	return insUint64(off, to, uint64(i))
}

func insUint64(off int, to []byte, i uint64) int {
	binary.BigEndian.PutUint64(to[off:], i)
	return off + sizeofI64
}

func insAttrs(off int, to []byte, attr ObjectAttrs) int {
	off = insInt64(off, to, attr.Size)
	off = insInt64(off, to, attr.Atime)
	off = insString(off, to, attr.CksumType)
	off = insString(off, to, attr.CksumValue)
	off = insString(off, to, attr.Version)
	return off
}

// addIdle
func (s *Stream) addIdle(beg time.Time) { atomic.AddInt64(&s.stats.IdleDur, int64(time.Since(beg))) }

//
// dry-run ---------------------------
//
func (s *Stream) dryrun() {
	buf := make([]byte, cmn.KiB*32)
	scloser := ioutil.NopCloser(s)
	it := iterator{trname: s.trname, body: scloser, headerBuf: make([]byte, maxHeaderSize)}
	for {
		objReader, _, _, err := it.next()
		if objReader != nil {
			written, _ := io.CopyBuffer(ioutil.Discard, objReader, buf)
			cmn.Assert(written == objReader.hdr.ObjAttrs.Size)
			continue
		}
		if err != nil {
			break
		}
	}
}

//
// nopReadCloser ---------------------------
//
func (r *nopReadCloser) Read([]byte) (n int, err error) { return }
func (r *nopReadCloser) Close() error                   { return nil }
