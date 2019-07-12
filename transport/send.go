// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
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
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xoshiro256"
	"github.com/pierrec/lz4"
)

// transport defaults
const (
	maxHeaderSize  = 1024
	lastMarker     = cmn.MaxInt64
	tickMarker     = cmn.MaxInt64 ^ 0xa5a5a5a5
	tickUnit       = time.Second
	defaultIdleOut = time.Second * 2
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
		sessST          atomic.Int64 // state of the TCP/HTTP session: active (connected) | inactive (disconnected)
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
			start   atomic.Int64  // to support idle(%)
			idleOut time.Duration // idle timeout
			posted  atomic.Int64  // num posted since the last GC do()
			ticks   int           // num 1s ticks until idle timeout
			index   int           // heap stuff
		}
		wg        sync.WaitGroup
		sendoff   sendoff
		maxheader []byte // max header buffer
		header    []byte // object header - slice of the maxheader with bucket/objname, etc. fields
		term      struct {
			barr   atomic.Int64
			err    error
			reason *string
		}
		lz4s lz4Stream
	}
	// advanced usage: additional stream control
	Extra struct {
		IdleTimeout time.Duration   // stream idle timeout: causes PUT to terminate (and renew on the next obj send)
		Ctx         context.Context // presumably, result of context.WithCancel(context.Background()) by the caller
		Callback    SendCallback    // typical usage: to free SGLs, close files, etc.
		Mem2        *memsys.Mem2    // compression-related buffering
		Burst       int             // SQ and CSQ buffer sizes: max num objects and send-completions
		DryRun      bool            // dry run: short-circuit the stream on the send side
		Compress    bool            // lz4
	}
	// stream stats
	Stats struct {
		Num            atomic.Int64 // number of transferred objects
		Size           atomic.Int64 // transferred size, in bytes
		CompressedSize atomic.Int64 // compressed size
		Offset         atomic.Int64 // stream offset, in bytes
		IdleDur        atomic.Int64 // the time stream was idle since the previous getStats call
		TotlDur        int64        // total time since --/---/---
		IdlePct        float64      // idle time % since --/---/--
	}
	EndpointStats map[uint64]*Stats // all stats for a given http endpoint defined by a tuple (network, trname) by session ID

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
		ObjAttrs        ObjectAttrs // attributes/metadata of the sent object
		Opaque          []byte      // custom control (optional)
		IsLocal         bool        // bucket is local?
	}
	// object-sent callback that has the following signature can optionally be defined on a:
	// a) per-stream basis (via NewStream constructor - see Extra struct above)
	// b) for a given object that is being sent (for instance, to support a call-per-batch semantics)
	// Naturally, object callback "overrides" the per-stream one: when object callback is defined
	// (i.e., non-nil), the stream callback is ignored/skipped.
	//
	// NOTE: if defined, the callback executes asynchronously as far as the sending part is concerned
	SendCallback func(Header, io.ReadCloser, unsafe.Pointer, error)

	StreamCollector struct {
		cmn.Named
	}
)

// internal
type (
	lz4Stream struct {
		s    *Stream
		zw   *lz4.Writer // orig reader => zw
		sgl  *memsys.SGL // zw => bb => network
		size int64
	}
	obj struct {
		hdr      Header         // object header
		reader   io.ReadCloser  // reader, to read the object, and close when done
		callback SendCallback   // callback fired when sending is done OR when the stream terminates (see term.reason)
		cmplPtr  unsafe.Pointer // local pointer that gets returned to the caller via Send completion callback
		prc      *atomic.Int64  // optional refcount; if present, SendCallback gets called if and when *prc reaches zero
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
	nextSID    = *atomic.NewInt64(100) // unique session IDs starting from 101
	sc         = &StreamCollector{}
	gc         *collector // real collector
)

// default HTTP client to be used with streams
// resulting transport will have all defaults, dial timeout=30s, timeout=no-timeout
func NewDefaultClient() *http.Client {
	config := cmn.GCO.Get()
	return cmn.NewClient(cmn.TransportArgs{SndRcvBufSize: config.Net.L4.SndRcvBufSize})
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
		if extra.Compress {
			s.lz4s.s = s
			s.lz4s.sgl = extra.Mem2.NewSGL(memsys.MaxSlabSize, memsys.MaxSlabSize) // TODO -- FIXME
		}
	}
	if s.time.idleOut < tickUnit {
		s.time.idleOut = tickUnit
	}
	s.time.ticks = int(s.time.idleOut / tickUnit)
	s.sessID = nextSID.Inc()
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
	s.sessST.Store(inactive)                  // NOTE: initiate HTTP session upon arrival of the first object

	var ctx context.Context
	if extra != nil && extra.Ctx != nil {
		ctx = extra.Ctx
	} else {
		ctx = background
	}

	s.time.start.Store(time.Now().UnixNano())
	s.term.reason = new(string)

	s.wg.Add(2)
	go s.sendLoop(ctx, dryrun) // handle SQ
	go s.cmplLoop()            // handle SCQ

	gc.ctrlCh <- ctrl{s, true /* collect */}
	return
}

func (s *Stream) compressed() bool { return s.lz4s.s == s }

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
func (s *Stream) Send(hdr Header, reader io.ReadCloser, callback SendCallback, cmplPtr unsafe.Pointer, prc ...*atomic.Int64) (err error) {
	if s.Terminated() {
		err = fmt.Errorf("%s terminated(%s, %v), cannot send [%s/%s(%d)]",
			s, *s.term.reason, s.term.err, hdr.Bucket, hdr.Objname, hdr.ObjAttrs.Size)
		glog.Errorln(err)
		return
	}
	s.time.posted.Inc()
	if s.sessST.CAS(inactive, active) {
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
	obj := obj{hdr: hdr, reader: reader, callback: callback, cmplPtr: cmplPtr}
	if len(prc) > 0 {
		obj.prc = prc[0]
	}
	s.workCh <- obj
	if glog.FastV(4, glog.SmoduleTransport) {
		glog.Infof("%s: send %s/%s(%d)[sq=%d]", s, hdr.Bucket, hdr.Objname, hdr.ObjAttrs.Size, len(s.workCh))
	}
	return
}

func (s *Stream) Fin() {
	hdr := Header{ObjAttrs: ObjectAttrs{Size: lastMarker}}
	_ = s.Send(hdr, nil, nil, nil)
	s.wg.Wait()
}
func (s *Stream) Stop()               { s.stopCh.Close() }
func (s *Stream) URL() string         { return s.toURL }
func (s *Stream) ID() (string, int64) { return s.trname, s.sessID }
func (s *Stream) String() string      { return s.lid }
func (s *Stream) Terminated() bool    { return s.term.barr.Load() != 0 }
func (s *Stream) terminate() {
	if s.term.barr.Swap(0xDEADBEEF) == 0xDEADBEEF {
		glog.Warningf("%s: already terminated (%s, %v)", s, *s.term.reason, s.term.err)
	} else {
		gc.remove(s)
		s.Stop()
		close(s.cmplCh)
	}
	if s.compressed() {
		s.lz4s.sgl.Free()
		if s.lz4s.zw != nil {
			s.lz4s.zw.Reset(nil)
		}
	}
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
	stats.Num.Store(s.stats.Num.Load())
	stats.Offset.Store(s.stats.Offset.Load())
	stats.Size.Store(s.stats.Size.Load())
	stats.CompressedSize.Store(s.stats.CompressedSize.Load())
	// idle(%)
	now := time.Now().UnixNano()
	stats.TotlDur = now - s.time.start.Load()
	stats.IdlePct = float64(s.stats.IdleDur.Load()) * 100 / float64(stats.TotlDur)
	stats.IdlePct = cmn.MinF64(100, stats.IdlePct) // getStats is async vis-à-vis IdleDur += deltas
	s.time.start.Store(now)
	s.stats.IdleDur.Store(0)
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
		if s.sessST.Load() == active {
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
		rc = obj.prc.Dec()
		cmn.Assert(rc >= 0) // remove
	}
	// SCQ completion callback
	if rc == 0 {
		if obj.callback != nil {
			obj.callback(obj.hdr, obj.reader, obj.cmplPtr, err)
		} else if s.callback != nil {
			s.callback(obj.hdr, obj.reader, obj.cmplPtr, err)
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
			s.sessST.Store(active)
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
		body     io.Reader = s
	)
	if s.compressed() {
		s.lz4s.sgl.Reset()
		if s.lz4s.zw == nil {
			s.lz4s.zw = lz4.NewWriter(s.lz4s.sgl)
		} else {
			s.lz4s.zw.Reset(s.lz4s.sgl)
		}
		body = &s.lz4s
	}
	if request, err = http.NewRequest(http.MethodPut, s.toURL, body); err != nil {
		return
	}
	if ctx != background {
		request = request.WithContext(ctx)
	}
	s.Numcur, s.Sizecur = 0, 0
	if glog.FastV(4, glog.SmoduleTransport) {
		glog.Infof("%s: Do", s)
	}
	if s.compressed() {
		request.Header.Set(cmn.HeaderCompress, cmn.LZ4Compression)
	}
	request.Header.Set(cmn.HeaderSessID, strconv.FormatInt(s.sessID, 10))
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
		num := s.stats.Num.Load()
		glog.Infof("%s: stopped (%d/%d)", s, s.Numcur, num)
		err = io.EOF
		return
	}
}

func (s *Stream) deactivate() (n int, err error) {
	err = io.EOF
	if glog.FastV(4, glog.SmoduleTransport) {
		num := s.stats.Num.Load()
		glog.Infof("%s: connection teardown (%d/%d)", s, s.Numcur, num)
	}
	return
}

func (s *Stream) sendHdr(b []byte) (n int, err error) {
	n = copy(b, s.header[s.sendoff.off:])
	s.sendoff.off += int64(n)
	if s.sendoff.off >= int64(len(s.header)) {
		cmn.Assert(s.sendoff.off == int64(len(s.header)))
		s.stats.Offset.Add(s.sendoff.off)
		if glog.FastV(4, glog.SmoduleTransport) {
			num := s.stats.Num.Load()
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
	s.stats.Offset.Add(s.sendoff.off)
	s.stats.Size.Add(s.sendoff.off)
	s.stats.CompressedSize.Add(s.lz4s.size)

	if err != nil {
		goto exit
	}
	if s.sendoff.off != obj.hdr.ObjAttrs.Size {
		err = fmt.Errorf("%s: obj %s/%s offset %d != %d size",
			s, s.sendoff.obj.hdr.Bucket, s.sendoff.obj.hdr.Objname, s.sendoff.off, obj.hdr.ObjAttrs.Size)
		goto exit
	}
	s.Numcur++
	s.stats.Num.Inc()

	if glog.FastV(4, glog.SmoduleTransport) {
		glog.Infof("%s: sent size=%d (%d/%d): %s", s, obj.hdr.ObjAttrs.Size, s.Numcur, s.stats.Num.Load(), obj.hdr.Objname)
	}
exit:
	if err != nil {
		glog.Errorln(err)
	}

	// next completion => SCQ
	s.cmplCh <- cmpl{s.sendoff.obj, err}

	s.sendoff = sendoff{}
	s.lz4s.size = 0
}

//
// stream helpers
//
func (s *Stream) insHeader(hdr Header) (l int) {
	l = cmn.SizeofI64 * 2
	l = insString(l, s.maxheader, hdr.Bucket)
	l = insString(l, s.maxheader, hdr.Objname)
	l = insBool(l, s.maxheader, hdr.IsLocal)
	l = insByte(l, s.maxheader, hdr.Opaque)
	l = insAttrs(l, s.maxheader, hdr.ObjAttrs)
	hlen := l - cmn.SizeofI64*2
	insInt64(0, s.maxheader, int64(hlen))
	checksum := xoshiro256.Hash(uint64(hlen))
	insUint64(cmn.SizeofI64, s.maxheader, checksum)
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
	off += cmn.SizeofI64
	n := copy(to[off:], b)
	cmn.Assert(n == l)
	return off + l
}

func insInt64(off int, to []byte, i int64) int {
	return insUint64(off, to, uint64(i))
}

func insUint64(off int, to []byte, i uint64) int {
	binary.BigEndian.PutUint64(to[off:], i)
	return off + cmn.SizeofI64
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
func (s *Stream) addIdle(beg time.Time) { s.stats.IdleDur.Add(int64(time.Since(beg))) }

//
// dry-run ---------------------------
//
func (s *Stream) dryrun() {
	buf := make([]byte, cmn.KiB*32)
	scloser := ioutil.NopCloser(s)
	it := iterator{trname: s.trname, body: scloser, headerBuf: make([]byte, maxHeaderSize)}
	for {
		objReader, _, err := it.next()
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

//
// lz4Stream ---------------------------
//

func (lz4s *lz4Stream) Read(b []byte) (n int, err error) {
	var (
		sendoff = &lz4s.s.sendoff
		last    = sendoff.obj.hdr.IsLast()
	)
	if lz4s.sgl.Len() > 0 {
		n, err = lz4s.sgl.Read(b)
		if err == io.EOF { // reusing/rewinding this buf multiple times
			err = nil
		}
		goto ex
	}
	n, err = lz4s.s.Read(b)
	_, _ = lz4s.zw.Write(b[:n])
	n, _ = lz4s.sgl.Read(b)
	lz4s.size += int64(n)

	// zw house-keep on object and end-stream boundaries -- TODO -- FIXME
	if sendoff.off >= sendoff.obj.hdr.ObjAttrs.Size {
		lz4s.zw.Flush()
	}
	if last {
		lz4s.zw.Close()
	}
ex:
	if lz4s.sgl.Len() == 0 {
		lz4s.sgl.Reset()
		if last && err == nil {
			err = io.EOF
		}
	} else if last && err == io.EOF {
		err = nil
	}
	return
}
