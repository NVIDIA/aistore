// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"runtime"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xoshiro256"
	"github.com/pierrec/lz4/v3"
)

// transport defaults
const (
	maxHeaderSize  = 1024
	lastMarker     = math.MaxInt64
	tickMarker     = math.MaxInt64 ^ 0xa5a5a5a5
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
	reasonUnknown = "unknown"
	reasonError   = "error"
	endOfStream   = "end-of-stream"
	reasonStopped = "stopped"
)

// API types
type (
	Stream struct {
		workCh   chan Obj     // aka SQ: next object to stream
		cmplCh   chan cmpl    // aka SCQ; note that SQ and SCQ together form a FIFO
		callback SendCallback // to free SGLs, close files, etc.
		sendoff  sendoff
		lz4s     lz4Stream
		streamBase
	}
	// advanced usage: additional stream control
	Extra struct {
		IdleTimeout time.Duration // stream idle timeout: causes PUT to terminate (and renew on the next obj send)
		Callback    SendCallback  // typical usage: to free SGLs, close files, etc.
		Compression string        // see CompressAlways, etc. enum
		MMSA        *memsys.MMSA  // compression-related buffering
		Config      *cmn.Config
	}
	// stream stats
	Stats struct {
		Num            atomic.Int64 // number of transferred objects including zero size (header-only) objects
		Size           atomic.Int64 // transferred object size (does not include transport headers)
		Offset         atomic.Int64 // stream offset, in bytes
		CompressedSize atomic.Int64 // compressed size (NOTE: converges to the actual compressed size over time)
	}
	EndpointStats map[uint64]*Stats // all stats for a given http endpoint defined by a tuple(network, trname) by session ID

	// object attrs
	ObjectAttrs struct {
		Atime      int64  // access time - nanoseconds since UNIX epoch
		Size       int64  // size of objects in bytes
		CksumType  string // checksum type
		CksumValue string // checksum of the object produced by given checksum type
		Version    string // version of the object
	}
	// object header
	Header struct {
		Bck      cmn.Bck
		ObjName  string
		ObjAttrs ObjectAttrs // attributes/metadata of the sent object
		Opaque   []byte      // custom control (optional)
	}
	// object to transmit
	Obj struct {
		Hdr      Header         // object header
		Reader   io.ReadCloser  // reader, to read the object, and close when done
		Callback SendCallback   // callback fired when sending is done OR when the stream terminates (see term.reason)
		CmplPtr  unsafe.Pointer // local pointer that gets returned to the caller via Send completion callback
		// private
		prc *atomic.Int64 // if present, ref-counts num sent objects to call SendCallback only once
	}

	// object-sent callback that has the following signature can optionally be defined on a:
	// a) per-stream basis (via NewStream constructor - see Extra struct above)
	// b) for a given object that is being sent (for instance, to support a call-per-batch semantics)
	// Naturally, object callback "overrides" the per-stream one: when object callback is defined
	// (i.e., non-nil), the stream callback is ignored/skipped.
	// NOTE: if defined, the callback executes asynchronously as far as the sending part is concerned
	SendCallback func(Header, io.ReadCloser, unsafe.Pointer, error)

	StreamCollector struct {
		cmn.Named
	}
)

// internal types
type (
	lz4Stream struct {
		s             *Stream
		zw            *lz4.Writer // orig reader => zw
		sgl           *memsys.SGL // zw => bb => network
		blockMaxSize  int         // *uncompressed* block max size
		frameChecksum bool        // true: checksum lz4 frames
	}
	sendoff struct {
		obj Obj
		// in progress
		off int64
		dod int64
	}
	cmpl struct { // send completions => SCQ
		obj Obj
		err error
	}
	nopReadCloser struct{}

	collector struct {
		streams map[string]*Stream
		heap    []*Stream
		ticker  *time.Ticker
		stopCh  *cmn.StopCh
		ctrlCh  chan ctrl
	}
	ctrl struct { // add/del channel to/from collector
		s   *Stream
		add bool
	}
)

var (
	nopRC   = &nopReadCloser{}      // read and close stubs
	nextSID = *atomic.NewInt64(100) // unique session IDs starting from 101
	sc      = &StreamCollector{}    // idle timer and house-keeping (slow path)
	gc      *collector              // real stream collector
)

////////////////////////////
// Stream: public methods //
////////////////////////////

func NewStream(client Client, toURL string, extra *Extra) (s *Stream) {
	s = &Stream{streamBase: *newStreamBase(client, toURL, extra)}

	if extra != nil {
		s.callback = extra.Callback
		if extra.Compressed() {
			config := extra.Config
			if config == nil {
				config = cmn.GCO.Get()
			}
			s.lz4s.s = s
			s.lz4s.blockMaxSize = config.Compression.BlockMaxSize
			s.lz4s.frameChecksum = config.Compression.Checksum
			mem := extra.MMSA
			if mem == nil {
				mem = memsys.DefaultPageMM()
				glog.Warningln("Using global memory manager for streaming inline compression")
			}
			if s.lz4s.blockMaxSize >= memsys.MaxPageSlabSize {
				s.lz4s.sgl = mem.NewSGL(memsys.MaxPageSlabSize, memsys.MaxPageSlabSize)
			} else {
				s.lz4s.sgl = mem.NewSGL(cmn.KiB*64, cmn.KiB*64)
			}

			s.lid = fmt.Sprintf("%s[%d[%s]]", s.trname, s.sessID, cmn.B2S(int64(s.lz4s.blockMaxSize), 0))
		}
	}

	// burst size: the number of objects the caller is permitted to post for sending
	// without experiencing any sort of back-pressure
	burst := burst()
	s.workCh = make(chan Obj, burst)  // Send Qeueue or SQ
	s.cmplCh = make(chan cmpl, burst) // Send Completion Queue or SCQ

	s.wg.Add(2)
	go s.sendLoop(dryrun()) // handle SQ
	go s.cmplLoop()         // handle SCQ

	gc.ctrlCh <- ctrl{s, true /* collect */}
	return
}

// Asynchronously send an object (transport.Obj) defined by its header and its reader.
//
// The sending pipeline is implemented as a pair (SQ, SCQ) where the former is a send
// queue realized as workCh, and the latter is a send completion queue (cmplCh).
// Together SQ and SCQ form a FIFO.
//
// * header-only objects are supported; when there's no data to send (that is,
//   when the header's Dsize field is set to zero), the reader is not required and the
//   corresponding argument in Send() can be set to nil.
// * object reader is always closed by the code that handles send completions.
//   In the case when SendCallback is provided (i.e., non-nil), the closing is done
//   right after calling this callback - see objDone below for details.
// * Optional reference counting is also done by (and in) the objDone, so that the
//   SendCallback gets called if and only when the refcount (if provided i.e., non-nil)
//   reaches zero.
// * For every transmission of every object there's always an objDone() completion
//   (with its refcounting and reader-closing). This holds true in all cases including
//   network errors that may cause sudden and instant termination of the underlying
//   stream(s).
func (s *Stream) Send(obj Obj) (err error) {
	s.time.inSend.Store(true) // an indication for Collector to postpone cleanup
	hdr := &obj.Hdr
	if s.Terminated() {
		err = fmt.Errorf("%s terminated(%s, %v), cannot send [%s/%s(%d)]",
			s, *s.term.reason, s.term.err, hdr.Bck, hdr.ObjName, hdr.ObjAttrs.Size)
		glog.Errorln(err)
		return
	}
	if s.sessST.CAS(inactive, active) {
		s.postCh <- struct{}{}
		if glog.FastV(4, glog.SmoduleTransport) {
			glog.Infof("%s: inactive => active", s)
		}
	}
	// next object => SQ
	if obj.Reader == nil {
		cmn.Assert(hdr.IsHeaderOnly())
		obj.Reader = nopRC
	} else if debug.Enabled && hdr.IsHeaderOnly() {
		b, _ := ioutil.ReadAll(obj.Reader)
		debug.Assert(len(b) == 0)
	}
	s.workCh <- obj
	if glog.FastV(4, glog.SmoduleTransport) {
		glog.Infof("%s: send %s/%s(%d)[sq=%d]", s, hdr.Bck, hdr.ObjName, hdr.ObjAttrs.Size, len(s.workCh))
	}
	return
}

func (s *Stream) Fin() {
	_ = s.Send(Obj{Hdr: Header{ObjAttrs: ObjectAttrs{Size: lastMarker}}})
	s.wg.Wait()
}

func (s *Stream) terminate() {
	s.term.mu.Lock()
	cmn.Assert(!s.term.terminated)
	s.term.terminated = true

	s.Stop()

	hdr := Header{ObjAttrs: ObjectAttrs{Size: lastMarker}}
	obj := Obj{Hdr: hdr}
	s.cmplCh <- cmpl{obj, s.term.err}
	s.term.mu.Unlock()

	// Remove stream after lock because we could deadlock between `do()`
	// (which checks for `Terminated` status) and this function which
	// would be under lock.
	gc.remove(s)

	if s.compressed() {
		s.lz4s.sgl.Free()
		if s.lz4s.zw != nil {
			s.lz4s.zw.Reset(nil)
		}
	}
}

//
// internal methods including sending and receiving-completions logic, each running in its own goroutine
//

func (s *Stream) compressed() bool { return s.lz4s.s == s }

func (s *Stream) sendLoop(dryrun bool) {
	for {
		if s.sessST.Load() == active {
			if dryrun {
				s.dryrun()
			} else if err := s.doRequest(); err != nil {
				*s.term.reason = reasonError
				s.term.err = err
				break
			}
		}
		if !s.isNextReq() {
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
		if s.sendoff.obj.Reader != nil {
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
		cmpl, ok := <-s.cmplCh
		obj := &cmpl.obj
		if !ok || obj.Hdr.IsLast() {
			break
		}
		s.objDone(&cmpl.obj, cmpl.err)
	}
	s.wg.Done()
}

// refcount, invoke Sendcallback, and *always* close the reader
func (s *Stream) objDone(obj *Obj, err error) {
	var rc int64
	if obj.prc != nil {
		rc = obj.prc.Dec()
		debug.Assert(rc >= 0)
	}
	// SCQ completion callback
	if rc == 0 {
		if obj.Callback != nil {
			obj.Callback(obj.Hdr, obj.Reader, obj.CmplPtr, err)
		} else if s.callback != nil {
			s.callback(obj.Hdr, obj.Reader, obj.CmplPtr, err)
		}
	}
	if obj.Reader != nil {
		cmn.Close(obj.Reader) // NOTE: always closing
	}
}

func (s *Stream) doRequest() (err error) {
	var body io.Reader = s
	s.Numcur, s.Sizecur = 0, 0
	if s.compressed() {
		s.lz4s.sgl.Reset()
		if s.lz4s.zw == nil {
			s.lz4s.zw = lz4.NewWriter(s.lz4s.sgl)
		} else {
			s.lz4s.zw.Reset(s.lz4s.sgl)
		}
		// lz4 framing spec at http://fastcompression.blogspot.com/2013/04/lz4-streaming-format-final.html
		s.lz4s.zw.Header.BlockChecksum = false
		s.lz4s.zw.Header.NoChecksum = !s.lz4s.frameChecksum
		s.lz4s.zw.Header.BlockMaxSize = s.lz4s.blockMaxSize
		body = &s.lz4s
	}
	return s.do(body)
}

// as io.Reader
func (s *Stream) Read(b []byte) (n int, err error) {
	s.time.inSend.Store(true) // indication for Collector to delay cleanup
	obj := &s.sendoff.obj
	if obj.Reader != nil { // have object - fast path
		if s.sendoff.dod != 0 {
			if !obj.Hdr.IsHeaderOnly() {
				return s.sendData(b)
			}
			if !obj.Hdr.IsLast() {
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
	var ok bool
	select {
	case s.sendoff.obj, ok = <-s.workCh: // next object OR idle tick
		if !ok {
			err = fmt.Errorf("%s closed prior to stopping", s)
			debug.Infof("%v", err)
			return
		}
		if s.sendoff.obj.Hdr.IsIdleTick() {
			if len(s.workCh) > 0 {
				goto repeat
			}
			return s.deactivate()
		}
		l := s.insHeader(s.sendoff.obj.Hdr)
		s.header = s.maxheader[:l]
		return s.sendHdr(b)
	case <-s.stopCh.Listen():
		num := s.stats.Num.Load()
		glog.Infof("%s: stopped (%d/%d)", s, s.Numcur, num)
		err = io.EOF
		return
	}
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
		if s.sendoff.obj.Hdr.IsLast() {
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
	var (
		obj     = &s.sendoff.obj
		objSize = obj.Hdr.ObjAttrs.Size
	)
	n, err = obj.Reader.Read(b)
	s.sendoff.off += int64(n)
	if err != nil {
		if err == io.EOF {
			if s.sendoff.off < objSize {
				return n, fmt.Errorf("%s: read (%d) shorter than expected (%d)", s, s.sendoff.off, objSize)
			}
			err = nil
		}
		s.eoObj(err)
	} else if s.sendoff.off >= objSize {
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
	if err != nil {
		goto exit
	}
	if s.sendoff.off != obj.Hdr.ObjAttrs.Size {
		err = fmt.Errorf("%s: obj %s/%s offset %d != %d size",
			s, s.sendoff.obj.Hdr.Bck, s.sendoff.obj.Hdr.ObjName, s.sendoff.off, obj.Hdr.ObjAttrs.Size)
		goto exit
	}
	s.stats.Size.Add(obj.Hdr.ObjAttrs.Size)
	s.Numcur++
	s.stats.Num.Inc()
	if glog.FastV(4, glog.SmoduleTransport) {
		glog.Infof("%s: sent size=%d (%d/%d): %s", s, obj.Hdr.ObjAttrs.Size, s.Numcur, s.stats.Num.Load(), obj.Hdr.ObjName)
	}
exit:
	if err != nil {
		glog.Errorln(err)
	}

	// next completion => SCQ
	s.cmplCh <- cmpl{s.sendoff.obj, err}
	s.sendoff = sendoff{}
}

////////////////////
// Obj and Header //
////////////////////

func (obj *Obj) SetPrc(n int) {
	// when there's a `sent` callback and more than one destination
	if n > 1 {
		obj.prc = atomic.NewInt64(int64(n))
	}
}

func (hdr *Header) IsLast() bool       { return hdr.ObjAttrs.Size == lastMarker }
func (hdr *Header) IsIdleTick() bool   { return hdr.ObjAttrs.Size == tickMarker }
func (hdr *Header) IsHeaderOnly() bool { return hdr.ObjAttrs.Size == 0 || hdr.IsLast() }

func (hdr *Header) FromHdrProvider(meta cmn.ObjHeaderMetaProvider, objName string, bck cmn.Bck, opaque []byte) {
	hdr.Bck = bck
	hdr.ObjName = objName
	hdr.Opaque = opaque
	hdr.ObjAttrs.Size = meta.Size()
	hdr.ObjAttrs.Atime = meta.AtimeUnix()
	if meta.Cksum() != nil {
		hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue = meta.Cksum().Get()
	}
	hdr.ObjAttrs.Version = meta.Version()
}

//////////////////////////
// header serialization //
//////////////////////////

func (s *Stream) insHeader(hdr Header) (l int) {
	l = cmn.SizeofI64 * 2
	l = insString(l, s.maxheader, hdr.Bck.Name)
	l = insString(l, s.maxheader, hdr.ObjName)
	l = insString(l, s.maxheader, hdr.Bck.Provider)
	l = insString(l, s.maxheader, hdr.Bck.Ns.Name)
	l = insString(l, s.maxheader, hdr.Bck.Ns.UUID)
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

func insByte(off int, to, b []byte) int {
	l := len(b)
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

/////////////
// dry-run //
/////////////

func (s *Stream) dryrun() {
	buf := make([]byte, memsys.DefaultBufSize)
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

///////////
// Stats //
///////////

func (stats *Stats) CompressionRatio() float64 {
	bytesRead := stats.Offset.Load()
	bytesSent := stats.CompressedSize.Load()
	return float64(bytesRead) / float64(bytesSent)
}

///////////////////
// nopReadCloser //
///////////////////

func (r *nopReadCloser) Read([]byte) (n int, err error) { return }
func (r *nopReadCloser) Close() error                   { return nil }

///////////////
// lz4Stream //
///////////////

func (lz4s *lz4Stream) Read(b []byte) (n int, err error) {
	var (
		sendoff = &lz4s.s.sendoff
		last    = sendoff.obj.Hdr.IsLast()
		retry   = 64 // insist on returning n > 0 (note that lz4 compresses /blocks/)
	)
	if lz4s.sgl.Len() > 0 {
		lz4s.zw.Flush()
		n, err = lz4s.sgl.Read(b)
		if err == io.EOF { // reusing/rewinding this buf multiple times
			err = nil
		}
		goto ex
	}
re:
	n, err = lz4s.s.Read(b)
	_, _ = lz4s.zw.Write(b[:n])
	if last {
		lz4s.zw.Flush()
		retry = 0
	} else if lz4s.s.sendoff.obj.Reader == nil /*eoObj*/ || err != nil {
		lz4s.zw.Flush()
		retry = 0
	}
	n, _ = lz4s.sgl.Read(b)
	if n == 0 {
		if retry > 0 {
			retry--
			runtime.Gosched()
			goto re
		}
		lz4s.zw.Flush()
		n, _ = lz4s.sgl.Read(b)
	}
ex:
	lz4s.s.stats.CompressedSize.Add(int64(n))
	if lz4s.sgl.Len() == 0 {
		lz4s.sgl.Reset()
	}
	if last && err == nil {
		err = io.EOF
	}
	return
}

///////////
// Extra //
///////////

func (extra *Extra) Compressed() bool {
	return extra.Compression != "" && extra.Compression != cmn.CompressNever
}
