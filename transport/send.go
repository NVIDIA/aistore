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

// private types
type (
	streamable interface {
		obj() *Obj
		msg() *Msg
	}
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
	nopReadCloser struct{}
)

var (
	nopRC   = &nopReadCloser{}      // read() and close() stubs
	nextSID = *atomic.NewInt64(100) // unique session IDs starting from 101
	sc      = &StreamCollector{}    // idle timer and house-keeping (slow path)
	gc      *collector              // real stream collector
)

// interface guard
var (
	_ streamable = &Obj{}
	_ streamable = &Msg{}
)

/////////////////////////////
// Stream: private methods //
/////////////////////////////

func (s *Stream) startSend(streamable fmt.Stringer, verbose bool) (err error) {
	s.time.inSend.Store(true) // StreamCollector to postpone cleanups
	if s.Terminated() {
		err = fmt.Errorf("%s terminated(%s, %v), dropping %s", s, *s.term.reason, s.term.err, streamable)
		glog.Error(err)
		return
	}
	if s.sessST.CAS(inactive, active) {
		s.postCh <- struct{}{}
		if verbose {
			glog.Infof("%s: inactive => active", s)
		}
	}
	return
}

func (s *Stream) terminate() {
	s.term.mu.Lock()
	cmn.Assert(!s.term.terminated)
	s.term.terminated = true

	s.Stop()

	obj := Obj{Hdr: ObjHdr{ObjAttrs: ObjectAttrs{Size: lastMarker}}}
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

func (s *Stream) initCompression(extra *Extra) {
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

		// second, handle the last interrupted send
		if s.inObj() {
			obj := &s.sendoff.obj
			s.objDone(obj, s.term.err)
		}
		// finally, handle pending SQ
		for streamable := range s.workCh {
			obj := streamable.obj() // TODO -- FIXME
			s.objDone(obj, s.term.err)
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
	if s.inObj() {
		obj := &s.sendoff.obj
		if s.sendoff.dod != 0 {
			if !obj.IsHeaderOnly() {
				return s.sendData(b)
			}
			if !obj.IsLast() {
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
	select {
	case streamable, ok := <-s.workCh: // next object OR idle tick
		if !ok {
			err = fmt.Errorf("%s closed prior to stopping", s)
			debug.Infof("%v", err)
			return
		}
		s.sendoff.obj = *streamable.obj() // TODO -- FIXME
		if s.sendoff.obj.IsIdleTick() {
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

// end-of-object updates stats, reset idle timeout, and post completion
// NOTE: reader.Close() is done by the completion handling code objDone
func (s *Stream) eoObj(err error) {
	obj := &s.sendoff.obj
	size := obj.Hdr.ObjAttrs.Size
	s.Sizecur += s.sendoff.off
	s.stats.Offset.Add(s.sendoff.off)
	if err != nil {
		goto exit
	}
	if s.sendoff.off != size {
		err = fmt.Errorf("%s: %s offset %d != size", s, obj, s.sendoff.off)
		goto exit
	}
	s.stats.Size.Add(size)
	s.Numcur++
	s.stats.Num.Inc()
	if glog.FastV(4, glog.SmoduleTransport) {
		glog.Infof("%s: sent %s (%d/%d)", s, obj, s.Numcur, s.stats.Num.Load())
	}
exit:
	if err != nil {
		glog.Errorln(err)
	}

	// next completion => SCQ
	s.cmplCh <- cmpl{s.sendoff.obj, err}
	s.sendoff = sendoff{}
}

func (s *Stream) inObj() bool { return s.sendoff.obj.Reader != nil } // see also: eoObj()

////////////////////
// Obj and ObjHdr //
////////////////////

func (obj Obj) obj() *Obj           { return &obj }
func (obj Obj) msg() *Msg           { return nil }
func (obj *Obj) IsLast() bool       { return obj.Hdr.IsLast() }
func (obj *Obj) IsIdleTick() bool   { return obj.Hdr.ObjAttrs.Size == tickMarker }
func (obj *Obj) IsHeaderOnly() bool { return obj.Hdr.ObjAttrs.Size == 0 || obj.Hdr.IsLast() }
func (obj *Obj) String() string {
	s := fmt.Sprintf("sobj-%s/%s", obj.Hdr.Bck, obj.Hdr.ObjName)
	if obj.IsHeaderOnly() {
		return s
	}
	return fmt.Sprintf("%s(size=%d)", s, obj.Hdr.ObjAttrs.Size)
}

func (obj *Obj) SetPrc(n int) {
	// when there's a `sent` callback and more than one destination
	if n > 1 {
		obj.prc = atomic.NewInt64(int64(n))
	}
}

func (hdr *ObjHdr) IsLast() bool { return hdr.ObjAttrs.Size == lastMarker }

func (hdr *ObjHdr) FromHdrProvider(meta cmn.ObjHeaderMetaProvider, objName string, bck cmn.Bck, opaque []byte) {
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

////////////////////
// Msg and MsgHdr //
////////////////////

func (msg Msg) obj() *Obj           { return nil }
func (msg Msg) msg() *Msg           { return &msg }
func (msg *Msg) IsLast() bool       { return msg.Flags == lastMarker }
func (msg *Msg) IsIdleTick() bool   { return msg.Flags == tickMarker }
func (msg *Msg) IsHeaderOnly() bool { return true }

func (msg *Msg) String() string { return "smsg-" + msg.RecvHandler }

//////////////////////////
// header serialization //
//////////////////////////

func (s *Stream) insHeader(hdr ObjHdr) (l int) {
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
