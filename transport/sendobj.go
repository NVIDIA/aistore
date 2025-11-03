// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"fmt"
	"io"
	"runtime"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/memsys"

	"github.com/pierrec/lz4/v4"
)

// object stream & private types
type (
	Stream struct {
		workCh  chan *Obj // send queue (SQ): next object to stream
		cmplCh  chan cmpl // SCQ (note: SQ and SCQ form a FIFO)
		sentCB  SentCB    // to free SGLs, close files, etc. cleanup
		lz4s    *lz4Stream
		sendoff sendoff
		streamBase
		chanFull cos.ChanFull
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
		off int64
		ins int // in-send enum
	}
	cmpl struct {
		err error
		obj Obj
	}
)

// interface guard
// (curerently, a single implementation but keeping it interfaced for possible futures)
var _ streamer = (*Stream)(nil)

///////////////////
// object stream //
///////////////////

func (s *Stream) terminate(err error, reason string) (actReason string, actErr error) {
	ok := s.term.done.CAS(false, true)
	debug.Assert(ok, s.String())

	s.term.mu.Lock()
	if s.term.err == nil {
		s.term.err = err
	}
	if s.term.reason == "" {
		s.term.reason = reason
	}
	s.Stop()
	err = s.term.err
	actReason, actErr = s.term.reason, s.term.err
	s.cmplCh <- cmpl{err, Obj{Hdr: ObjHdr{Opcode: opcFin}}}
	s.term.mu.Unlock()

	// Remove stream after lock because we could deadlock between `do()`
	// (which checks for `Terminated` status) and this function which
	// would be under lock.
	gc.remove(&s.streamBase)

	if s.compressed() {
		s.lz4s.sgl.Free()
		if s.lz4s.zw != nil {
			s.lz4s.zw.Reset(nil)
		}
	}
	return
}

func (s *Stream) initCompression(extra *Extra) {
	s.lz4s = &lz4Stream{}
	s.lz4s.s = s
	s.lz4s.blockMaxSize = int(extra.Config.Transport.LZ4BlockMaxSize)
	s.lz4s.frameChecksum = extra.Config.Transport.LZ4FrameChecksum
	if s.lz4s.blockMaxSize >= memsys.MaxPageSlabSize {
		s.lz4s.sgl = g.mm.NewSGL(memsys.MaxPageSlabSize, memsys.MaxPageSlabSize)
	} else {
		s.lz4s.sgl = g.mm.NewSGL(cos.KiB*64, cos.KiB*64)
	}
}

func (s *Stream) compressed() bool { return s.lz4s != nil }
func (s *Stream) usePDU() bool     { return s.pdu != nil }

func (s *Stream) resetCompression() {
	s.lz4s.sgl.Reset()
	s.lz4s.zw.Reset(nil)
}

func (s *Stream) cmplLoop() {
	for {
		cmpl, ok := <-s.cmplCh
		obj := &cmpl.obj
		if !ok || obj.Hdr.isFin() {
			break
		}
		s.doCmpl(&cmpl.obj, cmpl.err)
	}
	s.wg.Done()
}

// handle the last interrupted transmission and pending SQ/SCQ
func (s *Stream) abortPending(err error, completions bool) {
	for obj := range s.workCh {
		s.doCmpl(obj, err)
	}
	if completions {
		for cmpl := range s.cmplCh {
			if !cmpl.obj.Hdr.isFin() {
				s.doCmpl(&cmpl.obj, cmpl.err)
			}
		}
	}
}

// refcount to invoke the has-been-sent callback only once
// and *always* close the reader (sic!)
func (s *Stream) doCmpl(obj *Obj, err error) {
	var rc int64
	if obj.prc != nil {
		rc = obj.prc.Dec()
		debug.Assert(rc >= 0)
	}
	if obj.Reader != nil {
		if err != nil && cmn.IsFileAlreadyClosed(err) {
			nlog.Errorf("%s %s: %v", s, obj, err)
		} else {
			cos.Close(obj.Reader) // otherwise, always closing
		}
	}
	// SCQ completion callback
	if rc == 0 {
		if obj.SentCB != nil {
			obj.SentCB(&obj.Hdr, obj.Reader, obj.CmplArg, err)
		} else if s.sentCB != nil {
			s.sentCB(&obj.Hdr, obj.Reader, obj.CmplArg, err)
		}
	}
	freeSend(obj)
}

func (s *Stream) doRequest() error {
	s.numCur, s.sizeCur = 0, 0
	if !s.compressed() {
		return s.doPlain(s)
	}
	s.lz4s.sgl.Reset()
	if s.lz4s.zw == nil {
		s.lz4s.zw = lz4.NewWriter(s.lz4s.sgl)
	} else {
		s.lz4s.zw.Reset(s.lz4s.sgl)
	}

	err := s.lz4s.zw.Apply(
		lz4.BlockChecksumOption(false),
		lz4.ChecksumOption(s.lz4s.frameChecksum),
		lz4.BlockSizeOption(lz4.BlockSize(s.lz4s.blockMaxSize)),
	)
	debug.AssertNoErr(err)

	return s.doCmpr(s.lz4s)
}

// as io.Reader
func (s *Stream) Read(b []byte) (n int, err error) {
	s.time.inSend.Store(true) // for collector to delay cleanup
	if !s.inSend() {          // true when transmitting s.sendoff.obj
		goto repeat
	}
	switch s.sendoff.ins {
	case inData:
		obj := &s.sendoff.obj
		if !obj.IsHeaderOnly() {
			return s.sendData(b)
		}
		if obj.Hdr.isFin() {
			return 0, io.EOF
		}
		s.eoObj(nil)
	case inPDU:
		for !s.pdu.done {
			err = s.pdu.readFrom(&s.sendoff)
			if s.pdu.done {
				s.pdu.insHeader()
				break
			}
		}
		if s.pdu.rlength() > 0 {
			n = s.sendPDU(b)
			if s.pdu.rlength() == 0 {
				s.sendoff.off += int64(s.pdu.slength())
				if s.pdu.last {
					s.eoObj(nil)
				}
				s.pdu.reset()
			}
		}
		return n, err
	case inHdr:
		return s.sendHdr(b)
	}
repeat:
	select {
	case obj, ok := <-s.workCh: // next object OR idle tick
		if !ok {
			err := fmt.Errorf("%s closed prior to stopping", s)
			nlog.Warningln(err)
			return 0, err
		}
		s.sendoff.obj = *obj
		obj = &s.sendoff.obj
		if obj.Hdr.isIdleTick() {
			if len(s.workCh) > 0 {
				goto repeat
			}
			return s.deactivate()
		}
		l := insObjHeader(s.maxhdr, &obj.Hdr, s.usePDU())
		s.header = s.maxhdr[:l]
		s.sendoff.ins = inHdr
		return s.sendHdr(b)
	case <-s.stopCh.Listen():
		if cmn.Rom.V(5, cos.ModTransport) {
			nlog.Infoln(s.String(), "stopped [", s.numCur, "]")
		}
		return 0, io.EOF
	}
}

func (s *Stream) sendHdr(b []byte) (n int, err error) {
	n = copy(b, s.header[s.sendoff.off:])
	s.sendoff.off += int64(n)
	if s.sendoff.off < int64(len(s.header)) {
		return
	}
	debug.Assert(s.sendoff.off == int64(len(s.header)))

	obj := &s.sendoff.obj
	if s.usePDU() && !obj.IsHeaderOnly() {
		s.sendoff.ins = inPDU
	} else {
		s.sendoff.ins = inData
	}
	if cmn.Rom.V(5, cos.ModTransport) && s.numCur&0x3f == 2 {
		nlog.Infoln(s.String(), obj.Hdr.Cname(), "[", s.numCur, "]")
	}
	s.sendoff.off = 0
	if obj.Hdr.isFin() {
		err = io.EOF
		s.lastCh.Close()
	}
	return
}

func (s *Stream) sendData(b []byte) (n int, err error) {
	var (
		obj     = &s.sendoff.obj
		objSize = obj.Size()
	)
	n, err = obj.Reader.Read(b)
	s.sendoff.off += int64(n)
	if err != nil {
		if err == io.EOF {
			if s.sendoff.off < objSize {
				return n, fmt.Errorf("%s: read (%d) shorter than size (%d)", s, s.sendoff.off, objSize)
			}
			err = nil
		}
		s.eoObj(err)
	} else if s.sendoff.off >= objSize {
		s.eoObj(err)
	}
	return
}

func (s *Stream) sendPDU(b []byte) (n int) {
	n = s.pdu.read(b)
	return
}

// end-of-object:
// - update stats, reset idle timeout, and post completion
// - note that reader.Close() is done by `doCmpl`
// TODO: ideally, there's a way to flush buffered data to the underlying connection :NOTE
func (s *Stream) eoObj(err error) {
	obj := &s.sendoff.obj
	objSize := obj.Size()
	if obj.IsUnsized() {
		objSize = s.sendoff.off
	}
	s.sizeCur += s.sendoff.off
	if err != nil {
		goto exit
	}
	if s.sendoff.off != objSize {
		err = fmt.Errorf("%s: %s offset %d != size", s, obj, s.sendoff.off)
		goto exit
	}
	// this stream stats
	s.numCur++
	if cmn.Rom.V(5, cos.ModTransport) && s.numCur&0x3f == 3 {
		nlog.Infoln(s.String(), obj.Hdr.Cname(), "[", s.numCur, "]")
	}

	// target stats
	g.tstats.Inc(cos.StreamsOutObjCount)
	g.tstats.Add(cos.StreamsOutObjSize, objSize)
exit:
	if err != nil {
		nlog.Errorln(err)
	}

	// next completion => SCQ
	s.cmplCh <- cmpl{err, s.sendoff.obj}
	s.sendoff = sendoff{ins: inEOB}
}

func (s *Stream) inSend() bool { return s.sendoff.ins >= inHdr || s.sendoff.ins < inEOB }

func (s *Stream) dryrun() {
	var (
		body = io.NopCloser(s)
		h    = &handler{trname: s.trname}
		it   = iterator{
			handler: h,
			body:    body,
			hbuf:    make([]byte, cmn.DfltTransportHeader),
			loghdr:  s.String(),
		}
	)
	for {
		hlen, flags, err := it.nextProtoHdr()
		if err == io.EOF {
			break
		}
		debug.AssertNoErr(err)
		debug.Assert(flags&msgFl == 0)
		obj, err := it.nextObj(hlen)
		if obj != nil {
			cos.DrainReader(obj) // TODO: recycle `objReader` here
			continue
		}
		if err != nil {
			break
		}
	}
}

func (s *Stream) errCmpl(err error) {
	if s.inSend() {
		s.cmplCh <- cmpl{err, s.sendoff.obj}
	}
}

// gc: drain terminated stream
func (s *Stream) drain(err error) {
	for {
		select {
		case obj, ok := <-s.workCh:
			if ok {
				s.doCmpl(obj, err)
			}
		default:
			return
		}
	}
}

// gc:
func (s *Stream) closeAndFree() {
	close(s.workCh)
	close(s.cmplCh)

	g.mm.Free(s.maxhdr)
	if s.pdu != nil {
		s.pdu.free(g.mm)
	}
}

// gc: post idle tick if idle
func (s *Stream) idleTick() {
	if len(s.workCh) == 0 && s.sessST.CAS(active, inactive) {
		s.workCh <- &Obj{Hdr: ObjHdr{Opcode: opcIdleTick}}
		if cmn.Rom.V(5, cos.ModTransport) {
			nlog.Infoln(s.String(), "active => inactive")
		}
	}
}

///////////////
// lz4Stream //
///////////////

func (lz4s *lz4Stream) Read(b []byte) (n int, err error) {
	var (
		sendoff = &lz4s.s.sendoff
		last    = sendoff.obj.Hdr.isFin()
		retry   = maxInReadRetries // insist on returning n > 0 (note that lz4 compresses /blocks/)
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
	} else if lz4s.s.sendoff.ins == inEOB || err != nil {
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
	if lz4s.sgl.Len() == 0 {
		lz4s.sgl.Reset()
	}
	if last && err == nil {
		err = io.EOF
	}
	return n, err
}
