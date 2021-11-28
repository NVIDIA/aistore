// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"fmt"
	"io"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
)

// message stream & private types
type (
	MsgStream struct {
		workCh chan *Msg // ditto
		msgoff msgoff
		streamBase
	}
	msgoff struct {
		msg Msg
		off int
		ins int // in-send enum
	}
)

// interface guard
var _ streamer = (*MsgStream)(nil)

func (s *MsgStream) terminate() {
	s.term.mu.Lock()
	debug.Assert(!s.term.terminated)
	s.term.terminated = true

	s.Stop()

	s.term.mu.Unlock()

	gc.remove(&s.streamBase)
}

func (*MsgStream) abortPending(error, bool) {}
func (*MsgStream) errCmpl(error)            {}
func (*MsgStream) compressed() bool         { return false }
func (*MsgStream) resetCompression()        { debug.Assert(false) }

func (s *MsgStream) doRequest() error {
	s.Numcur, s.Sizecur = 0, 0
	return s.do(s)
}

func (s *MsgStream) Read(b []byte) (n int, err error) {
	s.time.inSend.Store(true) // indication for Collector to delay cleanup
	if s.inSend() {
		msg := &s.msgoff.msg
		n, err = s.send(b)
		if msg.isFin() {
			err = io.EOF
		}
		return
	}
repeat:
	select {
	case msg, ok := <-s.workCh:
		if !ok {
			err = fmt.Errorf("%s closed prior to stopping", s)
			glog.Warning(err)
			return
		}
		s.msgoff.msg = *msg
		if s.msgoff.msg.isIdleTick() {
			if len(s.workCh) > 0 {
				goto repeat
			}
			return s.deactivate()
		}
		l := insMsg(s.maxheader, &s.msgoff.msg)
		s.header = s.maxheader[:l]
		s.msgoff.ins = inHdr
		return s.send(b)
	case <-s.stopCh.Listen():
		num := s.stats.Num.Load()
		glog.Infof("%s: stopped (%d/%d)", s, s.Numcur, num)
		err = io.EOF
	}
	return
}

func (s *MsgStream) send(b []byte) (n int, err error) {
	n = copy(b, s.header[s.msgoff.off:])
	s.msgoff.off += n
	if s.msgoff.off >= len(s.header) {
		debug.Assert(s.msgoff.off == len(s.header))
		s.stats.Offset.Add(int64(s.msgoff.off))
		if verbose {
			num := s.stats.Num.Load()
			glog.Infof("%s: hlen=%d (%d/%d)", s, s.msgoff.off, s.Numcur, num)
		}
		s.msgoff.ins = inEOB
		s.msgoff.off = 0
		if s.msgoff.msg.isFin() {
			if verbose {
				glog.Infof("%s: sent last", s)
			}
			err = io.EOF
			s.lastCh.Close()
		}
	}
	return
}

func (s *MsgStream) inSend() bool { return s.msgoff.ins == inHdr }

func (s *MsgStream) dryrun() {
	var (
		body = io.NopCloser(s)
		h    = &handler{trname: s.trname, mm: memsys.ByteMM()}
		it   = iterator{handler: h, body: body, hbuf: make([]byte, maxHeaderSize)}
	)
	for {
		hlen, flags, err := it.nextProtoHdr(s.String())
		if err == io.EOF {
			break
		}
		debug.AssertNoErr(err)
		debug.Assert(flags&msgFlag != 0)
		_, _ = it.nextMsg(s.String(), hlen)
		if err != nil {
			break
		}
	}
}

// gc: drain terminated stream
func (s *MsgStream) drain() {
	for {
		select {
		case <-s.workCh:
		default:
			return
		}
	}
}

// gc:
func (s *MsgStream) closeAndFree() {
	close(s.workCh)

	s.mm.Free(s.maxheader)
}

// gc: post idle tick if idle
func (s *MsgStream) idleTick() {
	if len(s.workCh) == 0 && s.sessST.CAS(active, inactive) {
		s.workCh <- &Msg{Opcode: opcIdleTick}
		if verbose {
			glog.Infof("%s: active => inactive", s)
		}
	}
}
