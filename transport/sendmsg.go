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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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
	cos.Assert(!s.term.terminated)
	s.term.terminated = true

	s.Stop()

	s.term.mu.Unlock()

	gc.remove(&s.streamBase)
}

func (s *MsgStream) abortPending(error, bool) {}
func (s *MsgStream) errCmpl(error)            {}
func (s *MsgStream) compressed() bool         { return false }
func (s *MsgStream) resetCompression()        { debug.Assert(false) }

func (s *MsgStream) doRequest() error {
	s.Numcur, s.Sizecur = 0, 0
	return s.do(s)
}

func (s *MsgStream) Read(b []byte) (n int, err error) {
	s.time.inSend.Store(true) // indication for Collector to delay cleanup
	if s.inSend() {
		msg := &s.msgoff.msg
		n, err = s.send(b)
		if msg.IsLast() {
			err = io.EOF
		}
		return
	}
repeat:
	select {
	case msg, ok := <-s.workCh:
		if !ok {
			err = fmt.Errorf("%s closed prior to stopping", s)
			debug.Infof("%v", err)
			return
		}
		s.msgoff.msg = *msg
		if s.msgoff.msg.IsIdleTick() {
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
		cos.Assert(s.msgoff.off == len(s.header))
		s.stats.Offset.Add(int64(s.msgoff.off))
		if verbose {
			num := s.stats.Num.Load()
			glog.Infof("%s: hlen=%d (%d/%d)", s, s.msgoff.off, s.Numcur, num)
		}
		s.msgoff.ins = inEOB
		s.msgoff.off = 0
		if s.msgoff.msg.IsLast() {
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
		it   = iterator{trname: s.trname, body: body, headerBuf: make([]byte, maxHeaderSize)}
	)
	for {
		hlen, flags, err := it.nextProtoHdr(s.String())
		if err == io.EOF {
			break
		}
		cos.AssertNoErr(err)
		cos.Assert(flags&msgFlag != 0)
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
		s.workCh <- &Msg{Flags: tickMarker}
		if verbose {
			glog.Infof("%s: active => inactive", s)
		}
	}
}

////////////////////
// Msg and MsgHdr //
////////////////////

func (msg *Msg) IsLast() bool       { return msg.Flags == lastMarker }
func (msg *Msg) IsIdleTick() bool   { return msg.Flags == tickMarker }
func (msg *Msg) IsHeaderOnly() bool { return true }

func (msg *Msg) String() string {
	if msg.IsLast() {
		return "smsg-last"
	}
	if msg.IsIdleTick() {
		return "smsg-tick"
	}
	l := cos.Min(len(msg.Body), 16)
	return fmt.Sprintf("smsg-[%s](len=%d)", msg.Body[:l], l)
}
