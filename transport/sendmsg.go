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
	"github.com/NVIDIA/aistore/cmn"
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
		off int64
		ins int // in-send enum
	}
)

// interface guard
var (
	_ streamable = &Msg{}
	_ streamer   = &MsgStream{}
)

func (s *MsgStream) terminate() {
	s.term.mu.Lock()
	cmn.Assert(!s.term.terminated)
	s.term.terminated = true

	s.Stop()

	s.term.mu.Unlock()

	// TODO -- FIXME: un-collect
}

func (s *MsgStream) dryrun()                      { cmn.Assert(false) }
func (s *MsgStream) abortPending(_ error, _ bool) {}
func (s *MsgStream) doCmpl(_ streamable, _ error) {}
func (s *MsgStream) compressed() bool             { return false }

func (s *MsgStream) doRequest() (err error) {
	s.Numcur, s.Sizecur = 0, 0
	return s.do(s, io.Reader(s))
}

func (s *MsgStream) Read(b []byte) (n int, err error) {
	s.time.inSend.Store(true) // indication for Collector to delay cleanup
	if s.inSend() {
		msg := &s.msgoff.msg
		if msg.IsLast() {
			err = io.EOF
			return
		}
		return s.sendHdr(b)
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
		l := s.insMsg(&s.msgoff.msg)
		s.header = s.maxheader[:l]
		s.msgoff.ins = inHdr
		return s.sendHdr(b)
	case <-s.stopCh.Listen():
		num := s.stats.Num.Load()
		glog.Infof("%s: stopped (%d/%d)", s, s.Numcur, num)
		err = io.EOF
		return
	}
}

func (s *MsgStream) sendHdr(b []byte) (n int, err error) {
	n = copy(b, s.header[s.msgoff.off:])
	s.msgoff.off += int64(n)
	if s.msgoff.off >= int64(len(s.header)) {
		cmn.Assert(s.msgoff.off == int64(len(s.header)))
		s.stats.Offset.Add(s.msgoff.off)
		if glog.FastV(4, glog.SmoduleTransport) {
			num := s.stats.Num.Load()
			glog.Infof("%s: hlen=%d (%d/%d)", s, s.msgoff.off, s.Numcur, num)
		}
		s.msgoff.ins = inEOB
		s.msgoff.off = 0
		if s.msgoff.msg.IsLast() {
			if glog.FastV(4, glog.SmoduleTransport) {
				glog.Infof("%s: sent last", s)
			}
			err = io.EOF
			s.lastCh.Close()
		}
	} else if glog.FastV(4, glog.SmoduleTransport) {
		glog.Infof("%s: split header: copied %d < %d hlen", s, s.msgoff.off, len(s.header))
	}
	return
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
