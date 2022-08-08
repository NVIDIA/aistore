// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
)

// stream TCP/HTTP session: inactive <=> active transitions
const (
	inactive = iota
	active
)

// in-send states
const (
	inHdr = iota + 1
	inPDU
	inData
	inEOB
)

// termination: reasons
const (
	reasonError   = "error"
	endOfStream   = "end-of-stream"
	reasonStopped = "stopped"

	connErrWait = time.Second // ECONNREFUSED | ECONNRESET | EPIPE
	termErrWait = time.Second
)

type (
	streamer interface {
		compressed() bool
		dryrun()
		terminate(error, string) (string, error)
		doRequest() error
		inSend() bool
		abortPending(error, bool)
		errCmpl(error)
		resetCompression()
		// gc
		closeAndFree()
		drain(err error)
		idleTick()
	}
	streamBase struct {
		streamer  streamer
		client    Client      // stream's http client
		stopCh    *cos.StopCh // stop/abort stream
		lastCh    *cos.StopCh // end-of-stream
		pdu       *spdu       // PDU buffer
		mm        *memsys.MMSA
		postCh    chan struct{} // to indicate that workCh has work
		trname    string        // http endpoint: (trname, dstURL, dstID)
		dstURL    string
		dstID     string
		lid       string // log prefix
		maxheader []byte // max header buffer
		header    []byte // object header (slice of the maxheader with bucket/objName, etc. fields packed/serialized)
		term      struct {
			err    error
			reason string
			mu     sync.Mutex
			done   atomic.Bool
		}
		stats Stats // stream stats
		time  struct {
			idleTeardown time.Duration // idle timeout
			inSend       atomic.Bool   // true upon Send() or Read() - info for Collector to delay cleanup
			ticks        int           // num 1s ticks until idle timeout
			index        int           // heap stuff
		}
		wg      sync.WaitGroup
		sessST  atomic.Int64 // state of the TCP/HTTP session: active (connected) | inactive (disconnected)
		sessID  int64        // stream session ID
		Numcur  int64        // gets reset to zero upon each timeout
		Sizecur int64        // ditto
	}
)

////////////////
// streamBase //
////////////////

func newStreamBase(client Client, dstURL, dstID string, extra *Extra) (s *streamBase) {
	var sid string
	u, err := url.Parse(dstURL)
	cos.AssertNoErr(err)

	s = &streamBase{client: client, dstURL: dstURL, dstID: dstID}

	s.sessID = nextSID.Inc()
	s.trname = path.Base(u.Path)

	s.lastCh = cos.NewStopCh()
	s.stopCh = cos.NewStopCh()
	s.postCh = make(chan struct{}, 1)

	s.mm = memsys.PageMM()

	// default overrides
	if extra.SenderID != "" {
		sid = "-" + extra.SenderID
	}
	if extra.MMSA != nil {
		s.mm = extra.MMSA
	}
	// NOTE: PDU-based traffic - a MUST-have for "unsized" transmissions
	if extra.UsePDU() {
		if extra.SizePDU > MaxSizePDU {
			debug.Assert(false)
			extra.SizePDU = MaxSizePDU
		}
		buf, _ := s.mm.AllocSize(int64(extra.SizePDU))
		s.pdu = newSendPDU(buf)
	}
	if extra.IdleTeardown > 0 {
		s.time.idleTeardown = extra.IdleTeardown
	} else {
		s.time.idleTeardown = extra.Config.Transport.IdleTeardown.D()
		if s.time.idleTeardown == 0 {
			s.time.idleTeardown = dfltIdleTeardown
		}
	}
	debug.Assertf(s.time.idleTeardown >= dfltTick, "%v vs. %v", s.time.idleTeardown, dfltTick)
	s.time.ticks = int(s.time.idleTeardown / dfltTick)

	s.lid = fmt.Sprintf("s-%s%s[%d]=>%s", s.trname, sid, s.sessID, dstID)

	s.maxheader, _ = s.mm.AllocSize(int64(maxHeaderSize)) // must be large enough to accommodate max-size
	s.sessST.Store(inactive)                              // initiate HTTP session upon the first arrival
	return
}

func (s *streamBase) startSend(streamable fmt.Stringer) (err error) {
	s.time.inSend.Store(true) // StreamCollector to postpone cleanups
	if s.IsTerminated() {
		// slow path
		reason, errT := s.TermInfo()
		err = fmt.Errorf("%s terminated(%q, %v), dropping %s", s, reason, errT, streamable)
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

func (s *streamBase) Stop()               { s.stopCh.Close() }
func (s *streamBase) URL() string         { return s.dstURL }
func (s *streamBase) ID() (string, int64) { return s.trname, s.sessID }
func (s *streamBase) String() string      { return s.lid }

func (s *streamBase) Abort() { s.Stop() } // (DM =>) SB => s.Abort() sequence (e.g. usage see otherXreb.Abort())

func (s *streamBase) IsTerminated() bool { return s.term.done.Load() }

func (s *streamBase) TermInfo() (reason string, err error) {
	// to account for an unlikely delay between done.CAS() and mu.Lock - see terminate()
	sleep := cos.ProbingFrequency(termErrWait)
	for elapsed := time.Duration(0); elapsed < termErrWait; elapsed += sleep {
		s.term.mu.Lock()
		reason, err = s.term.reason, s.term.err
		s.term.mu.Unlock()
		if reason != "" {
			break
		}
		time.Sleep(sleep)
	}
	return
}

func (s *streamBase) GetStats() (stats Stats) {
	// byte-num transfer stats
	stats.Num.Store(s.stats.Num.Load())
	stats.Offset.Store(s.stats.Offset.Load())
	stats.Size.Store(s.stats.Size.Load())
	stats.CompressedSize.Store(s.stats.CompressedSize.Load())
	return
}

func (s *streamBase) isNextReq() (reason string) {
	for {
		select {
		case <-s.lastCh.Listen():
			if verbose {
				glog.Infof("%s: end-of-stream", s)
			}
			reason = endOfStream
			return
		case <-s.stopCh.Listen():
			if verbose {
				glog.Infof("%s: stopped", s)
			}
			reason = reasonStopped
			return
		case <-s.postCh:
			s.sessST.Store(active)
			if verbose {
				glog.Infof("%s: active <- posted", s)
			}
			return
		}
	}
}

func (s *streamBase) deactivate() (n int, err error) {
	err = io.EOF
	if verbose {
		num := s.stats.Num.Load()
		glog.Infof("%s: connection teardown (%d/%d)", s, s.Numcur, num)
	}
	return
}

func (s *streamBase) sendLoop(dryrun bool) {
	var (
		err     error
		reason  string
		retried bool
	)
	for {
		if s.sessST.Load() == active {
			if dryrun {
				s.streamer.dryrun()
			} else if errR := s.streamer.doRequest(); errR != nil {
				if !cos.IsRetriableConnErr(err) || retried {
					reason = reasonError
					err = errR
					s.streamer.errCmpl(err)
					break
				}
				retried = true
				glog.Errorf("%s: %v - retrying...", s, errR)
				time.Sleep(connErrWait)
			}
		}
		if reason = s.isNextReq(); reason != "" {
			break
		}
	}

	reason, err = s.streamer.terminate(err, reason)
	s.wg.Done()

	if reason == endOfStream {
		return
	}

	// termination is caused by anything other than Fin()
	// (reasonStopped is, effectively, abort via Stop() - totally legit)
	if reason != reasonStopped {
		glog.Errorf("%s: terminating (%s, %v)", s, reason, err)
	}

	// wait for the SCQ/cmplCh to empty
	s.wg.Wait()

	// cleanup
	s.streamer.abortPending(err, false /*completions*/)
}

///////////
// Extra //
///////////

func (extra *Extra) UsePDU() bool { return extra.SizePDU > 0 }

func (extra *Extra) Compressed() bool {
	return extra.Compression != "" && extra.Compression != apc.CompressNever
}

//
// misc
//

func dryrun() (dryrun bool) {
	var err error
	if a := os.Getenv("AIS_STREAM_DRY_RUN"); a != "" {
		if dryrun, err = strconv.ParseBool(a); err != nil {
			glog.Error(err)
		}
	}
	return
}
