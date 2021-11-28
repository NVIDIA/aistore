// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
)

// transport defaults
const (
	maxHeaderSize = memsys.PageSize
	burstNum      = 32 // default num msg-s that can be posted without any back-pressure
	tickUnit      = time.Second
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
	reasonUnknown = "unknown"
	reasonError   = "error"
	endOfStream   = "end-of-stream"
	reasonStopped = "stopped"

	retrySleep = time.Second // ECONNREFUSED | ECONNRESET | EPIPE
)

type (
	streamer interface {
		compressed() bool
		dryrun()
		terminate()
		doRequest() error
		inSend() bool
		abortPending(error, bool)
		errCmpl(error)
		resetCompression()
		// gc
		closeAndFree()
		drain()
		idleTick()
	}
	streamBase struct {
		streamer streamer
		client   Client // http client this send-stream will use

		// user-defined & queryable
		dstURL, dstID, trname string       // http endpoint
		sessID                int64        // stream session ID
		sessST                atomic.Int64 // state of the TCP/HTTP session: active (connected) | inactive (disconnected)
		stats                 Stats        // stream stats
		Numcur, Sizecur       int64        // gets reset to zero upon each timeout
		// internals
		lid    string        // log prefix
		lastCh *cos.StopCh   // end of stream
		stopCh *cos.StopCh   // stop/abort stream
		postCh chan struct{} // to indicate that workCh has work
		time   struct {
			idleTeardown time.Duration // idle timeout
			inSend       atomic.Bool   // true upon Send() or Read() - info for Collector to delay cleanup
			ticks        int           // num 1s ticks until idle timeout
			index        int           // heap stuff
		}
		wg        sync.WaitGroup
		mm        *memsys.MMSA
		pdu       *spdu  // PDU buffer
		maxheader []byte // max header buffer
		header    []byte // object header - slice of the maxheader with bucket/objName, etc. fields
		term      struct {
			mu         sync.RWMutex
			err        error
			reason     *string
			terminated bool
		}
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
	// NOTE: PDU-based traffic - MUST-have for "unsized" transmissions
	if extra.SizePDU > 0 {
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
		s.time.idleTeardown = extra.Config.Timeout.TransportIdleTeardown.D()
		// TODO: remove with the next config meta-version update
		if s.time.idleTeardown == 0 {
			s.time.idleTeardown = 4 * time.Second
		}
	}
	debug.Assertf(s.time.idleTeardown > 2*tickUnit, "%v vs. %v", s.time.idleTeardown, tickUnit)
	s.time.ticks = int(s.time.idleTeardown / tickUnit)

	s.lid = fmt.Sprintf("s-%s%s[%d]=>%s", s.trname, sid, s.sessID, dstID)

	s.maxheader, _ = s.mm.AllocSize(maxHeaderSize) // NOTE: must be large enough to accommodate max-size
	s.sessST.Store(inactive)                       // NOTE: initiate HTTP session upon the first arrival

	s.term.reason = new(string)
	return
}

func (s *streamBase) startSend(streamable fmt.Stringer) (err error) {
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

func (s *streamBase) Stop()               { s.stopCh.Close() }
func (s *streamBase) URL() string         { return s.dstURL }
func (s *streamBase) ID() (string, int64) { return s.trname, s.sessID }
func (s *streamBase) String() string      { return s.lid }

func (s *streamBase) Abort() { s.Stop() } // (DM =>) SB => s.Abort() sequence (e.g. usage see otherXreb.Abort())

func (s *streamBase) Terminated() (terminated bool) {
	s.term.mu.Lock()
	terminated = s.term.terminated
	s.term.mu.Unlock()
	return
}

func (s *streamBase) TermInfo() (string, error) {
	if s.Terminated() && *s.term.reason == "" {
		if s.term.err == nil {
			s.term.err = fmt.Errorf(reasonUnknown)
		}
		*s.term.reason = reasonUnknown
	}
	return *s.term.reason, s.term.err
}

func (s *streamBase) GetStats() (stats Stats) {
	// byte-num transfer stats
	stats.Num.Store(s.stats.Num.Load())
	stats.Offset.Store(s.stats.Offset.Load())
	stats.Size.Store(s.stats.Size.Load())
	stats.CompressedSize.Store(s.stats.CompressedSize.Load())
	return
}

func (s *streamBase) isNextReq() (next bool) {
	for {
		select {
		case <-s.lastCh.Listen():
			if verbose {
				glog.Infof("%s: end-of-stream", s)
			}
			*s.term.reason = endOfStream
			return
		case <-s.stopCh.Listen():
			if verbose {
				glog.Infof("stream %s stopped", s)
			}
			*s.term.reason = reasonStopped
			return
		case <-s.postCh:
			s.sessST.Store(active)
			next = true // initiate new HTTP/TCP session
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
	var retried bool
	for {
		if s.sessST.Load() == active {
			if dryrun {
				s.streamer.dryrun()
			} else if err := s.streamer.doRequest(); err != nil {
				if !cos.IsRetriableConnErr(err) || retried {
					*s.term.reason = reasonError
					s.term.err = err
					s.streamer.errCmpl(err)
					break
				}
				retried = true
				glog.Errorf("%s: %v - retrying...", s, err)
				time.Sleep(retrySleep)
			}
		}
		if !s.isNextReq() {
			break
		}
	}

	s.streamer.terminate()
	s.wg.Done()

	// handle termination caused by anything other than Fin()
	if *s.term.reason != endOfStream {
		var (
			err    error
			reason string
		)
		s.term.mu.RLock()
		err = s.term.err
		reason = *s.term.reason
		s.term.mu.RUnlock()
		if reason == reasonStopped {
			if verbose {
				glog.Infof("%s: stopped", s)
			}
		} else {
			glog.Errorf("%s: terminating (%s, %v)", s, reason, err)
		}
		// wait for the SCQ/cmplCh to empty
		s.wg.Wait()

		// cleanup
		s.streamer.abortPending(err, false /*completions*/)
	}
}

//////////////////
// misc helpers //
//////////////////

func burst() (burst int) {
	burst = burstNum
	if a := os.Getenv("AIS_STREAM_BURST_NUM"); a != "" {
		if burst64, err := strconv.ParseInt(a, 10, 0); err != nil {
			glog.Error(err)
			burst = burstNum
		} else {
			burst = int(burst64)
		}
	}
	return
}

func dryrun() (dryrun bool) {
	var err error
	if a := os.Getenv("AIS_STREAM_DRY_RUN"); a != "" {
		if dryrun, err = strconv.ParseBool(a); err != nil {
			glog.Error(err)
		}
	}
	return
}
