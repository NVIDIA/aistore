// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
)

// transport defaults
const (
	maxHeaderSize  = memsys.PageSize
	burstNum       = 32 // default max num objects that can be posted for sending without any back-pressure
	defaultIdleOut = time.Second * 2
	tickUnit       = time.Second
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

// send-side markers
const (
	lastMarker = math.MaxInt64              // end of stream: send and receive
	tickMarker = math.MaxInt64 ^ 0xa5a5a5a5 // idle tick - send side only
)

// termination: reasons
const (
	reasonUnknown = "unknown"
	reasonError   = "error"
	endOfStream   = "end-of-stream"
	reasonStopped = "stopped"
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
		toURL, trname   string       // http endpoint
		sessID          int64        // stream session ID
		sessST          atomic.Int64 // state of the TCP/HTTP session: active (connected) | inactive (disconnected)
		stats           Stats        // stream stats
		Numcur, Sizecur int64        // gets reset to zero upon each timeout
		// internals
		lid    string        // log prefix
		lastCh *cmn.StopCh   // end of stream
		stopCh *cmn.StopCh   // stop/abort stream
		postCh chan struct{} // to indicate that workCh has work
		time   struct {
			idleOut time.Duration // idle timeout
			inSend  atomic.Bool   // true upon Send() or Read() - info for Collector to delay cleanup
			ticks   int           // num 1s ticks until idle timeout
			index   int           // heap stuff
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

func newStreamBase(client Client, toURL string, extra *Extra) (s *streamBase) {
	u, err := url.Parse(toURL)
	cmn.AssertNoErr(err)

	s = &streamBase{client: client, toURL: toURL}

	s.time.idleOut = defaultIdleOut
	if extra != nil && extra.IdleTimeout > 0 {
		s.time.idleOut = extra.IdleTimeout
	}
	if s.time.idleOut < tickUnit {
		s.time.idleOut = tickUnit
	}
	s.time.ticks = int(s.time.idleOut / tickUnit)
	s.sessID = nextSID.Inc()
	s.trname = path.Base(u.Path)
	s.lid = fmt.Sprintf("%s[%d]", s.trname, s.sessID)

	s.lastCh = cmn.NewStopCh()
	s.stopCh = cmn.NewStopCh()
	s.postCh = make(chan struct{}, 1)

	s.mm = memsys.DefaultPageMM()
	if extra != nil && extra.MMSA != nil {
		s.mm = extra.MMSA
	}
	s.maxheader, _ = s.mm.Alloc(maxHeaderSize) // NOTE: must be large enough to accommodate max-size
	if extra != nil && extra.SizePDU > 0 {
		if extra.SizePDU > MaxSizePDU {
			debug.Assert(false)
			extra.SizePDU = MaxSizePDU
		}
		buf, _ := s.mm.Alloc(int64(extra.SizePDU))
		s.pdu = newSendPDU(buf)
	}

	s.sessST.Store(inactive) // NOTE: initiate HTTP session upon the first arrival

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
func (s *streamBase) URL() string         { return s.toURL }
func (s *streamBase) ID() (string, int64) { return s.trname, s.sessID }
func (s *streamBase) String() string      { return s.lid }
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
			glog.Infof("%s: stopped", s)
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
	for {
		if s.sessST.Load() == active {
			if dryrun {
				s.streamer.dryrun()
			} else if err := s.streamer.doRequest(); err != nil {
				*s.term.reason = reasonError
				s.term.err = err
				s.streamer.errCmpl(err)
				break
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
