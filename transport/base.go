// Package transport provides long-lived http/tcp connections for
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
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

const maxInReadRetries = 64 // Tx: lz4 stream read; Rx: partial object header

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
		streamer streamer
		client   Client        // stream's http client
		stopCh   cos.StopCh    // stop/abort stream
		lastCh   cos.StopCh    // end-of-stream
		pdu      *spdu         // PDU buffer
		postCh   chan struct{} // to indicate that workCh has work
		trname   string        // http endpoint: (trname, dstURL, dstID)
		dstURL   string
		dstID    string
		lid      string // log prefix
		maxhdr   []byte // header buf must be large enough to accommodate max-size for this stream
		header   []byte // object header (slice of the maxhdr with bucket/objName, etc. fields packed/serialized)
		term     struct {
			err    error
			reason string
			mu     sync.Mutex
			done   atomic.Bool
		}
		stats Stats // stream stats (send side - compare with rxStats)
		time  struct {
			idleTeardown time.Duration // idle timeout
			inSend       atomic.Bool   // true upon Send() or Read() - info for Collector to delay cleanup
			ticks        int           // num 1s ticks until idle timeout
			index        int           // heap stuff
		}
		wg       sync.WaitGroup
		sessST   atomic.Int64 // state of the TCP/HTTP session: active (connected) | inactive (disconnected)
		sessID   int64        // stream session ID
		numCur   int64        // gets reset to zero upon each timeout
		sizeCur  int64        // ditto
		chanFull atomic.Int64
	}
)

////////////////
// streamBase //
////////////////

func newBase(client Client, dstURL, dstID string, extra *Extra) (s *streamBase) {
	var (
		sid    string
		u, err = url.Parse(dstURL)
	)
	debug.AssertNoErr(err)

	s = &streamBase{client: client, dstURL: dstURL, dstID: dstID}

	s.sessID = nextSessionID.Inc()
	s.trname = path.Base(u.Path)

	s.lastCh.Init()
	s.stopCh.Init()
	s.postCh = make(chan struct{}, 1)

	// default overrides
	if extra.SenderID != "" {
		sid = "-" + extra.SenderID
	}
	// NOTE: PDU-based traffic - a MUST-have for "unsized" transmissions
	if extra.UsePDU() {
		if extra.SizePDU > maxSizePDU {
			debug.Assert(false)
			extra.SizePDU = maxSizePDU
		}
		buf, _ := g.mm.AllocSize(int64(extra.SizePDU))
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
	debug.Assert(s.time.idleTeardown >= dfltTick, s.time.idleTeardown, " vs ", dfltTick)
	s.time.ticks = int(s.time.idleTeardown / dfltTick)

	s._lid(sid, dstID, extra)

	s.maxhdr, _ = g.mm.AllocSize(_sizeHdr(extra.Config, int64(extra.MaxHdrSize)))

	s.sessST.Store(inactive) // initiate HTTP session upon the first arrival
	return
}

func (s *streamBase) _lid(sid, dstID string, extra *Extra) {
	var sb strings.Builder

	sb.WriteString("s-")
	sb.WriteString(s.trname)
	sb.WriteString(sid)
	sb.WriteByte('[')
	sb.WriteString(strconv.FormatInt(s.sessID, 10))

	extra.Lid(&sb)

	sb.WriteString("]=>")
	sb.WriteString(dstID)

	s.lid = sb.String() // "s-%s%s[%d]=>%s"
}

// (used on the receive side as well)
func _sizeHdr(config *cmn.Config, size int64) int64 {
	if size != 0 {
		debug.Assert(size <= cmn.MaxTransportHeader, size)
		size = min(size, cmn.MaxTransportHeader)
	} else if config.Transport.MaxHeaderSize != 0 {
		size = int64(config.Transport.MaxHeaderSize)
	} else {
		size = cmn.DfltTransportHeader
	}
	return size
}

func (s *streamBase) startSend(streamable fmt.Stringer) (err error) {
	s.time.inSend.Store(true) // StreamCollector to postpone cleanups

	if s.IsTerminated() {
		// slow path
		reason, errT := s.TermInfo()
		err = cmn.NewErrStreamTerminated(s.String(), errT, reason, "dropping "+streamable.String())
		nlog.Errorln(err)
		return
	}

	if s.sessST.CAS(inactive, active) {
		s.postCh <- struct{}{}
		if cmn.Rom.FastV(5, cos.SmoduleTransport) {
			nlog.Infoln(s.String(), "inactive => active")
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
			if cmn.Rom.FastV(5, cos.SmoduleTransport) {
				nlog.Infoln(s.String(), "end-of-stream")
			}
			reason = endOfStream
			return
		case <-s.stopCh.Listen():
			if cmn.Rom.FastV(5, cos.SmoduleTransport) {
				nlog.Infoln(s.String(), "stopped")
			}
			reason = reasonStopped
			return
		case <-s.postCh:
			s.sessST.Store(active)
			if cmn.Rom.FastV(5, cos.SmoduleTransport) {
				nlog.Infoln(s.String(), "active <- posted")
			}
			return
		}
	}
}

func (s *streamBase) deactivate() (n int, err error) {
	err = io.EOF
	if cmn.Rom.FastV(5, cos.SmoduleTransport) {
		nlog.Infoln(s.String(), "connection teardown: [", s.numCur, s.stats.Num.Load(), "]")
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
				nlog.Errorln(s.String(), "err: ", errR, "- retrying...")
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
		nlog.Errorf("%s: terminating (%s, %v)", s, reason, err)
	}

	// wait for the SCQ/cmplCh to empty
	s.wg.Wait()

	// cleanup
	s.streamer.abortPending(err, false /*completions*/)

	verbose := cmn.Rom.FastV(5, cos.SmoduleTransport)
	if cnt := s.chanFull.Load(); (cnt >= 10 && cnt <= 20) || (cnt > 0 && verbose) {
		nlog.Errorln(s.String(), cos.ErrWorkChanFull, "cnt:", cnt)
	}
}

///////////
// Extra //
///////////

func (extra *Extra) UsePDU() bool { return extra.SizePDU > 0 }

func (extra *Extra) Compressed() bool {
	return extra.Compression != "" && extra.Compression != apc.CompressNever
}

func (extra *Extra) Lid(sb *strings.Builder) {
	if extra.Compressed() {
		sb.WriteByte('[')
		sb.WriteString(cos.ToSizeIEC(int64(extra.Config.Transport.LZ4BlockMaxSize), 0))
		sb.WriteByte(']')
	}
}

//
// misc
//

func dryrun() (dryrun bool) {
	var err error
	if a := os.Getenv("AIS_STREAM_DRY_RUN"); a != "" {
		if dryrun, err = strconv.ParseBool(a); err != nil {
			nlog.Errorln(err)
		}
	}
	return
}
