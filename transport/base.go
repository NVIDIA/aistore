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
	"github.com/NVIDIA/aistore/xoshiro256"
)

// transport defaults
const (
	maxHeaderSize  = 1024
	burstNum       = 32 // default max num objects that can be posted for sending without any back-pressure
	defaultIdleOut = time.Second * 2
	tickUnit       = time.Second
)

// internal flags and markers
const (
	lastMarker = math.MaxInt64              // end of stream (send & receive)
	tickMarker = math.MaxInt64 ^ 0xa5a5a5a5 // idle tick (send side only)
	msgMask    = uint64(1 << 63)            // messages vs objects demux (send & receive)
)

// stream TCP/HTTP session: inactive <=> active transitions
const (
	inactive = iota
	active
)

// in-send states
const (
	inHdr = iota + 1
	inData
	inEOB
)

// termination: reasons
const (
	reasonUnknown = "unknown"
	reasonError   = "error"
	endOfStream   = "end-of-stream"
	reasonStopped = "stopped"
)

type (
	streamable interface {
		obj() *Obj
		msg() *Msg
	}
	streamer interface {
		compressed() bool
		dryrun()
		terminate()
		doRequest() error
		inSend() bool
		doCmpl(streamable, error)
		abortPending(error, bool)
		errCmpl(error)
		resetCompression()
		// gc
		closeSCQ()
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
		maxheader []byte // max header buffer
		header    []byte // object header - slice of the maxheader with bucket/objName, etc. fields
		term      struct {
			mu         sync.Mutex
			err        error
			reason     *string
			terminated bool
		}
	}
)

var nextSID = *atomic.NewInt64(100) // unique session IDs starting from 101

////////////////
// streamBase //
////////////////

func newStreamBase(client Client, toURL string, extra *Extra) (s *streamBase) {
	u, err := url.Parse(toURL)
	if err != nil {
		glog.Errorf("Failed to parse %s: %v", toURL, err)
		return
	}
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
	s.maxheader = make([]byte, maxHeaderSize) // NOTE: must be large enough to accommodate all max-size Header
	s.sessST.Store(inactive)                  // NOTE: initiate HTTP session upon arrival of the first object

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
		if *s.term.reason == reasonStopped {
			if verbose {
				glog.Infof("%s: stopped", s)
			}
		} else {
			glog.Errorf("%s: terminating (%s, %v)", s, *s.term.reason, s.term.err)
		}
		// wait for the SCQ/cmplCh to empty
		s.wg.Wait()

		// cleanup
		s.streamer.abortPending(s.term.err, false /*completions*/)
	}
}

//////////////////////////
// header serialization //
//////////////////////////

func (s *streamBase) insObjHeader(hdr *ObjHdr) (l int) {
	l = cmn.SizeofI64 * 2
	l = insString(l, s.maxheader, hdr.Bck.Name)
	l = insString(l, s.maxheader, hdr.ObjName)
	l = insString(l, s.maxheader, hdr.Bck.Provider)
	l = insString(l, s.maxheader, hdr.Bck.Ns.Name)
	l = insString(l, s.maxheader, hdr.Bck.Ns.UUID)
	l = insByte(l, s.maxheader, hdr.Opaque)
	l = insAttrs(l, s.maxheader, hdr.ObjAttrs)
	hlen := uint64(l - cmn.SizeofI64*2)
	insUint64(0, s.maxheader, hlen)
	checksum := xoshiro256.Hash(hlen)
	insUint64(cmn.SizeofI64, s.maxheader, checksum)
	return
}

func (s *streamBase) insMsg(msg *Msg) (l int) {
	l = cmn.SizeofI64 * 2
	l = insInt64(l, s.maxheader, msg.Flags)
	l = insByte(l, s.maxheader, msg.Body)
	hlen := uint64(l - cmn.SizeofI64*2)
	insUint64(0, s.maxheader, hlen|msgMask) // mask
	checksum := xoshiro256.Hash(hlen)
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
