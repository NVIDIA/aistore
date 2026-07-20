// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

// Stream Collector (gc)
//
// This is a slow-path transport-special housekeeper for long-lived streams.
// Transport stream (or simply "stream") is designed to outlive multiple HTTP requests.
// The collector uses per-stream countdown in 1-second (cmn.DfltTransportTick) ticks to manage two
// (separate) things:
// 1) request lifecycle (idle deactivation)
// 2) stream lifecycle  (termination/reap)
//
// Terminology
// -----------
// - Stream: a long-lived sender entity identified by sessID, with session state
//   s.sessST {active,inactive}
// - Termination: final stream shutdown (Stop/Fin/reason: error)
//   (see TermInfo)
//
// Idle deactivation (request teardown)
// --------------------------------------------------------
// Each stream has a configurable idleTeardown. When a stream stays idle long enough,
// the collector asks the stream to "deactivate" the current request in-band:
//   - collector calls streamer.idleTick() (only when not currently inSend)
//   - Stream.idleTick() CASes sessST: active→inactive and enqueues opcIdleTick
//   - Stream.Read() consumes opcIdleTick at a safe point and returns io.EOF
// Result: the current HTTP request ends cleanly; the Stream object remains alive
// and may be reactivated when new work arrives. No abortPending() is involved.
//
// Termination/reap (stream teardown, with subsequent cleanup and drain)
// -----------------------------------------------------
// Terminated streams are kept for a short grace period to allow draining/completion.
// Once the grace countdown expires, the collector performs final cleanup:
//   - closeAndFree() tears down resources/session
//   - abortPending(err, completions=true) releases any remaining waiters
// This is distinct from idle deactivation: it is a one-way stream shutdown.
//
// Summary
// -----------------------------------------------------
// - idleTeardown affects request churn (active => inactive via opcIdleTick);
// - termination paths trigger abortPending() to prevent stranded waiters.

type (
	ctrl struct { // collector mailbox operation
		s   *base
		add bool
	}
	collector struct {
		streams map[int64]*base // by session ID
		ticker  *time.Ticker
		stopCh  cos.StopCh
		none    atomic.Bool // no streams
		logPrev int64       // periodic-log throttle

		// unbounded mailbox (stream creation and termination; compare w/ hk)
		mtx     sync.Mutex
		pending []ctrl
		free    []ctrl        // recycled drain buffer
		workCh  chan struct{} // doorbell
	}
)

var (
	sc *StreamCollector // idle timer and house-keeping (slow path)
	gc *collector       // real stream collector
)

// interface guard
var _ cos.Runner = (*StreamCollector)(nil)

func (*StreamCollector) Name() string { return "stream-collector" }

func (sc *StreamCollector) Run() error {
	cos.Infoln("Intra-cluster networking:", whichClient(), "client")
	cos.Infoln("Starting", sc.Name())

	gc.ticker = time.NewTicker(dfltTickIdle)
	gc.none.Store(true)
	gc.run()
	gc.ticker.Stop()
	return nil
}

func (sc *StreamCollector) Stop(err error) {
	nlog.Infoln("Stopping", sc.Name(), "err:", err)
	gc.stop()
}

func (gc *collector) run() {
	for {
		gc.drain()

		select {
		case <-gc.ticker.C:
			gc.do()
			gc.log()

		case <-gc.workCh:
			// doorbell: drain at the top of the loop.

		case <-gc.stopCh.Listen():
			gc.drain() // process pending adds/removes
			for _, s := range gc.streams {
				s.Stop()
			}
			// node shutdown (concurrently created stream, if any, dies with the process)
			gc.streams = nil
			return
		}
	}
}

// enqueue (without blocking on chan cap or anything else; similar hk pattern)
func (gc *collector) enq(c ctrl) {
	gc.mtx.Lock()
	gc.pending = append(gc.pending, c)
	l := len(gc.pending)
	gc.mtx.Unlock()

	select {
	case gc.workCh <- struct{}{}:
	default: // doorbell already rung
	}

	if l >= warnCollectPending && l&(l-1) == 0 {
		nlog.Errorln("gc: pending queue not draining [ len:", l, "]")
	}
}

func (gc *collector) drain() {
	gc.mtx.Lock()
	if len(gc.pending) == 0 {
		gc.mtx.Unlock()
		return
	}

	ops := gc.pending

	// swap slices while retaining normal capacity; prevent excessive bursts
	switch {
	case cap(ops) > maxCollectRecycle:
		gc.pending = make([]ctrl, 0, dfltCollectCap)
	case cap(gc.free) < cap(ops):
		gc.pending = make([]ctrl, 0, cap(ops))
	default:
		gc.pending = gc.free[:0]
	}
	gc.mtx.Unlock()

	for i := range ops {
		gc.apply(&ops[i])
	}
	clear(ops)

	if cap(ops) <= maxCollectRecycle {
		gc.free = ops[:0]
	} else {
		gc.free = nil
	}
}

func (gc *collector) apply(c *ctrl) {
	s := c.s
	_, exists := gc.streams[s.sessID]

	if c.add {
		debug.Assert(!exists, s.String())
		gc.streams[s.sessID] = s

		if gc.none.CAS(true, false) {
			gc.ticker.Reset(cmn.DfltTransportTick)
			gc.logPrev = mono.NanoTime()
		}
		return
	}

	if exists {
		// one tick of grace for draining before final cleanup
		s.time.ticks = 1
	}
}

// periodic log
func (gc *collector) log() {
	if gc.none.Load() {
		return
	}

	now := mono.NanoTime()
	if time.Duration(now-gc.logPrev) < dfltCollectLog {
		return
	}

	var one *base
	for _, one = range gc.streams {
		break
	}
	nlog.Infoln("total:", len(gc.streams), "one:", one.String())
	gc.logPrev = now

	gc.mtx.Lock()
	pending := len(gc.pending)
	gc.mtx.Unlock()

	if pending > 0 {
		nlog.Warningln("gc: pending [ len:", pending, "]")
	}
}

func (gc *collector) stop() {
	gc.stopCh.Close()
}

func (gc *collector) add(s *base) {
	gc.enq(ctrl{s: s, add: true})
}

func (gc *collector) remove(s *base) {
	gc.enq(ctrl{s: s}) // schedule termination grace/reaping
}

func setCollectTicks(s *base, ticks int) {
	debug.Assert(ticks >= 0)
	s.time.ticks = ticks
}

// collector's main method
func (gc *collector) do() {
	for sessID, s := range gc.streams {
		if s.IsTerminated() {
			err := s.TermInfo()
			if s.time.inSend.Swap(false) {
				s.streamer.drain(err)
				s.time.ticks = 1
				continue
			}

			s.time.ticks--
			if s.time.ticks <= 0 {
				delete(gc.streams, sessID)
				if len(gc.streams) == 0 {
					gc.ticker.Reset(dfltTickIdle)
					debug.Assert(!gc.none.Load())
					gc.none.Store(true)
					nlog.Infoln("none")
				}
				s.streamer.closeAndFree()
				s.streamer.abortPending(err, true /*completions*/)
			}
		} else if s.sessST.Load() == active {
			setCollectTicks(s, s.time.ticks-1)
		}
	}

	for _, s := range gc.streams {
		if s.time.ticks > 0 {
			continue
		}

		setCollectTicks(s, int(s.time.idleTeardown/cmn.DfltTransportTick))
		if s.time.inSend.Swap(false) {
			continue
		}
		s.streamer.idleTick()
	}
}
