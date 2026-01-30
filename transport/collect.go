// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"container/heap"
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
	ctrl struct { // add/del channel to/from collector
		s   *base
		add bool
	}
	collector struct {
		streams map[int64]*base // by session ID
		ticker  *time.Ticker
		stopCh  cos.StopCh
		ctrlCh  chan ctrl
		heap    []*base
		none    atomic.Bool // no streams
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
	var prev int64
	for {
		select {
		case <-gc.ticker.C:
			gc.do()

			// periodic log
			if !gc.none.Load() {
				now := mono.NanoTime()
				if time.Duration(now-prev) >= dfltCollectLog {
					var s *base
					for _, s = range gc.streams {
						break
					}
					nlog.Infoln("total:", len(gc.streams), "one:", s.String())
					prev = now

					if l, c := len(gc.ctrlCh), cap(gc.ctrlCh); l > (c - c>>2) {
						nlog.Errorln("control channel full", l, c) // compare w/ cos.ErrWorkChanFull
					}
				}
			}

		case ctrl, ok := <-gc.ctrlCh:
			if !ok {
				return
			}
			s, add := ctrl.s, ctrl.add
			_, ok = gc.streams[s.sessID]
			if add {
				debug.Assert(!ok, s.String())
				gc.streams[s.sessID] = s
				heap.Push(gc, s)
				if gc.none.CAS(true, false) {
					gc.ticker.Reset(cmn.DfltTransportTick)
					prev = mono.NanoTime()
				}
			} else if ok {
				heap.Remove(gc, s.time.index)
				s.time.ticks = 1
			}
		case <-gc.stopCh.Listen():
			for _, s := range gc.streams {
				s.Stop()
			}
			gc.streams = nil
			return
		}
	}
}

func (gc *collector) stop() {
	gc.stopCh.Close()
}

func (gc *collector) remove(s *base) {
	gc.ctrlCh <- ctrl{s, false} // remove and close workCh
}

// as min-heap
func (gc *collector) Len() int { return len(gc.heap) }

func (gc *collector) Less(i, j int) bool {
	si := gc.heap[i]
	sj := gc.heap[j]
	return si.time.ticks < sj.time.ticks
}

func (gc *collector) Swap(i, j int) {
	gc.heap[i], gc.heap[j] = gc.heap[j], gc.heap[i]
	gc.heap[i].time.index = i
	gc.heap[j].time.index = j
}

func (gc *collector) Push(x any) {
	l := len(gc.heap)
	s := x.(*base)
	s.time.index = l
	gc.heap = append(gc.heap, s)
	heap.Fix(gc, s.time.index) // reorder the newly added stream right away
}

func (gc *collector) update(s *base, ticks int) {
	s.time.ticks = ticks
	debug.Assert(s.time.ticks >= 0)
	heap.Fix(gc, s.time.index)
}

func (gc *collector) Pop() any {
	old := gc.heap
	n := len(old)
	sl := old[n-1]
	gc.heap = old[0 : n-1]
	return sl
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
			gc.update(s, s.time.ticks-1)
		}
	}
	for _, s := range gc.streams {
		if s.time.ticks > 0 {
			continue
		}
		gc.update(s, int(s.time.idleTeardown/cmn.DfltTransportTick))
		if s.time.inSend.Swap(false) {
			continue
		}
		s.streamer.idleTick()
	}
	// at this point the following must be true for each i = range gc.heap:
	// 1. heap[i].index == i
	// 2. heap[i+1].ticks >= heap[i].ticks
}
