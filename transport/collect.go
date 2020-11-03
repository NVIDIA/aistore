// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"container/heap"
	"errors"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	ctrl struct { // add/del channel to/from collector
		s   *streamBase
		add bool
	}
	collector struct {
		streams map[string]*streamBase
		heap    []*streamBase
		ticker  *time.Ticker
		stopCh  *cmn.StopCh
		ctrlCh  chan ctrl
	}
)

var (
	sc = &StreamCollector{} // idle timer and house-keeping (slow path)
	gc *collector           // real stream collector
)

// Stream Collector - a singleton that:
// 1. controls part of the stream lifecycle:
//    - activation (followed by connection establishment and HTTP PUT), and
//    - deactivation (teardown)
// 2. provides each stream with its own idle timer (with timeout measured in ticks - see tickUnit)
// 3. deactivates idle streams

// interface guard
var (
	_ cmn.Runner = &StreamCollector{}
)

func Init() *StreamCollector {
	cmn.Assert(gc == nil)

	// real stream collector
	gc = &collector{
		stopCh:  cmn.NewStopCh(),
		ctrlCh:  make(chan ctrl, 64),
		streams: make(map[string]*streamBase, 64),
		heap:    make([]*streamBase, 0, 64), // min-heap sorted by stream.time.ticks
	}
	heap.Init(gc)

	return sc
}

func (sc *StreamCollector) Name() string { return "stream_collector" }

func (sc *StreamCollector) Run() (err error) {
	cmn.Printf("Intra-cluster networking: %s client", whichClient())
	cmn.Printf("Starting %s", sc.Name())
	return gc.run()
}

func (sc *StreamCollector) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", sc.Name(), err)
	gc.stop()
}

func (gc *collector) run() (err error) {
	gc.ticker = time.NewTicker(tickUnit)
	for {
		select {
		case <-gc.ticker.C:
			gc.do()
		case ctrl, ok := <-gc.ctrlCh:
			if !ok {
				return
			}
			s, add := ctrl.s, ctrl.add
			_, ok = gc.streams[s.lid]
			if add {
				cmn.AssertMsg(!ok, s.lid)
				gc.streams[s.lid] = s
				heap.Push(gc, s)
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

func (gc *collector) remove(s *streamBase) {
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

func (gc *collector) Push(x interface{}) {
	l := len(gc.heap)
	s := x.(*streamBase)
	s.time.index = l
	gc.heap = append(gc.heap, s)
	heap.Fix(gc, s.time.index) // reorder the newly added stream right away
}

func (gc *collector) update(s *streamBase, ticks int) {
	s.time.ticks = ticks
	cmn.Assert(s.time.ticks >= 0)
	heap.Fix(gc, s.time.index)
}

func (gc *collector) Pop() interface{} {
	old := gc.heap
	n := len(old)
	sl := old[n-1]
	gc.heap = old[0 : n-1]
	return sl
}

// collector's main method
func (gc *collector) do() {
	for lid, s := range gc.streams {
		if s.Terminated() {
			if s.time.inSend.Swap(false) {
				s.streamer.drain()
				s.time.ticks = 1
				continue
			}

			s.time.ticks--
			if s.time.ticks <= 0 {
				delete(gc.streams, lid)
				s.streamer.closeSCQ()
				if s.term.err == nil {
					s.term.err = errors.New(reasonUnknown)
				}
				s.streamer.abortPending(s.term.err, true /*completions*/)
			}
		} else if s.sessST.Load() == active {
			gc.update(s, s.time.ticks-1)
		}
	}
	for _, s := range gc.streams {
		if s.time.ticks > 0 {
			continue
		}
		gc.update(s, int(s.time.idleOut/tickUnit))
		if s.time.inSend.Swap(false) {
			continue
		}
		s.streamer.idleTick()
	}
	// at this point the following must be true for each i = range gc.heap:
	// 1. heap[i].index == i
	// 2. heap[i+1].ticks >= heap[i].ticks
}
