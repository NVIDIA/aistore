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

// Stream Collector - a singleton that:
// 1. controls part of the stream lifecycle:
//    - activation (followed by connection establishment and HTTP PUT), and
//    - deactivation (teardown)
// 2. provides each stream with its own idle timer (with timeout measured in ticks - see tickUnit)
// 3. deactivates idle streams

func Init() *StreamCollector {
	cmn.Assert(gc == nil)

	// real stream collector
	gc = &collector{
		stopCh:  cmn.NewStopCh(),
		ctrlCh:  make(chan ctrl, 64),
		streams: make(map[string]*Stream, 64),
		heap:    make([]*Stream, 0, 64), // min-heap sorted by stream.time.ticks
	}
	heap.Init(gc)

	return sc
}

func (sc *StreamCollector) Run() (err error) {
	cmn.Printf("Intra-cluster networking: %s client", whichClient())
	cmn.Printf("Starting %s", sc.GetRunName())
	return gc.run()
}

func (sc *StreamCollector) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", sc.GetRunName(), err)
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

func (gc *collector) remove(s *Stream) {
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
	s := x.(*Stream)
	s.time.index = l
	gc.heap = append(gc.heap, s)
	heap.Fix(gc, s.time.index) // reorder the newly added stream right away
}

func (gc *collector) update(s *Stream, ticks int) {
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
				gc.drain(s)
				s.time.ticks = 1
				continue
			}

			s.time.ticks--
			if s.time.ticks <= 0 {
				delete(gc.streams, lid)
				close(s.workCh) // delayed close
				close(s.cmplCh) // ditto
				if s.term.err == nil {
					s.term.err = errors.New(reasonUnknown)
				}
				for streamable := range s.workCh {
					obj := streamable.obj() // TODO -- FIXME
					s.objDone(obj, s.term.err)
				}
				for cmpl := range s.cmplCh {
					if !cmpl.obj.Hdr.IsLast() {
						s.objDone(&cmpl.obj, cmpl.err)
					}
				}
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
		if len(s.workCh) == 0 && s.sessST.CAS(active, inactive) {
			s.workCh <- Obj{Hdr: ObjHdr{ObjAttrs: ObjectAttrs{Size: tickMarker}}}
			if glog.FastV(4, glog.SmoduleTransport) {
				glog.Infof("%s: active => inactive", s)
			}
		}
	}
	// at this point the following must be true for each i = range gc.heap:
	// 1. heap[i].index == i
	// 2. heap[i+1].ticks >= heap[i].ticks
}

// drain terminated stream
func (gc *collector) drain(s *Stream) {
	for {
		select {
		case streamable := <-s.workCh:
			obj := streamable.obj() // TODO -- FIXME
			s.objDone(obj, s.term.err)
		default:
			return
		}
	}
}
