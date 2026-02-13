// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xs"
)

const (
	lazyDfltChanSize = 1024 // NOTE: may become 4096 if we ever hit chanFull
	lazyMaxChanSize  = 64 * 1024

	lazyDelayMin = 4 * time.Second

	lazyClipCap = 128

	lazyMiniBatch = 16
)

const lazyTag = "lazy-delete"

type lazydel struct {
	put      []core.LIF // pending
	get      []core.LIF // ready for deletion
	stopCh   *cos.StopCh
	workCh   chan core.LIF
	rebID    atomic.Int64
	running  atomic.Bool
	chanFull atomic.Bool // TODO: consider storing prev. full size instead
}

// is called via recvRegularAck -> ackLomAck
func (r *lazydel) enqueue(lif core.LIF, xname string, rebID int64) {
	if id := r.rebID.Load(); id != rebID {
		if id != 0 {
			nlog.Warningln(lazyTag, "enqueue from invalid (previous?)", xname, "[", rebID, "vs current", id, "]")
		}
		return
	}
	l, c := len(r.workCh), cap(r.workCh)
	debug.Assert(c >= lazyDfltChanSize)

	// (-8) should be enough to prevent racy blocking
	if l >= c-8 {
		r.chanFull.Store(true)
		nlog.Warningln(lazyTag, cos.ErrWorkChanFull, "- dropping [", lif.Cname(), xname, l, "]")
		return
	}
	if l == c-c>>2 || l == c-c>>3 {
		nlog.Warningln(lazyTag, cos.ErrWorkChanFull, "[", xname, l, "]")
	}
	r.workCh <- lif
}

func (r *lazydel) init() { r.stopCh = cos.NewStopCh() }
func (r *lazydel) stop() { r.stopCh.Close() }

// drop and drain
func (r *lazydel) cleanup() {
	if cap(r.put) > 0 {
		clear(r.put)
		r.put = r.put[:0]
		r.put = cos.ResetSliceCap(r.put, lazyClipCap) // clip cap
	}
	if cap(r.get) > 0 {
		clear(r.get)
		r.get = r.get[:0]
		r.get = cos.ResetSliceCap(r.get, lazyClipCap) // clip cap
	}
	var cnt int
	for {
		select {
		case <-r.workCh:
			cnt++
			if cnt >= lazyDfltChanSize {
				return
			}
		default:
			if cnt > 0 {
				nlog.Infoln(lazyTag, "cleanup: drained", cnt, "items")
			}
			return
		}
	}
}

// tunables
func lazytimes(config *cmn.Config) (delay, idle, busy time.Duration) {
	lazyDelay := max(config.Timeout.MaxHostBusy.D(), lazyDelayMin)
	return lazyDelay, lazyDelay << 2, lazyDelay >> 2
}

func (r *lazydel) run(xreb *xs.Rebalance, config *cmn.Config, rebID int64) {
	const (
		prompt = "waiting for the previous " + lazyTag
	)
	var (
		xid                 string
		delay, didle, dbusy = lazytimes(config)
		maxWait             = min(delay, 10*time.Second)
	)
	if r.running.Load() { // (unlikely)
		xid = r.xid()
		r.stop() // redundant no-op
		nlog.Warningln(prompt, "[ curr:", xreb.Name(), "prev: ", xid, "]")
		r.waitPrev(maxWait)
	}
	if !r.running.CAS(false, true) {
		nlog.Errorln("timed out", prompt, "to exit [ curr:", xreb.Name(), "prev: ", xid, "timeout:", maxWait, "]")
		return
	}
	if xreb.IsAborted() {
		return
	}

	// (re)init
	size := max(lazyDfltChanSize, config.Rebalance.Burst)
	if r.chanFull.Load() {
		r.chanFull.Store(false)
		size = min(size*2, lazyMaxChanSize)
		delay = max(delay>>1, lazyDelayMin)
	}
	if cap(r.put) == 0 {
		r.put = make([]core.LIF, 0, size)
	} else {
		// reuse both slices
		r.cleanup()
	}
	r.workCh = make(chan core.LIF, size)
	r.stopCh = cos.NewStopCh()
	r.rebID.Store(rebID)

	ticker := time.NewTicker(delay)

	// run
	nlog.Infoln(lazyTag, "start [", xreb.Name(), "]")
	var cnt int
	for {
		select {
		case lif := <-r.workCh:
			r.put = append(r.put, lif)

		case <-r.stopCh.Listen():
			nlog.Infoln(lazyTag, "stop [", xreb.Name(), cnt, "]")
			goto fin

		case <-ticker.C:
			if xreb.IsAborted() {
				nlog.Infoln(lazyTag, "abort [", xreb.Name(), cnt, "]")
				goto fin
			}
			if len(r.put) == 0 && len(r.get) == 0 {
				fintime := xreb.EndTime()
				if fintime.IsZero() {
					continue // rebalance still running && nothing to do
				}
				// self-terminate when:
				// - rebalance finished, and
				// - queues are empty, and
				// - it's been a while
				if time.Since(fintime) > didle {
					nlog.Infoln(lazyTag, "done [", xreb.Name(), cnt, "]")
					goto fin
				}
			}

			// go ahead and delete them all
			for _, lif := range r.get {
				lom, err := lif.LOM()
				if err != nil {
					continue
				}
				lom.Lock(true)
				err = lom.RemoveMain()
				if err == nil {
					for copyFQN := range lom.GetCopies() {
						cos.RemoveFile(copyFQN)
					}
				} else {
					core.T.FSHC(err, lom.Mountpath(), lom.FQN)
				}
				lom.Unlock(true)

				cnt++
				if cnt%lazyMiniBatch == 0 {
					select {
					case <-r.stopCh.Listen():
						nlog.Infoln(lazyTag, "stop [", xreb.Name(), cnt, "]")
						goto fin
					default:
					}
				}
			}

			// swap for the next round
			// - r.put (newly enqueued) becomes r.get (ready to delete next tick)
			// - r.get (just deleted) becomes r.put (ready to accumulate new items)
			r.get = r.get[:0]
			r.get, r.put = r.put, r.get

			l34 := size - size>>2
			if len(r.get) > l34 && delay > dbusy {
				// speed up via a more frequent ticker
				delay >>= 1
				ticker.Reset(delay)
			}
		}
	}

fin:
	ticker.Stop()
	r.rebID.Store(0)
	r.cleanup()
	r.running.Store(false)
}

// current rebalance xid
func (r *lazydel) xid() string { return xact.RebID2S(r.rebID.Load()) }

func (r *lazydel) waitPrev(maxWait time.Duration) {
	var (
		total time.Duration
		sleep = cos.ProbingFrequency(maxWait)
	)
	for r.running.Load() && total < maxWait {
		time.Sleep(sleep)
		total += sleep
		sleep += sleep >> 1
	}
}
