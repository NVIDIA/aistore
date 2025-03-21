// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/xact/xs"
)

const (
	// if need be, add to `config.RebalanceConf` (alongside `DestRetryTime`)
	lazyDelay = 16 * time.Second

	lazyIdle = 4 * lazyDelay
	lazyBusy = lazyDelay / 4

	lazySize = 1024
)

type lazydel struct {
	put     []core.LIF // pending
	get     []core.LIF // ready for deletion
	stopCh  *cos.StopCh
	workCh  chan core.LIF
	running atomic.Bool
}

// via recvRegularAck -> ackLomAck
func (r *lazydel) enqueue(lif core.LIF) {
	r.workCh <- lif
	if l, c := len(r.workCh), cap(r.workCh); l >= c-c>>2 {
		if l == c-c>>2 || l >= c-4 {
			nlog.Warningln(cos.ErrWorkChanFull)
		}
	}
}

func (r *lazydel) init() { r.stopCh = cos.NewStopCh() }
func (r *lazydel) stop() { r.stopCh.Close() }

// drop and drain
func (r *lazydel) cleanup() {
	clear(r.put)
	clear(r.get)
	for {
		select {
		case <-r.workCh:
		default:
			return
		}
	}
}

func (r *lazydel) waitPrev() {
	var (
		total   time.Duration
		maxWait = min(lazyDelay, 10*time.Second)
		sleep   = cos.ProbingFrequency(maxWait)
	)
	for r.running.Load() && total < maxWait {
		time.Sleep(sleep)
		total += sleep
		sleep += sleep >> 1
	}
}

func (r *lazydel) run(xreb *xs.Rebalance) {
	const prompt = "waiting for the previous lazy-delete"
	if r.running.Load() { // (unlikely)
		r.stop() // redundant; no-op
		nlog.Warningln(prompt, "[", xreb.Name(), "]")
		r.waitPrev()
	}
	if !r.running.CAS(false, true) {
		nlog.Errorln("timed out", prompt, "to exit [", xreb.Name(), "]")
		return
	}
	if xreb.IsAborted() {
		return
	}

	// (re)init
	r.put = make([]core.LIF, 0, lazySize)
	r.workCh = make(chan core.LIF, lazySize)
	r.stopCh = cos.NewStopCh()

	delay := lazyDelay
	ticker := time.NewTicker(lazyDelay)

	// run
	for {
		select {
		case lif := <-r.workCh:
			r.put = append(r.put, lif)

		case <-r.stopCh.Listen():
			goto fin

		case <-ticker.C:
			if xreb.IsAborted() {
				goto fin
			}
			// when queues remain empty
			if len(r.put) == 0 && len(r.get) == 0 {
				fintime := xreb.EndTime()
				if fintime.IsZero() {
					continue // rebalance still running && nothing to do
				}
				if time.Since(fintime) > lazyIdle {
					nlog.Infoln("idle for a while - exiting [", xreb.Name(), "]")
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
				debug.Assert(err == nil, lom.String())
				if err == nil {
					for copyFQN := range lom.GetCopies() {
						cos.RemoveFile(copyFQN)
					}
				}
				lom.Unlock(true)
			}

			// swap for the next round
			r.get = r.get[:0]
			r.get, r.put = r.put, r.get

			// speed up via a more frequent ticker
			if len(r.get) > lazySize-lazySize>>2 && delay > lazyBusy {
				delay >>= 1
				ticker.Reset(delay)
			}
		}
	}

fin:
	ticker.Stop()
	r.cleanup()
	r.running.Store(false)
}
