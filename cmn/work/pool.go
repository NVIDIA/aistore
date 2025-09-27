// Package work provides a bounded worker pool utility for concurrent processing of any kind.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package work

import (
	"sync"
	"sync/atomic"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// The pool maintains a fixed number of worker goroutines and a bounded work channel.
// Currently, work items are strings; the type can be easily changed to `any`
// (or anything else) if and when required.
//
// The main method is `TrySubmit` intended for non-blocking (best-effort type) submission.
// In addition:
// - Stop() - to terminate the pool
// - Wait() - to wait for all (pooled) goroutines to exit

type Callback func(item string)

type Pool struct {
	workCh chan string
	cb     Callback
	abort  <-chan error
	stop   *cos.StopCh
	cnt    atomic.Int64
	wg     sync.WaitGroup
}

// Other than self-explanatory parameters, the `abort <-chan error` is
// xaction's `xact.Base.ChanAbort()`
func New(numWorkers, chanCap int, cb Callback, abort <-chan error) *Pool {
	p := &Pool{
		workCh: make(chan string, chanCap),
		cb:     cb,
		abort:  abort,
		stop:   cos.NewStopCh(),
	}
	for range numWorkers {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

func (p *Pool) worker() {
	defer p.wg.Done()

	for {
		select {
		case item := <-p.workCh:
			p.cb(item)
			p.cnt.Add(1)
		case <-p.abort:
			return
		case <-p.stop.Listen():
			return
		}
	}
}

func (p *Pool) TrySubmit(item string) bool {
	select {
	case p.workCh <- item:
		return true
	default:
		return false
	}
}

func (p *Pool) NumDone() int64 { return p.cnt.Load() }
func (p *Pool) Stop()          { p.stop.Close() }
func (p *Pool) Wait()          { p.wg.Wait() }
func (p *Pool) IsBusy() bool   { return p.stop.Stopped() || len(p.workCh) == cap(p.workCh) }
