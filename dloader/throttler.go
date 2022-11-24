// Package dloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package dloader

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
)

var errThrottlerStopped = errors.New("throttler has been stopped")

type (
	throttler struct {
		sema    *cos.Semaphore
		emptyCh chan struct{} // Empty, closed channel (set only if `sema == nil`).

		maxBytesPerMinute int
		capacityCh        chan int
		giveBackCh        chan int
		ticker            *time.Ticker
		stopCh            *cos.StopCh
	}

	throughputThrottler interface {
		acquireAllowance(ctx context.Context, n int) error
	}

	throttledReader struct {
		t   throughputThrottler
		ctx context.Context
		r   io.ReadCloser
	}
)

func (t *throttler) init(limits Limits) {
	if limits.Connections > 0 {
		t.sema = cos.NewSemaphore(limits.Connections)
	} else {
		t.emptyCh = make(chan struct{})
		close(t.emptyCh)
	}
	if limits.BytesPerHour > 0 {
		t.initThroughputThrottling(limits.BytesPerHour / 60)
	}
}

func (t *throttler) initThroughputThrottling(maxBytesPerMinute int) {
	t.maxBytesPerMinute = maxBytesPerMinute
	t.capacityCh = make(chan int, 1)
	t.giveBackCh = make(chan int, 1)
	t.ticker = time.NewTicker(time.Minute)
	t.stopCh = cos.NewStopCh()
	go func() {
		defer func() {
			t.ticker.Stop()
			close(t.capacityCh)
		}()
		t.capacityCh <- t.maxBytesPerMinute

		// LOOP-INVARIANT: `t.capacityCh` has 1 element and `t.giveBackCh` has 0 elements.
		// LOOP-INVARIANT: `t.capacityCh` and `t.giveBackCh` can't have size > 0 at the same time.
		// Readers start to compete for resources on `t.capacityCh`.
		for {
			select {
			case <-t.stopCh.Listen():
				return
			case leftover := <-t.giveBackCh:
				// Reader took value from `t.capacityCh` and put it to `t.giveBackCh`.
				// `t.capacityCh` has 0 elements and `t.giveBackCh` has 0 elements
				// (we've just read from `t.giveBackCh`).
				if leftover > 0 {
					select {
					// By default put leftover to capacity channel.
					case t.capacityCh <- leftover:
						break
					// But if time has passed, put a big chunk.
					case <-t.ticker.C:
						t.capacityCh <- t.maxBytesPerMinute
					}
				} else {
					// Readers are faster than bandwidth, throttle here.
					<-t.ticker.C
					t.capacityCh <- t.maxBytesPerMinute
				}
				// Regardless of chosen if-branch we put 1 element to `t.capacityCh`.
			}
		}
	}()
}

func (t *throttler) tryAcquire() <-chan struct{} {
	if t.sema == nil {
		return t.emptyCh
	}
	return t.sema.TryAcquire()
}

func (t *throttler) release() {
	if t.sema == nil {
		return
	}
	t.sema.Release()
}

func (t *throttler) wrapReader(ctx context.Context, r io.ReadCloser) io.ReadCloser {
	if t.maxBytesPerMinute == 0 {
		return r
	}
	return &throttledReader{
		t:   t,
		ctx: ctx,
		r:   r,
	}
}

func (t *throttler) stop() {
	if t.ticker != nil {
		t.ticker.Stop()
	}
	if t.stopCh != nil {
		t.stopCh.Close()
	}
}

func (t *throttler) giveBack(leftoverSize int) {
	// Never waits. If we took value from `t.capacityCh`, it means that
	// `t.giveBack` was empty.
	t.giveBackCh <- leftoverSize
}

func (t *throttler) acquireAllowance(ctx context.Context, n int) error {
	select {
	case size, ok := <-t.capacityCh:
		if !ok {
			return errThrottlerStopped
		}
		t.giveBack(size - n)
		return nil
	case <-ctx.Done():
		return context.Canceled
	}
}

func (tr *throttledReader) Read(p []byte) (n int, err error) {
	if err := tr.t.acquireAllowance(tr.ctx, len(p)); err != nil {
		return 0, err
	}
	return tr.r.Read(p)
}

func (tr *throttledReader) Close() (err error) {
	return tr.r.Close()
}
