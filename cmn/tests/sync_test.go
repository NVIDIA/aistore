// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tutils"
)

func TestTimeoutGroupSmoke(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	wg := cos.NewTimeoutGroup()
	wg.Add(1)
	wg.Done()
	if wg.WaitTimeout(time.Second) {
		t.Error("wait timed out")
	}
}

func TestTimeoutGroupWait(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	wg := cos.NewTimeoutGroup()
	wg.Add(2)
	wg.Done()
	wg.Done()
	wg.Wait()
}

func TestTimeoutGroupGoroutines(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	wg := cos.NewTimeoutGroup()

	for i := 0; i < 100000; i++ {
		wg.Add(1)
		go func() {
			time.Sleep(time.Second * 2)
			wg.Done()
		}()
	}

	if wg.WaitTimeout(time.Second * 5) {
		t.Error("wait timed out")
	}
}

func TestTimeoutGroupTimeout(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	wg := cos.NewTimeoutGroup()
	wg.Add(1)

	go func() {
		time.Sleep(time.Second * 3)
		wg.Done()
	}()

	if !wg.WaitTimeout(time.Second) {
		t.Error("group did not time out")
	}

	if wg.WaitTimeout(time.Second * 3) { // now wait for actual end of the job
		t.Error("group timed out")
	}
}

func TestTimeoutGroupStop(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	wg := cos.NewTimeoutGroup()
	wg.Add(1)

	go func() {
		time.Sleep(time.Second * 3)
		wg.Done()
	}()

	if !wg.WaitTimeout(time.Second) {
		t.Error("group did not time out")
	}

	stopCh := make(chan struct{}, 1)
	stopCh <- struct{}{}

	timed, stopped := wg.WaitTimeoutWithStop(time.Second, stopCh)
	if timed {
		t.Error("group should not time out")
	}

	if !stopped {
		t.Error("group should be stopped")
	}

	if timed, stopped = wg.WaitTimeoutWithStop(time.Second*3, stopCh); timed || stopped {
		t.Error("group timed out or was stopped on finish")
	}
}

func TestTimeoutGroupStopAndTimeout(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	wg := cos.NewTimeoutGroup()
	wg.Add(1)

	go func() {
		time.Sleep(time.Second * 3)
		wg.Done()
	}()

	stopCh := make(chan struct{}, 1)
	timed, stopped := wg.WaitTimeoutWithStop(time.Second, stopCh)
	if !timed {
		t.Error("group should time out")
	}

	if stopped {
		t.Error("group should not be stopped")
	}

	if timed, stopped = wg.WaitTimeoutWithStop(time.Second*3, stopCh); timed || stopped {
		t.Error("group timed out or was stopped on finish")
	}
}

func TestSemaphore(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	sema := cos.NewSemaphore(2)
	sema.Acquire()
	sema.Acquire()

	select {
	case <-sema.TryAcquire():
		t.Error("unexpected acquire")
	default:
		break
	}

	sema.Release()

	select {
	case <-sema.TryAcquire():
	default:
		t.Error("expected acquire to happen")
	}
}

func TestDynSemaphore(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	limit := 10

	sema := cos.NewDynSemaphore(limit)

	var i atomic.Int32
	wg := &sync.WaitGroup{}
	ch := make(chan int32, 10*limit)

	for j := 0; j < 10*limit; j++ {
		sema.Acquire()
		wg.Add(1)
		go func() {
			ch <- i.Inc()
			time.Sleep(time.Millisecond)
			i.Dec()
			sema.Release()
			wg.Done()
		}()
	}

	wg.Wait()
	close(ch)

	res := int32(0)
	for c := range ch {
		res = cos.MaxI32(res, c)
	}

	if int(res) != limit {
		t.Fatalf("acutal limit %d was different than expected %d", res, limit)
	}
}
