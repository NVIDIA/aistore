/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn_test

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

func TestTimeoutGroupSmoke(t *testing.T) {
	wg := cmn.NewTimeoutGroup()
	wg.Add(1)
	wg.Done()
	if wg.WaitTimeout(time.Second) {
		t.Error("wait timed out")
	}
}

func TestTimeoutGroupWait(t *testing.T) {
	wg := cmn.NewTimeoutGroup()
	wg.Add(2)
	wg.Done()
	wg.Done()
	wg.Wait()
}

func TestTimeoutGroupGoroutines(t *testing.T) {
	wg := cmn.NewTimeoutGroup()

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
	wg := cmn.NewTimeoutGroup()
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
	wg := cmn.NewTimeoutGroup()
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
	wg := cmn.NewTimeoutGroup()
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
