/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn_test

import (
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/cmn"
)

func TestTimeoutGroupSmoke(t *testing.T) {
	wg := cmn.NewTimeoutGroup()
	wg.Add(1)
	wg.Done()
	if wg.WaitTimeout(time.Second) {
		t.Error("wait timed out")
	}
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
