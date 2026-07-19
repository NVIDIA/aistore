// Package hk: internal unit test
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package hk

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
)

func startHK(t *testing.T) {
	Init(true)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = HK.Run()
	}()
	WaitStarted()
	t.Cleanup(func() {
		HK.Stop(nil)
		wg.Wait()
	})
}

// verifyHeapInvariants checks index consistency AND the min-heap ordering property.
func verifyHeapInvariants() error {
	n := HK.actions.Len()
	if len(HK.actions.byName) != n {
		return fmt.Errorf("index/heap size mismatch: %d vs %d", len(HK.actions.byName), n)
	}
	for name, idx := range HK.actions.byName {
		if idx < 0 || idx >= n {
			return fmt.Errorf("out of bounds index for %q: %d (len %d)", name, idx, n)
		}
		if HK.actions.heap[idx].name != name {
			return fmt.Errorf("index mapping broken: %q -> %d -> %q", name, idx, HK.actions.heap[idx].name)
		}
	}
	// Verify min-heap tree invariant: parent <= child
	for i := 1; i < n; i++ {
		parent := (i - 1) / 2
		if HK.actions.heap[parent].updateTime > HK.actions.heap[i].updateTime {
			return fmt.Errorf("min-heap invariant violated: parent [%d] (%d) > child [%d] (%d)",
				parent, HK.actions.heap[parent].updateTime, i, HK.actions.heap[i].updateTime)
		}
	}
	return nil
}

func TestChurn(t *testing.T) {
	startHK(t)

	const population = 10_000
	for i := range population {
		Reg("pop-"+strconv.Itoa(i), func(int64) time.Duration { return time.Hour }, time.Hour)
	}
	const (
		workers = 16
		perW    = 1000
	)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range perW {
				name := "churn-" + strconv.Itoa(w) + "-" + strconv.Itoa(i)
				// 1. Immediate execution (interval=0) -> returns 1 hour
				Reg(name, func(int64) time.Duration { return time.Hour }, 0)

				// 2. Try duplicate reg (~0.05% chance -> ~8 times total across the suite)
				if rand.IntN(2000) == 0 {
					Reg(name, func(int64) time.Duration { return time.Hour }, time.Hour)
				}

				// 3. Remove (alternating direct unreg and unregIf)
				if i%2 == 0 {
					Unreg(name)
				} else {
					UnregIf(name, func(int64) time.Duration { return UnregInterval })
				}
			}
		}(w)
	}
	wg.Wait()

	deadline := time.Now().Add(30 * time.Second)
	for {
		HK.mtx.Lock()
		pending := len(HK.pending)
		HK.mtx.Unlock()
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending queue not draining: %d", pending)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Index and min-heap consistency verification inside the HK goroutine
	done := make(chan error, 1)
	Reg("verify", func(int64) time.Duration {
		defer close(done)
		// population + this "verify" action (still in heap while callback runs)
		if n := HK.actions.Len(); n != population+1 {
			done <- fmt.Errorf("expected %d remaining actions, got %d", population+1, n)
			return UnregInterval
		}
		if err := verifyHeapInvariants(); err != nil {
			done <- err
			return UnregInterval
		}
		return UnregInterval
	}, time.Millisecond)

	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestTimerExecution verifies that runDue() correctly fires callbacks,
// deduplicates nextWake resets, and handles self-unregistration via UnregInterval.
func TestTimerExecution(t *testing.T) {
	startHK(t)

	const (
		numTimers  = 50
		targetReps = 3
	)
	var (
		wg      sync.WaitGroup
		fired   atomic.Int32
		cleaned atomic.Int32
	)

	wg.Add(numTimers)
	for i := range numTimers {
		name := "timer-" + strconv.Itoa(i)
		reps := 0
		// Stagger intervals between 10ms and 30ms to force frequent runDue() & nextWake updates
		ival := time.Duration(10+rand.IntN(20)) * time.Millisecond

		Reg(name, func(now int64) time.Duration {
			reps++
			fired.Inc()
			if reps >= targetReps {
				cleaned.Inc()
				wg.Done()
				return UnregInterval // Self-clean from inside runDue()
			}
			return Jitter(ival, now) // Exercise Jitter while re-arming
		}, ival)
	}

	// Wait for all timers to fire targetReps times and self-unregister
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for timers to fire and clean up (fired: %d, cleaned: %d)",
			fired.Load(), cleaned.Load())
	}

	// Final verification: heap should be completely empty
	verifyDone := make(chan error, 1)
	Reg("verify-empty", func(int64) time.Duration {
		defer close(verifyDone)
		// Only "verify-empty" should be in the heap
		if n := HK.actions.Len(); n != 1 {
			verifyDone <- fmt.Errorf("expected heap len 1 after self-cleanup, got %d", n)
			return UnregInterval
		}
		if err := verifyHeapInvariants(); err != nil {
			verifyDone <- err
		}
		return UnregInterval
	}, time.Millisecond)

	if err := <-verifyDone; err != nil {
		t.Fatal(err)
	}
}
