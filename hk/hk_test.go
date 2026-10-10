// Package hk_test provides unit tests for `hk`
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package hk_test

import (
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/hk"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Housekeeper", func() {
	It("should register the callback and fire it", func() {
		var fired atomic.Bool
		hk.Reg("foo", func(int64) time.Duration {
			fired.Store(true)
			return time.Second
		}, 0)
		defer hk.Unreg("foo")
		time.Sleep(20 * time.Millisecond)
		Expect(fired.Load()).To(BeTrue()) // callback should be fired at the start
		fired.Store(false)

		time.Sleep(500 * time.Millisecond)
		Expect(fired.Load()).To(BeFalse())

		time.Sleep(600 * time.Millisecond)
		Expect(fired.Load()).To(BeTrue())
	})

	It("should register the callback and fire it after initial interval", func() {
		var fired atomic.Bool
		hk.Reg("foo", func(int64) time.Duration {
			fired.Store(true)
			return time.Second
		}, time.Second)
		defer hk.Unreg("foo")

		time.Sleep(500 * time.Millisecond)
		Expect(fired.Load()).To(BeFalse())

		time.Sleep(600 * time.Millisecond)
		Expect(fired.Load()).To(BeTrue())
	})

	It("should register multiple callbacks and fire it in correct order", func() {
		var fired [2]atomic.Bool
		hk.Reg("foo", func(int64) time.Duration {
			fired[0].Store(true)
			return 2 * time.Second
		}, 0)
		defer hk.Unreg("foo")
		hk.Reg("bar", func(int64) time.Duration {
			fired[1].Store(true)
			return time.Second + 500*time.Millisecond
		}, 0)
		defer hk.Unreg("bar")

		time.Sleep(20 * time.Millisecond)
		// "foo" and "bar" should fire at the start (no initial interval)
		for idx := range fired {
			Expect(fired[idx].Load()).To(BeTrue())
			fired[idx].Store(false)
		}

		time.Sleep(600 * time.Millisecond) // ~600ms

		// "foo" nor "bar" should fire
		Expect(fired[0].Load() || fired[1].Load()).To(BeFalse())

		time.Sleep(time.Second) // ~1.6s

		// "bar" should fire
		Expect(fired[0].Load()).To(BeFalse())
		Expect(fired[1].Load()).To(BeTrue())
		fired[1].Store(false)

		time.Sleep(500 * time.Millisecond) // ~2.1s

		// "foo" should fire
		Expect(fired[0].Load()).To(BeTrue())
		Expect(fired[1].Load()).To(BeFalse())

		time.Sleep(time.Second) // ~3.1s

		// "bar" should fire once again
		Expect(fired[0].Load() && fired[1].Load()).To(BeTrue())
	})

	It("should unregister callback", func() {
		var fired [2]atomic.Bool
		hk.Reg("bar", func(int64) time.Duration {
			fired[0].Store(true)
			return 400 * time.Millisecond
		}, 400*time.Millisecond)
		hk.Reg("foo", func(int64) time.Duration {
			fired[1].Store(true)
			return 200 * time.Millisecond
		}, 200*time.Millisecond)

		time.Sleep(500 * time.Millisecond)
		Expect(fired[0].Load() && fired[1].Load()).To(BeTrue())

		fired[0].Store(false)
		fired[1].Store(false)
		hk.Unreg("foo")

		time.Sleep(time.Second)
		Expect(fired[1].Load()).To(BeFalse())
		Expect(fired[0].Load()).To(BeTrue())

		hk.Unreg("bar")
	})

	It("should unregister callback that returns UnregInterval", func() {
		for range 3 {
			var fired [2]atomic.Bool
			hk.Reg("bar", func(int64) time.Duration {
				fired[0].Store(true)
				return 400 * time.Millisecond
			}, 400*time.Millisecond)
			hk.Reg("foo", func(int64) time.Duration {
				fired[1].Store(true)
				return hk.UnregInterval
			}, 100*time.Millisecond)

			time.Sleep(500 * time.Millisecond)
			Expect(fired[0].Load() && fired[1].Load()).To(BeTrue())

			fired[0].Store(false)
			fired[1].Store(false)

			time.Sleep(500 * time.Millisecond)
			Expect(fired[1].Load()).To(BeFalse()) // foo
			Expect(fired[0].Load()).To(BeTrue())  // bar

			hk.Unreg("bar")
		}
	})

	It("should register and unregister multiple callbacks", func() {
		var fired atomic.Bool
		f := func(name string) {
			Expect(fired.Load()).To(BeFalse())
			hk.Reg(name, func(int64) time.Duration {
				fired.Store(true)
				return 100 * time.Millisecond
			}, 100*time.Millisecond)

			time.Sleep(110 * time.Millisecond)
			Expect(fired.Load()).To(BeTrue())

			hk.Unreg(name)
			fired.Store(false)
		}

		f("foo")
		f("bar")
		f("baz")

		time.Sleep(time.Second)
		Expect(fired.Load()).To(BeFalse())
	})

	It("should correctly call multiple callbacks", func() {
		type action struct {
			d       time.Duration
			origIdx int
		}
		const (
			actionCnt = 30
		)
		var (
			counter atomic.Int32
			durs    = make([]action, 0, actionCnt)
			fired   = make([]atomic.Int32, actionCnt)
		)

		for i := range actionCnt {
			durs = append(durs, action{
				d:       50*time.Millisecond + time.Duration(40*i)*time.Millisecond,
				origIdx: i,
			})
			fired[i].Store(-1)
		}

		rand.Shuffle(actionCnt, func(i, j int) {
			durs[i], durs[j] = durs[j], durs[i]
		})

		for i := range actionCnt {
			index := i
			hk.Reg(strconv.Itoa(index), func(int64) time.Duration {
				if fired[index].Load() == -1 {
					fired[index].Store(counter.Inc() - 1)
				}
				return durs[index].d
			}, durs[index].d)
		}

		time.Sleep(100 * actionCnt * time.Millisecond)

		for i := range actionCnt {
			Expect(durs[i].origIdx).To(BeEquivalentTo(fired[i].Load()))
		}
	})
})

func TestCallbackSendUnderPressure(t *testing.T) {
	startHK(t)

	fired := make(chan struct{})
	hk.Reg("fini", func(int64) time.Duration {
		hk.UnregIf("sibling-gone", func(int64) time.Duration { return 0 })
		close(fired)
		return hk.UnregInterval
	}, 50*time.Millisecond)

	// pile up pending ops
	release := make(chan struct{})
	hk.Reg("slow", func(int64) time.Duration { <-release; return time.Hour }, 0)
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	for i := range 4_000 {
		hk.Reg("fill-"+strconv.Itoa(i), func(int64) time.Duration { return time.Hour }, time.Hour)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("senders blocked: 4k Reg took %v", d)
	}
	close(release)

	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		t.Fatal("hk goroutine deadlocked enqueuing from its own callback")
	}
	for i := range 4_000 {
		hk.Unreg("fill-" + strconv.Itoa(i))
	}
}
