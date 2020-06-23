// Package hk provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package hk

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Housekeeper", func() {
	BeforeEach(func() {
		initCleaner()
	})

	It("should register the callback and fire it", func() {
		fired := false
		Housekeeper.Register("", func() time.Duration {
			fired = true
			return time.Second
		})

		time.Sleep(20 * time.Millisecond)
		Expect(fired).To(BeTrue()) // callback should be fired at the start
		fired = false

		time.Sleep(500 * time.Millisecond)
		Expect(fired).To(BeFalse())

		time.Sleep(600 * time.Millisecond)
		Expect(fired).To(BeTrue())
	})

	It("should register the callback and fire it after initial interval", func() {
		fired := false
		Housekeeper.Register("", func() time.Duration {
			fired = true
			return time.Second
		}, time.Second)

		time.Sleep(500 * time.Millisecond)
		Expect(fired).To(BeFalse())

		time.Sleep(600 * time.Millisecond)
		Expect(fired).To(BeTrue())
	})

	It("should register multiple callbacks and fire it in correct order", func() {
		fired := make([]bool, 2)
		Housekeeper.Register("", func() time.Duration {
			fired[0] = true
			return 2 * time.Second
		})
		Housekeeper.Register("", func() time.Duration {
			fired[1] = true
			return time.Second
		})

		time.Sleep(20 * time.Millisecond)
		for idx := 0; idx < len(fired); idx++ {
			Expect(fired[idx]).To(BeTrue()) // callback should be fired at the start
			fired[idx] = false
		}

		time.Sleep(500 * time.Millisecond)
		Expect(fired[0] || fired[1]).To(BeFalse()) // no callback should be fired

		time.Sleep(600 * time.Millisecond)
		Expect(fired[1]).To(BeTrue()) // the later callback has shorter duration
		Expect(fired[0]).To(BeFalse())

		time.Sleep(time.Second)
		Expect(fired[0] && fired[1]).To(BeTrue()) // after ~2sec both callback should fire
	})

	It("should unregister callback", func() {
		fired := make([]bool, 2)
		Housekeeper.Register("second", func() time.Duration {
			fired[0] = true
			return 400 * time.Millisecond
		})
		Housekeeper.Register("first", func() time.Duration {
			fired[1] = true
			return 200 * time.Millisecond
		})

		time.Sleep(time.Second)
		Expect(fired[0] && fired[1]).To(BeTrue()) // after ~2sec both callback should fire

		fired[0] = false
		fired[1] = false
		Housekeeper.Unregister("first")

		time.Sleep(time.Second)
		Expect(fired[1]).To(BeFalse())
		Expect(fired[0]).To(BeTrue())

		Housekeeper.Unregister("second")
	})
})
