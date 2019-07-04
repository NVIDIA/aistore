// Package housekeeper provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package housekeeper

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Housekeeper", func() {
	BeforeEach(func() {
		initCleaner()
	})

	AfterEach(func() {
		Housekeeper.Stop(nil)
	})

	It("should register the callback and fire it", func() {
		fired := false
		Housekeeper.Register(func() time.Duration {
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

	It("should register multiple callbacks and fire it in correct order", func() {
		fired := make([]bool, 2)
		Housekeeper.Register(func() time.Duration {
			fired[0] = true
			return 2 * time.Second
		})
		Housekeeper.Register(func() time.Duration {
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
})
