// Package dsort provides APIs for distributed archive file shuffling.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func calcSemaLimit(acquire, release func()) int {
	var i atomic.Int32
	wg := &sync.WaitGroup{}
	ch := make(chan int32, 200)

	for j := 0; j < 200; j++ {
		acquire()
		wg.Add(1)
		go func() {
			ch <- i.Inc()
			time.Sleep(50 * time.Microsecond)
			i.Dec()
			release()
			wg.Done()
		}()
	}

	wg.Wait()
	close(ch)

	res := int32(0)
	for c := range ch {
		res = cos.MaxI32(res, c)
	}

	return int(res)
}

var _ = Describe("newConcAdjuster", func() {
	mios := ios.NewIOStaterMock()

	BeforeEach(func() {
		err := cos.CreateDir(testingConfigDir)
		Expect(err).ShouldNot(HaveOccurred())

		fs.TestNew(mios)
		_, _ = fs.Add(testingConfigDir, "daeID")

		config := cmn.GCO.BeginUpdate()
		config.Disk.IostatTimeShort = cos.Duration(10 * time.Millisecond)
		config.Disk.DiskUtilLowWM = 70
		config.Disk.DiskUtilHighWM = 80
		config.Disk.DiskUtilMaxWM = 95
		cmn.GCO.CommitUpdate(config)
	})

	AfterEach(func() {
		err := os.RemoveAll(testingConfigDir)
		Expect(err).ShouldNot(HaveOccurred())
	})

	It("should not have more goroutines than specified", func() {
		var (
			adjuster      = newConcAdjuster(0, 3)
			expectedLimit = defaultConcFuncLimit * 3
		)

		limit := calcSemaLimit(adjuster.acquireGoroutineSema, adjuster.releaseGoroutineSema)
		Expect(limit).To(Equal(expectedLimit))
	})

	It("should not have more goroutines than specified max limit", func() {
		var (
			adjuster      = newConcAdjuster(2, 3)
			expectedLimit = 2 * 3
		)

		limit := calcSemaLimit(adjuster.acquireGoroutineSema, adjuster.releaseGoroutineSema)
		Expect(limit).To(Equal(expectedLimit))
	})

	It("should converge to perfect limit", func() {
		cfg := cmn.GCO.Get()

		var (
			perfectLimit = 13
			perfectUtil  = int(cfg.Disk.DiskUtilMaxWM+cfg.Disk.DiskUtilHighWM) / 2
		)
		availablePaths := fs.GetAvail()
		mpathInfo := availablePaths[testingConfigDir]

		adjuster := newConcAdjuster(0, 1)

		adjuster.start()
		defer adjuster.stop()

		for {
			curLimit := calcSemaLimit(func() {
				adjuster.acquireSema(mpathInfo)
			}, func() {
				adjuster.releaseSema(mpathInfo)
			})

			// If we get enough close we can just break
			if cos.Abs(curLimit-perfectLimit) <= 1 {
				break
			}

			curUtil := perfectUtil * curLimit / perfectLimit
			mios.Utils.Store(testingConfigDir, int64(curUtil))

			time.Sleep(time.Millisecond) // make sure that concurrency adjuster processed all information
		}
	})
})
