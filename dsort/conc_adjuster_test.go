// Package dsort provides APIs for distributed archive file shuffling.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"math"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func calcSemaLimit(acquire func(), release func()) int64 {
	var i atomic.Int32
	wg := &sync.WaitGroup{}
	ch := make(chan int32, 200)

	for j := 0; j < int(200); j++ {
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
		res = cmn.MaxI32(res, c)
	}

	return int64(res)
}

var _ = Describe("newConcAdjuster", func() {
	BeforeEach(func() {
		err := cmn.CreateDir(testingConfigDir)
		Expect(err).ShouldNot(HaveOccurred())

		fs.Mountpaths = fs.NewMountedFS()
		fs.Mountpaths.Add(testingConfigDir)
	})

	AfterEach(func() {
		err := os.RemoveAll(testingConfigDir)
		Expect(err).ShouldNot(HaveOccurred())
	})

	It("should not have more goroutines than specified", func() {
		var (
			adjuster      = newConcAdjuster(10, 3)
			expectedLimit = int64(30) // 10 * 3 * num_paths (num_paths = 1)
		)

		limit := calcSemaLimit(adjuster.acquireGoroutineSema, adjuster.releaseGoroutineSema)
		Expect(limit).To(Equal(expectedLimit))
	})

	It("should converge to perfect limit by increasing limit", func() {
		var (
			perfectLimit      int64 = 20
			perfectThroughput int64 = 1000

			defaultLimit int64 = 10
		)
		availablePaths, _ := fs.Mountpaths.Get()
		mpathInfo := availablePaths[testingConfigDir]

		adjuster := newConcAdjuster(defaultLimit, defaultLimit)

		adjuster.start()
		defer adjuster.stop()

		for {
			curLimit := calcSemaLimit(func() {
				adjuster.acquireSema(mpathInfo)
			}, func() {
				adjuster.releaseSema(mpathInfo)
			})

			// If we get enough close we can just break
			if math.Abs(float64(curLimit-perfectLimit)) < 2 {
				break
			}

			curThroughput := perfectThroughput * curLimit / perfectLimit

			adjuster.inform(curThroughput, time.Second, mpathInfo)
			time.Sleep(10 * time.Millisecond) // make sure that `adjuster.inform` processed all information
		}
	})

	It("should converge to perfect limit by decreasing limit", func() {
		var (
			perfectLimit      int64 = 10
			perfectThroughput int64 = 1000

			defaultLimit int64 = 20
		)
		availablePaths, _ := fs.Mountpaths.Get()
		mpathInfo := availablePaths[testingConfigDir]

		adjuster := newConcAdjuster(defaultLimit, defaultLimit)

		adjuster.start()
		defer adjuster.stop()

		for {
			curLimit := calcSemaLimit(func() {
				adjuster.acquireSema(mpathInfo)
			}, func() {
				adjuster.releaseSema(mpathInfo)
			})

			// If we get enough close we can just break
			if math.Abs(float64(curLimit-perfectLimit)) < 2 {
				break
			}

			curThroughput := perfectThroughput * perfectLimit / curLimit
			adjuster.inform(curThroughput, time.Second, mpathInfo)
			time.Sleep(10 * time.Millisecond) // make sure that `adjuster.inform` processed all information
		}
	})
})
