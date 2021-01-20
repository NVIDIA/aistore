// Package cluster provides common interfaces and local access to cluster-level metadata.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
)

func TestUpgradeLock(t *testing.T) {
	for _, test := range []string{"downgrade", "unlock"} {
		t.Run(test, func(t *testing.T) {
			const threadCnt = 10000
			var (
				n       = &nlc{}
				wg      = &sync.WaitGroup{}
				sema    = cmn.NewDynSemaphore(threadCnt)
				counter = atomic.NewInt32(0)
				uname   = cmn.RandString(10)
			)
			n.init()

			sema.Acquire(threadCnt)
			wg.Add(threadCnt)
			for i := 0; i < threadCnt; i++ {
				go func() {
					defer wg.Done()

					n.Lock(uname, false)

					sema.Acquire()
					if finished := n.UpgradeLock(uname); finished {
						tassert.Fatalf(t, counter.Load() > 0, "counter should be already updated")
						n.Unlock(uname, false)
						return
					}
					counter.Inc()
					switch test {
					case "downgrade":
						n.DowngradeLock(uname)
						n.Unlock(uname, false)
					case "unlock":
						n.Unlock(uname, true)
					default:
						panic(test)
					}
				}()
			}

			// Wait a while to make sure that all goroutines started and blocked on semaphore.
			time.Sleep(time.Second)
			sema.Release(threadCnt)

			wg.Wait()
			tassert.Fatalf(t, counter.Load() == 1, "counter should be equal to 1")
		})
	}
}
