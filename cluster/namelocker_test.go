// Package cluster provides common interfaces and local access to cluster-level metadata.
/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"
)

func TestUpgradeLock(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping %s in short mode", t.Name())
	}
	for _, test := range []string{"downgrade", "unlock"} {
		t.Run(test, func(t *testing.T) {
			const threadCnt = 10000
			var (
				n       = &nlc{}
				wg      = &sync.WaitGroup{}
				sema    = cos.NewDynSemaphore(threadCnt)
				counter = atomic.NewInt32(0)
				uname   = trand.String(10)
			)
			n.init()

			// Additional stray reader which forces the other to block on `UpgradeLock`.
			n.Lock(uname, false)

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

					// Imitate doing job.
					counter.Inc()
					time.Sleep(time.Second)

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

			// Make sure all other threads are past `n.Lock` and blocked on `sema.Acquire`.
			time.Sleep(500 * time.Millisecond)

			sema.Release(threadCnt)
			// Wait a while to make sure that all goroutines started and blocked on `UpgradeLock`.
			time.Sleep(500 * time.Millisecond)

			// Should wake up one of the waiter which should start doing job.
			n.Unlock(uname, false)

			wg.Wait()
			tassert.Fatalf(t, counter.Load() == 1, "counter should be equal to 1 (counter: %d)", counter.Load())
		})
	}
}
