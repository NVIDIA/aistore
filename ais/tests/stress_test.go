// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/NVIDIA/aistore/tutils/tassert"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
)

func TestStressDeleteBucketSingle(t *testing.T) {
	var (
		workerCount          = 20
		objectCountPerWorker = 25000
		objSize              = int64(cmn.KiB)
		totalObjs            = objectCountPerWorker * workerCount
		bck                  = cmn.Bck{Name: t.Name() + "Bucket", Provider: cmn.ProviderAIS}
		proxyURL             = tutils.GetPrimaryURL()
		wg                   = &sync.WaitGroup{}
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer func() {
		startDelete := time.Now()
		tutils.DestroyBucket(t, proxyURL, bck)
		tutils.Logf("Took %v to DELETE bucket with %d objects\n", time.Since(startDelete), totalObjs)
	}()

	// Iterations of PUT
	startPut := time.Now()
	tutils.Logf("%d workers each performing PUT of %d objects of size %d\n", workerCount, objectCountPerWorker, objSize)
	for wid := 0; wid < workerCount; wid++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			reader, err := readers.NewRandReader(objSize, true)
			tassert.CheckFatal(t, err)
			objDir := tutils.RandomObjDir(10, 5)
			putRR(t, reader, bck, objDir, objectCountPerWorker)
		}()
	}
	wg.Wait()
	tutils.Logf("Took %v to PUT %d objects\n", time.Since(startPut), totalObjs)
}

func TestStressDeleteBucketMultiple(t *testing.T) {
	var (
		workerCount     = 10
		stressReps      = 5
		numObjIncrement = 2000
		objSize         = int64(cmn.KiB)
		wg              = &sync.WaitGroup{}
		bck             = cmn.Bck{Name: t.Name() + "Bucket", Provider: cmn.ProviderAIS}
		proxyURL        = tutils.GetPrimaryURL()
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	for i := 0; i < stressReps; i++ {
		numObjs := (i + 1) * numObjIncrement
		totalObjs := numObjs * workerCount

		tutils.CreateFreshBucket(t, proxyURL, bck)

		// Iterations of PUT
		startPut := time.Now()
		tutils.Logf("%d workers each performing PUT of %d objects of size %d\n", workerCount, numObjs, objSize)
		for wid := 0; wid < workerCount; wid++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				reader, err := readers.NewRandReader(objSize, true)
				tassert.CheckFatal(t, err)
				objDir := tutils.RandomObjDir(10, 5)
				putRR(t, reader, bck, objDir, numObjs)
			}()
		}
		wg.Wait()
		tutils.Logf("Took %v to PUT %d objects\n", time.Since(startPut), totalObjs)

		startDelete := time.Now()
		tutils.DestroyBucket(t, proxyURL, bck)
		tutils.Logf("Took %v to DELETE bucket with %d objects\n", time.Since(startDelete), totalObjs)
	}
}
