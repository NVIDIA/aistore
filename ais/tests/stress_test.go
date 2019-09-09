// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"sync"
	"testing"
	"time"

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
		bucket               = t.Name() + "Bucket"
		proxyURL             = getPrimaryURL(t, proxyURLReadOnly)
		wg                   = &sync.WaitGroup{}
		random               = cmn.NowRand()
	)

	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	tutils.CreateFreshBucket(t, proxyURL, bucket)
	defer func() {
		startDelete := time.Now()
		tutils.DestroyBucket(t, proxyURL, bucket)
		tutils.Logf("Took %v to DELETE bucket with %d total objects\n", time.Since(startDelete), totalObjs)
	}()

	// Iterations of PUT
	startPut := time.Now()
	tutils.Logf("\n%d workers each performing PUT of %d objects of size %d\n", workerCount, objectCountPerWorker, objSize)
	for wid := 0; wid < workerCount; wid++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			reader, err := tutils.NewRandReader(objSize, true)
			tassert.CheckFatal(t, err)
			objDir := tutils.RandomObjDir(random, 10, 5)
			putRR(t, reader, bucket, objDir, objectCountPerWorker)
		}()
	}
	wg.Wait()
	tutils.Logf("Took %v to PUT %d total objects\n", time.Since(startPut), totalObjs)
}

func TestStressDeleteBucketMultiple(t *testing.T) {
	var (
		workerCount     = 10
		stressReps      = 5
		numObjIncrement = 2000
		objSize         = int64(cmn.KiB)

		bucket   = t.Name() + "Bucket"
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		wg       = &sync.WaitGroup{}
		random   = cmn.NowRand()
	)

	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	for i := 0; i < stressReps; i++ {
		numObjs := (i + 1) * numObjIncrement
		totalObjs := numObjs * workerCount

		tutils.CreateFreshBucket(t, proxyURL, bucket)

		// Iterations of PUT
		startPut := time.Now()
		tutils.Logf("\n%d workers each performing PUT of %d objects of size %d\n", workerCount, numObjs, objSize)
		for wid := 0; wid < workerCount; wid++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				reader, err := tutils.NewRandReader(objSize, true)
				tassert.CheckFatal(t, err)
				objDir := tutils.RandomObjDir(random, 10, 5)
				putRR(t, reader, bucket, objDir, numObjs)
			}()
		}
		wg.Wait()
		tutils.Logf("Took %v to PUT %d total objects\n", time.Since(startPut), totalObjs)

		startDelete := time.Now()
		tutils.DestroyBucket(t, proxyURL, bucket)
		tutils.Logf("Took %v to DELETE bucket with %d total objects\n", time.Since(startDelete), totalObjs)
	}
}
