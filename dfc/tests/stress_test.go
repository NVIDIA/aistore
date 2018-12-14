/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc_test

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/tutils"
)

func TestStressDeleteBucketSingle(t *testing.T) {
	var (
		workerCount          = 20
		objectCountPerWorker = 25000
		objSize              = int64(cmn.KiB)
		totalObjs            = objectCountPerWorker * workerCount

		bucket   = t.Name() + "Bucket"
		proxyURL = getPrimaryURL(t, proxyURLRO)
		wg       = &sync.WaitGroup{}
		random   = rand.New(rand.NewSource(time.Now().UnixNano()))
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	createFreshLocalBucket(t, proxyURL, bucket)
	defer func() {
		startDelete := time.Now()
		destroyLocalBucket(t, proxyURL, bucket)
		tutils.Logf("Took %v to DELETE bucket with %d total objects\n", time.Since(startDelete), totalObjs)
	}()

	// Iterations of PUT
	startPut := time.Now()
	tutils.Logf("\n%d workers each performing PUT of %d objects of size %d\n", workerCount, objectCountPerWorker, objSize)
	for wid := 0; wid < workerCount; wid++ {
		wg.Add(1)
		go func(wid int) {
			reader, err := tutils.NewRandReader(objSize, true)
			tutils.CheckFatal(err, t)
			objDir := tutils.RandomObjDir(random, 10, 5)
			putRR(t, wid, proxyURL, reader, bucket, objDir, objectCountPerWorker)
			wg.Done()
		}(wid)
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
		proxyURL = getPrimaryURL(t, proxyURLRO)
		wg       = &sync.WaitGroup{}
		random   = rand.New(rand.NewSource(time.Now().UnixNano()))
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	for i := 0; i < stressReps; i++ {
		numObjs := (i + 1) * numObjIncrement
		totalObjs := numObjs * workerCount

		createFreshLocalBucket(t, proxyURL, bucket)

		// Iterations of PUT
		startPut := time.Now()
		tutils.Logf("\n%d workers each performing PUT of %d objects of size %d\n", workerCount, numObjs, objSize)
		for wid := 0; wid < workerCount; wid++ {
			wg.Add(1)
			go func(wid int) {
				reader, err := tutils.NewRandReader(objSize, true)
				tutils.CheckFatal(err, t)
				objDir := tutils.RandomObjDir(random, 10, 5)
				putRR(t, wid, proxyURL, reader, bucket, objDir, numObjs)
				wg.Done()
			}(wid)
		}
		wg.Wait()
		tutils.Logf("Took %v to PUT %d total objects\n", time.Since(startPut), totalObjs)

		startDelete := time.Now()
		destroyLocalBucket(t, proxyURL, bucket)
		tutils.Logf("Took %v to DELETE bucket with %d total objects\n", time.Since(startDelete), totalObjs)
	}
}
