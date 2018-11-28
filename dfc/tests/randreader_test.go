/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/tutils"
)

func TestRandomReaderPutStress(t *testing.T) {
	var (
		bseed      = time.Now().UnixNano()
		numworkers = 1000
		numobjects = 10 // NOTE: increase this number if need be ...
		bucket     = "RRTestBucket"
		proxyURL   = getPrimaryURL(t, proxyURLRO)
		wg         = &sync.WaitGroup{}
	)
	createFreshLocalBucket(t, proxyURL, bucket)
	for i := 0; i < numworkers; i++ {
		reader, err := tutils.NewRandReader(fileSize, true)
		tutils.CheckFatal(err, t)
		wg.Add(1)
		go func(workerId int, seed int64) {
			putRR(t, workerId, proxyURL, seed, reader, bucket, numobjects)
			wg.Done()
		}(i, bseed)
		bseed += 7
	}
	wg.Wait()
	destroyLocalBucket(t, proxyURL, bucket)
}

func putRR(t *testing.T, id int, proxyURL string, seed int64, reader tutils.Reader, bucket string, numobjects int) {
	var subdir = "dir"
	random := rand.New(rand.NewSource(seed))
	for i := 0; i < numobjects; i++ {
		fname := tutils.FastRandomFilename(random, fnlen)
		objname := filepath.Join(subdir, fname)
		err := api.PutObject(tutils.DefaultBaseAPIParams(t), bucket, objname, reader.XXHash(), reader)
		tutils.CheckFatal(err, t)

		if i%100 == 0 && id%100 == 0 {
			tutils.Logf("%2d: %d\n", id, i)
		}
	}
}
