/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/pkg/client"
)

func TestRandomReaderPutStress(t *testing.T) {
	var (
		bs         = time.Now().UnixNano()
		numworkers = 1000
		numobjects = 10 // NOTE: increase this number if need be ...
		bucket     = "RRTestBucket"
		proxyURL   = getPrimaryURL(t, proxyURLRO)
		wg         = &sync.WaitGroup{}
	)
	createFreshLocalBucket(t, proxyURL, bucket)
	for i := 0; i < numworkers; i++ {
		reader, err := client.NewRandReader(fileSize, true)
		checkFatal(err, t)
		wg.Add(1)
		go putRR(t, i, proxyURL, bs, reader, wg, bucket, numobjects)
		bs++
	}
	wg.Wait()
	destroyLocalBucket(t, proxyURL, bucket)

}

func putRR(t *testing.T, id int, proxyURL string, seed int64, reader client.Reader, wg *sync.WaitGroup, bucket string, numobjects int) {
	var subdir = "dir"
	random := rand.New(rand.NewSource(seed))
	for i := 0; i < numobjects; i++ {
		fname := client.FastRandomFilename(random, fnlen)
		objname := filepath.Join(subdir, fname)
		err := client.Put(proxyURL, reader, bucket, objname, true)
		checkFatal(err, t)

		if i%100 == 0 && id%100 == 0 {
			fmt.Printf("%2d: %d\n", id, i)
		}
	}
	wg.Done()
}
