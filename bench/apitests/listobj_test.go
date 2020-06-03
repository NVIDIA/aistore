// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package apitests

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

type testConfig struct {
	numObjectsPerBucket uint
	numBuckets          int
	objSize             int64
	cksumType           string
}

func (tc testConfig) name() string {
	return fmt.Sprintf("CntPerBck%d_Bcks%d_Size%d_cksum%s",
		tc.numObjectsPerBucket,
		tc.numBuckets,
		tc.objSize,
		tc.cksumType,
	)
}

func createAndFillBucket(b *testing.B, bckName string, objCnt uint, objSize int64, cksumType string) (string, cmn.Bck) {
	const workerCount = 10
	var (
		bck        = cmn.Bck{Name: bckName, Provider: cmn.ProviderAIS}
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		wg         = &sync.WaitGroup{}
	)
	objCntPerWorker := int(objCnt) / workerCount
	totalObjs := objCntPerWorker * workerCount

	tutils.CreateFreshBucket(b, proxyURL, bck)

	// Iterations of PUT
	startPut := time.Now()
	tutils.Logf("%d workers each performing PUT of %d objects of size %d\n", workerCount, objCntPerWorker, objSize)
	for wid := uint(0); wid < workerCount; wid++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			reader, err := readers.NewRandReader(objSize, cksumType)
			tassert.CheckFatal(b, err)
			objDir := tutils.RandomObjDir(10, 5)
			tutils.PutRR(b, baseParams, reader, bck, objDir, objCntPerWorker, 20)
		}()
	}
	wg.Wait()
	tutils.Logf("Took %v to PUT %d objects\n", time.Since(startPut), totalObjs)
	return proxyURL, bck
}

func BenchmarkListObject(b *testing.B) {
	defaultCkSum := cmn.DefaultBucketProps().Cksum.Type
	configs := []testConfig{
		{1000, 1, cmn.KiB, defaultCkSum},
		{1000, 10, cmn.KiB, defaultCkSum},
		{1000, 1, cmn.MiB, defaultCkSum},
		{1000, 10, cmn.MiB, defaultCkSum},
	}
	for _, config := range configs {
		b.Run(config.name(), func(b *testing.B) { runner(b, config) })
	}
}

func runner(b *testing.B, tc testConfig) {
	var (
		numObjectsPerBucket = tc.numObjectsPerBucket
		numBuckets          = tc.numBuckets
		objSize             = tc.objSize
		cksumType           = tc.cksumType
	)

	tutils.CheckSkip(b, tutils.SkipTestArgs{Long: true})
	proxies := make([]string, numBuckets)
	bcks := make([]cmn.Bck, numBuckets)
	for i := 0; i < numBuckets; i++ {
		elems := strings.Split(b.Name(), "/")
		name := strings.Join(elems, "_")
		bckName := fmt.Sprintf("%s_%d", name, i)
		proxyURL, bck := createAndFillBucket(b, bckName, numObjectsPerBucket, objSize, cksumType)
		proxies[i] = proxyURL
		bcks[i] = bck
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < numBuckets; i++ {
				proxyURL := proxies[i]
				bck := bcks[i]
				go func() {
					objs, err := tutils.ListObjects(proxyURL, bck, "", numObjectsPerBucket)
					tassert.CheckFatal(b, err)
					tassert.Fatalf(b, len(objs) == int(numObjectsPerBucket),
						"expected %d objects got % objects", numObjectsPerBucket, len(objs))
				}()
			}
		}
	})
}
