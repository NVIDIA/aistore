// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package apitests

import (
	"fmt"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tutils"
)

type testConfig struct {
	objectCnt uint
	pageSize  uint
	useCache  bool
}

func (tc testConfig) name() string {
	return fmt.Sprintf(
		"objs:%d/use_cache:%t/page_size:%d",
		tc.objectCnt, tc.useCache, tc.pageSize,
	)
}

func createAndFillBucket(b *testing.B, objCnt uint, u string) cmn.Bck {
	const workerCount = 10
	var (
		bck        = cmn.Bck{Name: cos.RandString(10), Provider: apc.ProviderAIS}
		baseParams = tutils.BaseAPIParams(u)

		wg              = &sync.WaitGroup{}
		objCntPerWorker = int(objCnt) / workerCount
	)

	tutils.CreateBucketWithCleanup(b, baseParams.URL, bck, nil)

	// Iterations of PUT
	for wid := uint(0); wid < workerCount; wid++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			objDir := tutils.RandomObjDir(10, 5)
			tutils.PutRR(b, baseParams, 128, cos.ChecksumXXHash, bck, objDir, objCntPerWorker)
		}()
	}
	wg.Wait()
	return bck
}

func BenchmarkListObject(b *testing.B) {
	tutils.CheckSkip(b, tutils.SkipTestArgs{Long: true})
	u := "http://127.0.0.1:8080"
	tests := []testConfig{
		{objectCnt: 1_000, pageSize: 10, useCache: false},
		{objectCnt: 1_000, pageSize: 10, useCache: true},

		{objectCnt: 10_000, pageSize: 100, useCache: false},
		{objectCnt: 10_000, pageSize: 100, useCache: true},

		{objectCnt: 10_000, pageSize: 10_000, useCache: false},
		{objectCnt: 10_000, pageSize: 10_000, useCache: true},

		// Hardcore cases, use only when needed.
		// {objectCnt: 100_000, pageSize: 10_000, useCache: true},
		// {objectCnt: 1_000_000, pageSize: 10_000, useCache: true},
	}
	for _, test := range tests {
		b.Run(test.name(), func(b *testing.B) {
			var (
				bck        = createAndFillBucket(b, test.objectCnt, u)
				baseParams = tutils.BaseAPIParams(u)
			)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				msg := &cmn.ListObjsMsg{PageSize: test.pageSize}
				if test.useCache {
					msg.SetFlag(cmn.UseListObjsCache)
				}
				objs, err := api.ListObjects(baseParams, bck, msg, 0)
				tassert.CheckFatal(b, err)
				tassert.Errorf(
					b, len(objs.Entries) == int(test.objectCnt),
					"expected %d objects got %d", test.objectCnt, len(objs.Entries),
				)
			}
			b.StopTimer() // Otherwise we will include `DestroyBucket`.
		})
	}
}
