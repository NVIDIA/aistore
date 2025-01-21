// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package apitests_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"
)

type testConfig struct {
	objectCnt int
	pageSize  int
}

func (tc testConfig) name() string {
	return fmt.Sprintf(
		"objs:%d/page_size:%d",
		tc.objectCnt, tc.pageSize,
	)
}

func createAndFillBucket(b *testing.B, objCnt int, u string) cmn.Bck {
	const workerCount = 10
	var (
		bck        = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		baseParams = tools.BaseAPIParams(u)

		wg              = &sync.WaitGroup{}
		objCntPerWorker = objCnt / workerCount
	)

	tools.CreateBucket(b, baseParams.URL, bck, nil, true /*cleanup*/)

	// Iterations of PUT
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			objDir := tools.RandomObjDir(10, 5)
			tools.PutRR(b, baseParams, 128, cos.ChecksumXXHash, bck, objDir, objCntPerWorker)
		}()
	}
	wg.Wait()
	return bck
}

func BenchmarkListObject(b *testing.B) {
	tools.CheckSkip(b, &tools.SkipTestArgs{Long: true})
	u := "http://127.0.0.1:8080"
	tests := []testConfig{
		{objectCnt: 1_000, pageSize: 10},
		{objectCnt: 10_000, pageSize: 100},
		{objectCnt: 10_000, pageSize: 10_000},
		// Hardcore cases, use only when needed.
		// {objectCnt: 100_000, pageSize: 10_000},
		// {objectCnt: 1_000_000, pageSize: 10_000},
	}
	for _, test := range tests {
		b.Run(test.name(), func(b *testing.B) {
			var (
				bck        = createAndFillBucket(b, test.objectCnt, u)
				baseParams = tools.BaseAPIParams(u)
			)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				msg := &apc.LsoMsg{PageSize: int64(test.pageSize)}
				objs, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
				tassert.CheckFatal(b, err)
				tassert.Errorf(
					b, len(objs.Entries) == test.objectCnt,
					"expected %d objects got %d", test.objectCnt, len(objs.Entries),
				)
			}
			b.StopTimer() // Otherwise we will include `DestroyBucket`.
		})
	}
}
