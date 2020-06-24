// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package apitests

import (
	"fmt"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
	"golang.org/x/sync/errgroup"
)

type testConfig struct {
	objectCnt uint
	bucketCnt int
	pageSize  uint
	objSize   int64
	cksumType string
}

func (tc testConfig) name() string {
	return fmt.Sprintf(
		"objs:%d/bcks:%d/page:%d/size:%d/cksum:%s",
		tc.objectCnt, tc.bucketCnt, tc.pageSize, tc.objSize, tc.cksumType,
	)
}

func createAndFillBucket(b *testing.B, bckName string, objCnt uint, objSize int64, cksumType string) (string, cmn.Bck) {
	const workerCount = 10
	var (
		bck        = cmn.Bck{Name: bckName, Provider: cmn.ProviderAIS}
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		wg         = &sync.WaitGroup{}

		objCntPerWorker = int(objCnt) / workerCount
	)

	tutils.CreateFreshBucket(b, proxyURL, bck)

	// Iterations of PUT
	for wid := uint(0); wid < workerCount; wid++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			objDir := tutils.RandomObjDir(10, 5)
			tutils.PutRR(b, baseParams, objSize, cksumType, bck, objDir, objCntPerWorker, 20)
		}()
	}
	wg.Wait()
	return proxyURL, bck
}

func BenchmarkListObject(b *testing.B) {
	tutils.CheckSkip(b, tutils.SkipTestArgs{Long: true})

	var (
		defaultCksum = cmn.DefaultBucketProps().Cksum.Type

		// TODO: Extend the number of cases.
		tests = []testConfig{
			{objectCnt: cmn.DefaultListPageSize, bucketCnt: 1, pageSize: 0, objSize: cmn.KiB, cksumType: defaultCksum},
			{objectCnt: cmn.DefaultListPageSize, bucketCnt: 10, pageSize: 0, objSize: cmn.KiB, cksumType: defaultCksum},
			{objectCnt: cmn.DefaultListPageSize, bucketCnt: 1, pageSize: 0, objSize: cmn.MiB, cksumType: defaultCksum},
			{objectCnt: cmn.DefaultListPageSize, bucketCnt: 10, pageSize: 0, objSize: cmn.MiB, cksumType: defaultCksum},

			{objectCnt: 5000, bucketCnt: 1, pageSize: cmn.DefaultListPageSize, objSize: 256, cksumType: defaultCksum},
			{objectCnt: 5000, bucketCnt: 10, pageSize: cmn.DefaultListPageSize, objSize: 256, cksumType: defaultCksum},
		}
	)
	for _, test := range tests {
		b.Run(test.name(), func(b *testing.B) {
			runner(b, test)
		})
	}
}

func runner(b *testing.B, tc testConfig) {
	var (
		bps  = make([]api.BaseParams, tc.bucketCnt)
		bcks = make([]cmn.Bck, tc.bucketCnt)
	)

	defer func() {
		for i := 0; i < tc.bucketCnt; i++ {
			tutils.DestroyBucket(b, bps[i].URL, bcks[i])
		}
	}()

	for i := 0; i < tc.bucketCnt; i++ {
		var (
			bckName       = cmn.RandString(10)
			proxyURL, bck = createAndFillBucket(b, bckName, tc.objectCnt, tc.objSize, tc.cksumType)
		)
		bps[i] = tutils.BaseAPIParams(proxyURL)
		bcks[i] = bck
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			group := &errgroup.Group{}
			for i := 0; i < tc.bucketCnt; i++ {
				var (
					bp  = bps[i]
					bck = bcks[i]
				)
				group.Go(func() error {
					msg := &cmn.SelectMsg{PageSize: tc.pageSize}
					objs, err := api.ListObjects(bp, bck, msg, tc.objectCnt)
					if err != nil {
						return err
					}
					if len(objs.Entries) != int(tc.objectCnt) {
						return fmt.Errorf("expected %d objects got %d", tc.objectCnt, len(objs.Entries))
					}
					return nil
				})
			}
			err := group.Wait()
			tassert.CheckFatal(b, err)
		}
	})
}
