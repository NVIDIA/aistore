// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools"
)

func TestRandomReaderPutStress(t *testing.T) {
	var (
		numworkers = 1000
		numobjects = 10 // NOTE: increase this number if need be ...
		bck        = cmn.Bck{
			Name:     "RRTestBucket",
			Provider: apc.AIS,
		}
		proxyURL   = tools.GetPrimaryURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		wg         = &sync.WaitGroup{}
		dir        = t.Name()
		cksumType  = bck.DefaultProps(initialClusterConfig).Cksum.Type
	)

	tools.CreateBucketWithCleanup(t, proxyURL, bck, nil)

	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tools.PutRR(t, baseParams, fileSize, cksumType, bck, dir, numobjects)
		}()
	}
	wg.Wait()
}
