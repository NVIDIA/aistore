// Package integration_test.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

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

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	for range numworkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tools.PutRR(t, baseParams, fileSize, cksumType, bck, dir, numobjects)
		}()
	}
	wg.Wait()
}
