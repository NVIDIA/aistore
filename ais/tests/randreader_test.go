// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestRandomReaderPutStress(t *testing.T) {
	var (
		numworkers = 1000
		numobjects = 10 // NOTE: increase this number if need be ...
		bck        = cmn.Bck{
			Name:     "RRTestBucket",
			Provider: cmn.ProviderAIS,
		}
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		wg         = &sync.WaitGroup{}
		dir        = t.Name()
		cksumType  = cmn.DefaultBucketProps().Cksum.Type
	)

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	for i := 0; i < numworkers; i++ {
		reader, err := readers.NewRandReader(fileSize, cksumType)
		tassert.CheckFatal(t, err)
		wg.Add(1)
		go func() {
			defer wg.Done()
			tutils.PutRR(t, baseParams, reader, bck, dir, numobjects, fnlen)
		}()
	}
	wg.Wait()
}
