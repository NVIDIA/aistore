// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils/readers"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/tutils"
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
	)

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	for i := 0; i < numworkers; i++ {
		reader, err := readers.NewRandReader(fileSize, true)
		tassert.CheckFatal(t, err)
		wg.Add(1)
		go func() {
			defer wg.Done()
			putRR(t, baseParams, reader, bck, dir, numobjects)
		}()
	}
	wg.Wait()
}

func putRR(t *testing.T, baseParams api.BaseParams, reader readers.Reader, bck cmn.Bck, dir string, objCount int) []string {
	var (
		objNames = make([]string, objCount)
	)
	for i := 0; i < objCount; i++ {
		fname := tutils.GenRandomString(fnlen)
		objName := filepath.Join(dir, fname)
		putArgs := api.PutObjectArgs{
			BaseParams: baseParams,
			Bck:        bck,
			Object:     objName,
			Hash:       reader.XXHash(),
			Reader:     reader,
		}
		err := api.PutObject(putArgs)
		tassert.CheckFatal(t, err)

		objNames[i] = objName
	}

	return objNames
}
