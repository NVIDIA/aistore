// Package integration_test.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/trand"
)

const (
	rwdir    = "rwstress"
	fileSize = 32 * cos.KiB
)

type opRes struct {
	op  string
	err error
}

// generates a list of random file names and a buffer to keep random data for filling up files
func generateRandomNames(fileCount int) {
	fileNames = make([]string, fileCount)
	for i := 0; i < fileCount; i++ {
		fileNames[i] = trand.String(20)
	}
}

var (
	fileNames []string
	numLoops  int
	numFiles  int
	opFuncMap = map[string]func(string, string, cmn.Bck) opRes{
		http.MethodPut:    opPut,
		http.MethodGet:    opGet,
		http.MethodDelete: opDelete,
	}
)

func parallelOpLoop(bck cmn.Bck, cksumType string,
	errCh chan opRes, opFunc func(string, string, cmn.Bck) opRes) {
	var (
		fileCount = len(fileNames)
		wg        = cos.NewLimitedWaitGroup(40, 0)
	)
	for i := 0; i < numLoops; i++ {
		for idx := 0; idx < fileCount; idx++ {
			objName := fmt.Sprintf("%s/%s", rwdir, fileNames[idx])
			wg.Add(1)
			go func(objName string) {
				defer wg.Done()
				errCh <- opFunc(objName, cksumType, bck)
			}(objName)
		}
	}
	wg.Wait()
}

func opPut(objName, cksumType string, bck cmn.Bck) opRes {
	r, err := readers.NewRand(fileSize, cksumType)
	if err != nil {
		return opRes{http.MethodPut, err}
	}
	putArgs := api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    objName,
		Cksum:      r.Cksum(),
		Reader:     r,
	}
	_, err = api.PutObject(&putArgs)
	return opRes{http.MethodPut, err}
}

func opGet(objName, _ string, bck cmn.Bck) opRes {
	_, err := api.GetObject(baseParams, bck, objName, nil)
	return opRes{http.MethodGet, err}
}

func opDelete(objName, _ string, bck cmn.Bck) opRes {
	err := api.DeleteObject(baseParams, bck, objName)
	return opRes{http.MethodDelete, err}
}

func multiOp(opNames ...string) func(string, string, cmn.Bck) opRes {
	var opr opRes
	for _, opName := range opNames {
		opr.op += opName
	}
	return func(objName, cksumType string, bck cmn.Bck) opRes {
		for _, opName := range opNames {
			opFunc := opFuncMap[opName]
			res := opFunc(objName, cksumType, bck)
			if res.err != nil {
				opr.err = res.err
				break
			}
		}
		return opr
	}
}

func reportErr(t *testing.T, errCh chan opRes, ignoreStatusNotFound bool) {
	const maxErrCount = 10
	var i int
	for opRes := range errCh {
		if opRes.err == nil {
			continue
		}
		status := api.HTTPStatus(opRes.err)
		if status == http.StatusNotFound && ignoreStatusNotFound {
			continue
		}
		i++
		if i > maxErrCount {
			t.Fatalf("%s failed %v", opRes.op, opRes.err)
			return
		}
		t.Errorf("%s failed %v", opRes.op, opRes.err)
	}
}

func initRWStress(t *testing.T, bck cmn.Bck, cksumType string) {
	errChanSize := numLoops * numFiles
	errCh := make(chan opRes, errChanSize)
	parallelOpLoop(bck, cksumType, errCh, opPut)
	close(errCh)
	reportErr(t, errCh, false)
}

func cleanRWStress(bck cmn.Bck, cksumType string) {
	errChanSize := numLoops * numFiles
	errCh := make(chan opRes, errChanSize)
	parallelOpLoop(bck, cksumType, errCh, opDelete)
	close(errCh)
	// Ignoring errors here since this is a post test cleanup
}

func parallelPutGetStress(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		if bck.IsCloud() {
			t.Skipf("skipping %s for Cloud bucket %s", t.Name(), bck.Bucket())
		}
		var (
			errChanSize = numLoops * numFiles * 2
			errCh       = make(chan opRes, errChanSize)
			cksumType   = bck.Props.Cksum.Type
			b           = bck.Clone()
		)

		initRWStress(t, b, cksumType)
		parallelOpLoop(b, cksumType, errCh, opPut)
		parallelOpLoop(b, cksumType, errCh, opGet)
		close(errCh)
		reportErr(t, errCh, false)
		cleanRWStress(b, cksumType)
	})
}

func multiOpStress(opNames ...string) func(t *testing.T) {
	return func(t *testing.T) {
		runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
			if bck.IsCloud() {
				t.Skipf("skipping %s for Cloud bucket %s", t.Name(), bck.Bucket())
			}
			var (
				errChanSize = numLoops * numFiles * 3
				errCh       = make(chan opRes, errChanSize)
				cksumType   = bck.Props.Cksum.Type
				b           = bck.Clone()
			)

			parallelOpLoop(b, cksumType, errCh, multiOp(opNames...))
			close(errCh)
			reportErr(t, errCh, true)
			cleanRWStress(b, cksumType)
		})
	}
}

// All sub-tests are skipped for GCP as GCP is flaky as most operations require backoff:
//  1. More than only 1(one) PUT per second for a single object ends with:
//     429 - backoff starts at `1 second` and increases up to `64s`
//  2. Too many requests may end with:
//     502 & 503 - backoff starts at `1 minute`
//  3. Too quick GET(HEAD) after PUT may return 404:
//     PUTGETDELETE failed {"status":404,"message":"storage: object doesn't exist","method":"GET"
//     Reason: PUT needs some time to update object version and if GET comes
//     in the middle, GET returns 404 because the new version is still processing
//
// Summing up: GCP is not suitable for any stress test, so it is skipped
func rwstress(t *testing.T) {
	generateRandomNames(numFiles)
	m := ioContext{t: t}
	m.saveCluState(tools.RandomProxyURL())
	t.Run("parallelputget", parallelPutGetStress)
	t.Run("putdelete", multiOpStress(http.MethodPut, http.MethodGet))
	t.Run("putgetdelete", multiOpStress(http.MethodPut, http.MethodGet, http.MethodDelete))
	m.checkCluState(m.smap)
}

func TestRWStressShort(t *testing.T) {
	numLoops = 8
	numFiles = 25
	rwstress(t)
}

func TestRWStress(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	numLoops = 30
	numFiles = 1000
	rwstress(t)
}
