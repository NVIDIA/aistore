// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/query"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func checkQueryDone(t *testing.T, handle string) {
	baseParams = tutils.BaseAPIParams(proxyURL)

	_, err := api.NextQueryResults(baseParams, handle, 1)
	tassert.Fatalf(t, err != nil, "expected an error to occur")
	httpErr, ok := err.(*cmn.HTTPError)
	tassert.Errorf(t, ok, "expected the error to be an http error")
	tassert.Errorf(t, httpErr.Status == http.StatusGone, "expected 410 on finished query")
}

func TestQueryBck(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     "TESTQUERYBUCKET",
			Provider: cmn.ProviderAIS,
		}
		numObjects       = 1000
		chunkSize        = 50
		queryObjectNames = make(cmn.StringSet, numObjects-1)
	)

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	putObjects := tutils.PutRR(t, baseParams, cmn.KiB, cmn.ChecksumNone, bck, "", numObjects, fnlen)
	sort.Strings(putObjects)
	for _, obj := range putObjects {
		queryObjectNames.Add(obj)
	}

	handle, err := api.InitQuery(baseParams, "", bck, nil)
	tassert.CheckFatal(t, err)

	objectsLeft := numObjects
	for objectsLeft > 0 {
		var (
			// Get random proxy for next request to simulate load balancer.
			baseParams   = tutils.BaseAPIParams()
			requestedCnt = cmn.Min(chunkSize, objectsLeft)
		)
		objects, err := api.NextQueryResults(baseParams, handle, uint(requestedCnt))
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, len(objects) == requestedCnt, "expected %d to be returned, got %d", requestedCnt, len(objects))
		objectsLeft -= requestedCnt

		for _, object := range objects {
			tassert.Fatalf(t, queryObjectNames.Contains(object.Name), "unexpected object %s", object.Name)
			queryObjectNames.Delete(object.Name)
		}
	}

	checkQueryDone(t, handle)
}

func TestQueryVersionFilter(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     "TESTQUERYBUCKET",
			Provider: cmn.ProviderAIS,
		}
		numObjects       = 10
		queryObjectNames = make(cmn.StringSet, numObjects-1)
	)

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	objName := fmt.Sprintf("object-%d.txt", 0)
	putRandomFile(t, baseParams, bck, objName, cmn.KiB)
	// increase version of object-0.txt, so filter should discard it
	putRandomFile(t, baseParams, bck, objName, cmn.KiB)
	for i := 1; i < numObjects; i++ {
		objName = fmt.Sprintf("object-%d.txt", i)
		queryObjectNames.Add(objName)
		putRandomFile(t, baseParams, bck, objName, cmn.KiB)
	}

	filter := query.VersionLEFilterMsg(1)
	handle, err := api.InitQuery(baseParams, "object-{0..100}.txt", bck, filter)
	tassert.CheckFatal(t, err)

	objectsNames, err := api.NextQueryResults(baseParams, handle, uint(numObjects))
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(objectsNames) == numObjects-1, "expected %d to be returned, got %d", numObjects-1, len(objectsNames))
	for _, object := range objectsNames {
		tassert.Errorf(t, queryObjectNames.Contains(object.Name), "unexpected object %s", objName)
		queryObjectNames.Delete(object.Name)
	}

	checkQueryDone(t, handle)
}

func TestQueryVersionAndAtime(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     "TESTQUERYBUCKET",
			Provider: cmn.ProviderAIS,
		}
		numObjects       = 10
		queryObjectNames = make(cmn.StringSet, numObjects-1)
	)

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	objName := fmt.Sprintf("object-%d.txt", 0)
	putRandomFile(t, baseParams, bck, objName, cmn.KiB)
	// increase version of object-0.txt, so filter should discard it
	putRandomFile(t, baseParams, bck, objName, cmn.KiB)
	for i := 1; i < numObjects; i++ {
		objName = fmt.Sprintf("object-%d.txt", i)
		queryObjectNames.Add(objName)
		putRandomFile(t, baseParams, bck, objName, cmn.KiB)
	}

	timestamp := time.Now()

	// object with Atime > timestamp
	objName = fmt.Sprintf("object-%d.txt", numObjects+1)
	putRandomFile(t, baseParams, bck, objName, cmn.KiB)

	filter := query.NewAndFilter(query.VersionLEFilterMsg(1), query.ATimeBeforeFilterMsg(timestamp))
	handle, err := api.InitQuery(baseParams, "object-{0..100}.txt", bck, filter)
	tassert.CheckFatal(t, err)

	objectsNames, err := api.NextQueryResults(baseParams, handle, uint(numObjects))
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(objectsNames) == numObjects-1, "expected %d to be returned, got %d", numObjects-1, len(objectsNames))
	for _, object := range objectsNames {
		tassert.Errorf(t, queryObjectNames.Contains(object.Name), "unexpected object %s", objName)
		queryObjectNames.Delete(object.Name)
	}

	checkQueryDone(t, handle)
}
