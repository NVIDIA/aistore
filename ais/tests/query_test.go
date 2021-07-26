// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/query"
	jsoniter "github.com/json-iterator/go"
)

func checkQueryDone(t *testing.T, handle string) {
	baseParams = tutils.BaseAPIParams(proxyURL)

	_, err := api.NextQueryResults(baseParams, handle, 1)
	tassert.Fatalf(t, err != nil, "expected an error to occur")
	tassert.Errorf(t, cmn.IsStatusGone(err), "expected 410 on finished query")
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
		queryObjectNames = make(cos.StringSet, numObjects-1)
	)

	tutils.CreateFreshBucket(t, proxyURL, bck, nil)

	putObjects := tutils.PutRR(t, baseParams, cos.KiB, cos.ChecksumNone, bck, "", numObjects)
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
			requestedCnt = cos.Min(chunkSize, objectsLeft)
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
		queryObjectNames = make(cos.StringSet, numObjects-1)
	)

	tutils.CreateFreshBucket(t, proxyURL, bck, nil)

	objName := fmt.Sprintf("object-%d.txt", 0)
	putRandomFile(t, baseParams, bck, objName, cos.KiB)
	// increase version of object-0.txt, so filter should discard it
	putRandomFile(t, baseParams, bck, objName, cos.KiB)
	for i := 1; i < numObjects; i++ {
		objName = fmt.Sprintf("object-%d.txt", i)
		queryObjectNames.Add(objName)
		putRandomFile(t, baseParams, bck, objName, cos.KiB)
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
		queryObjectNames = make(cos.StringSet, numObjects-1)
	)

	tutils.CreateFreshBucket(t, proxyURL, bck, nil)

	objName := fmt.Sprintf("object-%d.txt", 0)
	putRandomFile(t, baseParams, bck, objName, cos.KiB)
	// increase version of object-0.txt, so filter should discard it
	putRandomFile(t, baseParams, bck, objName, cos.KiB)
	for i := 1; i < numObjects; i++ {
		objName = fmt.Sprintf("object-%d.txt", i)
		queryObjectNames.Add(objName)
		putRandomFile(t, baseParams, bck, objName, cos.KiB)
	}

	timestamp := time.Now()

	// object with Atime > timestamp
	objName = fmt.Sprintf("object-%d.txt", numObjects+1)
	putRandomFile(t, baseParams, bck, objName, cos.KiB)

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

func TestQueryWorkersTargets(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     "TESTQUERYWORKERBUCKET",
			Provider: cmn.ProviderAIS,
		}
		smapDaemonIDs = cos.StringSet{}
		objName       = "object.txt"
	)

	tutils.CreateFreshBucket(t, proxyURL, bck, nil)

	smap, err := api.GetClusterMap(baseParams)
	tassert.CheckError(t, err)
	for _, t := range smap.Tmap {
		smapDaemonIDs.Add(t.DaemonID)
	}

	putRandomFile(t, baseParams, bck, objName, cos.KiB)
	handle, err := api.InitQuery(baseParams, "", bck, nil, uint(smap.CountActiveTargets()))
	tassert.CheckFatal(t, err)
	for i := 1; i <= smap.CountActiveTargets(); i++ {
		daemonID, err := api.QueryWorkerTarget(baseParams, handle, uint(i))
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, smapDaemonIDs.Contains(daemonID), "unexpected daemonID %s", daemonID)
		smapDaemonIDs.Delete(daemonID)
	}

	entries, err := api.NextQueryResults(baseParams, handle, 1)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(entries) == 1, "expected query to have 1 object, got %d", len(entries))
	tassert.Errorf(t, entries[0].Name == objName, "expected object name to be %q, got %q", objName, entries[0].Name)
}

func TestQueryWorkersTargetDown(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     "TESTQUERYWORKERBUCKET",
			Provider: cmn.ProviderAIS,
		}
		smapDaemonIDs = cos.StringSet{}
		objName       = "object.txt"
	)

	tutils.CreateFreshBucket(t, proxyURL, bck, nil)

	smap, err := api.GetClusterMap(baseParams)
	tassert.CheckError(t, err)
	for _, t := range smap.Tmap {
		smapDaemonIDs.Add(t.DaemonID)
	}

	putRandomFile(t, baseParams, bck, objName, cos.KiB)
	handle, err := api.InitQuery(baseParams, "", bck, nil, uint(smap.CountActiveTargets()))
	tassert.CheckFatal(t, err)

	_, err = api.QueryWorkerTarget(baseParams, handle, 1)
	tassert.CheckFatal(t, err)

	target, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	argsMnt := &cmn.ActValRmNode{DaemonID: target.ID(), SkipRebalance: true}
	_, err = api.StartMaintenance(baseParams, argsMnt)
	tassert.CheckFatal(t, err)

	defer func() {
		rebID, err := api.StopMaintenance(baseParams, argsMnt)
		tassert.CheckFatal(t, err)
		args := api.XactReqArgs{ID: rebID, Kind: cmn.ActRebalance, Timeout: rebalanceTimeout}
		api.WaitForXaction(baseParams, args)
	}()

	_, err = tutils.WaitForClusterState(
		proxyURL,
		"target is gone",
		smap.Version,
		smap.CountActiveProxies(),
		smap.CountActiveTargets()-1,
	)
	tassert.CheckFatal(t, err)

	_, err = api.QueryWorkerTarget(baseParams, handle, 1)
	tassert.Errorf(t, err != nil, "expected error to occur when target went down")
}

func TestQuerySingleWorkerNext(t *testing.T) {
	var (
		baseParams = tutils.BaseAPIParams()
		m          = ioContext{
			t:        t,
			num:      100,
			fileSize: 5 * cos.KiB,
		}
	)

	m.init()
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)

	m.puts()

	smap, err := api.GetClusterMap(baseParams)
	tassert.CheckError(t, err)

	handle, err := api.InitQuery(baseParams, "", m.bck, nil, uint(smap.CountActiveTargets()))
	tassert.CheckFatal(t, err)

	si, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	baseParams.URL = si.URL(cmn.NetworkPublic)

	buf := bytes.NewBuffer(nil)
	err = api.DoHTTPRequest(api.ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathQueryNext.S,
		Body:       cos.MustMarshal(query.NextMsg{Handle: handle, Size: 10}),
	}, buf)
	tassert.CheckFatal(t, err)

	objList := &cmn.BucketList{}
	err = jsoniter.Unmarshal(buf.Bytes(), objList)
	tassert.CheckFatal(t, err)
}
