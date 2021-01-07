// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools"
	"github.com/NVIDIA/aistore/devtools/tutils/readers"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
	"github.com/NVIDIA/aistore/stats"
)

const (
	// This value is holds the input of 'proxyURLFlag' from init_tests.go.
	// It is used in BaseAPIParams to determine if the cluster is running
	// on a
	// 	1. local instance (no docker) 	- works
	//	2. local docker instance		- works
	// 	3. AWS-deployed cluster 		- not tested (but runs mainly with Ansible)
	MockDaemonID       = "MOCK"
	proxyChangeLatency = 2 * time.Minute
	dsortFinishTimeout = 6 * time.Minute

	waitClusterStartup = 20 * time.Second
)

const (
	evictPrefetchTimeout = 2 * time.Minute
)

func Del(proxyURL string, bck cmn.Bck, object string, wg *sync.WaitGroup, errCh chan error, silent bool) error {
	if wg != nil {
		defer wg.Done()
	}
	if !silent {
		fmt.Printf("DEL: %s\n", object)
	}
	baseParams := BaseAPIParams(proxyURL)
	err := api.DeleteObject(baseParams, bck, object)
	if err != nil && errCh != nil {
		errCh <- err
	}
	return err
}

func CheckObjExists(proxyURL string, bck cmn.Bck, objName string) bool {
	baseParams := BaseAPIParams(proxyURL)
	_, err := api.HeadObject(baseParams, bck, objName, true /*checkExists*/)
	return err == nil
}

// Put sends a PUT request to the given URL
func Put(proxyURL string, bck cmn.Bck, object string, reader readers.Reader, errCh chan error) {
	baseParams := BaseAPIParams(proxyURL)
	putArgs := api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bck,
		Object:     object,
		Cksum:      reader.Cksum(),
		Reader:     reader,
	}
	err := api.PutObject(putArgs)
	if errCh == nil {
		if err != nil {
			fmt.Println("Error channel is not given, do not know how to report error", err)
		}
		return
	}
	if err != nil {
		errCh <- err
	}
}

// PutObject sends a PUT request to the given URL.
func PutObject(t *testing.T, bck cmn.Bck, objName string, reader readers.Reader) {
	var (
		proxyURL = RandomProxyURL()
		errCh    = make(chan error, 1)
	)
	Put(proxyURL, bck, objName, reader, errCh)
	tassert.SelectErr(t, errCh, "put", true)
}

// ListObjectNames returns a slice of object names of all objects that match the prefix in a bucket
func ListObjectNames(proxyURL string, bck cmn.Bck, prefix string, objectCountLimit uint) ([]string, error) {
	var (
		baseParams = BaseAPIParams(proxyURL)
		msg        = &cmn.SelectMsg{Flags: cmn.SelectCached, Prefix: prefix}
	)

	data, err := api.ListObjects(baseParams, bck, msg, objectCountLimit)
	if err != nil {
		return nil, err
	}

	objs := make([]string, 0, len(data.Entries))
	for _, obj := range data.Entries {
		objs = append(objs, obj.Name)
	}
	return objs, nil
}

func GetPrimaryURL() string {
	primary, err := GetPrimaryProxy(proxyURLReadOnly)
	if err != nil {
		return proxyURLReadOnly
	}
	return primary.URL(cmn.NetworkPublic)
}

// GetPrimaryProxy returns the primary proxy's url of a cluster
func GetPrimaryProxy(proxyURL string) (*cluster.Snode, error) {
	baseParams := BaseAPIParams(proxyURL)
	smap, err := api.GetClusterMap(baseParams)
	if err != nil {
		return nil, err
	}
	return smap.Primary, err
}

// CreateFreshBucket, destroys bucket if exists and creates new. The bucket is destroyed on test completion.
func CreateFreshBucket(tb testing.TB, proxyURL string, bck cmn.Bck, ops ...cmn.BucketPropsToUpdate) {
	DestroyBucket(tb, proxyURL, bck)
	baseParams := BaseAPIParams(proxyURL)
	err := api.CreateBucket(baseParams, bck, ops...)
	tassert.CheckFatal(tb, err)
	tb.Cleanup(func() {
		DestroyBucket(tb, proxyURL, bck)
	})
}

func DestroyBucket(tb testing.TB, proxyURL string, bck cmn.Bck) {
	baseParams := BaseAPIParams(proxyURL)
	exists, err := api.DoesBucketExist(baseParams, cmn.QueryBcks(bck))
	tassert.CheckFatal(tb, err)
	if exists {
		err = api.DestroyBucket(baseParams, bck)
		tassert.CheckFatal(tb, err)
	}
}

func EvictCloudBucket(tb testing.TB, proxyURL string, bck cmn.Bck) {
	if !bck.IsCloud() {
		return
	}
	if bck.HasBackendBck() {
		bck = *bck.BackendBck()
	}
	err := api.EvictCloudBucket(BaseAPIParams(proxyURL), bck)
	tassert.CheckFatal(tb, err)
}

func CleanupCloudBucket(t *testing.T, proxyURL string, bck cmn.Bck, prefix string) {
	if !bck.IsCloud() {
		return
	}

	toDelete, err := ListObjectNames(proxyURL, bck, prefix, 0)
	tassert.CheckFatal(t, err)
	defer EvictCloudBucket(t, proxyURL, bck)

	if len(toDelete) == 0 {
		return
	}

	baseParams := BaseAPIParams(proxyURL)
	xactID, err := api.DeleteList(baseParams, bck, toDelete)
	tassert.CheckFatal(t, err)
	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActDelete, Timeout: time.Minute}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)
}

func SetBackendBck(t *testing.T, baseParams api.BaseParams, srcBck, dstBck cmn.Bck) {
	p, err := api.HeadBucket(baseParams, dstBck) // We need to know real provider of the bucket
	tassert.CheckFatal(t, err)

	_, err = api.SetBucketProps(baseParams, srcBck, cmn.BucketPropsToUpdate{
		BackendBck: &cmn.BckToUpdate{
			Name:     api.String(dstBck.Name),
			Provider: api.String(p.Provider),
		},
	})
	tassert.CheckFatal(t, err)
}

func UnregisterNode(proxyURL string, args *cmn.ActValDecommision) error {
	return devtools.UnregisterNode(devtoolsCtx, proxyURL, args, registerTimeout)
}

// Internal API to remove a node from Smap: use it to unregister MOCK targets/proxies
func RemoveNodeFromSmap(proxyURL, sid string) error {
	baseParams := BaseAPIParams(proxyURL)
	baseParams.Method = http.MethodDelete
	return api.DoHTTPRequest(api.ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.URLPathClusterDaemon.S, sid),
	})
}

func WaitForObjectToBeDowloaded(baseParams api.BaseParams, bck cmn.Bck, objName string, timeout time.Duration) error {
	maxTime := time.Now().Add(timeout)
	for {
		if time.Now().After(maxTime) {
			return fmt.Errorf("timed out when downloading %s/%s", bck, objName)
		}
		reslist, err := api.ListObjects(baseParams, bck, &cmn.SelectMsg{}, 0)
		if err != nil {
			return err
		}
		for _, obj := range reslist.Entries {
			if obj.Name == objName {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func EnsureObjectsExist(t *testing.T, params api.BaseParams, bck cmn.Bck, objectsNames ...string) {
	for _, objName := range objectsNames {
		_, err := api.GetObject(params, bck, objName)
		if err != nil {
			t.Errorf("Unexpected GetObject(%s) error: %v.", objName, err)
		}
	}
}

func putObjs(proxyURL string, bck cmn.Bck, objPath string, objSize uint64,
	errCh chan error, objCh chan string, cksumType string, objsPutCh chan string) {
	var (
		size   = objSize
		reader readers.Reader
		err    error
	)
	for {
		objName, ok := <-objCh
		if !ok {
			return
		}
		if size == 0 {
			size = uint64(cmn.NowRand().Intn(1024)+1) * 1024
		}

		sgl := MMSA.NewSGL(int64(size))
		reader, err = readers.NewSGReader(sgl, int64(size), cksumType)
		cmn.AssertNoErr(err)

		fullObjName := path.Join(objPath, objName)
		// We could PUT while creating files, but that makes it
		// begin all the puts immediately (because creating random files is fast
		// compared to the list objects call that getRandomFiles does)
		baseParams := BaseAPIParams(proxyURL)
		putArgs := api.PutObjectArgs{
			BaseParams: baseParams,
			Bck:        bck,
			Object:     fullObjName,
			Cksum:      reader.Cksum(),
			Reader:     reader,
			Size:       size,
		}
		err = api.PutObject(putArgs)
		if err != nil {
			if errCh == nil {
				Logf("Error performing PUT of object with random data, provided error channel is nil\n")
			} else {
				errCh <- err
			}
		}
		if objsPutCh != nil {
			objsPutCh <- objName
		}

		// FIXME: due to critical bug (https://github.com/golang/go/issues/30597)
		// we need to postpone `sgl.Free` to a little bit later time, otherwise
		// we will experience 'read after free'. Sleep time is number taken
		// from thin air - increase if panics are still happening.
		go func() {
			time.Sleep(3 * time.Second)
			sgl.Free()
		}()
	}
}

func PutObjsFromList(proxyURL string, bck cmn.Bck, objPath string, objSize uint64, objList []string,
	errCh chan error, objsPutCh chan string, cksumType string, fixedSize ...bool) {
	var (
		wg        = &sync.WaitGroup{}
		objCh     = make(chan string, len(objList))
		workerCnt = 10
	)
	// if len(objList) < workerCnt, only need as many workers as there are objects to be PUT
	workerCnt = cmn.Min(workerCnt, len(objList))

	for i := 0; i < workerCnt; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			size := objSize
			// randomize sizes
			if len(fixedSize) == 0 || !fixedSize[0] {
				size = objSize + uint64(rand.Int63n(512))
			}
			putObjs(proxyURL, bck, objPath, size, errCh, objCh, cksumType, objsPutCh)
		}()
	}

	for _, objName := range objList {
		objCh <- objName
	}
	close(objCh)
	wg.Wait()
}

func PutRandObjs(proxyURL string, bck cmn.Bck, objPath string, objSize uint64, numPuts int, errCh chan error,
	objsPutCh chan string, cksumType string, fixedSize ...bool) {
	var (
		fNameLen = 16
		objList  = make([]string, 0, numPuts)
	)
	for i := 0; i < numPuts; i++ {
		fname := cmn.RandString(fNameLen)
		objList = append(objList, fname)
	}
	PutObjsFromList(proxyURL, bck, objPath, objSize, objList, errCh, objsPutCh, cksumType, fixedSize...)
}

// Put an object into a cloud bucket and evict it afterwards - can be used to test cold GET
func PutObjectInCloudBucketWithoutCachingLocally(t *testing.T, bck cmn.Bck, object string, objContent cmn.ReadOpenCloser) {
	baseParams := BaseAPIParams()

	err := api.PutObject(api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bck,
		Object:     object,
		Reader:     objContent,
	})
	tassert.CheckFatal(t, err)

	if err := api.EvictObject(baseParams, bck, object); err != nil {
		t.Fatal(err)
	}
}

func GetObjectAtime(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, object, timeFormat string) time.Time {
	msg := &cmn.SelectMsg{Props: cmn.GetPropsAtime, TimeFormat: timeFormat, Prefix: object}
	bucketList, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)

	for _, entry := range bucketList.Entries {
		if entry.Name == object {
			atime, err := time.Parse(timeFormat, entry.Atime)
			tassert.CheckFatal(t, err)
			return atime
		}
	}

	tassert.Fatalf(t, false, "Cannot find %s in bucket %s", object, bck)
	return time.Time{}
}

// WaitForDSortToFinish waits until all dSorts jobs finished without failure or
// all jobs abort.
func WaitForDSortToFinish(proxyURL, managerUUID string) (allAborted bool, err error) {
	Logln("waiting for distributed sort to finish...")

	baseParams := BaseAPIParams(proxyURL)
	deadline := time.Now().Add(dsortFinishTimeout)
	for time.Now().Before(deadline) {
		allMetrics, err := api.MetricsDSort(baseParams, managerUUID)
		if err != nil {
			return false, err
		}

		allAborted := true
		allFinished := true
		for _, metrics := range allMetrics {
			allAborted = allAborted && metrics.Aborted.Load()
			allFinished = allFinished &&
				!metrics.Aborted.Load() &&
				metrics.Extraction.Finished &&
				metrics.Sorting.Finished &&
				metrics.Creation.Finished
		}
		if allAborted {
			return true, nil
		}
		if allFinished {
			return false, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false, fmt.Errorf("deadline exceeded")
}

func BaseAPIParams(urls ...string) api.BaseParams {
	var u string
	if len(urls) > 0 && len(urls[0]) > 0 {
		u = urls[0]
	} else {
		u = RandomProxyURL()
	}

	return devtools.BaseAPIParams(devtoolsCtx, u)
}

// waitForBucket waits until all targets ack having ais bucket created or deleted
func WaitForBucket(proxyURL string, query cmn.QueryBcks, exists bool) error {
	baseParams := BaseAPIParams(proxyURL)
	smap, err := api.GetClusterMap(baseParams)
	if err != nil {
		return err
	}
	to := time.Now().Add(bucketTimeout)
	for _, s := range smap.Tmap {
		for {
			baseParams := BaseAPIParams(s.URL(cmn.NetworkPublic))
			bucketExists, err := api.DoesBucketExist(baseParams, query)
			if err != nil {
				return err
			}
			if bucketExists == exists {
				break
			}
			if time.Now().After(to) {
				return fmt.Errorf("wait for ais bucket timed out, target = %s", baseParams.URL)
			}
			time.Sleep(time.Second)
		}
	}
	return nil
}

func EvictObjects(t *testing.T, proxyURL string, bck cmn.Bck, objList []string) {
	baseParams := BaseAPIParams(proxyURL)
	xactID, err := api.EvictList(baseParams, bck, objList)
	if err != nil {
		t.Errorf("Evict bucket %s failed, err = %v", bck, err)
	}

	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActEvictObjects, Timeout: evictPrefetchTimeout}
	if _, err := api.WaitForXaction(baseParams, args); err != nil {
		t.Errorf("Wait for xaction to finish failed, err = %v", err)
	}
}

// Waits for both resilver and rebalance to complete.
// If they were not started, this function treats them as completed
// and returns. If timeout set, if any of rebalances doesn't complete before timeout
// the function ends with fatal.
func WaitForRebalanceToComplete(t testing.TB, baseParams api.BaseParams, timeouts ...time.Duration) {
	var (
		timeout = 2 * time.Minute
		wg      = &sync.WaitGroup{}
		errCh   = make(chan error, 2)
	)

	err := WaitForRebalanceToStart(baseParams)
	tassert.CheckFatal(t, err)

	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}
	Logf("Waiting for rebalance and resilver to complete (timeout: %v)...\n", timeout)
	wg.Add(2)
	go func() {
		defer wg.Done()
		xactArgs := api.XactReqArgs{Kind: cmn.ActRebalance, Latest: true, Timeout: timeout}
		if _, err := api.WaitForXaction(baseParams, xactArgs); err != nil {
			if cmn.IsStatusNotFound(err) {
				return
			}
			errCh <- err
		}
	}()

	go func() {
		defer wg.Done()
		xactArgs := api.XactReqArgs{Kind: cmn.ActResilver, Latest: true, Timeout: timeout}
		if _, err := api.WaitForXaction(baseParams, xactArgs); err != nil {
			if cmn.IsStatusNotFound(err) {
				return
			}
			errCh <- err
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func WaitForRebalanceByID(t *testing.T, baseParams api.BaseParams, rebID string, timeouts ...time.Duration) {
	timeout := 2 * time.Minute
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}
	xactArgs := api.XactReqArgs{ID: rebID, Kind: cmn.ActRebalance, Latest: true, Timeout: timeout}
	_, err := api.WaitForXaction(baseParams, xactArgs)
	tassert.CheckFatal(t, err)
}

// WaitForRebalanceToStart waits until Rebalance or Resilver starts
func WaitForRebalanceToStart(baseParams api.BaseParams) error {
	const (
		startTimeout      = 15 * time.Second
		xactRetryInterval = time.Second
	)
	ctx := context.Background()
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, startTimeout)
	defer cancel()

	args := api.XactReqArgs{Timeout: startTimeout}
	// Remember that the function has found a finished xaction in case of
	// it was called too late. Return `nil` for this case.
	foundFinished := false
	for {
		for _, act := range []string{cmn.ActRebalance, cmn.ActResilver} {
			args.Kind = act
			status, err := api.GetXactionStatus(baseParams, args)
			if err == nil {
				if !status.Finished() {
					return nil
				}
				foundFinished = true
			}
		}

		time.Sleep(xactRetryInterval)
		select {
		case <-ctx.Done():
			if foundFinished {
				return nil
			}
			return ctx.Err()
		default:
			break
		}
	}
}

func GetClusterStats(t *testing.T, proxyURL string) (stats stats.ClusterStats) {
	baseParams := BaseAPIParams(proxyURL)
	clusterStats, err := api.GetClusterStats(baseParams)
	tassert.CheckFatal(t, err)
	return clusterStats
}

func GetNamedTargetStats(trunner *stats.Trunner, name string) int64 {
	v, ok := trunner.Core.Tracker[name]
	if !ok {
		return 0
	}
	return v.Value
}

func GetDaemonStats(t *testing.T, u string) (stats map[string]interface{}) {
	baseParams := BaseAPIParams(u)
	baseParams.Method = http.MethodGet
	err := api.DoHTTPRequest(api.ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathDaemon.S,
		Query:      url.Values{cmn.URLParamWhat: {cmn.GetWhatStats}},
	}, &stats)
	tassert.CheckFatal(t, err)
	return
}

func GetDaemonConfig(t *testing.T, daeID string) *cmn.Config {
	baseParams := BaseAPIParams()
	config, err := api.GetDaemonConfig(baseParams, daeID)
	tassert.CheckFatal(t, err)
	return config
}

func GetClusterMap(t *testing.T, url string) (smap *cluster.Smap) {
	smap, _ = waitForStartup(BaseAPIParams(url), t)
	return
}

func getClusterConfig() (config *cmn.Config, err error) {
	proxyURL := GetPrimaryURL()
	primary, err := GetPrimaryProxy(proxyURL)
	if err != nil {
		return nil, err
	}
	return api.GetDaemonConfig(BaseAPIParams(proxyURL), primary.ID())
}

func GetClusterConfig(t *testing.T) (config *cmn.Config) {
	config, err := getClusterConfig()
	tassert.CheckFatal(t, err)
	return config
}

func SetClusterConfig(t *testing.T, nvs cmn.SimpleKVs) {
	proxyURL := GetPrimaryURL()
	baseParams := BaseAPIParams(proxyURL)
	err := api.SetClusterConfig(baseParams, nvs)
	tassert.CheckFatal(t, err)
}

func isErrAcceptable(err error) bool {
	if err == nil {
		return true
	}
	if hErr, ok := err.(*cmn.HTTPError); ok {
		// TODO: http.StatusBadGateway should be handled while performing reverseProxy (if a IC node is killed)
		return hErr.Status == http.StatusServiceUnavailable || hErr.Status == http.StatusBadGateway
	}

	// Below errors occur when the node is killed/just started while requesting
	return strings.Contains(err.Error(), "ContentLength") ||
		strings.Contains(err.Error(), "connection refused") ||
		strings.Contains(err.Error(), "EOF")
}

func WaitForXactionByID(id string, timeouts ...time.Duration) error {
	var (
		retryInterval = time.Second
		args          = api.XactReqArgs{ID: id}
		ctx           = context.Background()
	)

	if len(timeouts) > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeouts[0])
		defer cancel()
	}

	for {
		baseParams := BaseAPIParams()
		if status, err := api.GetXactionStatus(baseParams, args); !isErrAcceptable(err) || status.Finished() {
			return err
		}

		time.Sleep(retryInterval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			break
		}
	}
}

func CheckErrIsNotFound(t *testing.T, err error) {
	if err == nil {
		t.Fatalf("expected error")
		return
	}
	httpErr, ok := err.(*cmn.HTTPError)
	tassert.Fatalf(t, ok, "expected an error of type *cmn.HTTPError, but got: %T.", err)
	tassert.Fatalf(
		t, httpErr.Status == http.StatusNotFound,
		"expected status: %d, got: %d.", http.StatusNotFound, httpErr.Status,
	)
}

func waitForStartup(baseParams api.BaseParams, ts ...*testing.T) (*cluster.Smap, error) {
	for {
		smap, err := api.GetClusterMap(baseParams)
		if err != nil {
			if api.HTTPStatus(err) == http.StatusServiceUnavailable {
				Logln("waiting for the cluster to start up...")
				time.Sleep(waitClusterStartup)
				continue
			}

			Logf("unable to get usable cluster map, err: %v\n", err)
			if len(ts) > 0 {
				tassert.CheckFatal(ts[0], err)
			}
			return nil, err
		}
		return smap, nil
	}
}
