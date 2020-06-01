// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
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
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/NVIDIA/aistore/tutils/tassert"
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

func PingURL(url string) (err error) {
	addr := strings.TrimPrefix(url, "http://")
	if addr == url {
		addr = strings.TrimPrefix(url, "https://")
	}
	conn, err := net.Dial("tcp", addr)
	if err == nil {
		conn.Close()
	}
	return
}

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

// PutAsync sends a PUT request to the given URL
func PutAsync(wg *sync.WaitGroup, proxyURL string, bck cmn.Bck, object string, reader readers.Reader, errCh chan error) {
	defer wg.Done()
	baseParams := BaseAPIParams(proxyURL)
	putArgs := api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bck,
		Object:     object,
		Cksum:      reader.Cksum(),
		Reader:     reader,
	}
	err := api.PutObject(putArgs)
	if err != nil {
		if errCh == nil {
			fmt.Println("Error channel is not given, do not know how to report error", err)
		} else {
			errCh <- err
		}
	}
}

// ListObjects returns a slice of object names of all objects that match the prefix in a bucket
func ListObjects(proxyURL string, bck cmn.Bck, prefix string, objectCountLimit int) ([]string, error) {
	var (
		baseParams = BaseAPIParams(proxyURL)
		msg        = &cmn.SelectMsg{Cached: true, Prefix: prefix}
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
	return smap.ProxySI, err
}

func CreateFreshBucket(t *testing.T, proxyURL string, bck cmn.Bck) {
	DestroyBucket(t, proxyURL, bck)
	baseParams := BaseAPIParams(proxyURL)
	err := api.CreateBucket(baseParams, bck)
	tassert.CheckFatal(t, err)
}

func DestroyBucket(t *testing.T, proxyURL string, bck cmn.Bck) {
	baseParams := BaseAPIParams(proxyURL)
	exists, err := api.DoesBucketExist(baseParams, cmn.QueryBcks(bck))
	tassert.CheckFatal(t, err)
	if exists {
		err = api.DestroyBucket(baseParams, bck)
		tassert.CheckFatal(t, err)
	}
}

func CleanCloudBucket(t *testing.T, proxyURL string, bck cmn.Bck, prefix string) {
	bck.Provider = cmn.AnyCloud
	toDelete, err := ListObjects(proxyURL, bck, prefix, 0)
	tassert.CheckFatal(t, err)
	baseParams := BaseAPIParams(proxyURL)
	err = api.DeleteList(baseParams, bck, toDelete)
	tassert.CheckFatal(t, err)
}

func SetBackendBck(t *testing.T, baseParams api.BaseParams, srcBck, dstBck cmn.Bck) {
	p, err := api.HeadBucket(baseParams, dstBck) // We need to know real provider of the bucket
	tassert.CheckFatal(t, err)

	err = api.SetBucketProps(baseParams, srcBck, cmn.BucketPropsToUpdate{
		BackendBck: &cmn.BckToUpdate{
			Name:     api.String(dstBck.Name),
			Provider: api.String(p.Provider),
		},
	})
	tassert.CheckFatal(t, err)
}

func UnregisterNode(proxyURL, sid string) error {
	baseParams := BaseAPIParams(proxyURL)
	smap, err := api.GetClusterMap(baseParams)
	if err != nil {
		return fmt.Errorf("api.GetClusterMap failed, err: %v", err)
	}
	if err := api.UnregisterNode(baseParams, sid); err != nil {
		return err
	}

	// If node does not exists in cluster we should not wait for map version
	// sync because update will not be scheduled
	if node := smap.GetNode(sid); node != nil {
		return WaitMapVersionSync(time.Now().Add(registerTimeout), smap, smap.Version, []string{node.ID()})
	}
	return nil
}

func WaitForObjectToBeDowloaded(baseParams api.BaseParams, bck cmn.Bck, objName string, timeout time.Duration) error {
	maxTime := time.Now().Add(timeout)
	for {
		if time.Now().After(maxTime) {
			return fmt.Errorf("timed out when downloading %s/%s", bck, objName)
		}
		reslist, err := api.ListObjects(baseParams, bck, &cmn.SelectMsg{Fast: true}, 0)
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
		fname := GenRandomString(fNameLen)
		objList = append(objList, fname)
	}
	PutObjsFromList(proxyURL, bck, objPath, objSize, objList, errCh, objsPutCh, cksumType, fixedSize...)
}

// Put an object into a cloud bucket and evict it afterwards - can be used to test cold GET
func PutObjectInCloudBucketWithoutCachingLocally(t *testing.T, proxyURL string, bck cmn.Bck,
	object string, objContent cmn.ReadOpenCloser) {
	baseParams := BaseAPIParams()

	err := api.PutObject(api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bck,
		Object:     object,
		Reader:     objContent,
	})
	tassert.CheckFatal(t, err)

	EvictObjects(t, proxyURL, bck, []string{object})
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
	if len(urls) > 0 {
		u = urls[0]
	} else {
		u = RandomProxyURL()
	}
	return api.BaseParams{
		Client: HTTPClient, // TODO -- FIXME: make use of HTTPClientGetPut as well
		URL:    u,
	}
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
	err := api.EvictList(baseParams, bck, objList)
	if err != nil {
		t.Errorf("Evict bucket %s failed, err = %v", bck, err)
	}
	xactArgs := api.XactReqArgs{Kind: cmn.ActPrefetch, Bck: bck, Timeout: evictPrefetchTimeout}
	if err := api.WaitForXaction(baseParams, xactArgs); err != nil {
		t.Errorf("Wait for xaction to finish failed, err = %v", err)
	}
}

// Waits for both resilver and rebalance to complete.
// If they were not started, this function treats them as completed
// and returns. If timeout set, if any of rebalances doesn't complete before timeout
// the function ends with fatal.
func WaitForRebalanceToComplete(t *testing.T, baseParams api.BaseParams, timeouts ...time.Duration) {
	var (
		timeout time.Duration
		wg      = &sync.WaitGroup{}
		errCh   = make(chan error, 2)
	)

	// TODO: remove sleep
	// Kind of hack.. Making sure that rebalance starts. We cannot use
	// `api.WaitForXactionToStart` because we are not sure if a local and/or global
	// rebalance will be started for all the cases where we use this function.
	time.Sleep(15 * time.Second)

	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}
	Logf("waiting for rebalance and resilver to complete (timeout: %v)...\n", timeout)
	wg.Add(2)
	go func() {
		defer wg.Done()
		xactArgs := api.XactReqArgs{Kind: cmn.ActRebalance, Timeout: timeout}
		err := api.WaitForXaction(baseParams, xactArgs)
		if err != nil {
			errCh <- err
		}
	}()

	go func() {
		defer wg.Done()
		xactArgs := api.XactReqArgs{Kind: cmn.ActResilver, Timeout: timeout}
		err := api.WaitForXaction(baseParams, xactArgs)
		if err != nil {
			errCh <- err
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
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
		Path:       cmn.URLPath(cmn.Version, cmn.Daemon),
		Query:      url.Values{cmn.URLParamWhat: {cmn.GetWhatStats}},
	}, &stats)
	tassert.CheckFatal(t, err)
	return
}

func GetClusterMap(t *testing.T, url string) (smap *cluster.Smap) {
	var (
		httpErr    = &cmn.HTTPError{}
		baseParams = BaseAPIParams(url)
		err        error
	)
while503:
	smap, err = api.GetClusterMap(baseParams)
	if err != nil && errors.As(err, &httpErr) && httpErr.Status == http.StatusServiceUnavailable {
		t.Log("waiting for the cluster to start up...")
		time.Sleep(waitClusterStartup)
		goto while503
	}
	tassert.CheckFatal(t, err)
	return smap
}

func GetClusterConfig(t *testing.T) (config *cmn.Config) {
	proxyURL := GetPrimaryURL()
	primary, err := GetPrimaryProxy(proxyURL)
	tassert.CheckFatal(t, err)
	return GetDaemonConfig(t, primary.ID())
}

func GetDaemonConfig(t *testing.T, nodeID string) (config *cmn.Config) {
	var (
		err        error
		proxyURL   = GetPrimaryURL()
		baseParams = BaseAPIParams(proxyURL)
	)
	config, err = api.GetDaemonConfig(baseParams, nodeID)
	tassert.CheckFatal(t, err)
	return
}

func SetClusterConfig(t *testing.T, nvs cmn.SimpleKVs) {
	proxyURL := GetPrimaryURL()
	baseParams := BaseAPIParams(proxyURL)
	err := api.SetClusterConfig(baseParams, nvs)
	tassert.CheckFatal(t, err)
}
