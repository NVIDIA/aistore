// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/readers"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/trand"
	"github.com/NVIDIA/aistore/stats"
	"golang.org/x/sync/errgroup"
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

const evictPrefetchTimeout = 2 * time.Minute

const xactPollSleep = time.Second

type Ctx struct {
	Client *http.Client
	Log    func(format string, a ...any)
}

type PutObjectsArgs struct {
	ProxyURL  string
	Bck       cmn.Bck
	ObjPath   string
	CksumType string
	ObjSize   uint64
	ObjCnt    int
	WorkerCnt int
	FixedSize bool
	Ordered   bool // true - object names make sequence, false - names are random
	IgnoreErr bool
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

func CheckObjIsPresent(proxyURL string, bck cmn.Bck, objName string) bool {
	baseParams := BaseAPIParams(proxyURL)
	_, err := api.HeadObject(baseParams, bck, objName, apc.FltPresent)
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
	if err == nil {
		return
	}
	if errCh == nil {
		fmt.Printf("PUT %s/%s failed, err: %v (nil error channel)\n", bck, object, err)
	} else {
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
		msg        = &apc.ListObjsMsg{Flags: apc.LsPresent, Prefix: prefix}
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
	return primary.URL(cmn.NetPublic)
}

// GetPrimaryProxy returns the primary proxy
func GetPrimaryProxy(proxyURL string) (*cluster.Snode, error) {
	baseParams := BaseAPIParams(proxyURL)
	smap, err := api.GetClusterMap(baseParams)
	if err != nil {
		return nil, err
	}
	return smap.Primary, err
}

func GetProxyReadiness(proxyURL string) error {
	return api.GetProxyReadiness(BaseAPIParams(proxyURL))
}

// CreateBucketWithCleanup, destroys bucket if exists and creates new. The bucket is destroyed on test completion.
func CreateBucketWithCleanup(tb testing.TB, proxyURL string, bck cmn.Bck, props *cmn.BucketPropsToUpdate) {
	DestroyBucket(tb, proxyURL, bck)
	baseParams := BaseAPIParams(proxyURL)
	err := api.CreateBucket(baseParams, bck, props)
	tassert.CheckFatal(tb, err)
	tb.Cleanup(func() {
		DestroyBucket(tb, proxyURL, bck)
	})
}

func DestroyBucket(tb testing.TB, proxyURL string, bck cmn.Bck) {
	baseParams := BaseAPIParams(proxyURL)
	exists, err := api.QueryBuckets(baseParams, cmn.QueryBcks(bck), apc.FltExists)
	tassert.CheckFatal(tb, err)
	if exists {
		err = api.DestroyBucket(baseParams, bck)
		tassert.CheckFatal(tb, err)
	}
}

func EvictRemoteBucket(tb testing.TB, proxyURL string, bck cmn.Bck) {
	if backend := bck.Backend(); backend != nil {
		bck.Copy(backend)
	}
	err := api.EvictRemoteBucket(BaseAPIParams(proxyURL), bck, false)
	tassert.CheckFatal(tb, err)
}

func CleanupRemoteBucket(t *testing.T, proxyURL string, bck cmn.Bck, prefix string) {
	if !bck.IsRemote() {
		return
	}

	toDelete, err := ListObjectNames(proxyURL, bck, prefix, 0)
	tassert.CheckFatal(t, err)
	defer EvictRemoteBucket(t, proxyURL, bck)

	if len(toDelete) == 0 {
		return
	}

	baseParams := BaseAPIParams(proxyURL)
	xactID, err := api.DeleteList(baseParams, bck, toDelete)
	tassert.CheckFatal(t, err)
	args := api.XactReqArgs{ID: xactID, Kind: apc.ActDeleteObjects, Timeout: time.Minute}
	_, err = api.WaitForXactionIC(baseParams, args)
	tassert.CheckFatal(t, err)
}

func SetBackendBck(t *testing.T, baseParams api.BaseParams, srcBck, dstBck cmn.Bck) {
	// find out real provider of the bucket
	p, err := api.HeadBucket(baseParams, dstBck, true /* don't add to cluster MD */)
	tassert.CheckFatal(t, err)

	_, err = api.SetBucketProps(baseParams, srcBck, &cmn.BucketPropsToUpdate{
		BackendBck: &cmn.BackendBckToUpdate{
			Name:     api.String(dstBck.Name),
			Provider: api.String(p.Provider),
		},
	})
	tassert.CheckFatal(t, err)
}

func RmTargetSkipRebWait(t *testing.T, proxyURL string, smap *cluster.Smap) (*cluster.Smap, *cluster.Snode) {
	var (
		removeTarget, _ = smap.GetRandTarget()
		origTgtCnt      = smap.CountActiveTargets()
		args            = &apc.ActValRmNode{DaemonID: removeTarget.ID(), SkipRebalance: true}
	)
	_, err := api.StartMaintenance(BaseAPIParams(proxyURL), args)
	tassert.CheckFatal(t, err)
	newSmap, err := WaitForClusterState(
		proxyURL,
		"target is gone",
		smap.Version,
		smap.CountActiveProxies(),
		origTgtCnt-1,
	)
	tassert.CheckFatal(t, err)
	newTgtCnt := newSmap.CountActiveTargets()
	tassert.Fatalf(t, newTgtCnt == origTgtCnt-1,
		"new smap expected to have 1 target less: %d (v%d) vs %d (v%d)", newTgtCnt, origTgtCnt,
		newSmap.Version, smap.Version)
	return newSmap, removeTarget
}

// Internal API to remove a node from Smap: use it to unregister MOCK targets/proxies.
// Use `JoinCluster` to attach node back.
func RemoveNodeFromSmap(proxyURL, sid string) error {
	return _removeNodeFromSmap(gctx, proxyURL, sid, time.Minute)
}

func WaitForObjectToBeDowloaded(baseParams api.BaseParams, bck cmn.Bck, objName string, timeout time.Duration) error {
	maxTime := time.Now().Add(timeout)
	for {
		if time.Now().After(maxTime) {
			return fmt.Errorf("timed out when downloading %s/%s", bck, objName)
		}
		reslist, err := api.ListObjects(baseParams, bck, &apc.ListObjsMsg{}, 0)
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

func PutRandObjs(args PutObjectsArgs) ([]string, int, error) {
	var (
		errCnt = atomic.NewInt32(0)
		putCnt = atomic.NewInt32(0)

		workerCnt  = 40 // Default worker count.
		group      = &errgroup.Group{}
		objNames   = make([]string, 0, args.ObjCnt)
		baseParams = BaseAPIParams(args.ProxyURL)
	)

	if args.WorkerCnt > 0 {
		workerCnt = args.WorkerCnt
	}
	workerCnt = cos.Min(workerCnt, args.ObjCnt)

	for i := 0; i < args.ObjCnt; i++ {
		if args.Ordered {
			objNames = append(objNames, path.Join(args.ObjPath, fmt.Sprintf("%d", i)))
		} else {
			objNames = append(objNames, path.Join(args.ObjPath, trand.String(16)))
		}
	}
	chunkSize := (len(objNames) + workerCnt - 1) / workerCnt
	for i := 0; i < len(objNames); i += chunkSize {
		group.Go(func(start, end int) func() error {
			return func() error {
				for _, objName := range objNames[start:end] {
					size := args.ObjSize
					if size == 0 { // Size not specified so generate something.
						size = uint64(cos.NowRand().Intn(cos.KiB)+1) * cos.KiB
					} else if !args.FixedSize { // Randomize object size.
						size += uint64(rand.Int63n(cos.KiB))
					}

					if args.CksumType == "" {
						args.CksumType = cos.ChecksumNone
					}

					reader, err := readers.NewRandReader(int64(size), args.CksumType)
					cos.AssertNoErr(err)

					// We could PUT while creating files, but that makes it
					// begin all the puts immediately (because creating random files is fast
					// compared to the list objects call that getRandomFiles does)
					err = api.PutObject(api.PutObjectArgs{
						BaseParams: baseParams,
						Bck:        args.Bck,
						Object:     objName,
						Cksum:      reader.Cksum(),
						Reader:     reader,
						Size:       size,
						SkipVC:     true,
					})
					putCnt.Inc()
					if err != nil {
						if args.IgnoreErr {
							errCnt.Inc()
							return nil
						}
						return err
					}
				}
				return nil
			}
		}(i, cos.Min(i+chunkSize, len(objNames))))
	}

	err := group.Wait()
	cos.Assert(err != nil || len(objNames) == int(putCnt.Load()))
	return objNames, int(errCnt.Load()), err
}

// Put an object into a cloud bucket and evict it afterwards - can be used to test cold GET
func PutObjectInRemoteBucketWithoutCachingLocally(t *testing.T, bck cmn.Bck, object string, objContent cos.ReadOpenCloser) {
	baseParams := BaseAPIParams()

	err := api.PutObject(api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bck,
		Object:     object,
		Reader:     objContent,
	})
	tassert.CheckFatal(t, err)

	err = api.EvictObject(baseParams, bck, object)
	tassert.CheckFatal(t, err)
}

func GetObjectAtime(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, object, timeFormat string) (time.Time, string) {
	msg := &apc.ListObjsMsg{Props: apc.GetPropsAtime, TimeFormat: timeFormat, Prefix: object}
	bucketList, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)

	for _, entry := range bucketList.Entries {
		if entry.Name == object {
			atime, err := time.Parse(timeFormat, entry.Atime)
			tassert.CheckFatal(t, err)
			return atime, entry.Atime
		}
	}

	tassert.Fatalf(t, false, "Cannot find %s in bucket %s", object, bck)
	return time.Time{}, ""
}

// WaitForDSortToFinish waits until all dSorts jobs finished without failure or
// all jobs abort.
func WaitForDSortToFinish(proxyURL, managerUUID string) (allAborted bool, err error) {
	tlog.Logln("waiting for distributed sort to finish...")

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
	return api.BaseParams{Client: gctx.Client, URL: u, Token: LoggedUserToken, UA: "devtools/test"}
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
			baseParams := BaseAPIParams(s.URL(cmn.NetPublic))
			bucketExists, err := api.QueryBuckets(baseParams, query, apc.FltExists)
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

	args := api.XactReqArgs{ID: xactID, Kind: apc.ActEvictObjects, Timeout: evictPrefetchTimeout}
	if _, err := api.WaitForXactionIC(baseParams, args); err != nil {
		t.Errorf("Wait for xaction to finish failed, err = %v", err)
	}
}

// TODO -- FIXME: revise and rewrite
//
// Waits for both resilver and rebalance to complete.
// If they were not started, this function treats them as completed
// and returns. If timeout set, if any of rebalances doesn't complete before timeout
// the function ends with fatal.
func WaitForRebalAndResil(t testing.TB, baseParams api.BaseParams, timeouts ...time.Duration) {
	var (
		timeout = 2 * time.Minute
		wg      = &sync.WaitGroup{}
		errCh   = make(chan error, 2)
	)
	smap, err := api.GetClusterMap(baseParams)
	tassert.CheckFatal(t, err)
	if smap.CountActiveTargets() < 2 {
		return
	}

	_waitReToStart(baseParams)
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}
	tlog.Logf("Waiting for rebalance and resilver to complete (timeout %v)\n", timeout)
	wg.Add(2)
	go func() {
		defer wg.Done()
		xactArgs := api.XactReqArgs{Kind: apc.ActRebalance, OnlyRunning: true, Timeout: timeout}
		if _, err := api.WaitForXactionIC(baseParams, xactArgs); err != nil {
			if cmn.IsStatusNotFound(err) {
				return
			}
			errCh <- err
		}
	}()

	go func() {
		defer wg.Done()
		xactArgs := api.XactReqArgs{Kind: apc.ActResilver, OnlyRunning: true, Timeout: timeout}
		if _, err := api.WaitForXactionIC(baseParams, xactArgs); err != nil {
			if cmn.IsStatusNotFound(err) {
				return
			}
			errCh <- err
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		tassert.CheckFatal(t, err)
		return
	}
}

func WaitForResilver(t *testing.T, baseParams api.BaseParams, timeouts ...time.Duration) {
	timeout := cos.MaxDuration(10*xactPollSleep, 10*time.Second)
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}
	xactArgs := api.XactReqArgs{Kind: apc.ActResilver, OnlyRunning: true, Timeout: timeout}
	for i := 0; i < 10; i++ {
		_, err := api.WaitForXactionIC(baseParams, xactArgs)
		if err == nil {
			return
		}
		if httpErr, ok := err.(*cmn.ErrHTTP); ok {
			if httpErr.Status == http.StatusNotFound { // keep waiting for timeout
				time.Sleep(xactPollSleep)
				continue
			}
			tassert.CheckFatal(t, httpErr)
		} else {
			tassert.CheckFatal(t, err)
		}
	}
}

func WaitForRebalanceByID(t *testing.T, origTargetCnt int, baseParams api.BaseParams, rebID string, timeouts ...time.Duration) {
	if origTargetCnt >= 0 && origTargetCnt < 2 {
		return
	}
	timeout := 2 * time.Minute
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}
	xactArgs := api.XactReqArgs{ID: rebID, Kind: apc.ActRebalance, OnlyRunning: true, Timeout: timeout}
	_, err := api.WaitForXactionIC(baseParams, xactArgs)
	tassert.CheckFatal(t, err)
}

func _waitReToStart(baseParams api.BaseParams) {
	var (
		kinds   = []string{apc.ActRebalance, apc.ActResilver}
		timeout = cos.MaxDuration(10*xactPollSleep, 10*time.Second)
		retries = int(timeout / xactPollSleep)
		args    = api.XactReqArgs{Timeout: xactPollSleep, OnlyRunning: true}
	)
	for i := 0; i < retries; i++ {
		for _, kind := range kinds {
			args.Kind = kind
			status, err := api.GetXactionStatus(baseParams, args)
			if err == nil {
				if !status.Finished() {
					return
				}
			}
		}
		time.Sleep(xactPollSleep)
	}
	tlog.Logf("Warning: timed out (%v) waiting for rebalance or resilver to start\n", timeout)
}

func GetClusterStats(t *testing.T, proxyURL string) stats.ClusterStats {
	baseParams := BaseAPIParams(proxyURL)
	scs, err := api.GetClusterStats(baseParams)
	tassert.CheckFatal(t, err)
	return scs
}

func GetNamedStatsVal(ds *stats.DaemonStats, name string) int64 {
	v, ok := ds.Tracker[name]
	if !ok {
		return 0
	}
	return v.Value
}

// FIXME: obsolete - remove and reimpl. the test that calls it
func GetDaemonStats(t *testing.T, u string) (stats map[string]any) {
	baseParams := BaseAPIParams(u)
	baseParams.Method = http.MethodGet
	reqParams := &api.ReqParams{
		BaseParams: baseParams,
		Path:       apc.URLPathDae.S,
		Query:      url.Values{apc.QparamWhat: {apc.GetWhatStats}},
	}
	err := reqParams.DoHTTPReqResp(&stats)
	tassert.CheckFatal(t, err)
	return
}

func GetDaemonConfig(t *testing.T, node *cluster.Snode) *cmn.Config {
	baseParams := BaseAPIParams()
	config, err := api.GetDaemonConfig(baseParams, node)
	tassert.CheckFatal(t, err)
	return config
}

func GetClusterMap(tb testing.TB, url string) *cluster.Smap {
	smap, err := waitForStartup(BaseAPIParams(url), tb)
	_ = err // the caller must check smap for nil
	return smap
}

func getClusterConfig() (config *cmn.Config, err error) {
	proxyURL := GetPrimaryURL()
	primary, err := GetPrimaryProxy(proxyURL)
	if err != nil {
		return nil, err
	}
	return api.GetDaemonConfig(BaseAPIParams(proxyURL), primary)
}

func GetClusterConfig(t *testing.T) (config *cmn.Config) {
	config, err := getClusterConfig()
	tassert.CheckFatal(t, err)
	return config
}

func SetClusterConfig(t *testing.T, nvs cos.SimpleKVs) {
	proxyURL := GetPrimaryURL()
	baseParams := BaseAPIParams(proxyURL)
	err := api.SetClusterConfig(baseParams, nvs)
	tassert.CheckFatal(t, err)
}

func SetClusterConfigUsingMsg(t *testing.T, toUpdate *cmn.ConfigToUpdate) {
	proxyURL := GetPrimaryURL()
	baseParams := BaseAPIParams(proxyURL)
	err := api.SetClusterConfigUsingMsg(baseParams, toUpdate)
	tassert.CheckFatal(t, err)
}

func CheckErrIsNotFound(t *testing.T, err error) {
	if err == nil {
		t.Fatalf("expected error")
		return
	}
	httpErr, ok := err.(*cmn.ErrHTTP)
	tassert.Fatalf(t, ok, "expected an error of the type *cmn.ErrHTTP, got %v(%T)", err, err)
	tassert.Fatalf(
		t, httpErr.Status == http.StatusNotFound,
		"expected status: %d, got: %d.", http.StatusNotFound, httpErr.Status,
	)
}

func waitForStartup(baseParams api.BaseParams, ts ...testing.TB) (*cluster.Smap, error) {
	for {
		smap, err := api.GetClusterMap(baseParams)
		if err != nil {
			if api.HTTPStatus(err) == http.StatusServiceUnavailable {
				tlog.Logln("waiting for the cluster to start up...")
				time.Sleep(waitClusterStartup)
				continue
			}

			tlog.Logf("unable to get usable cluster map, err: %v\n", err)
			if len(ts) > 0 {
				tassert.CheckFatal(ts[0], err)
			}
			return nil, err
		}
		return smap, nil
	}
}
