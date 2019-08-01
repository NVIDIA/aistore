// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/tutils/tassert"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
)

func testBucketProps(t *testing.T) *cmn.BucketProps {
	proxyURL := getPrimaryURL(t, proxyURLReadOnly)
	globalConfig := getClusterConfig(t, proxyURL)

	return &cmn.BucketProps{
		Cksum: cmn.CksumConf{Type: cmn.PropInherit},
		LRU:   globalConfig.LRU,
	}
}

func TestDefaultBucketProps(t *testing.T) {
	const dataSlices = 7
	var (
		proxyURL     = getPrimaryURL(t, proxyURLReadOnly)
		globalConfig = getClusterConfig(t, proxyURL)
	)

	setClusterConfig(t, proxyURL, cmn.SimpleKVs{
		"ec_enabled":     "true",
		"ec_data_slices": strconv.FormatUint(dataSlices, 10),
	})
	defer setClusterConfig(t, proxyURL, cmn.SimpleKVs{
		"ec_enabled":     "false",
		"ec_data_slices": "2",
	})

	tutils.CreateFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer tutils.DestroyLocalBucket(t, proxyURL, TestLocalBucketName)
	p, err := api.HeadBucket(tutils.DefaultBaseAPIParams(t), TestLocalBucketName)
	tassert.CheckFatal(t, err)
	if p.LRU.Enabled || p.LRU.Enabled != globalConfig.LRU.LocalBuckets {
		t.Errorf("LRU should be disabled for local buckets (bucket.Enabled: %v, global.localBuckets: %v)",
			p.LRU.Enabled, globalConfig.LRU.LocalBuckets)
	}
	if !p.EC.Enabled {
		t.Errorf("EC should be enabled for local buckets")
	}
	if p.EC.DataSlices != dataSlices {
		t.Errorf("Invalid number of EC data slices: expected %d, got %d", dataSlices, p.EC.DataSlices)
	}
}

func TestResetBucketProps(t *testing.T) {
	var (
		proxyURL     = getPrimaryURL(t, proxyURLReadOnly)
		globalProps  cmn.BucketProps
		globalConfig = getClusterConfig(t, proxyURL)
	)

	setClusterConfig(t, proxyURL, cmn.SimpleKVs{
		"ec_enabled":       "true",
		"ec_parity_slices": "1",
	})
	defer setClusterConfig(t, proxyURL, cmn.SimpleKVs{
		"ec_enabled":       "false",
		"ec_parity_slices": "2",
	})

	tutils.CreateFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer tutils.DestroyLocalBucket(t, proxyURL, TestLocalBucketName)

	bucketProps := defaultBucketProps()
	bucketProps.Cksum.Type = cmn.ChecksumNone
	bucketProps.Cksum.ValidateWarmGet = true
	bucketProps.Cksum.EnableReadRange = true
	bucketProps.EC.DataSlices = 1
	bucketProps.EC.ParitySlices = 2
	bucketProps.EC.Enabled = false

	globalProps.CloudProvider = cmn.ProviderAIS
	globalProps.Cksum = globalConfig.Cksum
	globalProps.LRU = testBucketProps(t).LRU
	globalProps.EC.ParitySlices = 1
	// For local bucket, there is additional config option that affects LRU.Enabled
	globalProps.LRU.Enabled = globalProps.LRU.Enabled && globalProps.LRU.LocalBuckets

	bParams := tutils.DefaultBaseAPIParams(t)
	err := api.SetBucketPropsMsg(bParams, TestLocalBucketName, bucketProps)
	tassert.CheckFatal(t, err)

	p, err := api.HeadBucket(bParams, TestLocalBucketName)
	tassert.CheckFatal(t, err)

	// check that bucket props do get set
	validateBucketProps(t, bucketProps, *p)
	err = api.ResetBucketProps(bParams, TestLocalBucketName)
	tassert.CheckFatal(t, err)

	p, err = api.HeadBucket(bParams, TestLocalBucketName)
	tassert.CheckFatal(t, err)

	// check that bucket props are reset
	validateBucketProps(t, globalProps, *p)

	if isCloudBucket(t, bParams.URL, clibucket) {
		err = api.ResetBucketProps(bParams, clibucket)
		tassert.CheckFatal(t, err)
		p, err = api.HeadBucket(bParams, clibucket)
		tassert.CheckFatal(t, err)
		if p.EC.Enabled {
			t.Error("EC should be disabled for cloud bucket")
		}
	}
}

func TestCloudListObjectVersions(t *testing.T) {
	var (
		workerCount = 10
		objectDir   = "cloud-version-test"
		objectSize  = 256
		objectCount = 1340 // must be greater than 1000(AWS page size)

		bucket   = clibucket
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		wg       = &sync.WaitGroup{}
	)

	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}
	if !isCloudBucket(t, proxyURL, bucket) {
		t.Skip("test requires a cloud bucket")
	}

	// Enable local versioning management
	baseParams := tutils.BaseAPIParams(proxyURL)
	err := api.SetBucketProps(baseParams, bucket,
		cmn.SimpleKVs{cmn.HeaderBucketVerEnabled: "true"})
	if err != nil {
		t.Fatal(err)
	}
	defer api.SetBucketProps(baseParams, bucket,
		cmn.SimpleKVs{cmn.HeaderBucketVerEnabled: "false"})

	// Enabling local versioning may not work if the cloud bucket has
	// versioning disabled. So, request props and do double check
	bprops, err := api.HeadBucket(baseParams, bucket)
	if err != nil {
		t.Fatal(err)
	}

	if !bprops.Versioning.Enabled {
		t.Skip("test requires a cloud bucket with enabled versioning")
	}

	tutils.Logf("Filling the bucket %q\n", bucket)
	for wid := 0; wid < workerCount; wid++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			reader, err := tutils.NewRandReader(int64(objectSize), true)
			tassert.CheckFatal(t, err)
			objectsToPut := objectCount / workerCount
			if wid == workerCount-1 { // last worker puts leftovers
				objectsToPut += objectCount % workerCount
			}
			putRR(t, reader, bucket, objectDir, objectsToPut)
		}(wid)
	}
	wg.Wait()

	tutils.Logf("Reading bucket %q objects\n", bucket)
	msg := &cmn.SelectMsg{Prefix: objectDir, Props: cmn.GetPropsVersion}
	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, cmn.CloudBs)
	bckObjs, err := api.ListBucket(baseParams, bucket, msg, 0, query)
	tassert.CheckError(t, err)

	tutils.Logf("Checking bucket %q object versions[total: %d]\n", bucket, len(bckObjs.Entries))
	for _, entry := range bckObjs.Entries {
		if entry.Version == "" {
			t.Errorf("Object %s does not have version", entry.Name)
		}
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			err := api.DeleteObject(baseParams, bucket, name, cmn.CloudBs)
			tassert.CheckError(t, err)
		}(entry.Name)
	}
	wg.Wait()
}

func TestListObjects(t *testing.T) {
	var (
		iterations  = 10
		workerCount = 10
		dirLen      = 5
		objectSize  = 256

		bucket   = t.Name() + "Bucket"
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		wg       = &sync.WaitGroup{}
		random   = cmn.NowRand()
	)

	if testing.Short() {
		iterations = 3
	}

	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	// Iterations of PUT
	totalObjects := 0
	for iter := 0; iter < iterations; iter++ {
		objectCount := random.Intn(2048) + 2000
		totalObjects += objectCount
		for wid := 0; wid < workerCount; wid++ {
			wg.Add(1)
			go func(wid int) {
				defer wg.Done()

				reader, err := tutils.NewRandReader(int64(objectSize), true)
				tassert.CheckFatal(t, err)
				objDir := tutils.RandomObjDir(random, dirLen, 5)
				objectsToPut := objectCount / workerCount
				if wid == workerCount-1 { // last worker puts leftovers
					objectsToPut += objectCount % workerCount
				}
				putRR(t, reader, bucket, objDir, objectsToPut)
			}(wid)
		}
		wg.Wait()

		// Confirm PUTs
		bckObjs, err := tutils.ListObjects(proxyURL, bucket, cmn.LocalBs, "", 0)
		tassert.CheckFatal(t, err)

		if len(bckObjs) != totalObjects {
			t.Errorf("actual objects %d, expected: %d", len(bckObjs), totalObjects)
		}
	}
}

// Tests URL to quickly get all objects in a bucket
func TestListObjectFast(t *testing.T) {
	const (
		bucket       = "quick-list-bucket"
		numObjs      = 1234 // greater than default PageSize=1000
		objSize      = 1024
		commonPrefix = "quick"
	)
	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)

	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)
	sgl := tutils.Mem2.NewSGL(objSize)
	defer sgl.Free()

	tutils.Logf("Creating %d objects in %s bucket\n", numObjs, bucket)
	errCh := make(chan error, numObjs)
	objsPutCh := make(chan string, numObjs)
	objList := make([]string, 0, numObjs)
	for i := 0; i < numObjs; i++ {
		fname := fmt.Sprintf("q-%04d", i)
		objList = append(objList, fname)
	}
	tutils.PutObjsFromList(proxyURL, bucket, DeleteDir, readerType, commonPrefix, objSize, objList, errCh, objsPutCh, sgl)
	selectErr(errCh, "put", t, true /* fatal - if PUT does not work then it makes no sense to continue */)
	close(objsPutCh)

	tutils.Logln("Reading objects...")
	baseParams := tutils.BaseAPIParams(proxyURL)
	reslist, err := api.ListBucketFast(baseParams, bucket, nil)
	if err != nil {
		t.Fatalf("List bucket %s failed, err = %v", bucket, err)
	}
	tutils.Logf("Object count: %d\n", len(reslist.Entries))
	if len(reslist.Entries) != numObjs {
		t.Fatalf("Expected %d objects, received %d objects",
			numObjs, len(reslist.Entries))
	}

	// check that all props are zeros, except name
	// check if all names are different
	var empty cmn.BucketEntry
	uniqueNames := make(map[string]bool, len(reslist.Entries))
	for _, e := range reslist.Entries {
		if e.Name == "" {
			t.Errorf("Invalid size or name: %#v", *e)
			continue
		}
		if strings.Contains(e.Name, "q-") {
			uniqueNames[e.Name] = true
		}
		if e.Checksum != empty.Checksum ||
			e.Size != empty.Size ||
			e.Atime != empty.Atime ||
			e.Version != empty.Version ||
			e.TargetURL != empty.TargetURL ||
			e.Flags != empty.Flags ||
			e.Copies != empty.Copies {
			t.Errorf("Some fields do not have default values: %#v", *e)
		}
	}
	if len(reslist.Entries) != len(uniqueNames) {
		t.Fatalf("Expected %d unique objects, found only %d unique objects",
			len(reslist.Entries), len(uniqueNames))
	}

	query := make(url.Values)
	prefix := commonPrefix + "/q-009"
	query.Set(cmn.URLParamPrefix, prefix)
	reslist, err = api.ListBucketFast(baseParams, bucket, nil, query)
	if err != nil {
		t.Fatalf("List bucket %s with prefix %s failed, err = %v",
			bucket, prefix, err)
	}
	tutils.Logf("Object count (with prefix %s): %d\n", prefix, len(reslist.Entries))
	// Get should return only objects from q-0090 through q-0099
	if len(reslist.Entries) != 10 {
		t.Fatalf("Expected %d objects with prefix %s, received %d objects",
			numObjs, prefix, len(reslist.Entries))
	}
}

func TestBucketSingleProp(t *testing.T) {
	const (
		dataSlices      = 3
		paritySlices    = 4
		objLimit        = 300 * cmn.KiB
		mirrorThreshold = 15
	)
	var (
		bucket     = t.Name() + "Bucket"
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.DefaultBaseAPIParams(t)
	)

	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	tutils.Logf("Changing bucket %q properties...\n", bucket)
	// Enabling EC should set default value for number of slices if it is 0
	if err := api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "true"}); err != nil {
		t.Error(err)
	} else {
		p, err := api.HeadBucket(tutils.DefaultBaseAPIParams(t), bucket)
		tassert.CheckFatal(t, err)
		if !p.EC.Enabled {
			t.Error("EC was not enabled")
		}
		if p.EC.DataSlices != 2 {
			t.Errorf("Number of data slices is incorrect: %d (expected 2)", p.EC.DataSlices)
		}
		if p.EC.ParitySlices != 2 {
			t.Errorf("Number of parity slices is incorrect: %d (expected 2)", p.EC.DataSlices)
		}
	}

	// Enabling mirroring should set default value for number of copies if it is 0
	if err := api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketMirrorEnabled: "true"}); err != nil {
		t.Error(err)
	} else {
		p, err := api.HeadBucket(tutils.DefaultBaseAPIParams(t), bucket)
		tassert.CheckFatal(t, err)
		if !p.Mirror.Enabled {
			t.Error("Mirroring was not enabled")
		}
		if p.Mirror.Copies != 2 {
			t.Errorf("Number of copies is incorrect: %d (expected 2)", p.Mirror.Copies)
		}
	}

	// Need to disable EC first
	if err := api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "false"}); err != nil {
		t.Error(err)
	}

	// Change a few more bucket properties
	if err := api.SetBucketProps(baseParams, bucket,
		cmn.SimpleKVs{
			cmn.HeaderBucketECData:    strconv.Itoa(dataSlices),
			cmn.HeaderBucketECParity:  strconv.Itoa(paritySlices),
			cmn.HeaderBucketECMinSize: strconv.Itoa(objLimit),
		}); err != nil {
		t.Error(err)
	}

	// Enable EC again
	if err := api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "true"}); err != nil {
		t.Error(err)
	} else {
		p, err := api.HeadBucket(tutils.DefaultBaseAPIParams(t), bucket)
		tassert.CheckFatal(t, err)
		if p.EC.DataSlices != dataSlices {
			t.Errorf("Number of data slices was not changed to %d. Current value %d", dataSlices, p.EC.DataSlices)
		}
		if p.EC.ParitySlices != paritySlices {
			t.Errorf("Number of parity slices was not changed to %d. Current value %d", paritySlices, p.EC.ParitySlices)
		}
		if p.EC.ObjSizeLimit != objLimit {
			t.Errorf("Minimal EC object size was not changed to %d. Current value %d", objLimit, p.EC.ObjSizeLimit)
		}
	}

	if err := api.SetBucketProps(baseParams, bucket,
		cmn.SimpleKVs{cmn.HeaderBucketMirrorThresh: strconv.Itoa(mirrorThreshold)}); err != nil {
		t.Error(err)
	} else {
		p, err := api.HeadBucket(tutils.DefaultBaseAPIParams(t), bucket)
		tassert.CheckFatal(t, err)
		if p.Mirror.UtilThresh != mirrorThreshold {
			t.Errorf("Mirror utilization threshold was not changed to %d. Current value %d", mirrorThreshold, p.Mirror.UtilThresh)
		}
	}

	// Disable EC
	if err := api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "false"}); err != nil {
		t.Error(err)
	} else {
		p, err := api.HeadBucket(tutils.DefaultBaseAPIParams(t), bucket)
		tassert.CheckFatal(t, err)
		if p.EC.Enabled {
			t.Error("EC was not disabled")
		}
	}

	// Disable Mirroring
	if err := api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketMirrorEnabled: "false"}); err != nil {
		t.Error(err)
	} else {
		p, err := api.HeadBucket(tutils.DefaultBaseAPIParams(t), bucket)
		tassert.CheckFatal(t, err)
		if p.Mirror.Enabled {
			t.Error("Mirroring was not disabled")
		}
	}
}

func TestSetBucketPropsOfNonexistentBucket(t *testing.T) {
	var (
		bucket string

		baseParams = tutils.DefaultBaseAPIParams(t)
		query      = url.Values{}
	)
	query.Set(cmn.URLParamBckProvider, "cloud")

	bucket, err := tutils.GenerateNonexistentBucketName(t.Name()+"Bucket", baseParams)
	tassert.CheckFatal(t, err)

	err = api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "true"}, query)

	if err == nil {
		t.Fatalf("Expected SetBucketProps error, but got none.")
	}

	errAsHTTPError, ok := err.(*cmn.HTTPError)
	if !ok {
		t.Fatalf("Expected error of *cmn.HTTPError type.")
	}
	if errAsHTTPError.Status != http.StatusNotFound {
		t.Errorf("Expected status: %d, but got: %d.", http.StatusNotFound, errAsHTTPError.Status)
	}
}

func TestSetAllBucketPropsOfNonexistentBucket(t *testing.T) {
	var (
		bucket string

		baseParams  = tutils.DefaultBaseAPIParams(t)
		bucketProps = defaultBucketProps()
		query       = url.Values{}
	)
	query.Set(cmn.URLParamBckProvider, "cloud")

	bucket, err := tutils.GenerateNonexistentBucketName(t.Name()+"Bucket", baseParams)
	tassert.CheckFatal(t, err)

	err = api.SetBucketPropsMsg(baseParams, bucket, bucketProps, query)

	if err == nil {
		t.Fatalf("Expected SetBucketPropsMsg error, but got none.")
	}

	errAsHTTPError, ok := err.(*cmn.HTTPError)
	if !ok {
		t.Fatalf("Expected error of *cmn.HTTPError type.")
	}
	if errAsHTTPError.Status != http.StatusNotFound {
		t.Errorf("Expected status: %d, but got: %d.", http.StatusNotFound, errAsHTTPError.Status)
	}
}

func TestBucketInvalidName(t *testing.T) {
	var (
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.DefaultBaseAPIParams(t)
	)

	invalidNames := []string{
		cmn.ListAll, ".", "", " ", "bucket and name",
		"bucket/name",
	}

	for _, name := range invalidNames {
		if err := api.CreateLocalBucket(baseParams, name); err == nil {
			tutils.DestroyLocalBucket(t, proxyURL, name)
			t.Errorf("accepted bucket with name %s", name)
		}
	}
}

//===============================================================
//
// n-way mirror
//
//===============================================================
func TestLocalMirror(t *testing.T) {
	total, copies2, copies3 := testLocalMirror(t, 0, 0)
	if copies2 != total || copies3 != 0 {
		t.Fatalf("Expecting %d objects all to have 2 replicas, got copies2=%d, copies3=%d",
			total, copies2, copies3)
	}
}
func TestLocalMirror2_1(t *testing.T) {
	total, copies2, copies3 := testLocalMirror(t, 1, 0)
	if copies2 != 0 || copies3 != 0 {
		t.Fatalf("Expecting %d objects to have 1 replica, got %d", total, copies2)
	}
}

// NOTE: targets must have at least 4 mountpaths for this test to PASS
func TestLocalMirror2_1_3(t *testing.T) {
	total, copies2, copies3 := testLocalMirror(t, 1, 3)
	if copies3 != total || copies2 != 0 {
		t.Fatalf("Expecting %d objects to have 3 replicas, got %d", total, copies3)
	}
}

func testLocalMirror(t *testing.T, num1, num2 int) (total, copies2, copies3 int) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		m = ioContext{
			t:               t,
			num:             10000,
			numGetsEachFile: 5,
		}
	)

	m.saveClusterState()

	{
		targets := tutils.ExtractTargetNodes(m.smap)
		baseParams := tutils.DefaultBaseAPIParams(t)
		mpList, err := api.GetMountpaths(baseParams, targets[0])
		tassert.CheckFatal(t, err)

		l := len(mpList.Available)
		max := cmn.Max(cmn.Max(2, num1), num2) + 1
		if l < max {
			t.Skipf("test %q requires at least %d mountpaths (target %s has %d)", t.Name(), max, targets[0], l)
		}
	}

	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)

	{
		var (
			bucketProps = defaultBucketProps()
		)
		primary, err := tutils.GetPrimaryProxy(proxyURLReadOnly)
		tassert.CheckFatal(t, err)

		baseParams := tutils.DefaultBaseAPIParams(t)
		config, err := api.GetDaemonConfig(baseParams, primary.ID())
		tassert.CheckFatal(t, err)

		// copy default config and change one field
		bucketProps.Mirror = config.Mirror
		bucketProps.Mirror.Enabled = true
		err = api.SetBucketPropsMsg(baseParams, m.bucket, bucketProps)
		tassert.CheckFatal(t, err)

		p, err := api.HeadBucket(baseParams, m.bucket)
		tassert.CheckFatal(t, err)
		if p.Mirror.Copies != 2 {
			t.Fatalf("%d copies != 2", p.Mirror.Copies)
		}
	}

	m.puts()
	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()

	baseParams := tutils.BaseAPIParams(m.proxyURL)

	if num1 != 0 {
		makeNCopies(t, num1, m.bucket, baseParams)
	}
	if num2 != 0 {
		if num1 != 0 {
			time.Sleep(time.Second * 30)
		}
		makeNCopies(t, num2, m.bucket, baseParams)
	}

	// List Bucket - primarily for the copies
	msg := &cmn.SelectMsg{Props: cmn.GetPropsCopies + ", " + cmn.GetPropsAtime + ", " + cmn.GetPropsStatus}
	objectList, err := api.ListBucket(baseParams, m.bucket, msg, 0)
	tassert.CheckFatal(t, err)

	m.wg.Wait()

	for _, entry := range objectList.Entries {
		if entry.Atime == "" {
			t.Errorf("%s/%s: access time is empty", m.bucket, entry.Name)
		}
		total++
		if entry.Copies == 2 {
			copies2++
		} else if entry.Copies == 3 {
			copies3++
		}
	}
	tutils.Logf("objects (total, 2-copies, 3-copies) = (%d, %d, %d)\n", total, copies2, copies3)
	if total != m.num {
		t.Fatalf("listbucket: expecting %d objects, got %d", m.num, total)
	}
	return
}

func makeNCopies(t *testing.T, ncopies int, bucket string, baseParams *api.BaseParams) {
	tutils.Logf("Set copies = %d\n", ncopies)
	if err := api.MakeNCopies(baseParams, bucket, ncopies); err != nil {
		t.Fatalf("Failed to start copies=%d xaction, err: %v", ncopies, err)
	}
	timedout := 60 // seconds
	ok := false
	for i := 0; i < timedout+1; i++ {
		time.Sleep(time.Second)

		allDetails, err := api.MakeXactGetRequest(baseParams, cmn.ActMakeNCopies, cmn.ActXactStats, bucket, true)
		tassert.CheckFatal(t, err)
		ok = true
		for tid := range allDetails {
			detail := allDetails[tid][0] // TODO
			if detail.Running() {
				ok = false
				break
			}
		}
		if ok {
			break
		}
	}
	if !ok {
		t.Fatalf("timed-out waiting for %s to finish", cmn.ActMakeNCopies)
	}
}

func TestCloudMirror(t *testing.T) {
	var (
		num = 64
	)
	baseParams := tutils.DefaultBaseAPIParams(t)
	if !isCloudBucket(t, baseParams.URL, clibucket) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}

	// evict
	query := make(url.Values)
	query.Add(cmn.URLParamBckProvider, cmn.CloudBs)
	err := api.EvictCloudBucket(baseParams, clibucket, query)
	tassert.CheckFatal(t, err)

	// enable mirror
	err = api.SetBucketProps(baseParams, clibucket, cmn.SimpleKVs{cmn.HeaderBucketMirrorEnabled: "true"})
	tassert.CheckFatal(t, err)
	defer api.SetBucketProps(baseParams, clibucket, cmn.SimpleKVs{cmn.HeaderBucketMirrorEnabled: "false"})

	// list
	msg := &cmn.SelectMsg{}
	objectList, err := api.ListBucket(baseParams, clibucket, msg, 0)
	tassert.CheckFatal(t, err)

	l := len(objectList.Entries)
	if l < num {
		t.Skipf("%s: insufficient number of objects in the Cloud bucket %s, required %d", t.Name(), clibucket, num)
	}
	smap := getClusterMap(t, baseParams.URL)
	{
		target := tutils.ExtractTargetNodes(smap)[0]
		mpList, err := api.GetMountpaths(baseParams, target)
		tassert.CheckFatal(t, err)

		numps := len(mpList.Available)
		if numps < 4 {
			t.Skipf("test %q requires at least 4 mountpaths (target %s has %d)", t.Name(), target.ID(), numps)
		}
	}

	// cold GET - causes local mirroring
	tutils.Logf("cold GET %d object into a 2-way mirror...\n", num)
	j := int(time.Now().UnixNano() % int64(l))
	for i := 0; i < num; i++ {
		e := objectList.Entries[(j+i)%l]
		_, err := api.GetObject(baseParams, clibucket, e.Name)
		tassert.CheckFatal(t, err)
	}

	time.Sleep(time.Second * 10) // FIXME: better handle on when copying is done

	msg = &cmn.SelectMsg{Props: cmn.GetPropsCopies + ", " + cmn.GetPropsIsCached + ", " + cmn.GetPropsAtime}
	query = make(url.Values)
	query.Set(cmn.URLParamCached, "true")
	objectList, err = api.ListBucket(baseParams, clibucket, msg, 0, query)
	tassert.CheckFatal(t, err)

	total, copies2, copies3, cached := countObjects(objectList)
	tutils.Logf("objects (total, 2-copies, 3-copies, cached) = (%d, %d, %d, %d)\n", total, copies2, copies3, cached)
	if copies2 < num {
		t.Fatalf("listbucket: expecting %d 2-copies, got %d", num, copies2)
	}

	makeNCopies(t, 3, clibucket, baseParams)
	objectList, err = api.ListBucket(baseParams, clibucket, msg, 0, query)
	tassert.CheckFatal(t, err)

	total, copies2, copies3, cached = countObjects(objectList)
	tutils.Logf("objects (total, 2-copies, 3-copies, cached) = (%d, %d, %d, %d)\n", total, copies2, copies3, cached)
	if copies3 < num {
		t.Fatalf("listbucket: expecting %d 3-copies, got %d", num, copies3)
	}
}

func countObjects(objectList *cmn.BucketList) (total, copies2, copies3, cached int) {
	for _, entry := range objectList.Entries {
		total++
		if entry.Copies == 2 {
			copies2++
		} else if entry.Copies == 3 {
			copies3++
		}
		if entry.IsCached() {
			cached++
		}
	}
	return
}

func TestBucketReadOnly(t *testing.T) {
	var (
		m = ioContext{
			t:               t,
			num:             10,
			numGetsEachFile: 2,
		}
	)
	m.init()
	tutils.CreateFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyLocalBucket(t, m.proxyURL, m.bucket)
	baseParams := tutils.DefaultBaseAPIParams(t)

	m.puts()
	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()
	m.wg.Wait()

	p, err := api.HeadBucket(baseParams, m.bucket)
	tassert.CheckFatal(t, err)

	// make bucket read-only
	aattrs := cmn.MakeAccess(p.AccessAttrs, cmn.DenyAccess, cmn.AccessPUT|cmn.AccessDELETE)
	s := strconv.FormatUint(aattrs, 10)
	err = api.SetBucketProps(baseParams, m.bucket, cmn.SimpleKVs{cmn.HeaderBucketAccessAttrs: s})
	tassert.CheckFatal(t, err)

	m.init()
	nerr := m.puts(true /* don't fail */)
	if nerr != m.num {
		t.Fatalf("num failed PUTs %d, expecting %d", nerr, m.num)
	}

	// restore write access
	s = strconv.FormatUint(p.AccessAttrs, 10)
	err = api.SetBucketProps(baseParams, m.bucket, cmn.SimpleKVs{cmn.HeaderBucketAccessAttrs: s})
	tassert.CheckFatal(t, err)

	// write some more and destroy
	m.init()
	nerr = m.puts(true /* don't fail */)
	if nerr != 0 {
		t.Fatalf("num failed PUTs %d, expecting 0 (zero)", nerr)
	}
}
