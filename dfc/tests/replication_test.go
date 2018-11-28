/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc_test

import (
	"io"
	"math/rand"
	"path"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/tutils"
)

const (
	dummySrcURL = "http://127.0.0.1:10088"
	badChecksum = "badChecksumValue"
)

var (
	// TODO: not ready yet but will be - see daemon.go as well
	replTests = []Test{
		{"testReplicationReceiveOneObject", testReplicationReceiveOneObject},
		{"testReplicationReceiveOneObjectNoChecksum", testReplicationReceiveOneObjectNoChecksum},
		{"testReplicationReceiveOneObjectBadChecksum", testReplicationReceiveOneObjectBadChecksum},
		{"testReplicationReceiveManyObjectsCloudBucket", testReplicationReceiveManyObjectsCloudBucket},
		{"testReplicationEndToEndUsingLocalBucket", testReplicationEndToEndUsingLocalBucket},
		{"testReplicationEndToEndUsingCloudBucket", testReplicationEndToEndUsingCloudBucket},
	}
)

func TestReplication(t *testing.T) {
	t.Skip("skipping replication because it does not work yet")

	for _, test := range replTests {
		t.Run(test.name, test.method)
		if t.Failed() && abortonerr {
			t.FailNow()
		}
	}
}

func testReplicationReceiveOneObject(t *testing.T) {
	const (
		object  = "TestReplicationReceiveOneObject"
		objSize = int64(1024)
	)
	reader, err := tutils.NewRandReader(objSize, false)
	tutils.CheckFatal(err, t)

	proxyURLData := getPrimaryIntraDataURL(t, proxyURLRO)
	proxyURL := getPrimaryURL(t, proxyURLRO)
	xxhash := getXXHashChecksum(t, reader)
	isCloud := isCloudBucket(t, proxyURL, clibucket)

	createFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer destroyLocalBucket(t, proxyURL, TestLocalBucketName)

	tutils.Logf("Sending %s/%s for replication. Destination proxy: %s\n", TestLocalBucketName, object, proxyURLData)
	err = httpReplicationPut(t, dummySrcURL, proxyURLData, TestLocalBucketName, object, xxhash, reader)
	tutils.CheckFatal(err, t)

	if isCloud {
		tutils.Logf("Sending %s/%s for replication. Destination proxy: %s\n", clibucket, object, proxyURLData)
		err = httpReplicationPut(t, dummySrcURL, proxyURLData, clibucket, object, xxhash, reader)
		tutils.CheckFatal(err, t)
		tutils.Del(proxyURL, clibucket, object, nil, nil, true)
	}

}

func testReplicationReceiveOneObjectNoChecksum(t *testing.T) {
	const (
		object  = "TestReplicationReceiveOneObjectNoChecksum"
		objSize = int64(1024)
	)
	reader, err := tutils.NewRandReader(objSize, false)
	tutils.CheckFatal(err, t)

	proxyURLData := getPrimaryIntraDataURL(t, proxyURLRO)
	proxyURL := getPrimaryURL(t, proxyURLRO)
	isCloud := isCloudBucket(t, proxyURL, clibucket)

	createFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer destroyLocalBucket(t, proxyURL, TestLocalBucketName)

	tutils.Logf("Sending %s/%s for replication. Destination proxy: %s. Expecting to fail\n", TestLocalBucketName, object, proxyURLData)
	err = httpReplicationPut(t, dummySrcURL, proxyURLData, TestLocalBucketName, object, "", reader)

	if err == nil {
		t.Error("Replication PUT to local bucket without checksum didn't fail")
	}

	if isCloud {
		tutils.Logf("Sending %s/%s for replication. Destination proxy: %s. Expecting to fail\n", clibucket, object, proxyURLData)
		err = httpReplicationPut(t, dummySrcURL, proxyURLData, clibucket, object, "", reader)
		if err == nil {
			t.Error("Replication PUT to local bucket without checksum didn't fail")
		}
	}
}

func testReplicationReceiveOneObjectBadChecksum(t *testing.T) {
	const (
		object  = "TestReplicationReceiveOneObjectBadChecksum"
		objSize = int64(1024)
	)
	reader, err := tutils.NewRandReader(objSize, false)
	tutils.CheckFatal(err, t)

	proxyURLData := getPrimaryIntraDataURL(t, proxyURLRO)
	proxyURL := getPrimaryURL(t, proxyURLRO)
	isCloud := isCloudBucket(t, proxyURL, clibucket)

	createFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer destroyLocalBucket(t, proxyURL, TestLocalBucketName)

	tutils.Logf("Sending %s/%s for replication. Destination proxy: %s. Expecting to fail\n", TestLocalBucketName, object, proxyURLData)
	err = httpReplicationPut(t, dummySrcURL, proxyURLData, TestLocalBucketName, object, badChecksum, reader)
	if err == nil {
		t.Error("Replication PUT to local bucket with bad checksum didn't fail")
	}

	if isCloud {
		tutils.Logf("Sending %s/%s for replication. Destination proxy: %s. Expecting to fail\n", clibucket, object, proxyURLData)
		err = httpReplicationPut(t, dummySrcURL, proxyURLData, clibucket, object, clibucket, reader)
		if err == nil {
			t.Error("Replication PUT to local bucket with bad checksum didn't fail")
		}
	}
}

// The following test places objects into a local bucket then hits
// the replication endpoint to replicate the objects to the next tier cluster.
// It then checks to ensure the objects exist on the next tier cluster
// through get requests.
func testReplicationEndToEndUsingLocalBucket(t *testing.T) {
	const (
		objSize = int64(1024)
		numObj  = 100
	)
	var (
		bucket       = TestLocalBucketName
		sgl          *memsys.SGL
		err          error
		proxyURL     = getPrimaryURL(t, proxyURLRO)
		proxyURLNext string
		objNameCh    = make(chan string, numObj)
	)

	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	if usingSG {
		sgl = tutils.Mem2.NewSGL(objSize)
		defer sgl.Free()
	}

	if !tutils.DockerRunning() || tutils.ClusterCount() < 2 {
		t.Skip("test requires a docker running with two clusters")
	}

	proxyURLNext = getPrimaryURL(t, proxyNextTierURLRO)

	// 1. Create and Configure the Buckets on two different clusters
	tutils.Logln("Creating local buckets on first and second tier cluster for replication...")
	createFreshLocalBucket(t, proxyURL, bucket)
	defer destroyLocalBucket(t, proxyURL, bucket)

	createFreshLocalBucket(t, proxyURLNext, bucket)
	defer destroyLocalBucket(t, proxyURLNext, bucket)

	// 2. Send N Put Requests to first cluster, where N is equal to numObj
	tutils.Logf("Uploading %d objects to (local bucket: %s) for replication...\n", numObj, bucket)
	errChPut := make(chan error, numObj)
	tutils.PutRandObjs(proxyURL, bucket, ReplicationDir, readerType, ReplicationStr, uint64(objSize), numObj, errChPut, objNameCh, sgl)
	selectErr(errChPut, "put", t, true)
	close(objNameCh) // to exit for-range
	objList := make([]string, 0, numObj)
	for fname := range objNameCh {
		objList = append(objList, filepath.Join(ReplicationStr, fname))
	}

	// 3. Set Bucket Properties after placing the objects on the cluster to avoid the automatic replication
	// that occurs when the NextTierURL is set for the bucket.
	tutils.Logf("Setting local bucket properties to allow for replication...\n")
	bucketProps := defaultBucketProps()
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = proxyURLNext
	err = api.SetBucketProps(tutils.DefaultBaseAPIParams(t), bucket, bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, bucket, t)

	// 4. Send N Replication Requests to replicate and move N objects from
	// first cluster to second cluster
	objectsToReplicate := map[string][]string{bucket: objList}
	tutils.Logf("Using the replicate endpoint to replicate %d objects to the next tier cluster...\n", numObj)
	errMap := tutils.ReplicateMultipleObjects(proxyURL, objectsToReplicate)
	if len(errMap) != 0 {
		t.Fatalf("Error replicating the following objects %v", errMap)
	}

	// 5. Send N Get Requests to obtain N objects from the second cluster
	// that were just replicated
	tutils.Logf("Getting %d objects from the next tier cluster to ensure replication has successfully taken place...\n", numObj)
	errChGet := make(chan error, numObj)
	getfromfilelist(t, proxyURLNext, bucket, errChGet, objList, false)
	selectErr(errChGet, "get replica", t, true)
}

// The following test places objects into a cloud bucket then hits
// the replication endpoint to replicate the objects to the next tier cluster.
// It then checks to ensure the objects exist on the next tier cluster
// through get requests.
func testReplicationEndToEndUsingCloudBucket(t *testing.T) {
	const (
		objSize = int64(1024)
		numObj  = 100
	)
	var (
		bucket       = clibucket
		sgl          *memsys.SGL
		err          error
		proxyURL     = getPrimaryURL(t, proxyURLRO)
		proxyURLNext string
		objNameCh    = make(chan string, numObj)
	)

	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	if !isCloudBucket(t, proxyURL, bucket) {
		t.Skip("test requires a Cloud bucket")
	}

	if !tutils.DockerRunning() || tutils.ClusterCount() < 2 {
		t.Skip("test requires docker running with two clusters")
	}

	if usingSG {
		sgl = tutils.Mem2.NewSGL(objSize)
		defer sgl.Free()
	}

	proxyURLNext = getPrimaryURL(t, proxyNextTierURLRO)

	// 1. Send N Put Requests to first cluster, where N is equal to numObj
	tutils.Logf("Uploading %d objects to (cloud bucket: %s) for replication...\n", numObj, bucket)
	errChPut := make(chan error, numObj)
	tutils.PutRandObjs(proxyURL, bucket, ReplicationDir, readerType, ReplicationStr, uint64(objSize), numObj, errChPut, objNameCh, sgl)
	selectErr(errChPut, "put", t, true)
	close(objNameCh) // to exit for-range
	objList := make([]string, 0, numObj)
	for fname := range objNameCh {
		objList = append(objList, filepath.Join(ReplicationStr, fname))
	}

	defer func() {
		errChDel := make(chan error, numObj*2)
		deletefromfilelist(proxyURL, bucket, errChDel, objList)
		selectErr(errChDel, "delete", t, false)
	}()

	// 2. Set Bucket Properties after placing the objects on the cluster to avoid the automatic replication
	// that occurs when the NextTierURL is set for the bucket.
	tutils.Logf("Setting cloud bucket properties to allow for replication\n")
	bucketProps := defaultBucketProps()
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = proxyURLNext
	err = api.SetBucketProps(tutils.DefaultBaseAPIParams(t), bucket, bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, bucket, t)

	// 3. Send N Replication Requests to replicate and move N objects from
	// first cluster to second cluster
	objectsToReplicate := map[string][]string{bucket: objList}
	tutils.Logf("Using the replicate endpoint to replicate %d objects to the next tier cluster...\n", numObj)
	errMap := tutils.ReplicateMultipleObjects(proxyURL, objectsToReplicate)
	if len(errMap) != 0 {
		t.Fatalf("Error replicating the following objects %v", errMap)
	}

	defer func() {
		errChDelNext := make(chan error, numObj*2)
		deletefromfilelist(proxyURLNext, bucket, errChDelNext, objList)
		selectErr(errChDelNext, "delete", t, false)
	}()

	// 4. Send N Get Requests to obtain N objects from the second cluster
	// that were just replicated
	tutils.Logf("Getting %d objects from the next tier cluster to ensure replication has successfully taken place...\n", numObj)
	errChGet := make(chan error, numObj)
	getfromfilelist(t, proxyURLNext, bucket, errChGet, objList, false)
	selectErr(errChGet, "get replica", t, true)
}

func testReplicationReceiveManyObjectsCloudBucket(t *testing.T) {
	const (
		objSize   = 1024
		numObj    = 100
		seedValue = int64(111)
	)
	var (
		proxyURLData = getPrimaryIntraDataURL(t, proxyURLRO)
		proxyURL     = getPrimaryURL(t, proxyURLRO)
		bucket       = clibucket
		size         = int64(objSize)
		r            tutils.Reader
		sgl          *memsys.SGL
		errCnt       int
		err          error
	)

	if testing.Short() {
		t.Skip(skipping)
	}
	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip("test requires a Cloud bucket")
	}

	tutils.Logf("Sending %d objects (cloud bucket: %s) for replication...\n", numObj, bucket)

	objList := make([]string, 0, numObj)
	src := rand.NewSource(seedValue)
	random := rand.New(src)
	for i := 0; i < numObj; i++ {
		fname := tutils.FastRandomFilename(random, fnlen)
		objList = append(objList, fname)
	}

	if size == 0 {
		size = int64(random.Intn(1024)+1) * 1024
	}

	if usingSG {
		sgl = tutils.Mem2.NewSGL(size)
		defer sgl.Free()
	}

	for idx, fname := range objList {
		object := path.Join(SmokeStr, fname)
		if sgl != nil {
			sgl.Reset()
			r, err = tutils.NewSGReader(sgl, int64(size), true)
		} else {
			r, err = tutils.NewReader(tutils.ParamReader{Type: readerType, SGL: nil, Path: SmokeDir, Name: fname, Size: int64(size)})
		}

		if err != nil {
			t.Error(err)
			tutils.Logf("Failed to generate random object %s, err: %v\n", filepath.Join(SmokeDir, fname), err)
		}

		tutils.Logf("Receiving replica: %s (%d/%d)...\n", object, idx+1, numObj)
		err = httpReplicationPut(t, dummySrcURL, proxyURLData, bucket, object, r.XXHash(), r)
		if err != nil {
			errCnt++
			t.Errorf("ERROR: %v\n", err)
		}
	}
	tutils.Logf("Successful: %d/%d. Failed: %d/%d\n", numObj-errCnt, numObj, errCnt, numObj)
}

func getPrimaryIntraDataURL(t *testing.T, proxyURL string) string {
	smap := getClusterMap(t, proxyURL)
	return smap.ProxySI.IntraDataNet.DirectURL
}

func getXXHashChecksum(t *testing.T, reader io.Reader) string {
	buf, slab := tutils.Mem2.AllocFromSlab2(0)
	xxHashVal, errstr := cmn.ComputeXXHash(reader, buf)
	slab.Free(buf)
	if errstr != "" {
		t.Fatal("Failed to compute xxhash checksum")
	}
	return xxHashVal
}

func httpReplicationPut(t *testing.T, srcURL, dstProxyURL, bucket, object, xxhash string, reader tutils.Reader) error {
	url := dstProxyURL + cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)
	baseParams := tutils.BaseAPIParams(url)
	replicateParams := api.ReplicateObjectInput{SourceURL: srcURL}
	return api.PutObject(baseParams, bucket, object, xxhash, reader, replicateParams)
}
