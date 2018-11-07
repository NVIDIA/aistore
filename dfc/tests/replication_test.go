/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc_test

import (
	"io"
	"math/rand"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/tutils"
)

const (
	dummySrcURL = "http://127.0.0.1:10088"
	badChecksum = "badChecksumValue"
)

func TestReplicationReceiveOneObject(t *testing.T) {
	const (
		object   = "TestReplicationReceiveOneObject"
		fileSize = int64(1024)
	)
	reader, err := tutils.NewRandReader(fileSize, false)
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

func TestReplicationReceiveOneObjectNoChecksum(t *testing.T) {
	const (
		object   = "TestReplicationReceiveOneObjectNoChecksum"
		fileSize = int64(1024)
	)
	reader, err := tutils.NewRandReader(fileSize, false)
	tutils.CheckFatal(err, t)

	proxyURLData := getPrimaryIntraDataURL(t, proxyURLRO)
	proxyURL := getPrimaryURL(t, proxyURLRO)
	isCloud := isCloudBucket(t, proxyURL, clibucket)

	createFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer destroyLocalBucket(t, proxyURL, TestLocalBucketName)

	url := proxyURLData + cmn.URLPath(cmn.Version, cmn.Objects, TestLocalBucketName, object)
	headers := map[string]string{
		cmn.HeaderDFCReplicationSrc: dummySrcURL,
	}
	tutils.Logf("Sending %s/%s for replication. Destination proxy: %s. Expecting to fail\n", TestLocalBucketName, object, proxyURLData)
	err = tutils.HTTPRequest(http.MethodPut, url, reader, headers)

	if err == nil {
		t.Error("Replication PUT to local bucket without checksum didn't fail")
	}

	if isCloud {
		url = proxyURLData + cmn.URLPath(cmn.Version, cmn.Objects, clibucket, object)
		tutils.Logf("Sending %s/%s for replication. Destination proxy: %s. Expecting to fail\n", clibucket, object, proxyURLData)
		err = tutils.HTTPRequest(http.MethodPut, url, reader, headers)

		if err == nil {
			t.Error("Replication PUT to local bucket without checksum didn't fail")
		}
	}
}

func TestReplicationReceiveOneObjectBadChecksum(t *testing.T) {
	const (
		object   = "TestReplicationReceiveOneObjectBadChecksum"
		fileSize = int64(1024)
	)
	reader, err := tutils.NewRandReader(fileSize, false)
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

func TestReplicationReceiveManyObjectsCloudBucket(t *testing.T) {
	const (
		fileSize  = 1024
		numFiles  = 100
		seedValue = int64(111)
	)
	var (
		proxyURLData = getPrimaryIntraDataURL(t, proxyURLRO)
		proxyURL     = getPrimaryURL(t, proxyURLRO)
		bucket       = clibucket
		size         = int64(fileSize)
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

	tutils.Logf("Sending %d files (cloud bucket: %s) for replication...\n", numFiles, bucket)

	fileList := make([]string, 0, numFiles)
	src := rand.NewSource(seedValue)
	random := rand.New(src)
	for i := 0; i < numFiles; i++ {
		fname := tutils.FastRandomFilename(random, fnlen)
		fileList = append(fileList, fname)
	}

	if size == 0 {
		size = int64(random.Intn(1024)+1) * 1024
	}

	if usingSG {
		sgl = tutils.Mem2.NewSGL(size)
		defer sgl.Free()
	}

	for idx, fname := range fileList {
		object := SmokeStr + "/" + fname
		if sgl != nil {
			sgl.Reset()
			r, err = tutils.NewSGReader(sgl, int64(size), true)
		} else {
			r, err = tutils.NewReader(tutils.ParamReader{Type: readerType, SGL: nil, Path: SmokeDir, Name: fname, Size: int64(size)})
		}

		if err != nil {
			t.Error(err)
			tutils.Logf("Failed to generate random file %s, err: %v\n", filepath.Join(SmokeDir, fname), err)
		}

		tutils.Logf("Receiving replica: %s (%d/%d)...\n", object, idx+1, numFiles)
		err = httpReplicationPut(t, dummySrcURL, proxyURLData, bucket, object, r.XXHash(), r)
		if err != nil {
			errCnt++
			t.Errorf("ERROR: %v\n", err)
		}
	}
	tutils.Logf("Successful: %d/%d. Failed: %d/%d\n", numFiles-errCnt, numFiles, errCnt, numFiles)
}

func getPrimaryIntraDataURL(t *testing.T, proxyURL string) string {
	smap, err := tutils.GetClusterMap(proxyURL)
	if err != nil {
		t.Fatalf("Failed to get primary proxy intra data net URL, error: %v", err)
	}
	return smap.ProxySI.IntraDataNet.DirectURL
}

func getXXHashChecksum(t *testing.T, reader io.Reader) string {
	buf, slab := memsys.AllocFromSlab(0)
	xxHashVal, errstr := cmn.ComputeXXHash(reader, buf)
	slab.Free(buf)
	if errstr != "" {
		t.Fatal("Failed to compute xxhash checksum")
	}
	return xxHashVal
}

func httpReplicationPut(t *testing.T, srcURL, dstProxyURL, bucket, object, xxhash string, reader tutils.Reader) error {
	url := dstProxyURL + cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)
	headers := map[string]string{
		cmn.HeaderDFCReplicationSrc: srcURL,
		cmn.HeaderDFCChecksumType:   cmn.ChecksumXXHash,
		cmn.HeaderDFCChecksumVal:    xxhash,
	}
	return tutils.HTTPRequest(http.MethodPut, url, reader, headers)
}
