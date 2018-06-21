/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package dfc_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
	"github.com/NVIDIA/dfcpub/pkg/client/readers"
)

func TestGetObjectInNextTier(t *testing.T) {
	var (
		object = "multitier-test-object"
		data   = []byte("this is the object you want!")
	)

	nextTierMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == dfc.URLPath(dfc.Rversion, dfc.Robjects, clibucket, object) {
			if r.Method == http.MethodHead && r.URL.Query().Get(dfc.URLParamCheckCached) == "true" {
				w.WriteHeader(http.StatusOK)
			} else if r.Method == http.MethodGet {
				w.Write(data)
			} else {
				http.Error(w, "bad request", http.StatusBadRequest)
			}
		}
	}))
	defer nextTierMock.Close()

	err := client.SetBucketProps(proxyurl, clibucket, dfc.BucketProps{
		CloudProvider: dfc.ProviderDfc,
		NextTierURL:   nextTierMock.URL})
	checkFatal(err, t)
	defer resetBucketProps(clibucket, t)

	n, _, err := client.Get(proxyurl, clibucket, object, nil, nil, false, false)
	checkFatal(err, t)
	if int(n) != len(data) {
		t.Errorf("Expected object size: %d bytes, actual: %d bytes", len(data), int(n))
	}
}

func TestGetObjectInNextTierErrorOnGet(t *testing.T) {
	var (
		object = "multitier-test-object"
		data   = []byte("this is the object you want!")
	)

	if !isCloudBucket(t, proxyurl, clibucket) {
		t.Skipf("skipping test - bucket: %s is not a cloud bucket", clibucket)
	}

	nextTierMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == dfc.URLPath(dfc.Rversion, dfc.Robjects, clibucket, object) {
			if r.Method == http.MethodHead && r.URL.Query().Get(dfc.URLParamCheckCached) == "true" {
				w.WriteHeader(http.StatusOK)
			} else if r.Method == http.MethodGet {
				http.Error(w, "some arbitrary internal server error", http.StatusInternalServerError)
			} else {
				http.Error(w, "bad request", http.StatusBadRequest)
			}
		}
	}))
	defer nextTierMock.Close()

	u := proxyurl + dfc.URLPath(dfc.Rversion, dfc.Robjects, clibucket, object)
	req, err := http.NewRequest(http.MethodPut, u, bytes.NewReader(data))
	checkFatal(err, t)

	resp, err := http.DefaultClient.Do(req)
	checkFatal(err, t)

	if resp.StatusCode >= http.StatusBadRequest {
		t.Errorf("Expected status code 200, received status code %d", resp.StatusCode)
	}

	err = client.Evict(proxyurl, clibucket, object)
	checkFatal(err, t)

	err = client.SetBucketProps(proxyurl, clibucket, dfc.BucketProps{
		CloudProvider: dfc.ProviderDfc,
		NextTierURL:   nextTierMock.URL})
	checkFatal(err, t)
	defer resetBucketProps(clibucket, t)

	n, _, err := client.Get(proxyurl, clibucket, object, nil, nil, false, false)
	checkFatal(err, t)
	if int(n) != len(data) {
		t.Errorf("Expected object size: %d bytes, actual: %d bytes", len(data), int(n))
	}
}

func TestGetObjectNotInNextTier(t *testing.T) {
	var (
		object   = "multitier-get-test-object"
		data     = []byte("this is some other object - not the one you want!")
		filesize = 1024
	)

	if !isCloudBucket(t, proxyurl, clibucket) {
		t.Skipf("skipping test - bucket: %s is not a cloud bucket", clibucket)
	}

	nextTierMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == dfc.URLPath(dfc.Rversion, dfc.Robjects, clibucket, object) {
			if r.Method == http.MethodHead && r.URL.Query().Get(dfc.URLParamCheckCached) == "true" {
				http.Error(w, "not found", http.StatusNotFound)
			} else if r.Method == http.MethodGet {
				w.Write(data)
			} else {
				http.Error(w, "bad request", http.StatusBadRequest)
			}
		}
	}))
	defer nextTierMock.Close()

	reader, err := readers.NewRandReader(int64(filesize), false)
	checkFatal(err, t)

	err = client.Put(proxyurl, reader, clibucket, object, true)
	checkFatal(err, t)

	err = client.Evict(proxyurl, clibucket, object)
	checkFatal(err, t)

	err = client.SetBucketProps(proxyurl, clibucket, dfc.BucketProps{
		CloudProvider: dfc.ProviderDfc,
		NextTierURL:   nextTierMock.URL})
	checkFatal(err, t)
	defer resetBucketProps(clibucket, t)

	n, _, err := client.Get(proxyurl, clibucket, object, nil, nil, false, false)
	checkFatal(err, t)
	if int(n) != filesize {
		t.Errorf("Expected object size: %d bytes, actual: %d bytes", filesize, int(n))
	}

	if err = client.Del(proxyurl, clibucket, object, nil, nil, true); err != nil {
		t.Logf("bucket/object: %s/%s not deleted, err: %v", clibucket, object, err)
	}
}

func resetBucketProps(bucket string, t *testing.T) {
	if err := client.SetBucketProps(proxyurl, bucket, dfc.BucketProps{}); err != nil {
		t.Errorf("bucket: %s props not reset, err: %v", clibucket, err)
	}
}
