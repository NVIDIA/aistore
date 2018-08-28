/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package dfc_test

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
	"github.com/NVIDIA/dfcpub/pkg/client/readers"
)

func TestGetObjectInNextTier(t *testing.T) {
	var (
		object    = "TestGetObjectInNextTier"
		localData = []byte("Toto, I've got a feeling we're not in Kansas anymore.")
		cloudData = []byte("Here's looking at you, kid.")
	)

	if !isCloudBucket(t, proxyurl, clibucket) {
		t.Skipf("skipping test - bucket: %s is not a cloud bucket", clibucket)
	}

	nextTierMockForLocalBucket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == api.URLPath(api.Version, api.Objects, TestLocalBucketName, object) {
			if r.Method == http.MethodHead && r.URL.Query().Get(api.URLParamCheckCached) == "true" {
				w.WriteHeader(http.StatusOK)
			} else if r.Method == http.MethodGet {
				w.Write(localData)
			}
		} else {
			http.Error(w, "bad request to nextTierMockForLocalBucket", http.StatusBadRequest)
		}
	}))
	nextTierMockForCloudBucket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == api.URLPath(api.Version, api.Objects, clibucket, object) {
			if r.Method == http.MethodHead && r.URL.Query().Get(api.URLParamCheckCached) == "true" {
				w.WriteHeader(http.StatusOK)
			} else if r.Method == http.MethodGet {
				w.Write(cloudData)
			}
		} else {
			http.Error(w, "bad request to nextTierMockForCloudBucket", http.StatusBadRequest)
		}
	}))
	defer nextTierMockForLocalBucket.Close()
	defer nextTierMockForCloudBucket.Close()

	err := client.CreateLocalBucket(proxyurl, TestLocalBucketName)
	checkFatal(err, t)
	defer deleteLocalBucket(TestLocalBucketName, t)

	bucketProps := dfc.NewBucketProps()
	bucketProps.CloudProvider = api.ProviderDFC
	bucketProps.NextTierURL = nextTierMockForLocalBucket.URL
	err = client.SetBucketProps(proxyurl, TestLocalBucketName, *bucketProps)
	checkFatal(err, t)
	defer resetBucketProps(TestLocalBucketName, t)

	bucketProps = dfc.NewBucketProps()
	bucketProps.CloudProvider = api.ProviderDFC
	bucketProps.NextTierURL = nextTierMockForCloudBucket.URL
	err = client.SetBucketProps(proxyurl, clibucket, *bucketProps)
	checkFatal(err, t)
	defer resetBucketProps(clibucket, t)

	n, _, err := client.Get(proxyurl, TestLocalBucketName, object, nil, nil, false, false)
	checkFatal(err, t)
	if int(n) != len(localData) {
		t.Errorf("Expected object size: %d bytes, actual: %d bytes", len(localData), int(n))
	}

	n, _, err = client.Get(proxyurl, clibucket, object, nil, nil, false, false)
	checkFatal(err, t)
	if int(n) != len(cloudData) {
		t.Errorf("Expected object size: %d bytes, actual: %d bytes", len(cloudData), int(n))
	}
}

func TestGetObjectInNextTierErrorOnGet(t *testing.T) {
	var (
		object = "TestGetObjectInNextTierErrorOnGet"
		data   = []byte("this is the object you want!")
	)

	if !isCloudBucket(t, proxyurl, clibucket) {
		t.Skipf("skipping test - bucket: %s is not a cloud bucket", clibucket)
	}

	nextTierMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == api.URLPath(api.Version, api.Objects, clibucket, object) {
			if r.Method == http.MethodHead && r.URL.Query().Get(api.URLParamCheckCached) == "true" {
				w.WriteHeader(http.StatusOK)
			} else if r.Method == http.MethodGet {
				http.Error(w, "arbitrary internal server error from nextTierMock", http.StatusInternalServerError)
			} else {
				http.Error(w, "bad request to nextTierMock", http.StatusBadRequest)
			}
		}
	}))
	defer nextTierMock.Close()

	u := proxyurl + api.URLPath(api.Version, api.Objects, clibucket, object)
	req, err := http.NewRequest(http.MethodPut, u, bytes.NewReader(data))
	checkFatal(err, t)

	resp, err := http.DefaultClient.Do(req)
	checkFatal(err, t)
	defer deleteCloudObject(clibucket, object, t)

	if resp.StatusCode >= http.StatusBadRequest {
		t.Errorf("Expected status code 200, received status code %d", resp.StatusCode)
	}

	err = client.Evict(proxyurl, clibucket, object)
	checkFatal(err, t)

	bucketProps := dfc.NewBucketProps()
	bucketProps.CloudProvider = api.ProviderDFC
	bucketProps.NextTierURL = nextTierMock.URL
	err = client.SetBucketProps(proxyurl, clibucket, *bucketProps)
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
		object   = "TestGetObjectNotInNextTier"
		data     = []byte("this is some other object - not the one you want!")
		filesize = 1024
	)

	if !isCloudBucket(t, proxyurl, clibucket) {
		t.Skipf("skipping test - bucket: %s is not a cloud bucket", clibucket)
	}

	nextTierMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == api.URLPath(api.Version, api.Objects, clibucket, object) {
			if r.Method == http.MethodHead && r.URL.Query().Get(api.URLParamCheckCached) == "true" {
				http.Error(w, "not found in nextTierMock", http.StatusNotFound)
			} else if r.Method == http.MethodGet {
				w.Write(data)
			} else {
				http.Error(w, "bad request to nextTierMock", http.StatusBadRequest)

			}
		}
	}))
	defer nextTierMock.Close()

	reader, err := readers.NewRandReader(int64(filesize), false)
	checkFatal(err, t)

	err = client.Put(proxyurl, reader, clibucket, object, true)
	checkFatal(err, t)
	defer deleteCloudObject(clibucket, object, t)

	err = client.Evict(proxyurl, clibucket, object)
	checkFatal(err, t)

	bucketProps := dfc.NewBucketProps()
	bucketProps.CloudProvider = api.ProviderDFC
	bucketProps.NextTierURL = nextTierMock.URL
	err = client.SetBucketProps(proxyurl, clibucket, *bucketProps)
	checkFatal(err, t)
	defer resetBucketProps(clibucket, t)

	n, _, err := client.Get(proxyurl, clibucket, object, nil, nil, false, false)
	checkFatal(err, t)

	if int(n) != filesize {
		t.Errorf("Expected object size: %d bytes, actual: %d bytes", filesize, int(n))
	}
}

func TestPutObjectNextTierPolicy(t *testing.T) {
	const (
		object = "TestPutObjectNextTierPolicy"
	)
	var (
		localData                         = []byte("May the Force be with you.")
		cloudData                         = []byte("I'm going to make him an offer he can't refuse.")
		nextTierMockForLocalBucketReached int
		nextTierMockForCloudBucketReached int
	)

	if !isCloudBucket(t, proxyurl, clibucket) {
		t.Skipf("skipping test - bucket: %s is not a cloud bucket", clibucket)
	}

	nextTierMockForLocalBucket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == api.URLPath(api.Version, api.Objects, TestLocalBucketName, object) &&
			r.Method == http.MethodPut {
			b, err := ioutil.ReadAll(r.Body)
			checkFatal(err, t)
			expected := string(localData)
			received := string(b)
			if expected != received {
				t.Errorf("Expected object data: %s, received object data: %s", expected, received)
			}
			nextTierMockForLocalBucketReached += 1
		} else {
			http.Error(w, "bad request to nextTierMockForLocal", http.StatusBadRequest)
		}
	}))
	nextTierMockForCloudBucket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == api.URLPath(api.Version, api.Objects, clibucket, object) && r.Method == http.MethodPut {
			b, err := ioutil.ReadAll(r.Body)
			checkFatal(err, t)
			expected := string(cloudData)
			received := string(b)
			if expected != received {
				t.Errorf("Expected object data: %s, received object data: %s", expected, received)
			}
			nextTierMockForCloudBucketReached += 1
		} else {
			http.Error(w, "bad request to nextTierMockForCloud", http.StatusBadRequest)
		}
	}))
	defer nextTierMockForLocalBucket.Close()
	defer nextTierMockForCloudBucket.Close()

	err := client.CreateLocalBucket(proxyurl, TestLocalBucketName)
	checkFatal(err, t)
	defer deleteLocalBucket(TestLocalBucketName, t)

	bucketProps := dfc.NewBucketProps()
	bucketProps.CloudProvider = api.ProviderDFC
	bucketProps.NextTierURL = nextTierMockForLocalBucket.URL
	err = client.SetBucketProps(proxyurl, TestLocalBucketName, *bucketProps)
	checkFatal(err, t)
	defer resetBucketProps(TestLocalBucketName, t)

	bucketProps = dfc.NewBucketProps()
	bucketProps.CloudProvider = api.ProviderDFC
	bucketProps.NextTierURL = nextTierMockForCloudBucket.URL
	bucketProps.WritePolicy = dfc.RWPolicyNextTier
	err = client.SetBucketProps(proxyurl, clibucket, *bucketProps)
	checkFatal(err, t)
	defer resetBucketProps(clibucket, t)

	u := proxyurl + api.URLPath(api.Version, api.Objects, TestLocalBucketName, object)
	req, err := http.NewRequest(http.MethodPut, u, bytes.NewReader(localData))
	checkFatal(err, t)

	resp, err := http.DefaultClient.Do(req)
	checkFatal(err, t)

	if resp.StatusCode >= http.StatusBadRequest {
		t.Errorf("Expected status code 200, received status code %d", resp.StatusCode)
	}
	if nextTierMockForLocalBucketReached != 1 {
		t.Errorf("Expected to hit nextTierMockForLocalBucket 1 time, actual: %d",
			nextTierMockForLocalBucketReached)
	}

	u = proxyurl + api.URLPath(api.Version, api.Objects, clibucket, object)
	req, err = http.NewRequest(http.MethodPut, u, bytes.NewReader(cloudData))
	checkFatal(err, t)

	resp, err = http.DefaultClient.Do(req)
	checkFatal(err, t)

	if resp.StatusCode >= http.StatusBadRequest {
		t.Errorf("Expected status code 200, received status code %d", resp.StatusCode)
	}
	if nextTierMockForCloudBucketReached != 1 {
		t.Errorf("Expected to hit nextTierMockForCloudBucket 1 time, actual: %d",
			nextTierMockForCloudBucketReached)
	}
}

func TestPutObjectNextTierPolicyErrorOnPut(t *testing.T) {
	var (
		object = "TestPutObjectNextTierPolicyErrorOnPut"
		data   = []byte("this object will go to the cloud!")
	)

	if !isCloudBucket(t, proxyurl, clibucket) {
		t.Skipf("skipping test - bucket: %s is not a cloud bucket", clibucket)
	}

	nextTierMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "some arbitrary internal server error", http.StatusInternalServerError)
	}))
	defer nextTierMock.Close()

	bucketProps := dfc.NewBucketProps()
	bucketProps.CloudProvider = api.ProviderDFC
	bucketProps.NextTierURL = nextTierMock.URL
	bucketProps.ReadPolicy = dfc.RWPolicyCloud
	bucketProps.WritePolicy = dfc.RWPolicyNextTier
	err := client.SetBucketProps(proxyurl, clibucket, *bucketProps)
	checkFatal(err, t)
	defer resetBucketProps(clibucket, t)

	u := proxyurl + api.URLPath(api.Version, api.Objects, clibucket, object)
	req, err := http.NewRequest(http.MethodPut, u, bytes.NewReader(data))
	checkFatal(err, t)

	resp, err := http.DefaultClient.Do(req)
	checkFatal(err, t)
	defer deleteCloudObject(clibucket, object, t)

	if resp.StatusCode >= http.StatusBadRequest {
		t.Errorf("Expected status code 200, received status code %d", resp.StatusCode)
	}

	err = client.Evict(proxyurl, clibucket, object)
	checkFatal(err, t)

	n, _, err := client.Get(proxyurl, clibucket, object, nil, nil, false, false)
	checkFatal(err, t)

	if int(n) != len(data) {
		t.Errorf("Expected object size: %d bytes, actual: %d bytes", len(data), int(n))
	}
}

func TestPutObjectCloudPolicy(t *testing.T) {
	var (
		object = "TestPutObjectCloudPolicy"
		data   = []byte("this object will go to the cloud!")
	)

	if !isCloudBucket(t, proxyurl, clibucket) {
		t.Skipf("skipping test - bucket: %s is not a cloud bucket", clibucket)
	}

	nextTierMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer nextTierMock.Close()

	bucketProps := dfc.NewBucketProps()
	bucketProps.CloudProvider = api.ProviderDFC
	bucketProps.NextTierURL = nextTierMock.URL
	bucketProps.WritePolicy = dfc.RWPolicyCloud
	err := client.SetBucketProps(proxyurl, clibucket, *bucketProps)
	checkFatal(err, t)
	defer resetBucketProps(clibucket, t)

	u := proxyurl + api.URLPath(api.Version, api.Objects, clibucket, object)
	req, err := http.NewRequest(http.MethodPut, u, bytes.NewReader(data))
	checkFatal(err, t)

	resp, err := http.DefaultClient.Do(req)
	checkFatal(err, t)
	defer deleteCloudObject(clibucket, object, t)

	if resp.StatusCode >= http.StatusBadRequest {
		t.Errorf("Expected status code 200, received status code %d", resp.StatusCode)
	}
}

func resetBucketProps(bucket string, t *testing.T) {
	if err := client.SetBucketProps(proxyurl, bucket, *dfc.NewBucketProps()); err != nil {
		t.Errorf("bucket: %s props not reset, err: %v", clibucket, err)
	}
}

func deleteCloudObject(bucket, object string, t *testing.T) {
	if err := client.Del(proxyurl, bucket, object, nil, nil, true); err != nil {
		t.Errorf("bucket/object: %s/%s not deleted, err: %v", bucket, object, err)
	}
}

func deleteLocalBucket(bucket string, t *testing.T) {
	if err := client.DestroyLocalBucket(proxyurl, bucket); err != nil {
		t.Errorf("local bucket: %s not deleted, err: %v", bucket, err)
	}
}
