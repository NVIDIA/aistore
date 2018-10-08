/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package dfc_test

import (
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
		bucket    = TestLocalBucketName
	)

	proxyURL := getPrimaryURL(t, proxyURLRO)
	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip("TestGetObjectInNextTier requires a cloud bucket")
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

	createFreshLocalBucket(t, proxyURL, bucket)
	defer destroyLocalBucket(t, proxyURL, bucket)

	bucketProps := dfc.NewBucketProps()
	bucketProps.CloudProvider = api.ProviderDFC
	bucketProps.NextTierURL = nextTierMockForLocalBucket.URL
	err := client.SetBucketProps(proxyURL, bucket, *bucketProps)
	checkFatal(err, t)
	defer resetBucketProps(proxyURL, bucket, t)

	bucketProps = dfc.NewBucketProps()
	bucketProps.CloudProvider = api.ProviderDFC
	bucketProps.NextTierURL = nextTierMockForCloudBucket.URL
	err = client.SetBucketProps(proxyURL, clibucket, *bucketProps)
	checkFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	n, _, err := client.Get(proxyURL, bucket, object, nil, nil, false, false)
	checkFatal(err, t)
	if int(n) != len(localData) {
		t.Errorf("Expected object size: %d bytes, actual: %d bytes", len(localData), int(n))
	}

	n, _, err = client.Get(proxyURL, clibucket, object, nil, nil, false, false)
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

	proxyURL := getPrimaryURL(t, proxyURLRO)
	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip("TestGetObjectInNextTierErrorOnGet requires a cloud bucket")
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

	u := proxyURL + api.URLPath(api.Version, api.Objects, clibucket, object)
	err := client.HTTPRequest(http.MethodPut, u, readers.NewBytesReader(data))

	checkFatal(err, t)
	defer deleteCloudObject(proxyURL, clibucket, object, t)

	err = client.Evict(proxyURL, clibucket, object)
	checkFatal(err, t)

	bucketProps := dfc.NewBucketProps()
	bucketProps.CloudProvider = api.ProviderDFC
	bucketProps.NextTierURL = nextTierMock.URL
	err = client.SetBucketProps(proxyURL, clibucket, *bucketProps)
	checkFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	n, _, err := client.Get(proxyURL, clibucket, object, nil, nil, false, false)
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

	proxyURL := getPrimaryURL(t, proxyURLRO)
	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip("TestGetObjectNotInNextTier requires a cloud bucket")
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

	err = client.Put(proxyURL, reader, clibucket, object, true)
	checkFatal(err, t)
	defer deleteCloudObject(proxyURL, clibucket, object, t)

	err = client.Evict(proxyURL, clibucket, object)
	checkFatal(err, t)

	bucketProps := dfc.NewBucketProps()
	bucketProps.CloudProvider = api.ProviderDFC
	bucketProps.NextTierURL = nextTierMock.URL
	err = client.SetBucketProps(proxyURL, clibucket, *bucketProps)
	checkFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	n, _, err := client.Get(proxyURL, clibucket, object, nil, nil, false, false)
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

	proxyURL := getPrimaryURL(t, proxyURLRO)
	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip("TestPutObjectNextTierPolicy requires a cloud bucket")
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

	createFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer destroyLocalBucket(t, proxyURL, TestLocalBucketName)

	bucketProps := dfc.NewBucketProps()
	bucketProps.CloudProvider = api.ProviderDFC
	bucketProps.NextTierURL = nextTierMockForLocalBucket.URL
	err := client.SetBucketProps(proxyURL, TestLocalBucketName, *bucketProps)
	checkFatal(err, t)
	defer resetBucketProps(proxyURL, TestLocalBucketName, t)

	bucketProps = dfc.NewBucketProps()
	bucketProps.CloudProvider = api.ProviderDFC
	bucketProps.NextTierURL = nextTierMockForCloudBucket.URL
	bucketProps.WritePolicy = dfc.RWPolicyNextTier
	err = client.SetBucketProps(proxyURL, clibucket, *bucketProps)
	checkFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	u := proxyURL + api.URLPath(api.Version, api.Objects, TestLocalBucketName, object)
	err = client.HTTPRequest(http.MethodPut, u, readers.NewBytesReader(localData))
	checkFatal(err, t)

	if nextTierMockForLocalBucketReached != 1 {
		t.Errorf("Expected to hit nextTierMockForLocalBucket 1 time, actual: %d",
			nextTierMockForLocalBucketReached)
	}

	u = proxyURL + api.URLPath(api.Version, api.Objects, clibucket, object)
	err = client.HTTPRequest(http.MethodPut, u, readers.NewBytesReader(cloudData))
	checkFatal(err, t)

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

	proxyURL := getPrimaryURL(t, proxyURLRO)
	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip("TestPutObjectNextTierPolicyErrorOnPut requires a cloud bucket")
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
	err := client.SetBucketProps(proxyURL, clibucket, *bucketProps)
	checkFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	u := proxyURL + api.URLPath(api.Version, api.Objects, clibucket, object)
	err = client.HTTPRequest(http.MethodPut, u, readers.NewBytesReader(data))
	checkFatal(err, t)
	defer deleteCloudObject(proxyURL, clibucket, object, t)

	err = client.Evict(proxyURL, clibucket, object)
	checkFatal(err, t)

	n, _, err := client.Get(proxyURL, clibucket, object, nil, nil, false, false)
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

	proxyURL := getPrimaryURL(t, proxyURLRO)
	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip("TestPutObjectCloudPolicy requires a cloud bucket")
	}

	nextTierMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer nextTierMock.Close()

	bucketProps := dfc.NewBucketProps()
	bucketProps.CloudProvider = api.ProviderDFC
	bucketProps.NextTierURL = nextTierMock.URL
	bucketProps.WritePolicy = dfc.RWPolicyCloud
	err := client.SetBucketProps(proxyURL, clibucket, *bucketProps)
	checkFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	u := proxyURL + api.URLPath(api.Version, api.Objects, clibucket, object)
	err = client.HTTPRequest(http.MethodPut, u, readers.NewBytesReader(data))
	checkFatal(err, t)

	deleteCloudObject(proxyURL, clibucket, object, t)
}

func resetBucketProps(proxyURL, bucket string, t *testing.T) {
	if err := client.ResetBucketProps(proxyURL, bucket); err != nil {
		t.Errorf("bucket: %s props not reset, err: %v", clibucket, err)
	}
}

func deleteCloudObject(proxyURL, bucket, object string, t *testing.T) {
	if err := client.Del(proxyURL, bucket, object, nil, nil, true); err != nil {
		t.Errorf("bucket/object: %s/%s not deleted, err: %v", bucket, object, err)
	}
}
