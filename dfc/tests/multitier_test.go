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
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/tutils"
)

func TestGetObjectInNextTier(t *testing.T) {
	var (
		object    = "TestGetObjectInNextTier"
		localData = []byte("Toto, I've got a feeling we're not in Kansas anymore.")
		cloudData = []byte("Here's looking at you, kid.")
	)

	proxyURL := getPrimaryURL(t, proxyURLRO)
	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip("TestGetObjectInNextTier requires a cloud bucket")
	}

	nextTierMockForLocalBucket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == cmn.URLPath(cmn.Version, cmn.Objects, TestLocalBucketName, object) {
			if r.Method == http.MethodHead && r.URL.Query().Get(cmn.URLParamCheckCached) == "true" {
				w.WriteHeader(http.StatusOK)
			} else if r.Method == http.MethodGet {
				w.Write(localData)
			}
		} else {
			http.Error(w, "bad request to nextTierMockForLocalBucket", http.StatusBadRequest)
		}
	}))
	nextTierMockForCloudBucket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == cmn.URLPath(cmn.Version, cmn.Objects, clibucket, object) {
			if r.Method == http.MethodHead && r.URL.Query().Get(cmn.URLParamCheckCached) == "true" {
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

	createFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer destroyLocalBucket(t, proxyURL, TestLocalBucketName)

	bucketProps := testBucketProps(t)
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = nextTierMockForLocalBucket.URL
	err := api.SetBucketProps(tutils.HTTPClient, proxyURL, TestLocalBucketName, *bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, TestLocalBucketName, t)

	bucketProps = testBucketProps(t)
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = nextTierMockForCloudBucket.URL
	err = api.SetBucketProps(tutils.HTTPClient, proxyURL, clibucket, *bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	n, _, err := tutils.Get(proxyURL, TestLocalBucketName, object, nil, nil, false, false)
	tutils.CheckFatal(err, t)
	if int(n) != len(localData) {
		t.Errorf("Expected object size: %d bytes, actual: %d bytes", len(localData), int(n))
	}

	n, _, err = tutils.Get(proxyURL, clibucket, object, nil, nil, false, false)
	tutils.CheckFatal(err, t)
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
		if r.URL.Path == cmn.URLPath(cmn.Version, cmn.Objects, clibucket, object) {
			if r.Method == http.MethodHead && r.URL.Query().Get(cmn.URLParamCheckCached) == "true" {
				w.WriteHeader(http.StatusOK)
			} else if r.Method == http.MethodGet {
				http.Error(w, "arbitrary internal server error from nextTierMock", http.StatusInternalServerError)
			} else {
				http.Error(w, "bad request to nextTierMock", http.StatusBadRequest)
			}
		}
	}))
	defer nextTierMock.Close()

	u := proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, clibucket, object)
	err := tutils.HTTPRequest(http.MethodPut, u, tutils.NewBytesReader(data))

	tutils.CheckFatal(err, t)
	defer deleteCloudObject(proxyURL, clibucket, object, t)

	err = tutils.Evict(proxyURL, clibucket, object)
	tutils.CheckFatal(err, t)

	bucketProps := testBucketProps(t)
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = nextTierMock.URL
	err = api.SetBucketProps(tutils.HTTPClient, proxyURL, clibucket, *bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	n, _, err := tutils.Get(proxyURL, clibucket, object, nil, nil, false, false)
	tutils.CheckFatal(err, t)

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
		if r.URL.Path == cmn.URLPath(cmn.Version, cmn.Objects, clibucket, object) {
			if r.Method == http.MethodHead && r.URL.Query().Get(cmn.URLParamCheckCached) == "true" {
				http.Error(w, "not found in nextTierMock", http.StatusNotFound)
			} else if r.Method == http.MethodGet {
				w.Write(data)
			} else {
				http.Error(w, "bad request to nextTierMock", http.StatusBadRequest)

			}
		}
	}))
	defer nextTierMock.Close()

	reader, err := tutils.NewRandReader(int64(filesize), false)
	tutils.CheckFatal(err, t)

	err = tutils.Put(proxyURL, reader, clibucket, object, true)
	tutils.CheckFatal(err, t)
	defer deleteCloudObject(proxyURL, clibucket, object, t)

	err = tutils.Evict(proxyURL, clibucket, object)
	tutils.CheckFatal(err, t)

	bucketProps := testBucketProps(t)
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = nextTierMock.URL
	err = api.SetBucketProps(tutils.HTTPClient, proxyURL, clibucket, *bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	n, _, err := tutils.Get(proxyURL, clibucket, object, nil, nil, false, false)
	tutils.CheckFatal(err, t)

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
		if r.URL.Path == cmn.URLPath(cmn.Version, cmn.Objects, TestLocalBucketName, object) &&
			r.Method == http.MethodPut {
			b, err := ioutil.ReadAll(r.Body)
			tutils.CheckFatal(err, t)
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
		if r.URL.Path == cmn.URLPath(cmn.Version, cmn.Objects, clibucket, object) && r.Method == http.MethodPut {
			b, err := ioutil.ReadAll(r.Body)
			tutils.CheckFatal(err, t)
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

	bucketProps := testBucketProps(t)
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = nextTierMockForLocalBucket.URL
	err := api.SetBucketProps(tutils.HTTPClient, proxyURL, TestLocalBucketName, *bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, TestLocalBucketName, t)

	bucketProps = testBucketProps(t)
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = nextTierMockForCloudBucket.URL
	bucketProps.WritePolicy = cmn.RWPolicyNextTier
	err = api.SetBucketProps(tutils.HTTPClient, proxyURL, clibucket, *bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	u := proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, TestLocalBucketName, object)
	err = tutils.HTTPRequest(http.MethodPut, u, tutils.NewBytesReader(localData))
	tutils.CheckFatal(err, t)

	if nextTierMockForLocalBucketReached != 1 {
		t.Errorf("Expected to hit nextTierMockForLocalBucket 1 time, actual: %d",
			nextTierMockForLocalBucketReached)
	}

	u = proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, clibucket, object)
	err = tutils.HTTPRequest(http.MethodPut, u, tutils.NewBytesReader(cloudData))
	tutils.CheckFatal(err, t)

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

	bucketProps := testBucketProps(t)
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = nextTierMock.URL
	bucketProps.ReadPolicy = cmn.RWPolicyCloud
	bucketProps.WritePolicy = cmn.RWPolicyNextTier
	err := api.SetBucketProps(tutils.HTTPClient, proxyURL, clibucket, *bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	u := proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, clibucket, object)
	err = tutils.HTTPRequest(http.MethodPut, u, tutils.NewBytesReader(data))
	tutils.CheckFatal(err, t)
	defer deleteCloudObject(proxyURL, clibucket, object, t)

	err = tutils.Evict(proxyURL, clibucket, object)
	tutils.CheckFatal(err, t)

	n, _, err := tutils.Get(proxyURL, clibucket, object, nil, nil, false, false)
	tutils.CheckFatal(err, t)

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

	bucketProps := testBucketProps(t)
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = nextTierMock.URL
	bucketProps.WritePolicy = cmn.RWPolicyCloud
	err := api.SetBucketProps(tutils.HTTPClient, proxyURL, clibucket, *bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	u := proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, clibucket, object)
	err = tutils.HTTPRequest(http.MethodPut, u, tutils.NewBytesReader(data))
	tutils.CheckFatal(err, t)

	deleteCloudObject(proxyURL, clibucket, object, t)
}

func resetBucketProps(proxyURL, bucket string, t *testing.T) {
	if err := api.ResetBucketProps(tutils.HTTPClient, proxyURL, bucket); err != nil {
		t.Errorf("bucket: %s props not reset, err: %v", clibucket, err)
	}
}

func deleteCloudObject(proxyURL, bucket, object string, t *testing.T) {
	if err := tutils.Del(proxyURL, bucket, object, nil, nil, true); err != nil {
		t.Errorf("bucket/object: %s/%s not deleted, err: %v", bucket, object, err)
	}
}
