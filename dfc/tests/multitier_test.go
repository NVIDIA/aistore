/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package dfc_test

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/tutils"
)

func TestGetObjectInNextTier(t *testing.T) {
	var (
		object              = t.Name()
		localData           = []byte("Toto, I've got a feeling we're not in Kansas anymore.")
		cloudData           = []byte("Here's looking at you, kid.")
		localBucketListener net.Listener
		cloudBucketListener net.Listener
		proxyURL            = getPrimaryURL(t, proxyURLRO)
		baseParams          = tutils.BaseAPIParams(proxyURL)
		err                 error
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip(fmt.Sprintf("test %q requires a cloud bucket", t.Name()))
	}

	if tutils.DockerRunning() {
		// use the default docker0 bridge ip address
		localBucketListener, err = net.Listen("tcp", "172.17.0.1:0")
		if err != nil {
			t.Errorf("Failed to initialize test server listener when using docker mode for local bucket")
		}

		cloudBucketListener, err = net.Listen("tcp", "172.17.0.1:0")
		if err != nil {
			t.Errorf("Failed to initialize test server listener when using docker mode for cloud bucket")
		}
	}

	nextTierMockForLocalBucket := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	nextTierMockForCloudBucket := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	if tutils.DockerRunning() {
		nextTierMockForLocalBucket.Listener.Close()
		nextTierMockForLocalBucket.Listener = localBucketListener
		nextTierMockForCloudBucket.Listener.Close()
		nextTierMockForCloudBucket.Listener = cloudBucketListener
	}
	nextTierMockForLocalBucket.Start()
	nextTierMockForCloudBucket.Start()
	defer nextTierMockForLocalBucket.Close()
	defer nextTierMockForCloudBucket.Close()

	createFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer destroyLocalBucket(t, proxyURL, TestLocalBucketName)

	bucketProps := testBucketProps(t)
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = nextTierMockForLocalBucket.URL
	err = api.SetBucketProps(baseParams, TestLocalBucketName, *bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, TestLocalBucketName, t)

	bucketProps = testBucketProps(t)
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = nextTierMockForCloudBucket.URL
	err = api.SetBucketProps(baseParams, clibucket, *bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	n, err := api.GetObject(baseParams, TestLocalBucketName, object)
	tutils.CheckFatal(err, t)
	if int(n) != len(localData) {
		t.Errorf("Expected object size: %d bytes, actual: %d bytes", len(localData), int(n))
	}

	n, err = api.GetObject(baseParams, clibucket, object)
	tutils.CheckFatal(err, t)
	if int(n) != len(cloudData) {
		t.Errorf("Expected object size: %d bytes, actual: %d bytes", len(cloudData), int(n))
	}
}

func TestGetObjectInNextTierErrorOnGet(t *testing.T) {
	var (
		object   = t.Name()
		data     = []byte("this is the object you want!")
		listener net.Listener
		proxyURL = getPrimaryURL(t, proxyURLRO)
		err      error
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip(fmt.Sprintf("%q requires a cloud bucket", t.Name()))
	}
	if tutils.DockerRunning() {
		// use the default docker0 bridge ip address
		listener, err = net.Listen("tcp", "172.17.0.1:0")
		if err != nil {
			t.Errorf("Failed to initialize test server listener when using docker mode")
		}
	}

	nextTierMock := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	if tutils.DockerRunning() {
		nextTierMock.Listener.Close()
		nextTierMock.Listener = listener
	}
	nextTierMock.Start()
	defer nextTierMock.Close()

	err = api.PutObject(tutils.DefaultBaseAPIParams(t), clibucket, object, "", tutils.NewBytesReader(data))
	tutils.CheckFatal(err, t)
	defer deleteCloudObject(proxyURL, clibucket, object, t)

	err = api.EvictObject(tutils.DefaultBaseAPIParams(t), clibucket, object)
	tutils.CheckFatal(err, t)

	bucketProps := testBucketProps(t)
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = nextTierMock.URL
	err = api.SetBucketProps(tutils.DefaultBaseAPIParams(t), clibucket, *bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	n, err := api.GetObject(tutils.DefaultBaseAPIParams(t), clibucket, object)
	tutils.CheckFatal(err, t)

	if int(n) != len(data) {
		t.Errorf("Expected object size: %d bytes, actual: %d bytes", len(data), int(n))
	}
}

func TestGetObjectNotInNextTier(t *testing.T) {
	var (
		object   = t.Name()
		data     = []byte("this is some other object - not the one you want!")
		filesize = 1024
		listener net.Listener
		proxyURL = getPrimaryURL(t, proxyURLRO)
		err      error
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip(fmt.Sprintf("test %q requires a cloud bucket", t.Name()))
	}

	if tutils.DockerRunning() {
		// use the default docker0 bridge ip address
		listener, err = net.Listen("tcp", "172.17.0.1:0")
		if err != nil {
			t.Errorf("Failed to initialize test server listener when using docker mode")
		}
	}

	nextTierMock := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	if tutils.DockerRunning() {
		nextTierMock.Listener.Close()
		nextTierMock.Listener = listener
	}
	nextTierMock.Start()
	defer nextTierMock.Close()

	reader, err := tutils.NewRandReader(int64(filesize), false)
	tutils.CheckFatal(err, t)
	err = api.PutObject(tutils.DefaultBaseAPIParams(t), clibucket, object, reader.XXHash(), reader)
	tutils.CheckFatal(err, t)
	defer deleteCloudObject(proxyURL, clibucket, object, t)

	err = api.EvictObject(tutils.DefaultBaseAPIParams(t), clibucket, object)
	tutils.CheckFatal(err, t)

	bucketProps := testBucketProps(t)
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = nextTierMock.URL
	err = api.SetBucketProps(tutils.DefaultBaseAPIParams(t), clibucket, *bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	n, err := api.GetObject(tutils.DefaultBaseAPIParams(t), clibucket, object)
	tutils.CheckFatal(err, t)

	if int(n) != filesize {
		t.Errorf("Expected object size: %d bytes, actual: %d bytes", filesize, int(n))
	}
}

func TestPutObjectNextTierPolicy(t *testing.T) {
	var (
		object                            = t.Name()
		localData                         = []byte("May the Force be with you.")
		cloudData                         = []byte("I'm going to make him an offer he can't refuse.")
		nextTierMockForLocalBucketReached int
		nextTierMockForCloudBucketReached int
		localBucketListener               net.Listener
		cloudBucketListener               net.Listener
		proxyURL                          = getPrimaryURL(t, proxyURLRO)
		baseParams                        = tutils.BaseAPIParams(proxyURL)
		err                               error
	)
	// TODO: enable and review when the time is right
	if true {
		t.Skip(fmt.Sprintf("test %q requires next-version tiering", t.Name()))
	}

	tutils.Logf("proxyurl: %q \n", proxyURL)
	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip(fmt.Sprintf("test %q requires a cloud bucket", t.Name()))
	}

	if tutils.DockerRunning() {
		// use the default docker0 bridge ip address
		localBucketListener, err = net.Listen("tcp", "172.17.0.1:0")
		if err != nil {
			t.Errorf("Failed to initialize test server listener when using docker mode for local bucket")
		}

		cloudBucketListener, err = net.Listen("tcp", "172.17.0.1:0")
		if err != nil {
			t.Errorf("Failed to initialize test server listener when using docker mode for cloud bucket")
		}
	}

	nextTierMockForLocalBucket := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	nextTierMockForCloudBucket := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	if tutils.DockerRunning() {
		nextTierMockForLocalBucket.Listener.Close()
		nextTierMockForLocalBucket.Listener = localBucketListener
		nextTierMockForCloudBucket.Listener.Close()
		nextTierMockForCloudBucket.Listener = cloudBucketListener
	}
	nextTierMockForLocalBucket.Start()
	nextTierMockForCloudBucket.Start()
	defer nextTierMockForLocalBucket.Close()
	defer nextTierMockForCloudBucket.Close()

	createFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer destroyLocalBucket(t, proxyURL, TestLocalBucketName)

	bucketProps := testBucketProps(t)
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = nextTierMockForLocalBucket.URL
	err = api.SetBucketProps(baseParams, TestLocalBucketName, *bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, TestLocalBucketName, t)

	bucketProps = testBucketProps(t)
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = nextTierMockForCloudBucket.URL
	bucketProps.WritePolicy = cmn.RWPolicyNextTier
	err = api.SetBucketProps(baseParams, clibucket, *bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	api.PutObject(baseParams, TestLocalBucketName, object, "", tutils.NewBytesReader(localData))
	tutils.CheckFatal(err, t)

	if nextTierMockForLocalBucketReached != 1 {
		t.Errorf("Expected to hit nextTierMockForLocalBucket 1 time, actual: %d",
			nextTierMockForLocalBucketReached)
	}

	api.PutObject(baseParams, clibucket, object, "", tutils.NewBytesReader(cloudData))
	tutils.CheckFatal(err, t)

	if nextTierMockForCloudBucketReached != 1 {
		t.Errorf("Expected to hit nextTierMockForCloudBucket 1 time, actual: %d",
			nextTierMockForCloudBucketReached)
	}
}

func TestPutObjectNextTierPolicyErrorOnPut(t *testing.T) {
	var (
		object   = t.Name()
		data     = []byte("this object will go to the cloud!")
		listener net.Listener
		proxyURL = getPrimaryURL(t, proxyURLRO)
		err      error
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip(fmt.Sprintf("test %q requires a cloud bucket", t.Name()))
	}

	if tutils.DockerRunning() {
		// use the default docker0 bridge ip address
		listener, err = net.Listen("tcp", "172.17.0.1:0")
		if err != nil {
			t.Errorf("Failed to initialize test server listener when using docker mode")
		}
	}

	nextTierMock := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "some arbitrary internal server error", http.StatusInternalServerError)
	}))
	if tutils.DockerRunning() {
		nextTierMock.Listener.Close()
		nextTierMock.Listener = listener
	}
	nextTierMock.Start()
	defer nextTierMock.Close()

	bucketProps := testBucketProps(t)
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = nextTierMock.URL
	bucketProps.ReadPolicy = cmn.RWPolicyCloud
	bucketProps.WritePolicy = cmn.RWPolicyNextTier
	err = api.SetBucketProps(tutils.DefaultBaseAPIParams(t), clibucket, *bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	err = api.PutObject(tutils.DefaultBaseAPIParams(t), clibucket, object, "", tutils.NewBytesReader(data))
	tutils.CheckFatal(err, t)
	defer deleteCloudObject(proxyURL, clibucket, object, t)

	err = api.EvictObject(tutils.DefaultBaseAPIParams(t), clibucket, object)
	tutils.CheckFatal(err, t)

	n, err := api.GetObject(tutils.DefaultBaseAPIParams(t), clibucket, object)
	tutils.CheckFatal(err, t)

	if int(n) != len(data) {
		t.Errorf("Expected object size: %d bytes, actual: %d bytes", len(data), int(n))
	}
}

func TestPutObjectCloudPolicy(t *testing.T) {
	var (
		object   = t.Name()
		data     = []byte("this object will go to the cloud!")
		listener net.Listener
		proxyURL = getPrimaryURL(t, proxyURLRO)
		err      error
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip(fmt.Sprintf("test %q requires a cloud bucket", t.Name()))
	}

	if tutils.DockerRunning() {
		// use the default docker0 bridge ip address
		listener, err = net.Listen("tcp", "172.17.0.1:0")
		if err != nil {
			t.Errorf("Failed to initialize test server listener when using docker mode")
		}
	}

	nextTierMock := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	if tutils.DockerRunning() {
		nextTierMock.Listener.Close()
		nextTierMock.Listener = listener
	}
	nextTierMock.Start()
	defer nextTierMock.Close()

	bucketProps := testBucketProps(t)
	bucketProps.CloudProvider = cmn.ProviderDFC
	bucketProps.NextTierURL = nextTierMock.URL
	bucketProps.WritePolicy = cmn.RWPolicyCloud
	err = api.SetBucketProps(tutils.DefaultBaseAPIParams(t), clibucket, *bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	err = api.PutObject(tutils.DefaultBaseAPIParams(t), clibucket, object, "", tutils.NewBytesReader(data))
	tutils.CheckFatal(err, t)

	deleteCloudObject(proxyURL, clibucket, object, t)
}

func resetBucketProps(proxyURL, bucket string, t *testing.T) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	if err := api.ResetBucketProps(baseParams, bucket); err != nil {
		t.Errorf("bucket: %s props not reset, err: %v", clibucket, err)
	}
}

func deleteCloudObject(proxyURL, bucket, object string, t *testing.T) {
	if err := tutils.Del(proxyURL, bucket, object, nil, nil, true); err != nil {
		t.Errorf("bucket/object: %s/%s not deleted, err: %v", bucket, object, err)
	}
}
