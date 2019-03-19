// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
)

func waitForDownload(t *testing.T, id string) {
	for {
		all := true
		if resp, err := api.DownloadStatus(tutils.DefaultBaseAPIParams(t), id); err == nil {
			if resp.Finished != resp.Total {
				all = false
			}
		}

		if all {
			break
		}
		time.Sleep(time.Second)
	}

}

func TestDownloadSingle(t *testing.T) {
	var (
		proxyURL      = getPrimaryURL(t, proxyURLReadOnly)
		bucket        = TestLocalBucketName
		objname       = "object"
		objnameSecond = "object-second"
		link          = "https://storage.googleapis.com/lpr-vision/imagenet/imagenet_train-000001.tgz"
		linkSmall     = "https://github.com/NVIDIA/aistore"
	)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	id, err := api.DownloadSingle(tutils.DefaultBaseAPIParams(t), bucket, objname, link)
	tutils.CheckError(err, t)

	time.Sleep(time.Second)

	// Schedule second object
	idSecond, err := api.DownloadSingle(tutils.DefaultBaseAPIParams(t), bucket, objnameSecond, link)
	tutils.CheckError(err, t)

	// Cancel second object
	err = api.DownloadCancel(tutils.DefaultBaseAPIParams(t), idSecond)
	tutils.CheckError(err, t)

	resp, err := api.DownloadStatus(tutils.DefaultBaseAPIParams(t), id)
	tutils.CheckError(err, t)

	err = api.DownloadCancel(tutils.DefaultBaseAPIParams(t), id)
	tutils.CheckError(err, t)

	time.Sleep(time.Second)

	if resp, err = api.DownloadStatus(tutils.DefaultBaseAPIParams(t), id); err == nil {
		t.Errorf("expected error when getting status for link that is not being downloaded: %v", resp)
	}

	if err = api.DownloadCancel(tutils.DefaultBaseAPIParams(t), id); err == nil {
		t.Error("expected error when cancelling for link that is not being downloaded and is not in queue")
	}

	id, err = api.DownloadSingle(tutils.DefaultBaseAPIParams(t), bucket, objname, linkSmall)
	tutils.CheckError(err, t)

	waitForDownload(t, id)

	objs, err := tutils.ListObjects(proxyURL, bucket, cmn.LocalBs, "", 0)
	tutils.CheckError(err, t)
	if len(objs) != 1 || objs[0] != objname {
		t.Errorf("expected single object (%s), got: %s", objname, objs)
	}
}

func TestDownloadRange(t *testing.T) {
	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		bucket   = TestLocalBucketName
		base     = "https://storage.googleapis.com/lpr-vision/"
		template = "imagenet/imagenet_train-{000000..000007}.tgz"
	)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	id, err := api.DownloadRange(tutils.DefaultBaseAPIParams(t), bucket, base, template)
	tutils.CheckFatal(err, t)

	time.Sleep(3 * time.Second)

	err = api.DownloadCancel(tutils.DefaultBaseAPIParams(t), id)
	tutils.CheckFatal(err, t)
}

func TestDownloadMultiMap(t *testing.T) {
	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		bucket   = TestLocalBucketName
		m        = map[string]string{
			"ais": "https://raw.githubusercontent.com/NVIDIA/aistore/master/README.md",
			"k8s": "https://raw.githubusercontent.com/kubernetes/kubernetes/master/README.md",
		}
	)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	id, err := api.DownloadMulti(tutils.DefaultBaseAPIParams(t), bucket, m)
	tutils.CheckFatal(err, t)

	waitForDownload(t, id)

	objs, err := tutils.ListObjects(proxyURL, bucket, cmn.LocalBs, "", 0)
	tutils.CheckFatal(err, t)
	if len(objs) != len(m) {
		t.Errorf("expected objects (%s), got: %s", m, objs)
	}
}

func TestDownloadMultiList(t *testing.T) {
	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		bucket   = TestLocalBucketName
		l        = []string{
			"https://raw.githubusercontent.com/NVIDIA/aistore/master/README.md",
			"https://raw.githubusercontent.com/kubernetes/kubernetes/master/LICENSE?query=values",
		}
		expectedObjs = []string{"LICENSE", "README.md"}
	)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	id, err := api.DownloadMulti(tutils.DefaultBaseAPIParams(t), bucket, l)
	tutils.CheckFatal(err, t)

	waitForDownload(t, id)

	objs, err := tutils.ListObjects(proxyURL, bucket, cmn.LocalBs, "", 0)
	tutils.CheckFatal(err, t)
	if !reflect.DeepEqual(objs, expectedObjs) {
		t.Errorf("expected objs: %s, got: %s", expectedObjs, objs)
	}
}

func TestDownloadTimeout(t *testing.T) {
	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		bucket   = TestLocalBucketName
		objname  = "object"
		link     = "https://storage.googleapis.com/lpr-vision/imagenet/imagenet_train-000001.tgz"
	)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	body := cmn.DlSingleBody{
		DlObj: cmn.DlObj{
			Objname: objname,
			Link:    link,
		},
	}
	body.Bucket = bucket
	body.Timeout = "1ms" // super small timeout to see if the request will be canceled

	id, err := api.DownloadSingleWithParam(tutils.DefaultBaseAPIParams(t), body)
	tutils.CheckFatal(err, t)

	time.Sleep(time.Second)

	if _, err := api.DownloadStatus(tutils.DefaultBaseAPIParams(t), id); err == nil {
		// TODO: we should get response that some files has been canceled or not finished.
		// For now we cannot do that since we don't collect information about
		// task being canceled.
		// t.Errorf("expected error when getting status for link that is not being downloaded: %s", string(resp))
		tutils.Logf("%v", err)
	}

	objs, err := tutils.ListObjects(proxyURL, bucket, cmn.LocalBs, "", 0)
	tutils.CheckFatal(err, t)
	if len(objs) != 0 {
		t.Errorf("expected 0 objects, got: %s", objs)
	}
}

// NOTE: For now this test is disabled since we don't have a clear way to wait
// for the download.
func TestDownloadBucket(t *testing.T) {
	t.Skip("Not yet fully implemented")

	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		bucket   = "lpr-vision"
		prefix   = "imagenet/imagenet_train-"
		suffix   = ".tgz"
	)

	if !isCloudBucket(t, proxyURL, bucket) {
		t.Skip("test requires a Cloud bucket")
	}

	_, err := tutils.ListObjects(proxyURL, bucket, cmn.CloudBs, prefix, 0)
	tutils.CheckFatal(err, t)

	err = api.DownloadBucket(tutils.DefaultBaseAPIParams(t), bucket, prefix, suffix)
	tutils.CheckFatal(err, t)

	// FIXME: How to wait?
}

func TestDownloadStatus(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		bucket   = TestLocalBucketName
		m        = map[string]string{
			"short": "https://raw.githubusercontent.com/NVIDIA/aistore/master/README.md",
			"long":  "http://releases.ubuntu.com/18.04/ubuntu-18.04.2-desktop-amd64.iso",
		}
	)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	id, err := api.DownloadMulti(tutils.DefaultBaseAPIParams(t), bucket, m)
	tutils.CheckFatal(err, t)
	defer api.DownloadCancel(tutils.DefaultBaseAPIParams(t), id)

	// Wait for the short download to complete
	time.Sleep(2 * time.Second)

	resp, err := api.DownloadStatus(tutils.DefaultBaseAPIParams(t), id)
	tutils.CheckFatal(err, t)

	if resp.Total != len(m) {
		t.Errorf("expected %d objects, got %d", len(m), resp.Total)
	}
	if resp.Finished != 1 {
		t.Errorf("expected the short file to be downloaded")
	}
	if len(resp.CurrentTasks) != 1 {
		t.Fatal("did not expect the long file to be already downloaded")
	}
	if resp.CurrentTasks[0].Name != "long" {
		t.Errorf("invalid file name in status message, expected: %s, got: %s", "long", resp.CurrentTasks[0].Name)
	}
}
