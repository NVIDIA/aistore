// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
)

func waitForDownload(t *testing.T, bucket string, v interface{}) {
	for {
		all := true
		switch ty := v.(type) {
		case map[string]string:
			for objname, link := range ty {
				if resp, err := api.DownloadStatus(tutils.DefaultBaseAPIParams(t), bucket, objname, link); err == nil {
					if !strings.Contains(string(resp), "total size") {
						all = false
					}
				}
			}
		case []string:
			for _, link := range ty {
				objname := path.Base(link)
				if resp, err := api.DownloadStatus(tutils.DefaultBaseAPIParams(t), bucket, objname, link); err == nil {
					if !strings.Contains(string(resp), "total size") {
						all = false
					}
				}
			}
		default:
			t.Fatalf("invalid type: %T", ty)
		}

		if all {
			break
		}
		time.Sleep(time.Second)
	}

}

func TestDownloadSingle(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

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

	err := api.DownloadSingle(tutils.DefaultBaseAPIParams(t), bucket, objname, link)
	tutils.CheckFatal(err, t)

	time.Sleep(time.Second * 3)

	if err := api.DownloadSingle(tutils.DefaultBaseAPIParams(t), bucket, objname, link); err == nil {
		t.Error("expected error when trying to download currently downloading file")
	}

	// Schedule second object
	err = api.DownloadSingle(tutils.DefaultBaseAPIParams(t), bucket, objnameSecond, link)
	tutils.CheckFatal(err, t)

	// Cancel second object
	err = api.DownloadCancel(tutils.DefaultBaseAPIParams(t), bucket, objnameSecond, link)
	tutils.CheckFatal(err, t)

	resp, err := api.DownloadStatus(tutils.DefaultBaseAPIParams(t), bucket, objname, link)
	tutils.CheckFatal(err, t)
	if len(resp) < 10 {
		t.Errorf("expected longer response, got: %s", string(resp))
	}

	err = api.DownloadCancel(tutils.DefaultBaseAPIParams(t), bucket, objname, link)
	tutils.CheckFatal(err, t)

	time.Sleep(time.Second)

	if resp, err = api.DownloadStatus(tutils.DefaultBaseAPIParams(t), bucket, objname, link); err == nil {
		t.Errorf("expected error when getting status for link that is not being downloaded: %s", string(resp))
	}

	if err = api.DownloadCancel(tutils.DefaultBaseAPIParams(t), bucket, objname, link); err == nil {
		t.Error("expected error when cancelling for link that is not being downloaded and is not in queue")
	}

	err = api.DownloadSingle(tutils.DefaultBaseAPIParams(t), bucket, objname, linkSmall)
	tutils.CheckFatal(err, t)

	waitForDownload(t, bucket, map[string]string{objname: linkSmall})

	objs, err := tutils.ListObjects(proxyURL, bucket, cmn.LocalBs, "", 0)
	tutils.CheckFatal(err, t)
	if len(objs) != 1 || objs[0] != objname {
		t.Errorf("expected single object (%s), got: %s", objname, objs)
	}
}

func TestDownloadRange(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		bucket   = TestLocalBucketName
		base     = "https://storage.googleapis.com/lpr-vision/"
		template = "imagenet/imagenet_train-{000000..000007}.tgz"
	)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	err := api.DownloadRange(tutils.DefaultBaseAPIParams(t), bucket, base, template)
	tutils.CheckFatal(err, t)

	time.Sleep(3 * time.Second)

	for i := 0; i <= 7; i++ {
		objname := fmt.Sprintf("imagenet/imagenet_train-00000%d.tgz", i)
		err := api.DownloadCancel(tutils.DefaultBaseAPIParams(t), bucket, objname, base+objname)
		tutils.CheckFatal(err, t)
	}
}

func TestDownloadMultiMap(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

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

	err := api.DownloadMulti(tutils.DefaultBaseAPIParams(t), bucket, m)
	tutils.CheckFatal(err, t)

	waitForDownload(t, bucket, m)

	objs, err := tutils.ListObjects(proxyURL, bucket, cmn.LocalBs, "", 0)
	tutils.CheckFatal(err, t)
	if len(objs) != len(m) {
		t.Errorf("expected objects (%s), got: %s", m, objs)
	}
}

func TestDownloadMultiList(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		bucket   = TestLocalBucketName
		l        = []string{
			"https://raw.githubusercontent.com/NVIDIA/aistore/master/README.md",
			"https://raw.githubusercontent.com/kubernetes/kubernetes/master/LICENSE",
		}
	)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	err := api.DownloadMulti(tutils.DefaultBaseAPIParams(t), bucket, l)
	tutils.CheckFatal(err, t)

	waitForDownload(t, bucket, l)

	objs, err := tutils.ListObjects(proxyURL, bucket, cmn.LocalBs, "", 0)
	tutils.CheckFatal(err, t)
	if len(objs) != len(l) {
		t.Errorf("expected objects (%s), got: %s", l, objs)
	}
}

func TestDownloadTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		bucket   = TestLocalBucketName
		objname  = "object"
		link     = "https://storage.googleapis.com/lpr-vision/imagenet/imagenet_train-000001.tgz"
	)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	body := cmn.DlBody{
		Objname: objname,
		Link:    link,
	}
	body.Bucket = bucket
	body.Timeout = "1ms" // super small timeout to see if the request will be canceled

	err := api.DownloadSingleWithParam(tutils.DefaultBaseAPIParams(t), body)
	tutils.CheckFatal(err, t)

	time.Sleep(time.Second)

	if resp, err := api.DownloadStatus(tutils.DefaultBaseAPIParams(t), bucket, objname, link); err == nil {
		t.Errorf("expected error when getting status for link that is not being downloaded: %s", string(resp))
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

	if testing.Short() {
		t.Skip(skipping)
	}

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
