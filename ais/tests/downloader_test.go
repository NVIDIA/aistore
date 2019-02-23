// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
)

func waitForDownload(t *testing.T, bucket string, m map[string]string) {
	for {
		all := true
		for objname, link := range m {
			if resp, err := api.DownloadObjectStatus(tutils.DefaultBaseAPIParams(t), bucket, objname, link); err == nil {
				if !strings.Contains(string(resp), "total size") {
					all = false
				}
			}
		}
		if all {
			break
		}
		time.Sleep(time.Second)
	}

}

func TestDownloadObject(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	var (
		proxyURL      = getPrimaryURL(t, proxyURLReadOnly)
		bucket        = TestLocalBucketName
		objname       = "object"
		objnameSecond = "object-second"
		link          = "https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz"
		linkSmall     = "https://github.com/NVIDIA/aistore"
	)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	err := api.DownloadObject(tutils.DefaultBaseAPIParams(t), bucket, objname, link)
	tutils.CheckFatal(err, t)

	time.Sleep(time.Second * 3)

	if err := api.DownloadObject(tutils.DefaultBaseAPIParams(t), bucket, objname, link); err == nil {
		t.Error("expected error when trying to download currently downloading file")
	}

	// Schedule second object
	err = api.DownloadObject(tutils.DefaultBaseAPIParams(t), bucket, objnameSecond, link)
	tutils.CheckFatal(err, t)

	// Cancel second object
	err = api.DownloadObjectCancel(tutils.DefaultBaseAPIParams(t), bucket, objnameSecond, link)
	tutils.CheckFatal(err, t)

	resp, err := api.DownloadObjectStatus(tutils.DefaultBaseAPIParams(t), bucket, objname, link)
	tutils.CheckFatal(err, t)
	if len(resp) < 10 {
		t.Errorf("expected longer response, got: %s", string(resp))
	}

	err = api.DownloadObjectCancel(tutils.DefaultBaseAPIParams(t), bucket, objname, link)
	tutils.CheckFatal(err, t)

	time.Sleep(time.Second)

	if resp, err = api.DownloadObjectStatus(tutils.DefaultBaseAPIParams(t), bucket, objname, link); err == nil {
		t.Errorf("expected error when getting status for link that is not being downloaded: %s", string(resp))
	}

	if err = api.DownloadObjectCancel(tutils.DefaultBaseAPIParams(t), bucket, objname, link); err == nil {
		t.Error("expected error when cancelling for link that is not being downloaded and is not in queue")
	}

	err = api.DownloadObject(tutils.DefaultBaseAPIParams(t), bucket, objname, linkSmall)
	tutils.CheckFatal(err, t)

	waitForDownload(t, bucket, map[string]string{objname: linkSmall})

	objs, err := tutils.ListObjects(proxyURL, bucket, cmn.LocalBs, "", 0)
	tutils.CheckFatal(err, t)
	if len(objs) != 1 || objs[0] != objname {
		t.Errorf("expected single object (%s), got: %s", objname, objs)
	}
}

func TestDownloadObjectMulti(t *testing.T) {
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

	err := api.DownloadObjectMulti(tutils.DefaultBaseAPIParams(t), bucket, m)
	tutils.CheckFatal(err, t)

	waitForDownload(t, bucket, m)

	objs, err := tutils.ListObjects(proxyURL, bucket, cmn.LocalBs, "", 0)
	tutils.CheckFatal(err, t)
	if len(objs) != len(m) {
		t.Errorf("expected objects (%s), got: %s", m, objs)
	}
}
