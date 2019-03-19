// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"fmt"
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

func TestDownloadCloud(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	var (
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.DefaultBaseAPIParams(t)
		bucket     = clibucket

		fileCnt = 5
		prefix  = "imagenet/imagenet_train-"
		suffix  = ".tgz"
	)

	if !isCloudBucket(t, proxyURL, bucket) {
		t.Skip("test requires a cloud bucket")
	}

	tutils.CleanCloudBucket(t, proxyURL, bucket, prefix)
	defer tutils.CleanCloudBucket(t, proxyURL, bucket, prefix)

	expectedObjs := make([]string, 0, fileCnt)
	for i := 0; i < fileCnt; i++ {
		reader, err := tutils.NewRandReader(cmn.MiB, false /* withHash */)
		tutils.CheckFatal(err, t)

		objName := fmt.Sprintf("%s%0*d%s", prefix, 5, i, suffix)
		err = api.PutObject(api.PutObjectArgs{
			BaseParams:     baseParams,
			Bucket:         bucket,
			BucketProvider: cmn.CloudBs,
			Object:         objName,
			Reader:         reader,
		})
		tutils.CheckFatal(err, t)

		expectedObjs = append(expectedObjs, objName)
	}

	// Test download
	err := api.EvictList(baseParams, bucket, cmn.CloudBs, expectedObjs, true, 0)
	tutils.CheckFatal(err, t)

	id, err := api.DownloadCloud(baseParams, bucket, prefix, suffix)
	tutils.CheckFatal(err, t)

	waitForDownload(t, id)

	objs, err := tutils.ListObjects(proxyURL, bucket, cmn.CloudBs, prefix, 0)
	tutils.CheckFatal(err, t)
	if !reflect.DeepEqual(objs, expectedObjs) {
		t.Errorf("expected objs: %s, got: %s", expectedObjs, objs)
	}

	// Test cancellation
	err = api.EvictList(baseParams, bucket, cmn.CloudBs, expectedObjs, true, 0)
	tutils.CheckFatal(err, t)

	id, err = api.DownloadCloud(baseParams, bucket, prefix, suffix)
	tutils.CheckFatal(err, t)

	time.Sleep(200 * time.Millisecond)

	err = api.DownloadCancel(baseParams, id)
	tutils.CheckFatal(err, t)
}

// This test can be flaky. It requires at least 2 targets or at least 2 mountpaths if there is only 1 target
func TestDownloadStatus(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		bucket   = TestLocalBucketName
		files    = map[string]string{
			"shortFile": "https://raw.githubusercontent.com/NVIDIA/aistore/master/README.md",
			"longFile":  "http://releases.ubuntu.com/18.04/ubuntu-18.04.2-desktop-amd64.iso",
		}
	)

	// Test requires both files to start downloading simultaneously, so it requires AT LEAST 2 mpaths (maybe more,
	// if a hash collision happens.
	validateTotalNumberOfMpaths(t, 2)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	id, err := api.DownloadMulti(tutils.DefaultBaseAPIParams(t), bucket, files)
	tutils.CheckFatal(err, t)
	defer api.DownloadCancel(tutils.DefaultBaseAPIParams(t), id)

	// Wait for the short download to complete
	time.Sleep(2 * time.Second)

	resp, err := api.DownloadStatus(tutils.DefaultBaseAPIParams(t), id)
	tutils.CheckFatal(err, t)

	if resp.Total != 2 {
		t.Errorf("expected %d objects, got %d", 2, resp.Total)
	}
	if resp.Finished != 1 {
		t.Errorf("expected the short file to be downloaded")
	}
	if len(resp.CurrentTasks) != 1 {
		t.Fatal("did not expect the long file to be already downloaded")
	}
	if resp.CurrentTasks[0].Name != "longFile" {
		t.Errorf("invalid file name in status message, expected: %s, got: %s", "longFile", resp.CurrentTasks[0].Name)
	}
}

// This test can be flaky. It requires at least 2 targets or at least 2 mountpaths if there is only 1 target
func TestDownloadStatusError(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		bucket   = TestLocalBucketName
		files    = map[string]string{
			"invalidURL":   "http://some.invalid.url",
			"notFoundFile": "http://releases.ubuntu.com/18.04/amd65.iso",
		}
		apiParams = tutils.DefaultBaseAPIParams(t)
	)

	// Test requires both files to start downloading simultaneously, so it requires AT LEAST 2 mpaths (maybe more,
	// if a hash collision happens.
	validateTotalNumberOfMpaths(t, 2)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	id, err := api.DownloadMulti(apiParams, bucket, files)
	tutils.CheckFatal(err, t)
	defer api.DownloadCancel(apiParams, id)

	// Wait for the short download to complete
	time.Sleep(2 * time.Second)

	resp, err := api.DownloadStatus(apiParams, id)
	tutils.CheckFatal(err, t)

	if resp.Total != 2 {
		t.Errorf("expected %d objects, got %d", 2, resp.Total)
	}
	if resp.Finished != 0 {
		t.Errorf("expected 0 files to be finished")
	}
	if len(resp.Errs) != 2 {
		t.Fatalf("expected 2 downloading errors, but got: %d", len(resp.Errs))
	}

	invalidAddressCausedError := resp.Errs[0].Name == "invalidURL" || resp.Errs[1].Name == "invalidURL"
	notFoundFileCausedError := resp.Errs[0].Name == "notFoundFile" || resp.Errs[1].Name == "notFoundFile"

	if !(invalidAddressCausedError && notFoundFileCausedError) {
		t.Errorf("expected objects that cause errors to be (%s, %s), but got: (%s, %s)",
			"invalidURL", "notFoundFile", resp.Errs[0].Name, resp.Errs[1].Name)
	}
}

func validateTotalNumberOfMpaths(t *testing.T, minCount int) {
	m := metadata{t: t}
	m.saveClusterState()

	if m.originalTargetCount >= minCount {
		return
	}

	var targetURL string
	for _, target := range m.smap.Tmap {
		targetURL = target.PublicNet.DirectURL
	}

	baseParams := tutils.BaseAPIParams(targetURL)
	mpList, err := api.GetMountpaths(baseParams)
	tutils.CheckFatal(err, t)

	if len(mpList.Available) < minCount {
		t.Fatalf("Must have at least %d mountpaths", minCount)
	}
}
