// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/transform"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/NVIDIA/aistore/tutils/tassert"
	"github.com/NVIDIA/go-tfdata/tfdata/core"
	jsoniter "github.com/json-iterator/go"
)

func TestKubeTar2TFS3(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Kubernetes: true})

	const (
		tarObjName   = "small-mnist-3.tar"
		tfRecordFile = "small-mnist-3.record"
	)

	var (
		tarPath      = filepath.Join("data", tarObjName)
		tfRecordPath = filepath.Join("data", tfRecordFile)
		proxyURL     = tutils.RandomProxyURL()
		bck          = cmn.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	// PUT TAR to the cluster
	f, err := readers.NewFileReaderFromFile(tarPath, cmn.ChecksumXXHash)
	tassert.CheckFatal(t, err)
	putArgs := api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bck,
		Object:     tarObjName,
		Cksum:      f.Cksum(),
		Reader:     f,
	}
	tassert.CheckFatal(t, api.PutObject(putArgs))
	defer api.DeleteObject(baseParams, bck, tarObjName)

	transformerTemplate := filepath.Join("templates", "transformer", tar2tf, "pod.yaml")
	spec, err := ioutil.ReadFile(transformerTemplate)
	tassert.CheckError(t, err)

	pod, err := transform.ParsePodSpec(spec)
	tassert.CheckError(t, err)
	spec, _ = jsoniter.Marshal(pod)

	// Starting transformer
	uuid, err := api.TransformInit(baseParams, spec)
	tassert.CheckFatal(t, err)
	defer func() {
		tassert.CheckFatal(t, api.TransformStop(baseParams, uuid))
	}()

	// GET TFRecord from TAR
	outFileGet, err := ioutil.TempFile("", "tar2tf-")
	tassert.CheckFatal(t, err)

	_, err = api.GetObjectS3(baseParams, bck, tarObjName+"!"+uuid, api.GetObjectInput{Writer: outFileGet})
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, outFileGet.Sync())
	_, err = outFileGet.Seek(0, io.SeekStart)
	tassert.CheckFatal(t, err)

	// Comparing actual vs expected
	tfRecord, err := os.Open(tfRecordPath)
	tassert.CheckFatal(t, err)
	defer tfRecord.Close()

	expectedRecords, err := core.NewTFRecordReader(tfRecord).ReadAllExamples()
	tassert.CheckFatal(t, err)
	actualRecords, err := core.NewTFRecordReader(outFileGet).ReadAllExamples()
	tassert.CheckFatal(t, err)

	equal, err := tfRecordsEqual(expectedRecords, actualRecords)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, equal == true, "actual and expected records different")
}
