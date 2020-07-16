// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/NVIDIA/aistore/tutils/tassert"
	"github.com/NVIDIA/go-tfdata/tfdata/archive"
	"github.com/NVIDIA/go-tfdata/tfdata/core"
	"github.com/NVIDIA/go-tfdata/tfdata/transform"
)

func TestTarToTFRecordFile(t *testing.T) {
	t.Skip("Should PASS when transformations are supported outside of k8s.")

	const (
		tarPath    = "data/small-mnist-3.tar"
		tarObjName = "small-mnist-3.tar"
	)

	var (
		proxyURL = tutils.RandomProxyURL()
		bck      = cmn.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}
		baseParams = tutils.BaseAPIParams(proxyURL)
		aisEx, ex  *core.TFExample
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

	// GET TFRecord from TAR
	outFileGet, err := ioutil.TempFile("", "tar2tf-")
	tassert.CheckFatal(t, err)
	_, err = api.GetObjectS3(baseParams, bck, tarObjName+"!tf", api.GetObjectInput{Writer: outFileGet})
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, outFileGet.Sync())
	_, err = outFileGet.Seek(0, io.SeekStart)
	tassert.CheckFatal(t, err)
	getTFExamplesReader := core.NewTFRecordReader(outFileGet)

	// locally transform TAR to TFRecord
	sourceTar, err := os.Open(tarPath)
	tassert.CheckFatal(t, err)
	defer sourceTar.Close()
	reader, err := archive.NewTarReader(sourceTar)
	tassert.CheckFatal(t, err)
	tfExamplesReader := transform.SamplesToTFExample(reader)

	// compare TFRecords
	for aisEx, err = tfExamplesReader.Read(); err == nil; aisEx, err = tfExamplesReader.Read() {
		ex, err = getTFExamplesReader.Read()
		tassert.CheckFatal(t, err)

		for k := range aisEx.GetFeatures().Feature {
			sampleKey := string(ex.GetBytesList("__key__"))
			tassert.Errorf(t, bytes.Equal(ex.GetBytesList(k), aisEx.GetBytesList(k)), "TFRecords different for sample %s on entry %s", sampleKey, k)
		}
	}

	tassert.Errorf(t, err == io.EOF, "local TFRecord reader failed with %v", err)
	_, err = getTFExamplesReader.Read()
	if err == nil {
		t.Errorf("TFRecord produced by the cluster should have the same length as locally created TFRecord")
	}
	tassert.Errorf(t, err == io.EOF, "TFRecord produced by the cluster failed with %v", err)
}
