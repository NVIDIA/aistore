// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tetl"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/go-tfdata/tfdata/core"
)

func startTar2TfTransformer(t *testing.T) (etlName string) {
	etlName = tetl.Tar2TF // TODO: add more

	spec, err := tetl.GetTransformYaml(etlName)
	tassert.CheckError(t, err)

	msg := &etl.InitSpecMsg{}
	{
		msg.IDX = etlName
		msg.CommTypeX = etl.Hpull
		msg.Spec = spec
	}
	tassert.CheckError(t, msg.Validate())
	tassert.Fatalf(t, msg.Name() == tetl.Tar2TF, "%q vs %q", msg.Name(), tetl.Tar2TF)

	// Starting transformer
	xid, err := api.ETLInit(baseParams, msg)
	tassert.CheckFatal(t, err)

	tlog.Logf("ETL %q: running x-etl-spec[%s]\n", etlName, xid)
	return
}

func TestETLTar2TFS3(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})

	const (
		tarObjName   = "small-mnist-3.tar"
		tfRecordFile = "small-mnist-3.record"
	)

	var (
		tarPath      = filepath.Join("data", tarObjName)
		tfRecordPath = filepath.Join("data", tfRecordFile)
		proxyURL     = tools.RandomProxyURL()
		bck          = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.AIS,
		}
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	// PUT TAR to the cluster
	f, err := readers.NewExistingFile(tarPath, cos.ChecksumXXHash)
	tassert.CheckFatal(t, err)
	putArgs := api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    tarObjName,
		Cksum:      f.Cksum(),
		Reader:     f,
	}
	_, err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)
	defer api.DeleteObject(baseParams, bck, tarObjName)

	etlName := startTar2TfTransformer(t)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, etlName) })
	// GET TFRecord from TAR
	outFileBuffer := bytes.NewBuffer(nil)

	// This is to mimic external S3 clients like Tensorflow
	bck.Provider = ""

	_, err = api.GetObjectS3(
		baseParams,
		bck,
		tarObjName,
		api.GetArgs{
			Writer: outFileBuffer,
			Query:  url.Values{apc.QparamETLName: {etlName}},
		})
	tassert.CheckFatal(t, err)

	// Comparing actual vs expected
	tfRecord, err := os.Open(tfRecordPath)
	tassert.CheckFatal(t, err)
	defer tfRecord.Close()

	expectedRecords, err := core.NewTFRecordReader(tfRecord).ReadAllExamples()
	tassert.CheckFatal(t, err)
	actualRecords, err := core.NewTFRecordReader(outFileBuffer).ReadAllExamples()
	tassert.CheckFatal(t, err)

	equal, err := tfRecordsEqual(expectedRecords, actualRecords)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, equal == true, "actual and expected records different")
}

func TestETLTar2TFRanges(t *testing.T) {
	// TestETLTar2TFS3 already runs in short tests, no need for short here as well.
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, Long: true})

	type testCase struct {
		start, end int64
	}

	var (
		tarObjName = "small-mnist-3.tar"
		tarPath    = filepath.Join("data", tarObjName)
		proxyURL   = tools.RandomProxyURL()
		bck        = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.AIS,
		}
		baseParams     = tools.BaseAPIParams(proxyURL)
		rangeBytesBuff = bytes.NewBuffer(nil)

		tcs = []testCase{
			{start: 0, end: 1},
			{start: 0, end: 50},
			{start: 1, end: 20},
			{start: 15, end: 100},
			{start: 120, end: 240},
			{start: 123, end: 1234},
		}
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	// PUT TAR to the cluster
	f, err := readers.NewExistingFile(tarPath, cos.ChecksumXXHash)
	tassert.CheckFatal(t, err)
	putArgs := api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    tarObjName,
		Cksum:      f.Cksum(),
		Reader:     f,
	}
	_, err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)

	etlName := startTar2TfTransformer(t)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, etlName) })

	// This is to mimic external S3 clients like Tensorflow
	bck.Provider = ""

	// GET TFRecord from TAR
	wholeTFRecord := bytes.NewBuffer(nil)
	_, err = api.GetObjectS3(
		baseParams,
		bck,
		tarObjName,
		api.GetArgs{
			Writer: wholeTFRecord,
			Query:  url.Values{apc.QparamETLName: {etlName}},
		})
	tassert.CheckFatal(t, err)

	for _, tc := range tcs {
		rangeBytesBuff.Reset()

		// Request only a subset of bytes
		header := http.Header{}
		header.Set(cos.HdrRange, fmt.Sprintf("bytes=%d-%d", tc.start, tc.end))
		_, err = api.GetObjectS3(
			baseParams,
			bck,
			tarObjName,
			api.GetArgs{
				Writer: rangeBytesBuff,
				Header: header,
				Query:  url.Values{apc.QparamETLName: {etlName}},
			})
		tassert.CheckFatal(t, err)

		tassert.Errorf(t, bytes.Equal(rangeBytesBuff.Bytes(),
			wholeTFRecord.Bytes()[tc.start:tc.end+1]), "[start: %d, end: %d] bytes different", tc.start, tc.end)
	}
}
