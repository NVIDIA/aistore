// Package integration_test.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

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
	"github.com/NVIDIA/aistore/tools/trand"
)

func TestETLPipeline(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)

		objCnt = 10
		prefix = "prefix-" + cos.GenUUID()

		bcktests = []testBucketConfig{{false, false, false}}
	)

	if cliBck.IsRemote() {
		bcktests = append(bcktests,
			testBucketConfig{true, false, false},
			testBucketConfig{true, true, false},
			testBucketConfig{false, false, true},
		)
	}

	tests := []struct {
		pipeline  []string
		commTypes []string
		transform transformFunc
		onlyLong  bool
	}{
		{
			pipeline:  []string{tetl.MD5, tetl.Echo, tetl.MD5},
			commTypes: []string{etl.Hpush, etl.Hpush, etl.Hpush},
			transform: func(r io.Reader) io.Reader { return tetl.MD5Transform(tetl.MD5Transform(r)) },
			onlyLong:  false,
		},
		{
			pipeline:  []string{tetl.MD5, tetl.Echo},
			commTypes: []string{etl.WebSocket, etl.Hpush},
			transform: tetl.MD5Transform,
			onlyLong:  true,
		},
	}

	for _, bcktest := range bcktests {
		bckFrom, tname := bcktest.setupBckFrom(t, prefix, objCnt) // setup source bucket & cleanup
		t.Run(tname, func(t *testing.T) {
			for _, test := range tests {
				tools.CheckSkip(t, &tools.SkipTestArgs{Long: test.onlyLong})
				tassert.Fatalf(t, len(test.pipeline) == len(test.commTypes), "pipeline and commTypes must have the same length")
				t.Run(strings.Join(test.pipeline, "->"), func(t *testing.T) {
					etlNames := initPipelineETLs(t, baseParams, test.pipeline, test.commTypes)
					testETLBucket(t, baseParams, etlNames[0], prefix, bckFrom, objCnt, fileSize, time.Minute*3, false, bcktest, test.transform, etlNames[:1]... /*the first ETL applies by default*/)
				})
			}
		})
	}
}

func TestETLPipelineInlineTransform(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	tests := []struct {
		pipeline  []string
		commTypes []string
		transform transformFunc
	}{
		{
			pipeline:  []string{tetl.Echo, tetl.Echo, tetl.Echo},
			commTypes: []string{etl.WebSocket, etl.Hpush, etl.Hpush},
			transform: tetl.EchoTransform,
		},
		{
			pipeline:  []string{tetl.MD5, tetl.Echo, tetl.MD5, tetl.MD5},
			commTypes: []string{etl.WebSocket, etl.Hpush, etl.WebSocket, etl.Hpush},
			transform: func(r io.Reader) io.Reader {
				return tetl.MD5Transform(tetl.MD5Transform(tetl.MD5Transform(r)))
			},
		},
	}

	for _, test := range tests {
		t.Run(strings.Join(test.pipeline, "->"), func(t *testing.T) {
			tassert.Fatalf(t, len(test.pipeline) == len(test.commTypes), "pipeline and commTypes must have the same length")
			etlNames := initPipelineETLs(t, baseParams, test.pipeline, test.commTypes)
			testETLObject(t, etlNames[0], nil, "", "", test.transform, tools.FilesEqual, etlNames[1:]...)
			tlog.Logfln("ETL pipeline: %v", etlNames)
			t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, etlNames[0]) })
		})
	}
}

func TestETLPipelineNotFound(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Provider: apc.AIS, Name: "etl-test" + trand.String(5)}
		objName    = "object" + trand.String(5)
	)

	etlNames := initPipelineETLs(t, baseParams, []string{tetl.Echo, tetl.Echo, tetl.Echo}, []string{etl.WebSocket, etl.Hpush, etl.Hpush})
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, etlNames[0]) })
	etlNames[1] = "non-existing-etl"

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: int64(fileSize), CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)
	tools.PutObject(t, bck, objName, r, fileSize)
	etlPipeline := &api.ETL{ETLName: etlNames[0]}
	for _, name := range etlNames[1:] {
		etlPipeline = etlPipeline.Chain(&api.ETL{ETLName: name})
	}
	_, err = api.ETLObject(baseParams, etlPipeline, bck, objName, nil)
	testETLAllErrors(t, err, "entry not found")
	tlog.Logfln("Received expected error: %v", err)
}

func TestETLPipelineChain(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Provider: apc.AIS, Name: "etl-test" + trand.String(5)}
		objName    = "object" + trand.String(5)
	)

	// Setup ETL pods
	etlNames := initPipelineETLs(t, baseParams, []string{tetl.MD5, tetl.Echo}, []string{etl.Hpush, etl.Hpush})
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, etlNames[0]) })

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	originalData := []byte("Test data for ETL pipeline transformation")
	r := readers.NewBytes(originalData)
	tools.PutObject(t, bck, objName, r, uint64(len(originalData)))

	// Test 3-stage pipeline (MD5 -> Echo -> MD5)
	etlA := &api.ETL{ETLName: etlNames[0]}
	etlB := &api.ETL{ETLName: etlNames[1]}
	etlC := &api.ETL{ETLName: etlNames[0]}
	threeStagePipeline := etlA.Chain(etlB).Chain(etlC)

	buf := &bytes.Buffer{}
	_, err := api.ETLObject(baseParams, threeStagePipeline, bck, objName, buf)
	tassert.CheckFatal(t, err)

	originalReader := bytes.NewReader(originalData)
	expectedReader := tetl.MD5Transform(tetl.MD5Transform(originalReader)) // double MD5 transformation
	expectedData, err := io.ReadAll(expectedReader)
	tassert.CheckFatal(t, err)

	actualData := buf.Bytes()
	tassert.Fatalf(t, bytes.Equal(actualData, expectedData),
		"Pipeline output doesn't match expected transformation.\nExpected: %q\nActual: %q",
		string(expectedData), string(actualData))

	tlog.Logfln("Go SDK ETL pipeline test completed")
}

// Helper function to initialize ETLs for pipelines
func initPipelineETLs(t *testing.T, baseParams api.BaseParams, pipeline, commTypes []string) []string {
	etlNames := make([]string, 0, len(pipeline))
	for i, transformer := range pipeline {
		msg := tetl.InitSpec(t, baseParams, transformer, commTypes[i])
		if i != 0 { // the first ETL will be cleaned up by testETLBucket
			t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg.Name()) })
		}
		etlNames = append(etlNames, msg.Name())
	}
	return etlNames
}
