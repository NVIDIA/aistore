// Package integration_test.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ext/dload"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tetl"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

const (
	squadDatasetURL = "https://huggingface.co/datasets/rajpurkar/squad/resolve/main/plain_text/train-00000-of-00001.parquet"
	rangeTemplate   = "https://huggingface.co/datasets/EleutherAI/the_pile_deduplicated/resolve/refs%2Fconvert%2Fparquet/default/train/000{0..2}.parquet"
)

var squadRequiredFields = []string{`"id"`, `"title"`, `"context"`, `"question"`, `"answers"`}

// TestETLDownloadIntegration tests ETL download: parquet->JSON transformation with squad dataset
func TestETLDownloadIntegration(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, Long: true})
	// TODO: resolve the error: "429 Too Many Requests We had to rate limit your IP"
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: "etl-download-test-" + trand.String(5), Provider: apc.AIS}
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	tetl.CheckNoRunningETLContainers(t, baseParams)

	// Use parquet-parser ETL to transform squad dataset
	etlMsg := tetl.InitSpec(t, baseParams, tetl.ParquetParser, etl.Hpush)
	etlName := etlMsg.Name()
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, etlName) })

	// Test different download types
	testETLDownloadSingle(t, baseParams, bck, etlName)
	testETLDownloadMulti(t, baseParams, bck, etlName)
	testETLDownloadRange(t, baseParams, bck, etlName) // EleutherAI dataset
}

// testETLDownloadSingle tests single file ETL download
func testETLDownloadSingle(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, etlName string) {
	t.Run("download-single", func(t *testing.T) {
		objName := "squad-" + trand.String(4) + ".json"

		tlog.Logfln("ETL download: %s -> %s (via %s)", squadDatasetURL, objName, etlName)

		// Download + transform
		id, err := api.DownloadWithParam(baseParams, dload.TypeSingle, &dload.SingleBody{
			Base:      dload.Base{Bck: bck, ETLName: etlName},
			SingleObj: dload.SingleObj{ObjName: objName, Link: squadDatasetURL},
		})
		tassert.CheckFatal(t, err)

		waitForETLDownload(t, id, baseParams)
		verifyETLTransform(t, baseParams, bck, objName)
		tlog.Logfln("Single ETL download successful")
	})
}

// testETLDownloadMulti tests multi-file ETL download
func testETLDownloadMulti(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, etlName string) {
	t.Run("download-multi", func(t *testing.T) {
		objName := "squad-multi-" + trand.String(4) + ".json"

		tlog.Logfln("Multi ETL download: %s -> %s (via %s)", squadDatasetURL, objName, etlName)

		// Download + transform using multi type
		id, err := api.DownloadWithParam(baseParams, dload.TypeMulti, &dload.MultiBody{
			Base: dload.Base{Bck: bck, ETLName: etlName},
			ObjectsPayload: map[string]any{
				objName: squadDatasetURL,
			},
		})
		tassert.CheckFatal(t, err)

		waitForETLDownload(t, id, baseParams)
		verifyETLTransform(t, baseParams, bck, objName)
		tlog.Logfln("Multi ETL download successful")
	})
}

// testETLDownloadRange tests range-based ETL download using templated URLs
func testETLDownloadRange(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, etlName string) {
	t.Run("download-range", func(t *testing.T) {
		tlog.Logfln("Range ETL download with template (via %s)", etlName)

		// Download parquet files 0000.parquet ... 0002.parquet + transform
		id, err := api.DownloadWithParam(baseParams, dload.TypeRange, &dload.RangeBody{
			Base: dload.Base{
				Bck:     bck,
				ETLName: etlName,
			},
			Template: rangeTemplate,
		})
		tassert.CheckFatal(t, err)

		waitForETLDownload(t, id, baseParams)

		// Verify multiple objects were downloaded and transformed
		objList, err := api.ListObjects(baseParams, bck, &apc.LsoMsg{}, api.ListArgs{})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, len(objList.Entries) >= 3, "Expected at least 3 objects downloaded from range, got %d", len(objList.Entries))

		// Check first object was transformed (parquet -> JSON)
		objName := objList.Entries[0].Name
		reader, _, err := api.GetObjectReader(baseParams, bck, objName, nil)
		tassert.CheckFatal(t, err)
		defer reader.Close()

		data, err := io.ReadAll(reader)
		tassert.CheckFatal(t, err)

		tassert.Fatalf(t, len(data) > 0, "Expected transformed data")
		tassert.Fatalf(t, bytes.Contains(data, []byte("{")), "Expected JSON content")

		tlog.Logfln("Range ETL download successful - transformed %d objects, first: %s (%d bytes)", len(objList.Entries), objName, len(data))
	})
}

// waitForETLDownload waits for download completion
func waitForETLDownload(t *testing.T, id string, baseParams api.BaseParams) {
	deadline := time.Now().Add(2 * time.Minute)
	var resp *dload.StatusResp
	for time.Now().Before(deadline) {
		time.Sleep(time.Second)
		var err error
		resp, err = api.DownloadStatus(baseParams, id, true)
		if err != nil {
			continue
		}
		if resp.JobFinished() {
			break
		}
	}
	if resp == nil || !resp.JobFinished() {
		err := api.AbortDownload(baseParams, id)
		tassert.CheckFatal(t, err)
		t.Fatal("Download timed out after 2 minutes")
	}
	tassert.Fatalf(t, len(resp.Errs) == 0, "Download should have no errors: %v", resp.Errs)
}

// verifyETLTransform verifies ETL transformation was applied
// SQuAD specific
func verifyETLTransform(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objName string) {
	reader, _, err := api.GetObjectReader(baseParams, bck, objName, nil)
	tassert.CheckFatal(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	tassert.CheckFatal(t, err)

	tassert.Fatalf(t, len(data) > 0, "Expected transformed data")
	tassert.Fatalf(t, bytes.Contains(data, []byte("{")), "Expected JSON content")

	// Verify squad dataset structure - all required fields must be present
	for _, field := range squadRequiredFields {
		tassert.Fatalf(t, bytes.Contains(data, []byte(field)), "Missing required squad field: %s", field)
	}
}
