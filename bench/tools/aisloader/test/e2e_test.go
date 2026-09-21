//go:build aisloader

// E2E tests for AISLoader.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package test_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
)

const (
	bucketEnv   = "BUCKET"
	durationEnv = "DURATION"
	e2eWorkers  = 8
)

// tools.InitLocalCluster writes package-level state and must not run concurrently.
var initClusterOnce sync.Once

type (
	opStats struct {
		Count  int64 `json:"count,string"`
		Bytes  int64 `json:"bytes,string"`
		Errors int64 `json:"errors"`
	}

	statsReport struct {
		Get      opStats `json:"get"`
		Put      opStats `json:"put"`
		PutMPU   opStats `json:"put_multipart"`
		GetBatch opStats `json:"get_batch"`
	}

	e2eRunner struct {
		binary     string
		proxyURL   string
		bck        cmn.Bck
		baseArgs   []string
		rootPrefix string // isolates this test's objects in a bucket
		created    bool   // bucket did not exist and the test created it
	}
)

func TestAISLoaderWorkloads(t *testing.T) {
	duration := configuredDuration(t)
	shorter := cos.ClampDuration(duration/2, 10*time.Second, time.Minute)

	runner := newE2ERunner(t, "workloads")
	regularPrefix := runner.rootPrefix + "/regular"
	multipartPrefix := runner.rootPrefix + "/multipart"

	if !t.Run("Populate Bucket", func(t *testing.T) {
		report := runner.run(t, "populate", duration, time.Minute, false,
			"-subdir="+regularPrefix,
			"-pctput=100",
			"-maxsize=10MiB",
			"-minsize=1MiB",
			"-totalputsize=256MiB",
		)
		assertOperation(t, "PUT", report.Put)
		runner.assertObjectsPresent(t, regularPrefix)
	}) {
		t.FailNow()
	}

	if !t.Run("Mixed PUT And Multipart Uploads", func(t *testing.T) {
		report := runner.run(t, "multipart", duration, time.Minute, false,
			"-subdir="+multipartPrefix,
			"-pctput=100",
			"-maxsize=24MiB",
			"-minsize=20MiB",
			"-totalputsize=128MiB",
			"-multipart-chunks=4",
			"-pctmultipart=30",
		)
		assertOperation(t, "regular PUT", report.Put)
		assertOperation(t, "multipart PUT", report.PutMPU)
		runner.assertChunkedObjectsPresent(t, multipartPrefix)
	}) {
		t.FailNow()
	}

	if !t.Run("Mixed PUT And GET", func(t *testing.T) {
		report := runner.run(t, "mixed", shorter, time.Minute, false,
			"-subdir="+runner.rootPrefix,
			"-pctput=10",
			"-maxsize=10MiB",
			"-minsize=1MiB",
			"-totalputsize=32MiB",
		)
		assertOperation(t, "mixed PUT", report.Put)
		assertOperation(t, "mixed GET", report.Get)
	}) {
		t.FailNow()
	}

	t.Run("GetBatch And Cleanup", func(t *testing.T) {
		report := runner.run(t, "get-batch", shorter, time.Minute, runner.created,
			"-subdir="+runner.rootPrefix,
			"-pctput=0",
			"-get-batchsize=32",
		)
		assertOperation(t, "GetBatch", report.GetBatch)
		if runner.created {
			runner.assertBucketDestroyed(t)
		}
	})
}

func configuredDuration(t *testing.T) time.Duration {
	t.Helper()
	value := cos.GetEnvOrDefault(durationEnv, "20s")
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		t.Fatalf("invalid %s=%q: %v", durationEnv, value, err)
	}
	return duration
}

func newE2ERunner(t *testing.T, name string) *e2eRunner {
	t.Helper()
	binary, err := exec.LookPath("aisloader")
	if err != nil {
		t.Fatalf("aisloader binary not found in PATH: %v", err)
	}

	initClusterOnce.Do(tools.InitLocalCluster)
	proxyURL := tools.RandomProxyURL(t)
	rootPrefix := "aisloader-e2e-" + name + "-" + strings.ToLower(trand.String(8))
	bck, created := configuredBucket(t, proxyURL)
	// Tests that create the same explicitly named bucket must not overlap.
	if !created || os.Getenv(bucketEnv) == "" {
		t.Parallel()
	}

	r := &e2eRunner{
		binary:     binary,
		proxyURL:   proxyURL,
		bck:        bck,
		rootPrefix: rootPrefix,
		created:    created,
		baseArgs: []string{
			"-bucket=" + bck.String(),
			fmt.Sprintf("-numworkers=%d", e2eWorkers),
			"-statsinterval=0",
			"-json",
			"-quiet",
		},
	}
	if created {
		t.Cleanup(func() { tools.DestroyBucket(t, proxyURL, bck) })
	} else {
		t.Cleanup(func() { r.deleteObjects(t) })
	}
	return r
}

func configuredBucket(t *testing.T, proxyURL string) (cmn.Bck, bool) {
	t.Helper()
	uri := os.Getenv(bucketEnv)
	if uri == "" {
		return cmn.Bck{Name: "aisloader-" + strings.ToLower(trand.String(10)), Provider: apc.AIS}, true
	}
	bck, objName, err := cmn.ParseBckObjectURI(uri, cmn.ParseURIOpts{})
	if err != nil {
		t.Fatalf("invalid %s=%q: %v", bucketEnv, uri, err)
	}
	tassert.Fatalf(t, objName == "", "unexpected object name %q", objName)
	if err := bck.Validate(); err != nil {
		t.Fatalf("invalid %s=%q: %v", bucketEnv, uri, err)
	}
	exists, _ := tools.BucketExists(t, proxyURL, bck)
	return bck, bck.IsAIS() && !exists
}

func (r *e2eRunner) deleteObjects(t *testing.T) {
	t.Helper()
	prefix := r.rootPrefix + "/"
	bp := tools.BaseAPIParams(r.proxyURL)
	xid, err := api.DeleteMultiObj(bp, r.bck, &apc.EvdMsg{ListRange: apc.ListRange{Template: prefix}})
	if err != nil {
		t.Errorf("failed to delete objects under %s: %v", r.bck.Cname(prefix), err)
		return
	}
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActDeleteObjects, Timeout: tools.BucketCleanupTimeout}
	if _, err := api.WaitForXactionIC(bp, &args); err != nil {
		t.Errorf("failed to delete objects under %s: %v", r.bck.Cname(prefix), err)
	}
}

//nolint:unparam // lint does not see invocation inside `e2e_etl_test.go` that uses a different ctxTimeout
func (r *e2eRunner) run(t *testing.T, name string, duration, ctxTimeout time.Duration, cleanup bool, args ...string) statsReport {
	t.Helper()
	statsPath := filepath.Join(t.TempDir(), name+".json")
	cmdArgs := append([]string{}, r.baseArgs...)
	cmdArgs = append(cmdArgs, "-stats-output="+statsPath, "-duration="+duration.String(), "-cleanup="+strconv.FormatBool(cleanup))
	cmdArgs = append(cmdArgs, args...)

	ctx, cancel := context.WithTimeout(t.Context(), duration+ctxTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, r.binary, cmdArgs...)
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("aisloader %s timed out: %v\n%s", name, ctx.Err(), output)
	}
	if err != nil {
		t.Fatalf("aisloader %s failed: %v\n%s", name, err, output)
	}

	data, err := os.ReadFile(statsPath)
	if err != nil {
		t.Fatalf("failed to read %s stats: %v\n%s", name, err, output)
	}
	var reports []statsReport
	if err := json.Unmarshal(data, &reports); err != nil {
		t.Fatalf("failed to parse %s stats %q: %v\n%s", name, data, err, output)
	}
	tassert.Fatalf(t, len(reports) == 1, "expected one final %s stats report, got %d: %s", name, len(reports), data)
	// aisloader can exit successfully even when individual operations fail.
	// Preserve its diagnostics before assertOperation reports the error count.
	if report := reports[0]; report.Get.Errors != 0 || report.Put.Errors != 0 || report.PutMPU.Errors != 0 || report.GetBatch.Errors != 0 {
		t.Logf("aisloader %s reported operation errors\n%s\n%s", name, data, output)
	}
	return reports[0]
}

func (r *e2eRunner) assertObjectsPresent(t *testing.T, prefix string) {
	t.Helper()
	result, err := api.ListObjects(tools.BaseAPIParams(r.proxyURL), r.bck, &apc.LsoMsg{Prefix: prefix}, api.ListArgs{})
	if err != nil {
		t.Fatalf("failed to list %s: %v", r.bck.Cname(prefix), err)
	}
	tassert.Fatalf(t, len(result.Entries) > 0, "expected objects under %s", r.bck.Cname(prefix))
}

func (r *e2eRunner) assertChunkedObjectsPresent(t *testing.T, prefix string) {
	t.Helper()
	msg := &apc.LsoMsg{Prefix: prefix, Props: apc.GetPropsChunked}
	// The chunked flag belongs to AIS's local metadata, not the cloud listing.
	if r.bck.IsRemote() {
		msg.SetFlag(apc.LsCached)
	}
	result, err := api.ListObjects(tools.BaseAPIParams(r.proxyURL), r.bck, msg, api.ListArgs{})
	if err != nil {
		t.Fatalf("failed to list %s: %v", r.bck.Cname(prefix), err)
	}
	for _, entry := range result.Entries {
		if entry.IsAnyFlagSet(apc.EntryIsChunked) {
			return
		}
	}
	t.Fatalf("expected at least one chunked object under %s", r.bck.Cname(prefix))
}

func (r *e2eRunner) assertBucketDestroyed(t *testing.T) {
	t.Helper()
	exists, _ := tools.BucketExists(t, r.proxyURL, r.bck) // handles errors internally
	tassert.Fatalf(t, !exists, "expected bucket %s to be destroyed", r.bck.String())
}

func assertOperation(t *testing.T, name string, stats opStats) {
	t.Helper()
	tassert.Fatalf(t, stats.Errors == 0, "%s reported %d errors", name, stats.Errors)
	tassert.Fatalf(t, stats.Count > 0, "%s count is zero", name)
	tassert.Fatalf(t, stats.Bytes > 0, "%s bytes = %d, expected a positive value", name, stats.Bytes)
}
