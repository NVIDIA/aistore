//go:build aisloader && etl

// E2E tests for AISLoader.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package test_test

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tetl"
)

func TestAISLoaderETL(t *testing.T) {
	duration := configuredDuration(t)
	runner := newE2ERunner(t, "etl")
	// CheckSkip needs the cluster state initialized by newE2ERunner.
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, RequiresETL: true})
	etlName, etlTimeout := specETLInfo(t, tetl.Echo)
	// aisloader deletes the ETL on a clean exit; this also handles crashes or timeouts.
	t.Cleanup(func() { _ = api.ETLDelete(tools.BaseAPIParams(runner.proxyURL), etlName) })
	prefix := runner.rootPrefix + "/objects"

	if !t.Run("Populate", func(t *testing.T) {
		report := runner.run(t, "etl-populate", duration, time.Minute, false,
			"-subdir="+prefix,
			"-pctput=100",
			"-maxsize=10MiB",
			"-minsize=1MiB",
			"-totalputsize=128MiB",
		)
		assertOperation(t, "PUT", report.Put)
	}) {
		t.FailNow()
	}

	if !t.Run("Echo", func(t *testing.T) {
		// ETL initialization runs before the workload timer starts.
		report := runner.run(t, "etl-echo", duration, etlTimeout, runner.created,
			"-subdir="+prefix,
			"-pctput=0",
			"-etl="+tetl.Echo,
		)
		assertOperation(t, "ETL GET", report.Get)
		if runner.created {
			runner.assertBucketDestroyed(t)
		}
		runner.assertETLDeleted(t, etlName)
	}) {
		t.FailNow()
	}
}

func (r *e2eRunner) assertETLDeleted(t *testing.T, etlName string) {
	t.Helper()
	list, err := api.ETLList(tools.BaseAPIParams(r.proxyURL))
	if err != nil {
		t.Fatalf("failed to list ETLs: %v", err)
	}
	for _, info := range list {
		tassert.Fatalf(t, info.Name != etlName, "expected ETL %s to be deleted, found it in stage %q", etlName, info.Stage)
	}
}

// Returns the ETL name and init timeout from the spec aisloader loads for the given `-etl` value.
func specETLInfo(t *testing.T, transformer string) (string, time.Duration) {
	t.Helper()
	spec, err := tetl.GetTransformYaml(transformer)
	if err != nil {
		t.Fatalf("failed to get %s spec: %v", transformer, err)
	}
	msg, err := tetl.SpecToInitMsg(spec)
	if err != nil {
		t.Fatalf("failed to parse %s spec: %v", transformer, err)
	}
	return msg.Name(), msg.InitTimeout.D()
}
