// Package tools provides common tools and utilities for all unit and integration tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package tools

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/tools/tlog"
)

// EnableClusterFeatures enables the given feature flags for the duration of
// the test. Cleanup removes only the flags that this call added.
//
// Tests using this helper must not run in parallel with other tests that
// modify cluster features.
func EnableClusterFeatures(t *testing.T, flags feat.Flags) {
	t.Helper()

	config := GetClusterConfig(t)
	added := flags &^ config.Features
	if added == 0 {
		return
	}

	features := config.Features.Set(added)
	tlog.Logfln("Enable cluster feature flag(s) %v", added.Names())
	SetClusterConfig(t, cos.StrKVs{
		feat.PropName: features.String(),
	})

	t.Cleanup(func() {
		config := GetClusterConfig(t)
		features := config.Features &^ added

		tlog.Logfln("Disable cluster feature flag(s) %v", added.Names())
		SetClusterConfig(t, cos.StrKVs{
			feat.PropName: features.String(),
		})
	})
}

// PrepForS3RebuildClientOrSkip prepares the cluster for an S3 client that rebuilds
// redirected requests. Existing reverse-proxy mode needs no change; otherwise,
// the helper enables S3RedirectRebuild or skips when proxy mediation is required.
func PrepForS3RebuildClientOrSkip(t *testing.T) {
	t.Helper()

	config := GetClusterConfig(t)
	switch {
	case config.Features.IsSet(feat.S3ReverseProxy):
		// nothing to do
	case config.Auth.RequiresProxyMediation():
		t.Skipf("cannot enable %v: cluster configuration requires proxy mediation", feat.S3RedirectRebuild.Names())
	default:
		EnableClusterFeatures(t, feat.S3RedirectRebuild) // with t.Cleanup inside
	}
}
