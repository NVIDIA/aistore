// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package tracing_test

import (
	"testing"

	"github.com/NVIDIA/aistore/tools"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestTracing(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}
