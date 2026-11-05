//go:build aisloader

// Package test_test provides E2E tests of the aisloader binary against a running cluster
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package test_test

import (
	"flag"
	"os"
	"testing"

	"github.com/NVIDIA/aistore/tools"
)

func TestMain(m *testing.M) {
	flag.Parse()
	tools.InitLocalCluster()
	os.Exit(m.Run())
}
