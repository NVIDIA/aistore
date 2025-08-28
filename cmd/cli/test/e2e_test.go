// Package test provides E2E tests of AIS CLI
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package test_test

import (
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/aistore/tools"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// TestE2E runs end-to-end CLI tests based on `*.in` files in this directory.
// Run specific tests: go test -v --ginkgo.focus="multipart"
// Skip specific tests: go test -v --ginkgo.skip="etl"
func TestE2E(t *testing.T) {
	tools.InitLocalCluster()
	cmd := exec.Command("which", "ais")
	if err := cmd.Run(); err != nil {
		t.Skip("'ais' binary not found")
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E")
}

var _ = Describe("E2E CLI Tests", func() {
	var (
		f        = &tools.E2EFramework{}
		files, _ = filepath.Glob("./*.in")
		args     = make([]any, 0, len(files)+1)
	)
	args = append(args, f.RunE2ETest)
	for _, fileName := range files {
		fileName = fileName[:len(fileName)-len(filepath.Ext(fileName))]
		args = append(args, Entry(fileName, fileName))
	}
	DescribeTable("e2e-cli", args...)
})
