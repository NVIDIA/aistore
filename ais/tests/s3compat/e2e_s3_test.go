// Package s3_integration provides tests of compatibility with AWS S3
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package s3_integration

import (
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/devtools/tutils"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

func TestE2ES3(t *testing.T) {
	cmd := exec.Command("which", "aws")
	if err := cmd.Run(); err != nil {
		t.Skip("'aws' binary not found")
	}

	config.DefaultReporterConfig.SlowSpecThreshold = 15 * time.Second.Seconds()
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E")
}

var _ = Describe("E2E AWS Compatibility Tests", func() {
	var (
		entries []TableEntry

		f        = &tutils.E2EFramework{}
		files, _ = filepath.Glob("./*.in")
	)

	for _, fileName := range files {
		fileName = fileName[:len(fileName)-len(filepath.Ext(fileName))]
		entries = append(entries, Entry(fileName, fileName))
	}

	DescribeTable(
		"e2e",
		f.RunE2ETest,
		entries...,
	)
})
