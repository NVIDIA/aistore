// Package s3_integration provides tests of compatibility with AWS S3
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

func TestE2ES3(t *testing.T) {
	tools.InitLocalCluster()
	cmd := exec.Command("which", "s3cmd")
	if err := cmd.Run(); err != nil {
		t.Skip("'s3cmd' binary not found")
	}

	config.DefaultReporterConfig.SlowSpecThreshold = 15 * time.Second.Seconds()
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}

var _ = Describe("E2E AWS Compatibility Tests", func() {
	var (
		host   string
		params string
	)

	if value := os.Getenv(env.AIS.UseHTTPS); cos.IsParseBool(value) {
		host = "https://localhost:8080/s3"
		params = "--no-check-certificate"
	} else {
		host = "http://localhost:8080/s3"
		params = "--no-ssl --no-check-certificate"
	}

	var (
		entries []TableEntry

		f = &tools.E2EFramework{
			Vars: map[string]string{"HOST": host, "PARAMS": params},
		}
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
