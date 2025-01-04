// Package s3_integration provides tests of compatibility with AWS S3
/*
 * Copyright (c) 2018=2025, NVIDIA CORPORATION. All rights reserved.
 */
package s3_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestE2ES3(t *testing.T) {
	tools.InitLocalCluster()
	cmd := exec.Command("which", "s3cmd")
	if err := cmd.Run(); err != nil {
		t.Skip("'s3cmd' binary not found")
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}

var _ = Describe("E2E AWS Compatibility Tests", func() {
	var (
		host   string
		params string
	)

	if value := os.Getenv(env.AisUseHTTPS); cos.IsParseBool(value) {
		host = "https://localhost:8080/s3"
		params = "--no-check-certificate"
	} else {
		host = "http://localhost:8080/s3"
		params = "--no-ssl --no-check-certificate"
	}

	var (
		f = &tools.E2EFramework{
			Vars: map[string]string{"HOST": host, "PARAMS": params},
		}
		files, _ = filepath.Glob("./*.in")
		args     = make([]any, 0, len(files)+1)
	)
	args = append(args, f.RunE2ETest)
	for _, fileName := range files {
		fileName = fileName[:len(fileName)-len(filepath.Ext(fileName))]
		args = append(args, Entry(fileName, fileName))
	}
	DescribeTable("e2e-s3", args...)
})
