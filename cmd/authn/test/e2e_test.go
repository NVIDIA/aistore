// Package test provides E2E tests of AIS CLI
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package test

import (
	"os"
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

var authnURL string

func TestAuthE2E(t *testing.T) {
	tutils.InitLocalCluster()
	cmd := exec.Command("which", "ais")
	if err := cmd.Run(); err != nil {
		t.Skip("'ais' binary not found")
	}
	authnURL = os.Getenv("AIS_AUTHN_URL")
	if authnURL == "" {
		t.Skip("AuthN URL is undefined")
	}

	config.DefaultReporterConfig.SlowSpecThreshold = 15 * time.Second.Seconds()
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E")
}

var _ = Describe("E2E AuthN Tests", func() {
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
