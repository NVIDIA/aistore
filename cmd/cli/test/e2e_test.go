// Package test provides E2E tests of AIS CLI
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package test

import (
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/tools"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

func TestE2E(t *testing.T) {
	tools.InitLocalCluster()
	cmd := exec.Command("which", "ais")
	if err := cmd.Run(); err != nil {
		t.Skip("'ais' binary not found")
	}

	config.DefaultReporterConfig.SlowSpecThreshold = 15 * time.Second.Seconds()
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E")
}

var _ = Describe("E2E CLI Tests", func() {
	var (
		entries []TableEntry

		f        = &tools.E2EFramework{}
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
