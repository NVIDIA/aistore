// Package test provides tests for command-line mounting utility for aisfs.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/tools"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

func TestE2E(t *testing.T) {
	tools.InitLocalCluster()
	cmd := exec.Command("which", "aisfs")
	if err := cmd.Run(); err != nil {
		t.Skip("'aisfs' binary not found")
	}

	cmd = exec.Command("pidof", "aisfs")
	if err := cmd.Run(); err == nil {
		t.Skip("'aisfs' is already running")
	}

	config.DefaultReporterConfig.SlowSpecThreshold = 10 * time.Second.Seconds()
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}

var _ = Describe("E2E FUSE Tests", func() {
	var (
		f          *tools.E2EFramework
		fuseDir    string
		bck        cmn.Bck
		entries    []TableEntry
		baseParams api.BaseParams

		files, _ = filepath.Glob("./*.in")
	)

	for _, fileName := range files {
		fileName = fileName[:len(fileName)-len(filepath.Ext(fileName))]
		entries = append(entries, Entry(fileName, fileName))
	}

	BeforeEach(func() {
		var err error

		proxyURL := tools.GetPrimaryURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		bck = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
		fuseDir, err = os.MkdirTemp("/tmp", "")
		Expect(err).NotTo(HaveOccurred())

		err = api.CreateBucket(baseParams, bck, nil)
		Expect(err).NotTo(HaveOccurred())

		// Retry couple times to start `aisfs`.
		for i := 0; i < 3; i++ {
			out, err := exec.Command("aisfs", bck.Name, fuseDir).Output()
			if err != nil {
				stderr := ""
				if ee, ok := err.(*exec.ExitError); ok {
					stderr = string(ee.Stderr)
				}
				Skip(fmt.Sprintf("failed to run 'aisfs': %s (stderr: %s), err: %v", out, stderr, err))
			}

			// Ensure that `aisfs` is running.
			if err = exec.Command("pgrep", "-x", "aisfs").Run(); err == nil {
				break
			}
		}
		Expect(err).NotTo(HaveOccurred(), "'aisfs' is not running after being started")

		f = &tools.E2EFramework{Dir: fuseDir}
	})

	AfterEach(func() {
		exec.Command("pkill", "aisfs").Run()
		exec.Command("fusermount", "-u", fuseDir).Run()
		os.RemoveAll(fuseDir)

		exists, err := api.QueryBuckets(baseParams, cmn.QueryBcks(bck), 0 /*fltPresence*/)
		Expect(err).NotTo(HaveOccurred())
		if exists {
			err = api.DestroyBucket(baseParams, bck)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	DescribeTable(
		"end-to-end tests",
		func(fileName string) { f.RunE2ETest(fileName) },
		entries...,
	)
})
