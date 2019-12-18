package test

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/tutils"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

func TestE2E(t *testing.T) {
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
	RunSpecs(t, "E2E")
}

var _ = Describe("E2E FUSE Tests", func() {
	var (
		f               *tutils.E2EFramework
		fuseDir, bucket string
		entries         []TableEntry
		baseParams      api.BaseParams

		files, _ = filepath.Glob("./*.in")
	)

	for _, fileName := range files {
		fileName = fileName[:len(fileName)-len(filepath.Ext(fileName))]
		entries = append(entries, Entry(fileName, fileName+".in", fileName+".stdout"))
	}

	BeforeEach(func() {
		var err error

		proxyURL := tutils.GetPrimaryURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bucket = tutils.GenRandomString(10)
		fuseDir, err = ioutil.TempDir("/tmp", "")
		Expect(err).NotTo(HaveOccurred())

		err = api.CreateBucket(baseParams, bucket)
		Expect(err).NotTo(HaveOccurred())

		out, err := exec.Command("aisfs", bucket, fuseDir).Output()
		if err != nil {
			stderr := ""
			if ee, ok := err.(*exec.ExitError); ok {
				stderr = string(ee.Stderr)
			}
			Skip(fmt.Sprintf("failed to run 'aisfs': %s (stderr: %s), err: %v", out, stderr, err))
		}

		// Ensure that `aisfs` is running.
		err = exec.Command("pidof", "aisfs").Run()
		Expect(err).NotTo(HaveOccurred(), "'aisfs' is not running after being started")

		f = &tutils.E2EFramework{Dir: fuseDir}
	})

	AfterEach(func() {
		exec.Command("pkill", "aisfs").Run()
		exec.Command("fusermount", "-u", fuseDir).Run()
		os.RemoveAll(fuseDir)

		exists, err := api.DoesBucketExist(baseParams, bucket)
		Expect(err).NotTo(HaveOccurred())
		if exists {
			err = api.DestroyBucket(baseParams, bucket)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	DescribeTable(
		"end-to-end tests",
		func(inputFileName, outputFileName string) {
			f.RunE2ETest(inputFileName, outputFileName)
		},
		entries...,
	)
})
