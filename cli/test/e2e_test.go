package test

import (
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/aistore/tutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

func TestE2E(t *testing.T) {
	cmd := exec.Command("which", "ais")
	if err := cmd.Run(); err != nil {
		t.Skip("'ais' binary not found")
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E")
}

var _ = Describe("E2E CLI Tests", func() {
	var (
		entries []TableEntry

		f        = &tutils.E2EFramework{}
		files, _ = filepath.Glob("./*.in")
	)

	for _, fileName := range files {
		fileName = fileName[:len(fileName)-len(filepath.Ext(fileName))]
		entries = append(entries, Entry(fileName, fileName+".in", fileName+".stdout"))
	}

	DescribeTable(
		"e2e",
		f.RunE2ETest,
		entries...,
	)
})
