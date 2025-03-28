// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ios_test

import (
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/tools"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("fsutils", func() {
	Describe("GetFSUsedPercentage", func() {
		It("should", func() {
			percentage, ok := ios.GetFSUsedPercentage("/")
			Expect(ok).To(BeTrue(), "Unable to retrieve FS used percentage!")
			Expect(percentage).To(BeNumerically("<=", 100), "Invalid FS used percentage: %d", percentage)
		})
	})

	Describe("DirSizeOnDisk", func() {
		var (
			rootDir string
			files   []string
		)

		BeforeEach(func() {
			rootDir, files = tools.PrepareDirTree(GinkgoTB(), tools.DirTreeDesc{
				InitDir:  "",
				Dirs:     10,
				Files:    10,
				FileSize: 1024,
				Depth:    5,
				Empty:    true,
			})
		})

		Describe("withoutPrefix", func() {
			It("should calculate size correctly", func() {
				size, err := ios.DirSizeOnDisk(rootDir, false /*withNonDirPrefix*/)
				Expect(err).NotTo(HaveOccurred())
				Expect(size).To(BeNumerically(">", 50*1024))
			})
		})

		Describe("withPrefix", func() {
			It("should calculate size correctly", func() {
				size, err := ios.DirSizeOnDisk(files[0], true /*withNonDirPrefix*/)
				Expect(err).NotTo(HaveOccurred())
				Expect(size).To(BeNumerically(">=", 1024))
			})
		})
	})
})
