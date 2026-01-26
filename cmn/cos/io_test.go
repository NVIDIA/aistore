// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const testContent = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz"

var _ = Describe("I/O utils", func() {
	Describe("FileSectionHandle", func() {
		var tempFile string

		BeforeEach(func() {
			// Create temporary test file
			tmpDir := os.TempDir()
			tempFile = filepath.Join(tmpDir, "test_section_file.txt")

			file, err := os.Create(tempFile)
			Expect(err).ShouldNot(HaveOccurred())

			_, err = file.WriteString(testContent)
			Expect(err).ShouldNot(HaveOccurred())

			err = file.Close()
			Expect(err).ShouldNot(HaveOccurred())
		})

		AfterEach(func() {
			// Clean up temporary file
			if tempFile != "" {
				os.Remove(tempFile)
			}
		})

		It("should read a basic section from file", func() {
			// Test reading bytes 5-14 (10 bytes): testContent[5:15]
			fsh, err := cos.NewFileSectionHandle(tempFile, 5, 10)
			Expect(err).ShouldNot(HaveOccurred())
			defer fsh.Close()

			buf := make([]byte, 10)
			n, err := fsh.Read(buf)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(n).To(Equal(10))
			Expect(string(buf)).To(Equal(testContent[5:15]))
		})

		It("should handle multiple non-overlapping section readers", func() {
			// Create three different section readers
			fsh1, err := cos.NewFileSectionHandle(tempFile, 0, 10) // testContent[0:10]
			Expect(err).ShouldNot(HaveOccurred())
			defer fsh1.Close()

			fsh2, err := cos.NewFileSectionHandle(tempFile, 10, 10) // testContent[10:20]
			Expect(err).ShouldNot(HaveOccurred())
			defer fsh2.Close()

			fsh3, err := cos.NewFileSectionHandle(tempFile, 20, 10) // testContent[20:30]
			Expect(err).ShouldNot(HaveOccurred())
			defer fsh3.Close()

			// Read from each independently
			buf1 := make([]byte, 10)
			n1, err := fsh1.Read(buf1)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(n1).To(Equal(10))
			Expect(string(buf1)).To(Equal(testContent[0:10]))

			buf2 := make([]byte, 10)
			n2, err := fsh2.Read(buf2)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(n2).To(Equal(10))
			Expect(string(buf2)).To(Equal(testContent[10:20]))

			buf3 := make([]byte, 10)
			n3, err := fsh3.Read(buf3)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(n3).To(Equal(10))
			Expect(string(buf3)).To(Equal(testContent[20:30]))
		})

		It("should handle overlapping section readers", func() {
			// Create overlapping section readers
			fshA, err := cos.NewFileSectionHandle(tempFile, 5, 10) // testContent[5:15]
			Expect(err).ShouldNot(HaveOccurred())
			defer fshA.Close()

			fshB, err := cos.NewFileSectionHandle(tempFile, 10, 10) // testContent[10:20]
			Expect(err).ShouldNot(HaveOccurred())
			defer fshB.Close()

			// Read from both and verify content
			bufA := make([]byte, 10)
			nA, err := fshA.Read(bufA)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(nA).To(Equal(10))
			Expect(string(bufA)).To(Equal(testContent[5:15]))

			bufB := make([]byte, 10)
			nB, err := fshB.Read(bufB)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(nB).To(Equal(10))
			Expect(string(bufB)).To(Equal(testContent[10:20]))

			// Verify overlap: last 5 chars of A should match first 5 chars of B
			Expect(string(bufA[5:10])).To(Equal(string(bufB[0:5]))) // testContent[10:15]
		})

		It("should create independent readers via Open() method", func() {
			// Create initial section reader
			fsh1, err := cos.NewFileSectionHandle(tempFile, 5, 15) // testContent[5:20]
			Expect(err).ShouldNot(HaveOccurred())
			defer fsh1.Close()

			// Read partially from first reader
			buf1 := make([]byte, 7)
			n1, err := fsh1.Read(buf1)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(n1).To(Equal(7))
			Expect(string(buf1)).To(Equal(testContent[5:12]))

			// Create second reader via Open()
			roc, err := fsh1.Open()
			Expect(err).ShouldNot(HaveOccurred())
			fsh2, ok := roc.(*cos.FileSectionHandle)
			Expect(ok).To(BeTrue())
			defer fsh2.Close()

			// Read from second reader (should start from beginning of section)
			buf2 := make([]byte, 15)
			n2, err := fsh2.Read(buf2)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(n2).To(Equal(15))
			Expect(string(buf2)).To(Equal(testContent[5:20]))

			// Continue reading from first reader (should continue from where it left off)
			bufRest := make([]byte, 8)
			nRest, err := fsh1.Read(bufRest)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(nRest).To(Equal(8))
			Expect(string(bufRest)).To(Equal(testContent[12:20]))
		})

		It("should handle edge cases", func() {
			// Test zero-size section
			fshZero, err := cos.NewFileSectionHandle(tempFile, 10, 0)
			Expect(err).ShouldNot(HaveOccurred())
			defer fshZero.Close()

			buf := make([]byte, 10)
			n, err := fshZero.Read(buf)
			Expect(err).To(Equal(io.EOF))
			Expect(n).To(Equal(0))

			// Test reading from very end of file
			fileLen := int64(len(testContent))
			fshEnd, err := cos.NewFileSectionHandle(tempFile, fileLen-5, 5)
			Expect(err).ShouldNot(HaveOccurred())
			defer fshEnd.Close()

			bufEnd := make([]byte, 5)
			nEnd, err := fshEnd.Read(bufEnd)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(nEnd).To(Equal(5))
			Expect(string(bufEnd)).To(Equal(testContent[fileLen-5:]))

			// Test section extending beyond file
			fshBeyond, err := cos.NewFileSectionHandle(tempFile, fileLen-5, 10)
			Expect(err).ShouldNot(HaveOccurred())
			defer fshBeyond.Close()

			bufBeyond := make([]byte, 10)
			nBeyond, err := fshBeyond.Read(bufBeyond)
			Expect(err).To(Equal(io.EOF))
			Expect(nBeyond).To(Equal(5)) // Only 5 bytes available
			Expect(string(bufBeyond[:nBeyond])).To(Equal(testContent[fileLen-5:]))
		})

		It("should handle different offset and size combinations", func() {
			testCases := []struct {
				offset   int64
				size     int64
				expected string
			}{
				{0, 5, testContent[0:5]},
				{1, 3, testContent[1:4]},
				{26, 10, testContent[26:36]},
				{36, 10, testContent[36:46]},
				{int64(len(testContent)) - 10, 10, testContent[len(testContent)-10:]},
			}

			for _, tc := range testCases {
				fsh, err := cos.NewFileSectionHandle(tempFile, tc.offset, tc.size)
				Expect(err).ShouldNot(HaveOccurred())

				buf := make([]byte, tc.size)
				n, err := fsh.Read(buf)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(n).To(Equal(int(tc.size)))
				Expect(string(buf)).To(Equal(tc.expected))

				err = fsh.Close()
				Expect(err).ShouldNot(HaveOccurred())
			}
		})

		It("should handle multiple reads from same section reader", func() {
			fsh, err := cos.NewFileSectionHandle(tempFile, 5, 15) // testContent[5:20]
			Expect(err).ShouldNot(HaveOccurred())
			defer fsh.Close()

			// First read - partial
			buf1 := make([]byte, 8)
			n1, err := fsh.Read(buf1)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(n1).To(Equal(8))
			Expect(string(buf1)).To(Equal(testContent[5:13]))

			// Second read - remaining bytes
			buf2 := make([]byte, 10)
			n2, err := fsh.Read(buf2)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(n2).To(Equal(7)) // Only 7 bytes left
			Expect(string(buf2[:n2])).To(Equal(testContent[13:20]))

			// Third read - should return EOF
			buf3 := make([]byte, 5)
			n3, err := fsh.Read(buf3)
			Expect(err).To(Equal(io.EOF))
			Expect(n3).To(Equal(0))
		})

		It("should handle cleanup and Close() method", func() {
			fsh, err := cos.NewFileSectionHandle(tempFile, 0, 10)
			Expect(err).ShouldNot(HaveOccurred())

			// Read something to ensure it's working
			buf := make([]byte, 5)
			n, err := fsh.Read(buf)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(n).To(Equal(5))
			Expect(string(buf)).To(Equal(testContent[0:5]))

			// Close should succeed
			err = fsh.Close()
			Expect(err).ShouldNot(HaveOccurred())

			// Note: Multiple Close() calls on FileSectionHandle are not idempotent
			// because the underlying os.File.Close() is not idempotent
		})

		It("should handle non-existent file gracefully", func() {
			nonExistentFile := "/tmp/non_existent_file_xyz.txt"
			_, err := cos.NewFileSectionHandle(nonExistentFile, 0, 10)
			Expect(err).Should(HaveOccurred())
			Expect(strings.Contains(err.Error(), "no such file")).To(BeTrue())
		})
	})
})
