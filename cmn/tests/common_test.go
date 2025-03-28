// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"github.com/NVIDIA/aistore/cmn/cos"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Common file", func() {
	const (
		tmpDir = "/tmp/cmn-tests"
	)

	var (
		nonExistingFile        = filepath.Join(tmpDir, "file.txt")
		nonExistingRenamedFile = filepath.Join(tmpDir, "some", "path", "fi.txt")
		nonExistingPath        = filepath.Join(tmpDir, "non", "existing", "directory")
	)

	createFile := func(fqn string) {
		file, err := cos.CreateFile(fqn)
		Expect(err).NotTo(HaveOccurred())
		Expect(file.Close()).NotTo(HaveOccurred())
		Expect(fqn).To(BeARegularFile())
	}

	validateSaveReaderOutput := func(fqn string, sourceData []byte) {
		Expect(fqn).To(BeARegularFile())

		data, err := os.ReadFile(fqn)
		Expect(err).NotTo(HaveOccurred())
		Expect(reflect.DeepEqual(data, sourceData)).To(BeTrue())
	}

	AfterEach(func() {
		os.RemoveAll(tmpDir)
	})

	Context("CopyStruct", func() {
		type (
			Tree struct {
				left, right *Tree
				value       int
			}

			NonPrimitiveStruct struct {
				m map[string]int
				s []int
			}
		)

		It("should correctly copy empty struct", func() {
			var emptySructResult struct{}
			cos.CopyStruct(&emptySructResult, &struct{}{})

			Expect(reflect.DeepEqual(struct{}{}, emptySructResult)).To(BeTrue())
		})

		It("should correctly copy self-referencing struct", func() {
			loopNode := Tree{}
			loopNode.left, loopNode.right, loopNode.value = &loopNode, &loopNode, 0
			var copyLoopNode Tree
			cos.CopyStruct(&copyLoopNode, &loopNode)

			Expect(loopNode).To(Equal(copyLoopNode))

			loopNode.value += 100

			Expect(loopNode).NotTo(Equal(copyLoopNode))
		})

		It("should correctly copy nested structs, perisiting references", func() {
			left := Tree{nil, nil, 0}
			right := Tree{nil, nil, 1}
			root := Tree{&left, &right, 2}
			var rootCopy Tree
			cos.CopyStruct(&rootCopy, &root)

			Expect(root).To(Equal(rootCopy))

			left.value += 100

			Expect(root).To(Equal(rootCopy))
		})

		It("should correctly copy structs with nonprimitive types", func() {
			nonPrimitive := NonPrimitiveStruct{}
			nonPrimitive.m = make(map[string]int)
			nonPrimitive.m["one"] = 1
			nonPrimitive.m["two"] = 2
			nonPrimitive.s = []int{1, 2}
			var nonPrimitiveCopy NonPrimitiveStruct
			cos.CopyStruct(&nonPrimitiveCopy, &nonPrimitive)

			Expect(nonPrimitive).To(Equal(nonPrimitive))

			nonPrimitive.m["one"] = 0
			nonPrimitive.s[0] = 0

			Expect(nonPrimitive).To(Equal(nonPrimitive))
		})
	})

	Context("SaveReader", func() {
		It("should save the reader content into a file", func() {
			const bytesToRead = 1000
			byteBuffer := make([]byte, bytesToRead)

			_, err := cos.SaveReader(nonExistingFile, rand.Reader, byteBuffer, cos.ChecksumNone, bytesToRead)
			Expect(err).NotTo(HaveOccurred())

			validateSaveReaderOutput(nonExistingFile, byteBuffer)
		})

		It("should save the reader without specified size", func() {
			const bytesLimit = 500
			byteBuffer := make([]byte, bytesLimit*2)
			reader := &io.LimitedReader{R: rand.Reader, N: bytesLimit}

			_, err := cos.SaveReader(nonExistingFile, reader, byteBuffer, cos.ChecksumNone, -1)
			Expect(err).NotTo(HaveOccurred())

			validateSaveReaderOutput(nonExistingFile, byteBuffer[:bytesLimit])
		})
	})

	Context("CreateFile", func() {
		It("should create a file when it does not exist", func() {
			createFile(nonExistingFile)
		})

		It("should not complain when creating a file which already exists", func() {
			createFile(nonExistingFile)
			createFile(nonExistingFile)
		})
	})

	Context("CreateDir", func() {
		It("should successfully create directory", func() {
			err := cos.CreateDir(nonExistingPath)
			Expect(err).NotTo(HaveOccurred())

			Expect(nonExistingPath).To(BeADirectory())
		})

		It("should not error when creating directory which already exists", func() {
			err := cos.CreateDir(nonExistingPath)
			Expect(err).NotTo(HaveOccurred())
			err = cos.CreateDir(nonExistingPath)
			Expect(err).NotTo(HaveOccurred())

			Expect(nonExistingPath).To(BeADirectory())
		})

		It("should error when directory is not valid", func() {
			err := cos.CreateDir("")
			Expect(err).To(HaveOccurred())
		})
	})

	Context("CopyFile", func() {
		var (
			srcFilename = filepath.Join(tmpDir, "copyfilesrc.txt")
			dstFilename = filepath.Join(tmpDir, "copyfiledst.txt")
		)

		It("should copy file and preserve the content", func() {
			_, err := cos.SaveReader(srcFilename, rand.Reader, make([]byte, 1000), cos.ChecksumNone, 1000)
			Expect(err).NotTo(HaveOccurred())
			_, _, err = cos.CopyFile(srcFilename, dstFilename, make([]byte, 1000), cos.ChecksumNone)
			Expect(err).NotTo(HaveOccurred())

			srcData, err := os.ReadFile(srcFilename)
			Expect(err).NotTo(HaveOccurred())

			dstData, err := os.ReadFile(dstFilename)
			Expect(err).NotTo(HaveOccurred())

			Expect(srcData).To(Equal(dstData))
		})

		It("should copy a object and compute its checksum", func() {
			expectedCksum, err := cos.SaveReader(srcFilename, rand.Reader, make([]byte, 1000), cos.ChecksumCesXxh, 1000)
			Expect(err).NotTo(HaveOccurred())

			_, cksum, err := cos.CopyFile(srcFilename, dstFilename, make([]byte, 1000), cos.ChecksumCesXxh)
			Expect(err).NotTo(HaveOccurred())
			Expect(cksum).To(Equal(expectedCksum))

			srcData, err := os.ReadFile(srcFilename)
			Expect(err).NotTo(HaveOccurred())

			dstData, err := os.ReadFile(dstFilename)
			Expect(err).NotTo(HaveOccurred())

			Expect(srcData).To(Equal(dstData))
		})
	})

	Context("Rename", func() {
		It("should not error when dst file does not exist", func() {
			createFile(nonExistingFile)

			err := cos.Rename(nonExistingFile, nonExistingRenamedFile)
			Expect(err).NotTo(HaveOccurred())

			Expect(nonExistingRenamedFile).To(BeARegularFile())
			Expect(nonExistingFile).NotTo(BeAnExistingFile())
		})

		It("should not error when dst file already exists", func() {
			createFile(nonExistingFile)
			createFile(nonExistingRenamedFile)

			err := cos.Rename(nonExistingFile, nonExistingRenamedFile)
			Expect(err).NotTo(HaveOccurred())

			Expect(nonExistingRenamedFile).To(BeARegularFile())
			Expect(nonExistingFile).NotTo(BeAnExistingFile())
		})

		It("should error when src does not exist", func() {
			err := cos.Rename("/some/non/existing/file.txt", "/tmp/file.txt")
			Expect(err).To(HaveOccurred())
		})
	})

	Context("RemoveFile", func() {
		AfterEach(func() {
			os.RemoveAll(tmpDir)
		})

		It("should remove regular file", func() {
			createFile(nonExistingFile)

			err := cos.RemoveFile(nonExistingFile)
			Expect(err).NotTo(HaveOccurred())
			Expect(nonExistingFile).NotTo(BeAnExistingFile())
		})

		It("should not complain when regular file does not exist", func() {
			err := cos.RemoveFile("/some/non/existing/file.txt")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("ParseQuantity", func() {
		DescribeTable("parse quantity without error",
			func(quantity, ty string, value int) {
				pq, err := cos.ParseQuantity(quantity)
				Expect(err).NotTo(HaveOccurred())

				Expect(pq).To(Equal(cos.ParsedQuantity{Type: ty, Value: uint64(value)}))
			},
			Entry("simple number", "80B", cos.QuantityBytes, 80),
			Entry("simple percent", "80%", cos.QuantityPercent, 80),
			Entry("number with spaces", "  8 0 KB  ", cos.QuantityBytes, 80*cos.KiB),
			Entry("percent with spaces", "80 %", cos.QuantityPercent, 80),
		)

		DescribeTable("parse quantity with error",
			func(template string) {
				_, err := cos.ParseQuantity(template)
				Expect(err).Should(HaveOccurred())
			},
			Entry("contains alphabet", "a80B"),
			Entry("multiple percent signs", "80%%"),
			Entry("empty percent sign", "%"),
			Entry("negative number", "-1000"),
			Entry("101 percent", "101%"),
			Entry("-1 percent", "-1%"),
		)
	})

	Context("ParseBool", func() {
		It("should correctly parse different values into bools", func() {
			trues := []string{"y", "yes", "on", "1", "t", "T", "true", "TRUE", "True"}
			falses := []string{"n", "no", "off", "0", "f", "F", "false", "FALSE", "False", ""}
			errs := []string{"2", "enable", "nothing"}

			for _, s := range trues {
				v, err := cos.ParseBool(s)
				Expect(err).NotTo(HaveOccurred())
				Expect(v).To(BeTrue())
			}

			for _, s := range falses {
				v, err := cos.ParseBool(s)
				Expect(err).NotTo(HaveOccurred())
				Expect(v).To(BeFalse())
			}

			for _, s := range errs {
				_, err := cos.ParseBool(s)
				Expect(err).To(HaveOccurred())
			}
		})
	})

	Context("StrSlicesEqual", func() {
		DescribeTable("parse quantity with error",
			func(lhs, rhs []string, expected bool) {
				Expect(cos.StrSlicesEqual(lhs, rhs)).To(Equal(expected))
			},
			Entry("empty slices", []string{}, []string{}, true),
			Entry("single item", []string{"one"}, []string{"one"}, true),
			Entry("multiple items", []string{"one", "two", "three"}, []string{"one", "two", "three"}, true),
			Entry("multiple items in different order", []string{"two", "three", "one"}, []string{"one", "two", "three"}, true),

			Entry("empty and single item", []string{"one"}, []string{}, false),
			Entry("empty and single item (swapped)", []string{}, []string{"one"}, false),
			Entry("same number of elements but different content", []string{"two", "three", "four"}, []string{"one", "two", "three"}, false),
			Entry("same number of elements but different content (swapped)", []string{"two", "three", "one"}, []string{"four", "two", "three"}, false),
		)
	})

	Context("ExpandPath", func() {
		It("should expand short path with current home", func() {
			shortPath := "~"
			path := cos.ExpandPath(shortPath)
			Expect(path).ToNot(Equal(shortPath))
		})

		It("should expand long path with current home", func() {
			longPath := "~/tmp"
			path := cos.ExpandPath(longPath)
			Expect(path).ToNot(Equal(longPath))
		})

		It("should not expand path when prefixed with more than one tilde", func() {
			shortPath := "~~.tmp"
			path := cos.ExpandPath(shortPath)
			Expect(path).To(Equal(shortPath))
		})

		It("should expand empty path to current directory (dot)", func() {
			emptyPath := ""
			path := cos.ExpandPath(emptyPath)
			Expect(path).To(Equal("."))
		})
	})
})
