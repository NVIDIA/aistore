// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"crypto/rand"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"

	"github.com/NVIDIA/aistore/cmn"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Common file", func() {
	const (
		tmpDir = "/tmp/cmn-tests"
	)

	var (
		nonExistingFile        = filepath.Join(tmpDir, "file.txt")
		nonExistingRenamedFile = filepath.Join(tmpDir, "some/path/fi.txt")

		nonExistingPath = filepath.Join(tmpDir, "non/existing/directory")
	)

	createFile := func(fqn string) {
		file, err := cmn.CreateFile(fqn)
		Expect(err).NotTo(HaveOccurred())
		Expect(file.Close()).NotTo(HaveOccurred())
		Expect(fqn).To(BeARegularFile())
	}

	validateSaveReaderOutput := func(fqn string, sourceData []byte) {
		Expect(fqn).To(BeARegularFile())

		data, err := ioutil.ReadFile(fqn)
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
			cmn.CopyStruct(&emptySructResult, &struct{}{})

			Expect(reflect.DeepEqual(struct{}{}, emptySructResult)).To(BeTrue())
		})

		It("should correctly copy self-referencing struct", func() {
			loopNode := Tree{}
			loopNode.left, loopNode.right, loopNode.value = &loopNode, &loopNode, 0
			var copyLoopNode Tree
			cmn.CopyStruct(&copyLoopNode, &loopNode)

			Expect(loopNode).To(Equal(copyLoopNode))

			loopNode.value += 100

			Expect(loopNode).NotTo(Equal(copyLoopNode))
		})

		It("should correctly copy nested structs, perisiting references", func() {
			left := Tree{nil, nil, 0}
			right := Tree{nil, nil, 1}
			root := Tree{&left, &right, 2}
			var rootCopy Tree
			cmn.CopyStruct(&rootCopy, &root)

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
			cmn.CopyStruct(&nonPrimitiveCopy, &nonPrimitive)

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

			_, err := cmn.SaveReader(nonExistingFile, rand.Reader, byteBuffer, false, bytesToRead, "")
			Expect(err).NotTo(HaveOccurred())

			validateSaveReaderOutput(nonExistingFile, byteBuffer)
		})

		It("should save the reader without specified size", func() {
			const bytesLimit = 500
			byteBuffer := make([]byte, bytesLimit*2)
			reader := &io.LimitedReader{R: rand.Reader, N: bytesLimit}

			_, err := cmn.SaveReader(nonExistingFile, reader, byteBuffer, false, -1, "")
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
			err := cmn.CreateDir(nonExistingPath)
			Expect(err).NotTo(HaveOccurred())

			Expect(nonExistingPath).To(BeADirectory())
		})

		It("should not error when creating directory which already exists", func() {
			err := cmn.CreateDir(nonExistingPath)
			Expect(err).NotTo(HaveOccurred())
			err = cmn.CreateDir(nonExistingPath)
			Expect(err).NotTo(HaveOccurred())

			Expect(nonExistingPath).To(BeADirectory())
		})

		It("should error when directory is not valid", func() {
			err := cmn.CreateDir("")
			Expect(err).To(HaveOccurred())
		})
	})

	Context("CopyFile", func() {
		var (
			srcFilename = filepath.Join(tmpDir, "copyfilesrc.txt")
			dstFilename = filepath.Join(tmpDir, "copyfiledst.txt")
		)

		It("should copy file and preserve the content", func() {
			_, err := cmn.SaveReader(srcFilename, rand.Reader, make([]byte, 1000), false, 1000, "")
			Expect(err).NotTo(HaveOccurred())
			_, _, err = cmn.CopyFile(srcFilename, dstFilename, make([]byte, 1000), false)
			Expect(err).NotTo(HaveOccurred())

			srcData, err := ioutil.ReadFile(srcFilename)
			Expect(err).NotTo(HaveOccurred())

			dstData, err := ioutil.ReadFile(dstFilename)
			Expect(err).NotTo(HaveOccurred())

			Expect(srcData).To(Equal(dstData))
		})

		It("should copy a object and compute its checksum", func() {
			expectedCksum, err := cmn.SaveReader(srcFilename, rand.Reader, make([]byte, 1000), true, 1000, "")
			Expect(err).NotTo(HaveOccurred())

			_, cksum, err := cmn.CopyFile(srcFilename, dstFilename, make([]byte, 1000), true)
			Expect(err).NotTo(HaveOccurred())
			Expect(cksum).To(Equal(expectedCksum))

			srcData, err := ioutil.ReadFile(srcFilename)
			Expect(err).NotTo(HaveOccurred())

			dstData, err := ioutil.ReadFile(dstFilename)
			Expect(err).NotTo(HaveOccurred())

			Expect(srcData).To(Equal(dstData))
		})
	})

	Context("Rename", func() {
		It("should not error when dst file does not exist", func() {
			createFile(nonExistingFile)

			err := cmn.Rename(nonExistingFile, nonExistingRenamedFile)
			Expect(err).NotTo(HaveOccurred())

			Expect(nonExistingRenamedFile).To(BeARegularFile())
			Expect(nonExistingFile).NotTo(BeAnExistingFile())
		})

		It("should not error when dst file already exists", func() {
			createFile(nonExistingFile)
			createFile(nonExistingRenamedFile)

			err := cmn.Rename(nonExistingFile, nonExistingRenamedFile)
			Expect(err).NotTo(HaveOccurred())

			Expect(nonExistingRenamedFile).To(BeARegularFile())
			Expect(nonExistingFile).NotTo(BeAnExistingFile())
		})

		It("should error when src does not exist", func() {
			err := cmn.Rename("/some/non/existing/file.txt", "/tmp/file.txt")
			Expect(err).To(HaveOccurred())
		})
	})

	Context("RemoveFile", func() {
		AfterEach(func() {
			os.RemoveAll(tmpDir)
		})

		It("should remove regular file", func() {
			createFile(nonExistingFile)

			err := cmn.RemoveFile(nonExistingFile)
			Expect(err).NotTo(HaveOccurred())
			Expect(nonExistingFile).NotTo(BeAnExistingFile())
		})

		It("should not complain when regular file does not exist", func() {
			err := cmn.RemoveFile("/some/non/existing/file.txt")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("ParseBashTemplate", func() {
		DescribeTable("parse bash template without error",
			func(template string, expectedPt cmn.ParsedTemplate) {
				pt, err := cmn.ParseBashTemplate(template)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(pt).To(Equal(expectedPt))
			},
			Entry("with step", "prefix-{0010..0111..2}-suffix", cmn.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cmn.TemplateRange{{
					Start:      10,
					End:        111,
					Step:       2,
					DigitCount: 4,
					Gap:        "-suffix",
				}},
			}),
			Entry("without step and suffix", "prefix-{0010..0111}", cmn.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cmn.TemplateRange{{
					Start:      10,
					End:        111,
					Step:       1,
					DigitCount: 4,
					Gap:        "",
				}},
			}),
			Entry("minimal", "{1..2}", cmn.ParsedTemplate{
				Prefix: "",
				Ranges: []cmn.TemplateRange{{
					Start:      1,
					End:        2,
					Step:       1,
					DigitCount: 1,
					Gap:        "",
				}},
			}),
			Entry("minimal multiple digits", "{110..220..10}", cmn.ParsedTemplate{
				Prefix: "",
				Ranges: []cmn.TemplateRange{{
					Start:      110,
					End:        220,
					Step:       10,
					DigitCount: 3,
					Gap:        "",
				}},
			}),
			Entry("minimal with digit count", "{1..02}", cmn.ParsedTemplate{
				Prefix: "",
				Ranges: []cmn.TemplateRange{{
					Start:      1,
					End:        2,
					Step:       1,
					DigitCount: 1,
					Gap:        "",
				}},
			}),
			Entry("minimal with special suffix", "{1..02}}", cmn.ParsedTemplate{
				Prefix: "",
				Ranges: []cmn.TemplateRange{{
					Start:      1,
					End:        2,
					Step:       1,
					DigitCount: 1,
					Gap:        "}",
				}},
			}),
			Entry("multi-range", "prefix-{0010..0111..2}-gap-{10..12}-gap2-{0040..0099..4}-suffix", cmn.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cmn.TemplateRange{
					{
						Start:      10,
						End:        111,
						Step:       2,
						DigitCount: 4,
						Gap:        "-gap-",
					},
					{
						Start:      10,
						End:        12,
						Step:       1,
						DigitCount: 2,
						Gap:        "-gap2-",
					},
					{
						Start:      40,
						End:        99,
						Step:       4,
						DigitCount: 4,
						Gap:        "-suffix",
					},
				},
			}),
		)

		DescribeTable("parse bash template with error",
			func(template string) {
				_, err := cmn.ParseBashTemplate(template)
				Expect(err).Should(HaveOccurred())
			},
			Entry("missing {", "prefix-0010..0111..2}-suffix"),
			Entry("missing }", "prefix-{001..009-suffix"),
			Entry("start after end", "prefix-{002..001}-suffix"),
			Entry("negative start", "prefix-{-001..009}-suffix"),
			Entry("negative step", "prefix-{001..009..-1}-suffix"),
			Entry("invalid step", "prefix-{0010..0111..2s}-suffix"),
			Entry("invalid end", "prefix-{0010..0111s..2}-suffix"),
			Entry("invalid start", "prefix-{0010s..0111..2}-suffix"),
			Entry("too many arms", "prefix-{00..10..0111..2}-suffix"),
			Entry("missing end", "prefix-{010..}-suffix"),
			Entry("missing start", "prefix-{..009}-suffix"),
			Entry("missing start and end", "prefix-{..}-suffix"),
			Entry("empty template with prefix and suffix", "prefix-{}-suffix"),
			Entry("empty template", "{}"),
			Entry("nested templates", "{{}}"),
			Entry("nested templates with numbers", "{{1..2}}"),
			Entry("interleaving templates", "{1..2{3..4}}"),
		)
	})

	Context("ParseAtTemplate", func() {
		DescribeTable("parse at template without error",
			func(template string, expectedPt cmn.ParsedTemplate) {
				pt, err := cmn.ParseAtTemplate(template)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(pt).To(Equal(expectedPt))
			},
			Entry("full featured template", "prefix-@010-suffix", cmn.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cmn.TemplateRange{{
					Start:      0,
					End:        10,
					Step:       1,
					DigitCount: 3,
					Gap:        "-suffix",
				}},
			}),
			Entry("minimal with prefix", "pref@9", cmn.ParsedTemplate{
				Prefix: "pref",
				Ranges: []cmn.TemplateRange{{
					Start:      0,
					End:        9,
					Step:       1,
					DigitCount: 1,
					Gap:        "",
				}},
			}),
			Entry("minimal", "@0010", cmn.ParsedTemplate{
				Prefix: "",
				Ranges: []cmn.TemplateRange{{
					Start:      0,
					End:        10,
					Step:       1,
					DigitCount: 4,
					Gap:        "",
				}},
			}),
			Entry("multi-range", "pref@9-gap-@0100-suffix", cmn.ParsedTemplate{
				Prefix: "pref",
				Ranges: []cmn.TemplateRange{
					{
						Start:      0,
						End:        9,
						Step:       1,
						DigitCount: 1,
						Gap:        "-gap-",
					},
					{
						Start:      0,
						End:        100,
						Step:       1,
						DigitCount: 4,
						Gap:        "-suffix",
					},
				},
			}),
		)

		DescribeTable("parse at template with error",
			func(template string) {
				_, err := cmn.ParseAtTemplate(template)
				Expect(err).Should(HaveOccurred())
			},
			Entry("missing @", "prefix-01-suffix"),
			Entry("negative end", "prefix-@-0001-suffix"),
			Entry("repetitive @", "prefix-@@0010-suffix"),
			Entry("non-digit range", "prefix-@d@0010-suffix"),
		)
	})

	Context("ParsedTemplate", func() {
		DescribeTable("iter method",
			func(template string, expectedStrs ...string) {
				pt, err := cmn.ParseBashTemplate(template)
				Expect(err).NotTo(HaveOccurred())

				var (
					i  int
					it = pt.Iter()
				)
				for str, hasNext := it(); hasNext; str, hasNext = it() {
					Expect(str).To(Equal(expectedStrs[i]))
					i++
				}
				Expect(i).To(Equal(len(expectedStrs)))
				Expect(i).To(Equal(int(pt.Count())))
			},
			Entry(
				"simple template", "prefix-{0010..0013..2}-suffix",
				"prefix-0010-suffix", "prefix-0012-suffix",
			),
			Entry(
				"multi-range template", "prefix-{0010..0013..2}-gap-{1..2}-suffix",
				"prefix-0010-gap-1-suffix", "prefix-0010-gap-2-suffix", "prefix-0012-gap-1-suffix", "prefix-0012-gap-2-suffix",
			),
			Entry(
				"large step", "prefix-{0010..0013..2}-gap-{1..2..3}-suffix",
				"prefix-0010-gap-1-suffix", "prefix-0012-gap-1-suffix",
			),
		)
	})

	Context("ParseQuantity", func() {
		DescribeTable("parse quantity without error",
			func(quantity, ty string, value int) {
				pq, err := cmn.ParseQuantity(quantity)
				Expect(err).NotTo(HaveOccurred())

				Expect(pq).To(Equal(cmn.ParsedQuantity{Type: ty, Value: uint64(value)}))
			},
			Entry("simple number", "80B", cmn.QuantityBytes, 80),
			Entry("simple percent", "80%", cmn.QuantityPercent, 80),
			Entry("number with spaces", "  8 0 KB  ", cmn.QuantityBytes, 80*cmn.KiB),
			Entry("percent with spaces", "80 %", cmn.QuantityPercent, 80),
		)

		DescribeTable("parse quantity with error",
			func(template string) {
				_, err := cmn.ParseQuantity(template)
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
			trues := []string{"1", "ON", "yes", "Y", "trUe"}
			falses := []string{"0", "off", "No", "n", "falsE", ""}
			errs := []string{"2", "enable", "nothing"}

			for _, s := range trues {
				v, err := cmn.ParseBool(s)
				Expect(err).NotTo(HaveOccurred())
				Expect(v).To(BeTrue())
			}

			for _, s := range falses {
				v, err := cmn.ParseBool(s)
				Expect(err).NotTo(HaveOccurred())
				Expect(v).To(BeFalse())
			}

			for _, s := range errs {
				_, err := cmn.ParseBool(s)
				Expect(err).To(HaveOccurred())
			}
		})
	})

	Context("StrSlicesEqual", func() {
		DescribeTable("parse quantity with error",
			func(lhs, rhs []string, expected bool) {
				Expect(cmn.StrSlicesEqual(lhs, rhs)).To(Equal(expected))
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
			path := cmn.ExpandPath(shortPath)
			Expect(path).ToNot(Equal(shortPath))
		})

		It("should expand long path with current home", func() {
			longPath := "~/tmp"
			path := cmn.ExpandPath(longPath)
			Expect(path).ToNot(Equal(longPath))
		})

		It("should not expand path when prefixed with more than one tilde", func() {
			shortPath := "~~.tmp"
			path := cmn.ExpandPath(shortPath)
			Expect(path).To(Equal(shortPath))
		})

		It("should expand empty path to current directory (dot)", func() {
			emptyPath := ""
			path := cmn.ExpandPath(emptyPath)
			Expect(path).To(Equal("."))
		})
	})
})
