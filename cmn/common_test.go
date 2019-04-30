// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"crypto/rand"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

const (
	tmpDir = "/tmp/cmn-tests"
)

func TestCopyStruct(t *testing.T) {
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

	var emptySructResult struct{}
	CopyStruct(&emptySructResult, &struct{}{})

	if !reflect.DeepEqual(struct{}{}, emptySructResult) {
		t.Error("CopyStruct should correctly copy empty struct")
	}

	loopNode := Tree{}
	loopNode.left, loopNode.right, loopNode.value = &loopNode, &loopNode, 0
	var copyLoopNode Tree
	CopyStruct(&copyLoopNode, &loopNode)

	if !reflect.DeepEqual(loopNode, copyLoopNode) {
		t.Error("CopyStruct should correctly copy self referencing struct")
	}

	loopNode.value += 100

	if reflect.DeepEqual(loopNode, copyLoopNode) {
		t.Error("Changing value of source struct should not affect resulting struct")
	}

	left := Tree{nil, nil, 0}
	right := Tree{nil, nil, 1}
	root := Tree{&left, &right, 2}
	var rootCopy Tree
	CopyStruct(&rootCopy, &root)

	if !reflect.DeepEqual(root, rootCopy) {
		t.Error("CopyStruct should correctly copy nested structs")
	}

	left.value += 100

	if !reflect.DeepEqual(root, rootCopy) {
		t.Error("CopyStruct should perform shallow copy, perisiting references")
	}

	nonPrimitive := NonPrimitiveStruct{}
	nonPrimitive.m = make(map[string]int)
	nonPrimitive.m["one"] = 1
	nonPrimitive.m["two"] = 2
	nonPrimitive.s = []int{1, 2}
	var nonPrimitiveCopy NonPrimitiveStruct
	CopyStruct(&nonPrimitiveCopy, &nonPrimitive)

	if !reflect.DeepEqual(nonPrimitive, nonPrimitiveCopy) {
		t.Error("CopyStruct should correctly copy structs with nonprimitive types")
	}

	nonPrimitive.m["one"] = 0
	nonPrimitive.s[0] = 0

	if !reflect.DeepEqual(nonPrimitive, nonPrimitiveCopy) {
		t.Error("CopyStruct should not make a deep copy of nonprimitive types")
	}
}

func TestSaveReader(t *testing.T) {
	const bytesToRead = 1000
	filename := filepath.Join(tmpDir, "savereadertest.txt")
	byteBuffer := make([]byte, bytesToRead)

	if _, err := SaveReader(filename, rand.Reader, byteBuffer, false, bytesToRead); err != nil {
		t.Errorf("SaveReader failed to read %d bytes", bytesToRead)
	}

	validateSaveReaderOutput(t, filename, byteBuffer)
	os.Remove(filename)
}

func TestSaveReaderWithNoSize(t *testing.T) {
	const bytesLimit = 500
	filename := filepath.Join(tmpDir, "savereadertest.txt")
	byteBuffer := make([]byte, bytesLimit*2)
	reader := &io.LimitedReader{R: rand.Reader, N: bytesLimit}

	if _, err := SaveReader(filename, reader, byteBuffer, false); err != nil {
		t.Errorf("SaveReader failed to read %d bytes", bytesLimit)
	}

	validateSaveReaderOutput(t, filename, byteBuffer[:bytesLimit])
	os.Remove(filename)
}

func validateSaveReaderOutput(t *testing.T, filename string, sourceData []byte) {
	ensurePathExists(t, filename, false)

	dat, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(dat, sourceData) {
		t.Error("SaveReader saved different data than it was fed with")
	}
}

func TestCreateDir(t *testing.T) {
	nonExistingPath := filepath.Join(tmpDir, "non/existing/directory")
	if err := CreateDir(nonExistingPath); err != nil {
		t.Error(err)
	}

	ensurePathExists(t, nonExistingPath, true)

	// Should not error when creating directory which already exists
	if err := CreateDir(nonExistingPath); err != nil {
		t.Error(err)
	}

	ensurePathExists(t, nonExistingPath, true)

	// Should error when directory is not valid
	if err := CreateDir(""); err == nil {
		t.Error("CreateDir should fail when given directory is empty")
	}

	os.RemoveAll(tmpDir)
}

func TestCreateFile(t *testing.T) {
	nonExistingFile := filepath.Join(tmpDir, "file.txt")
	if file, err := CreateFile(nonExistingFile); err != nil {
		t.Error(err)
	} else {
		file.Close()
	}

	ensurePathExists(t, nonExistingFile, false)

	// Should not return error when creating file which already exists
	if file, err := CreateFile(nonExistingFile); err != nil {
		t.Error(err)
	} else {
		file.Close()
	}

	os.RemoveAll(tmpDir)
}

func TestCopyFile(t *testing.T) {
	srcFilename := filepath.Join(tmpDir, "copyfilesrc.txt")
	dstFilename := filepath.Join(tmpDir, "copyfiledst.txt")

	// creates a file of random bytes
	SaveReader(srcFilename, rand.Reader, make([]byte, 1000), false, 1000)
	if err := CopyFile(srcFilename, dstFilename, make([]byte, 1000)); err != nil {
		t.Error(err)
	}

	srcData, err := ioutil.ReadFile(srcFilename)
	if err != nil {
		t.Error(err)
	}

	dstData, err := ioutil.ReadFile(dstFilename)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(srcData, dstData) {
		t.Error("Contents of copied file are different than source file")
	}

	os.Remove(srcFilename)
	os.Remove(dstFilename)
}

func TestMvFile(t *testing.T) {
	// Should error when src does not exist
	if err := MvFile("/some/non/existing/file.txt", "/tmp/file.txt"); !os.IsNotExist(err) {
		t.Error("MvFile should fail when src file does not exist")
	}

	nonExistingFile := filepath.Join(tmpDir, "file.txt")
	nonExistingRenamedFile := filepath.Join(tmpDir, "some/path/fi.txt")

	// Should not error when dst file does not exist
	file, _ := CreateFile(nonExistingFile)
	file.Close()
	if err := MvFile(nonExistingFile, nonExistingRenamedFile); err != nil {
		t.Error(err)
	}

	ensurePathExists(t, nonExistingRenamedFile, false)
	ensurePathNotExists(t, nonExistingFile)

	// Should not error when dst file already exists
	file, _ = CreateFile(nonExistingFile)
	file.Close()
	if err := MvFile(nonExistingFile, nonExistingRenamedFile); err != nil {
		t.Error(err)
	}

	ensurePathExists(t, nonExistingRenamedFile, false)
	ensurePathNotExists(t, nonExistingFile)

	os.RemoveAll(tmpDir)
}

var _ = Describe("Common file", func() {
	Context("ParseBashTemplate", func() {
		DescribeTable("parse bash template without error",
			func(template string, expectedPt ParsedTemplate) {
				pt, err := ParseBashTemplate(template)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(pt).To(Equal(expectedPt))
			},
			Entry("with step", "prefix-{0010..0111..2}-suffix", ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []TemplateRange{{
					Start:      10,
					End:        111,
					Step:       2,
					DigitCount: 4,
					Gap:        "-suffix",
				}},
			}),
			Entry("without step and suffix", "prefix-{0010..0111}", ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []TemplateRange{{
					Start:      10,
					End:        111,
					Step:       1,
					DigitCount: 4,
					Gap:        "",
				}},
			}),
			Entry("minimal", "{1..2}", ParsedTemplate{
				Prefix: "",
				Ranges: []TemplateRange{{
					Start:      1,
					End:        2,
					Step:       1,
					DigitCount: 1,
					Gap:        "",
				}},
			}),
			Entry("minimal multiple digits", "{110..220..10}", ParsedTemplate{
				Prefix: "",
				Ranges: []TemplateRange{{
					Start:      110,
					End:        220,
					Step:       10,
					DigitCount: 3,
					Gap:        "",
				}},
			}),
			Entry("minimal with digit count", "{1..02}", ParsedTemplate{
				Prefix: "",
				Ranges: []TemplateRange{{
					Start:      1,
					End:        2,
					Step:       1,
					DigitCount: 1,
					Gap:        "",
				}},
			}),
			Entry("minimal with special suffix", "{1..02}}", ParsedTemplate{
				Prefix: "",
				Ranges: []TemplateRange{{
					Start:      1,
					End:        2,
					Step:       1,
					DigitCount: 1,
					Gap:        "}",
				}},
			}),
			Entry("multi-range", "prefix-{0010..0111..2}-gap-{10..12}-gap2-{0040..0099..4}-suffix", ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []TemplateRange{
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
				_, err := ParseBashTemplate(template)
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
			func(template string, expectedPt ParsedTemplate) {
				pt, err := ParseAtTemplate(template)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(pt).To(Equal(expectedPt))
			},
			Entry("full featured template", "prefix-@010-suffix", ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []TemplateRange{{
					Start:      0,
					End:        10,
					Step:       1,
					DigitCount: 3,
					Gap:        "-suffix",
				}},
			}),
			Entry("minimal with prefix", "pref@9", ParsedTemplate{
				Prefix: "pref",
				Ranges: []TemplateRange{{
					Start:      0,
					End:        9,
					Step:       1,
					DigitCount: 1,
					Gap:        "",
				}},
			}),
			Entry("minimal", "@0010", ParsedTemplate{
				Prefix: "",
				Ranges: []TemplateRange{{
					Start:      0,
					End:        10,
					Step:       1,
					DigitCount: 4,
					Gap:        "",
				}},
			}),
			Entry("multi-range", "pref@9-gap-@0100-suffix", ParsedTemplate{
				Prefix: "pref",
				Ranges: []TemplateRange{
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
				_, err := ParseAtTemplate(template)
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
				pt, err := ParseBashTemplate(template)
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
				Expect(i).To(Equal(pt.Count()))
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
				pq, err := ParseQuantity(quantity)
				Expect(err).NotTo(HaveOccurred())

				Expect(pq).To(Equal(ParsedQuantity{Type: ty, Value: uint64(value)}))
			},
			Entry("simple number", "80B", QuantityBytes, 80),
			Entry("simple percent", "80%", QuantityPercent, 80),
			Entry("number with spaces", "  8 0 KB  ", QuantityBytes, 80*KiB),
			Entry("percent with spaces", "80 %", QuantityPercent, 80),
		)

		DescribeTable("parse quantity with error",
			func(template string) {
				_, err := ParseQuantity(template)
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
})

func ensurePathExists(t *testing.T, path string, dir bool) {
	if fi, err := os.Stat(path); err != nil {
		t.Error(err)
	} else {
		if dir && !fi.IsDir() {
			t.Errorf("expected path %q to be directory", path)
		} else if !dir && fi.IsDir() {
			t.Errorf("expected path %q to not be directory", path)
		}
	}
}

func ensurePathNotExists(t *testing.T, path string) {
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error(err)
	}
}
