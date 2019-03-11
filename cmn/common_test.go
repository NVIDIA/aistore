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

	if err := SaveReader(filename, rand.Reader, byteBuffer, bytesToRead); err != nil {
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

	if err := SaveReader(filename, reader, byteBuffer); err != nil {
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
	SaveReader(srcFilename, rand.Reader, make([]byte, 1000), 1000)
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
			func(template, expectedPrefix, expectedSuffix string, expectedStart, expectedEnd, expectedStep, expectedDigitCount int) {
				prefix, suffix, start, end, step, digitCount, err := ParseBashTemplate(template)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(prefix).To(Equal(expectedPrefix))
				Expect(suffix).To(Equal(expectedSuffix))
				Expect(start).To(Equal(expectedStart))
				Expect(end).To(Equal(expectedEnd))
				Expect(step).To(Equal(expectedStep))
				Expect(digitCount).To(Equal(expectedDigitCount))
			},
			Entry("with step", "prefix-{0010..0111..2}-suffix", "prefix-", "-suffix", 10, 111, 2, 4),
			Entry("without step and suffix", "prefix-{0010..0111}", "prefix-", "", 10, 111, 1, 4),
			Entry("minimal", "{1..2}", "", "", 1, 2, 1, 1),
			Entry("minimal multiple digits", "{110..220..10}", "", "", 110, 220, 10, 3),
			Entry("minimal with digit count", "{1..02}", "", "", 1, 2, 1, 1),
			Entry("minimal with special suffix", "{1..02}}", "", "}", 1, 2, 1, 1),
		)

		DescribeTable("parse bash template with error",
			func(template string) {
				_, _, _, _, _, _, err := ParseBashTemplate(template)
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
		)
	})

	Context("ParseAtTemplate", func() {
		DescribeTable("parse at template without error",
			func(template, expectedPrefix, expectedSuffix string, expectedStart, expectedEnd, expectedDigitCount int) {
				prefix, suffix, start, end, step, digitCount, err := ParseAtTemplate(template)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(prefix).To(Equal(expectedPrefix))
				Expect(suffix).To(Equal(expectedSuffix))
				Expect(start).To(Equal(expectedStart))
				Expect(end).To(Equal(expectedEnd))
				Expect(step).To(Equal(1))
				Expect(digitCount).To(Equal(expectedDigitCount))
			},
			Entry("full featured template", "prefix-@010-suffix", "prefix-", "-suffix", 0, 10, 3),
			Entry("minimal with prefix", "pref@9", "pref", "", 0, 9, 1),
			Entry("minimal", "@0010", "", "", 0, 10, 4),
		)

		DescribeTable("parse at template with error",
			func(template string) {
				_, _, _, _, _, _, err := ParseAtTemplate(template)
				Expect(err).Should(HaveOccurred())
			},
			Entry("missing @", "prefix-01-suffix"),
			Entry("negative end", "prefix-@-0001-suffix"),
			Entry("additional @", "prefix-@@0010-suffix"),
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
