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
