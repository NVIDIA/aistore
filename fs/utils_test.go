// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	tmpDir = "/tmp/cmn-tests"
)

func TestMvFile(t *testing.T) {
	defer os.RemoveAll(tmpDir)

	// Should error when src does not exist
	if err := MvFile("/some/non/existing/file.txt", "/tmp/file.txt"); !os.IsNotExist(err) {
		t.Error("MvFile should fail when src file does not exist")
	}

	nonExistingFile := filepath.Join(tmpDir, "file.txt")
	nonExistingMovedFile := filepath.Join(tmpDir, "some/path/fi.txt")

	// Should not error when dst file does not exist
	{
		file, _ := cmn.CreateFile(nonExistingFile)
		file.Close()
		if err := MvFile(nonExistingFile, nonExistingMovedFile); err != nil {
			t.Error(err)
		}

		ensureFileExists(t, nonExistingMovedFile)
		ensureFileNotExists(t, nonExistingFile)
	}

	// Should not error when dst file already exists
	{
		file, _ := cmn.CreateFile(nonExistingFile)
		file.Close()
		if err := MvFile(nonExistingFile, nonExistingMovedFile); err != nil {
			t.Error(err)
		}

		ensureFileExists(t, nonExistingMovedFile)
		ensureFileNotExists(t, nonExistingFile)
	}

	// File sizes should match
	{
		file, _ := cmn.CreateFile(nonExistingFile)
		fileSize := int64(51*cmn.MiB + 31*cmn.KiB + 101)
		buf := make([]byte, fileSize)
		rand.Read(buf)
		file.Write(buf)
		file.Close()

		if err := MvFile(nonExistingFile, nonExistingMovedFile); err != nil {
			t.Error(err)
		}

		ensureFileSize(t, nonExistingMovedFile, fileSize)
		ensureFileExists(t, nonExistingMovedFile)
		ensureFileNotExists(t, nonExistingFile)
	}

	// LOM, BMD xattrs should be copied as well
	{
		lomMD := []byte{0, 1, 3}

		file, _ := cmn.CreateFile(nonExistingFile)
		file.Close()
		if err := SetXattr(nonExistingFile, cmn.XattrLOM, lomMD); err != nil {
			t.Fatal(err)
		}

		if err := MvFile(nonExistingFile, nonExistingMovedFile); err != nil {
			t.Error(err)
		}

		ensureFileExists(t, nonExistingMovedFile)
		ensureFileNotExists(t, nonExistingFile)

		value, err := GetXattr(nonExistingMovedFile, cmn.XattrLOM)
		if err != nil {
			t.Error(err)
		}

		if !reflect.DeepEqual(lomMD, value) {
			t.Errorf("xattr does not match, expected: %v; got: %v", lomMD, value)
		}
	}
}

func ensureFileSize(t *testing.T, path string, fileSize int64) {
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if fi.Size() != fileSize {
		t.Errorf("size of the file does not match")
	}
}

func ensureFileExists(t *testing.T, path string) {
	if fi, err := os.Stat(path); err != nil {
		t.Error(err)
	} else if fi.IsDir() {
		t.Errorf("expected path %q to not be directory", path)
	}
}

func ensureFileNotExists(t *testing.T, path string) {
	if _, err := os.Stat(path); err != nil && !os.IsNotExist(err) {
		t.Error(err)
	}
}
