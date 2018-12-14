// Package cmn provides common low-level types and utilities for all dfcpub projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"os"
	"path/filepath"
	"testing"
)

const (
	tmpDir = "/tmp/cmn-tests"
)

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
