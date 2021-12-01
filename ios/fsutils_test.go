// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"os"
	"path"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
)

func TestGetFSUsedPercentage(t *testing.T) {
	percentage, ok := GetFSUsedPercentage("/")
	if !ok {
		t.Error("Unable to retrieve FS used percentage!")
	}
	if percentage > 100 {
		t.Errorf("Invalid FS used percentage [%d].", percentage)
	}
}

func TestGetDirSize(t *testing.T) {
	name, err := os.MkdirTemp("/tmp", t.Name())
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(name)

	mkFile(t, name)

	totalSize, err := GetDirSize(name)
	if err != nil {
		t.Error(err)
	}
	if totalSize == 0 {
		t.Fatal("Directory size was not determined correctly")
	}
}

func TestGetFilesCount(t *testing.T) {
	name, err := os.MkdirTemp("/tmp", t.Name())
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(name)

	checkFileCount(t, name, 0)

	nameInside, err := os.MkdirTemp(name, "")
	if err != nil {
		t.Error(err)
	}

	checkFileCount(t, name, 0)

	mkFile(t, name)
	checkFileCount(t, name, 1)

	mkFile(t, nameInside)
	checkFileCount(t, name, 2)
}

func mkFile(t *testing.T, dir string) {
	f, err := os.Create(path.Join(dir, "file.txt"))
	if err != nil {
		t.Error(err)
	}
	f.Write(make([]byte, cos.KiB))
	f.Close()
}

func checkFileCount(t *testing.T, dir string, n int) {
	fileCount, err := GetFileCount(dir)
	if err != nil {
		t.Error(err)
	}
	if fileCount != n {
		t.Fatalf("Expected %d files inside the directories", n)
	}
}
