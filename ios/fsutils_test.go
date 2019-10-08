// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"testing"
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
	size, err := GetDirSize("/tmp")
	if err != nil {
		t.Error("Unable to get the directory size")
	}
	if size == 0 {
		t.Fatal("The size of the directory was not determined correctly")
	}
}

func TestGetFilesCount(t *testing.T) {
	fileCount, err := GetFileCount("/tmp")
	if err != nil {
		t.Error("Unable to get the number of files inside the directory")
	}
	if fileCount == 0 {
		t.Fatal("The number of files inside the directory was not determined correctly")
	}
}
