// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ios_test

import (
	"os"
	"path"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/ios"
)

func TestGetFSUsedPercentage(t *testing.T) {
	percentage, ok := ios.GetFSUsedPercentage("/")
	if !ok {
		t.Error("Unable to retrieve FS used percentage!")
	}
	if percentage > 100 {
		t.Errorf("Invalid FS used percentage [%d].", percentage)
	}
}

func TestDirSize(t *testing.T) {
	name, err := os.MkdirTemp("/tmp", t.Name())
	if err != nil {
		t.Error(err)
		return
	}
	defer os.RemoveAll(name)

	size := mkFile(t, name, "file.txt")

	totalSize, err := ios.DirSizeOnDisk(name, false /*withNonDirPrefix*/)
	if err != nil {
		t.Error(err)
	}
	if totalSize < uint64(size) {
		t.Fatalf("Dir size %d < %d file", totalSize, size)
	}
}

func mkFile(t *testing.T, dir, fname string) (written int) {
	k := mono.NanoTime() & 0xff
	f, err := os.Create(path.Join(dir, fname))
	if err != nil {
		t.Error(err)
		return
	}
	size := cos.KiB * int(k)
	written, err = f.Write(make([]byte, size))
	f.Close()
	if err != nil {
		t.Error(err)
	}
	if written != size {
		t.Fatalf("written %d != %d", size, written)
	}
	return
}
