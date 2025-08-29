// Package fs: internal unit test for fs package
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"os"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/tools/tassert"
)

// helper: create a temp file with mount-like contents and rewind
func mkProcMounts(t *testing.T, content string) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "proc-mounts-*")
	tassert.Errorf(t, err == nil, "CreateTemp: %v", err)
	_, err = f.WriteString(content)
	tassert.Errorf(t, err == nil, "write: %v", err)
	_, err = f.Seek(0, 0)
	tassert.Errorf(t, err == nil, "seek: %v", err)
	return f
}

func TestResolve_DeepestAncestor(t *testing.T) {
	// f[0]=device, f[1]=mountpoint, f[2]=fstype
	const mounts = `
/dev/sda1 / ext4 rw,relatime 0 0
/dev/sda2 /home ext4 rw,relatime 0 0
/dev/sdb1 /home/mac xfs rw,relatime 0 0
/dev/loop0 /snap/core/123 squashfs ro 0 0
`
	f := mkProcMounts(t, strings.TrimSpace(mounts)+"\n")
	defer f.Close()

	dev, typ, err := _resolve("/home/mac/project", f)
	tassert.Errorf(t, err == nil, "unexpected err: %v", err)
	tassert.Errorf(t, dev == "/dev/sdb1" && typ == "xfs",
		"got (%q,%q), want (%q,%q)", dev, typ, "/dev/sdb1", "xfs")
}

func TestResolve_ExactMatch(t *testing.T) {
	const mounts = `
/dev/sda1 / ext4 rw 0 0
/dev/nvme0n1p1 /mnt/data xfs rw 0 0
`
	f := mkProcMounts(t, strings.TrimSpace(mounts)+"\n")
	defer f.Close()

	dev, typ, err := _resolve("/mnt/data", f)
	tassert.Errorf(t, err == nil, "unexpected err: %v", err)
	tassert.Errorf(t, dev == "/dev/nvme0n1p1" && typ == "xfs",
		"got (%q,%q), want (%q,%q)", dev, typ, "/dev/nvme0n1p1", "xfs")
}

func TestResolve_SkipsNonAncestorsAndBadLines(t *testing.T) {
	const mounts = `
badline
/dev/sda1 / ext4 rw 0 0
/dev/sdc1 /var xfs rw 0 0
/dev/sdd1 /opt/data ext4 rw 0 0
`
	f := mkProcMounts(t, strings.TrimSpace(mounts)+"\n")
	defer f.Close()

	dev, typ, err := _resolve("/home/mac", f)
	tassert.Errorf(t, err == nil, "unexpected err: %v", err)
	// neither /var nor /opt/data are ancestors of /home/mac, so it should fall back to "/"
	tassert.Errorf(t, dev == "/dev/sda1" && typ == "ext4",
		"got (%q,%q), want (%q,%q)", dev, typ, "/dev/sda1", "ext4")
}

func TestResolve_NoMountFound(t *testing.T) {
	// Intentionally omit "/" so nothing covers /var/lib/foo
	const mounts = `
/dev/sdb1 /mnt/data xfs rw 0 0
/dev/loop0 /snap/core/1 squashfs ro 0 0
`
	f := mkProcMounts(t, strings.TrimSpace(mounts)+"\n")
	defer f.Close()

	_, _, err := _resolve("/var/lib/foo", f)
	tassert.Errorf(t, err != nil, "expected error, got nil")
}
