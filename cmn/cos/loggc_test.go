// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestGCLogs(t *testing.T) {
	const maxTotal = 2 * cos.MiB

	dir := t.TempDir()
	base := time.Now().Add(-time.Hour)
	names := []string{"ais.host.INFO.1", "ais.host.INFO.2", "ais.host.INFO.3", "ais.host.INFO.4", "ais.host.INFO.5"}
	for i, name := range names {
		fqn := filepath.Join(dir, name)
		tassert.CheckFatal(t, os.WriteFile(fqn, make([]byte, cos.MiB), 0o644))
		mtime := base.Add(time.Duration(i) * time.Minute) // last is the newest (current) log
		tassert.CheckFatal(t, os.Chtimes(fqn, mtime, mtime))
	}

	cos.GCLogs(dir, maxTotal, true /*verbose*/)

	// Removal runs in a goroutine (wait for the total to settle under the cap)
	var total int64
	for range 20 {
		if total = dirSize(t, dir); total <= maxTotal {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	tassert.Errorf(t, total <= maxTotal, "total %d exceeds max %d", total, maxTotal)

	_, err := os.Stat(filepath.Join(dir, "ais.host.INFO.5"))
	tassert.Errorf(t, err == nil, "newest log must be retained: %v", err)
	_, err = os.Stat(filepath.Join(dir, "ais.host.INFO.1"))
	tassert.Errorf(t, os.IsNotExist(err), "oldest log must be removed")
}

func TestGCLogsKeepsSingleLargeLog(t *testing.T) {
	dir := t.TempDir()
	fqn := filepath.Join(dir, "ais.host.INFO.1")
	tassert.CheckFatal(t, os.WriteFile(fqn, make([]byte, 4*cos.MiB), 0o644))

	cos.GCLogs(dir, 2*cos.MiB, true /*verbose*/)

	_, err := os.Stat(fqn)
	tassert.Errorf(t, err == nil, "the only (current) log must not be removed: %v", err)
	tassert.Errorf(t, dirSize(t, dir) == 4*cos.MiB, "size must be unchanged")
}

func dirSize(t *testing.T, dir string) (total int64) {
	dents, err := os.ReadDir(dir)
	tassert.CheckFatal(t, err)
	for _, d := range dents {
		fi, err := d.Info()
		if os.IsNotExist(err) {
			continue // removed concurrently by GCLogs
		}
		tassert.CheckFatal(t, err)
		total += fi.Size()
	}
	return total
}
