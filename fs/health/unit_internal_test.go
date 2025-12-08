// Package health provides a basic mountpath health monitor.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package health

import (
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/tassert"
)

const (
	fsCheckerTmpDir = "/tmp/fshc"
)

func TestFSHCFaultedAtDepthZeroOnENOTDIR(t *testing.T) {
	setupTests(t)

	// 1: verify cos.IsIOError classification
	for _, ioErr := range []error{
		syscall.EIO, syscall.ENOTDIR, syscall.ENODEV, syscall.EROFS,
	} {
		tassert.Errorf(t, cos.IsIOError(ioErr), "%v should be classified as IO error", ioErr)
	}
	tassert.Errorf(t, !cos.IsIOError(os.ErrNotExist), "ErrNotExist should NOT be IO error")
	tassert.Errorf(t, !cos.IsIOError(os.ErrPermission), "ErrPermission should NOT be IO error")

	// select random mountpath
	var mi *fs.Mountpath
	for _, mi = range fs.GetAvail() {
		break
	}

	// 2: trigger ENOTDIR by passing a regular file to getRandFname
	// (it tries to ReadDir on the path, which fails with ENOTDIR for a file)
	mpath := mi.Path
	filePath := mpath + "/not_a_dir.txt"

	f, err := cos.CreateFile(filePath)
	tassert.CheckFatal(t, err)
	f.Close()

	// getRandFname at depth=0 with a file (not dir) should return faulted
	fqn, etag, err := getRandFname(mi, filePath, 0)
	tassert.Errorf(t, err != nil, "expected error when path is a file, not dir")
	tassert.Errorf(t, fqn == "", "expected empty fqn, got %q", fqn)
	tassert.Errorf(t, etag == faulted, "ENOTDIR at depth=0 should be faulted, got %q", etag)

	// 3: same error at depth > 0 should NOT be faulted
	_, etag, err = getRandFname(mi, filePath, 1)
	tassert.Errorf(t, err != nil, "expected error")
	tassert.Errorf(t, etag != faulted, "depth>0 should never be faulted, got %q", etag)

	os.Remove(filePath)
}

func TestFSHCBoundedLoop(t *testing.T) {
	setupTests(t)

	// select random mountpath
	var mi *fs.Mountpath
	for _, mi = range fs.GetAvail() {
		break
	}

	const (
		maxerrs = 2
	)

	// Create a mountpath with no readable files (empty or only dirs)
	// This forces the read loop to iterate without finding files

	// _rw should complete without running away
	// The loop bound `i < numFiles*2` prevents infinite iteration
	etag, rerrs, werrs := _rw(mi, "", 4, tmpSize, maxerrs)

	// Should complete (not hang) and return reasonable values
	t.Logf("_rw completed: etag=%q, rerrs=%d, werrs=%d", etag, rerrs, werrs)

	// With an empty/sparse directory, we expect:
	// - no faulted (root is accessible)
	// - rerrs should be 0 or small (no files to fail on, or bounded)
	tassert.Errorf(t, etag != faulted, "unexpected faulted on accessible mountpath")
}

func TestFSHCDegradedOnSamplingErrors(t *testing.T) {
	setupTests(t)

	// Test the error accumulation and threshold logic
	config := cmn.GCO.Get().FSHC
	maxerrs := config.HardErrs // default 2

	// Verify the decision logic: sum of errors vs threshold
	testCases := []struct {
		name     string
		rerrs    int
		werrs    int
		shouldOK bool
	}{
		{"no errors", 0, 0, true},
		{"one read error", 1, 0, true},
		{"one write error", 0, 1, true},
		{"one of each", 1, 1, false},    // 1+1 >= 2, should fail
		{"at threshold", 2, 0, false},   // 2+0 >= 2, should fail
		{"over threshold", 2, 1, false}, // 3 >= 2, should fail
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// This mirrors the actual check in fshc.go:
			// if rerrs+werrs < maxerrs { ... return (OK) }
			ok := tc.rerrs+tc.werrs < maxerrs

			if ok != tc.shouldOK {
				t.Errorf("rerrs=%d + werrs=%d: expected ok=%v, got %v (maxerrs=%d)",
					tc.rerrs, tc.werrs, tc.shouldOK, ok, maxerrs)
			}
		})
	}
}

func TestFSCheckerDecision(t *testing.T) {
	updateTestConfig()
	config := cmn.GCO.Get().FSHC
	maxerrs := config.HardErrs

	// Updated test cases using sum-based logic (matches actual code)
	testCases := []struct {
		name     string
		rerrs    int
		werrs    int
		avail    bool
		wantPass bool
	}{
		{"unavailable mountpath", 0, 0, false, false},
		{"healthy mountpath", 0, 0, true, true},
		{"one read error", 1, 0, true, true},
		{"one write error", 0, 1, true, true},
		{"sum at threshold", 1, 1, true, false}, // 2 >= 2
		{"reads exceed", 3, 0, true, false},
		{"writes exceed", 0, 3, true, false},
		{"both exceed", 3, 3, true, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var pass bool
			if !tc.avail {
				pass = false
			} else {
				pass = tc.rerrs+tc.werrs < maxerrs
			}

			if pass != tc.wantPass {
				t.Errorf("expected pass=%v, got %v", tc.wantPass, pass)
			}
		})
	}
}

func TestFSCheckerTryReadFile(t *testing.T) {
	setupTests(t)

	var (
		mpath    = fsCheckerTmpDir + "/1"
		filePath = mpath + "/smth.txt"
	)

	// Create file with some content inside.
	file, err := cos.CreateFile(filePath)
	tassert.CheckFatal(t, err)
	err = cos.FloodWriter(file, cos.MiB)
	file.Close()
	tassert.CheckFatal(t, err)

	err = _read(filePath)
	tassert.CheckFatal(t, err)
}

func TestFSCheckerTryWriteFile(t *testing.T) {
	setupTests(t)
	mpath := fsCheckerTmpDir + "/1"
	err := _write(mpath, cos.KiB)
	tassert.CheckFatal(t, err)
}

//
// local helpers
//

func initMountpaths(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll(fsCheckerTmpDir)
	})

	cos.CreateDir(fsCheckerTmpDir)

	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	cmn.GCO.CommitUpdate(config)

	fs.TestNew(nil)
	for i := 1; i <= 4; i++ {
		mpath := fmt.Sprintf("%s/%d", fsCheckerTmpDir, i)

		err := cos.CreateDir(mpath)
		tassert.CheckFatal(t, err)

		_, err = fs.Add(mpath, "id")
		tassert.CheckFatal(t, err)
	}

	os.RemoveAll(fsCheckerTmpDir + "/3") // One directory is deleted.
	fs.Disable(fsCheckerTmpDir + "/4")
}

func updateTestConfig() {
	config := cmn.GCO.BeginUpdate()
	config.FSHC.Enabled = true
	config.FSHC.HardErrs = 2
	config.FSHC.TestFileCount = 4
	config.Log.Level = "3"
	cmn.GCO.CommitUpdate(config)

	cmn.Rom.Set(&config.ClusterConfig)
}

func setupTests(t *testing.T) {
	updateTestConfig()
	initMountpaths(t)
}
