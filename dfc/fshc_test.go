/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package dfc

import (
	"fmt"
	"os"
	"testing"

	"github.com/NVIDIA/dfcpub/fs"
)

const (
	fsCheckerTmpDir = "/tmp/fshc"
)

func testTmpFileName(fname string) string {
	return fname + "-tmp"
}

func testCheckerMountPaths() *fs.MountedFS {
	CreateDir(fsCheckerTmpDir)
	CreateDir(fsCheckerTmpDir + "/1")
	CreateDir(fsCheckerTmpDir + "/2")

	avail := make(map[string]*fs.MountpathInfo)
	unavail := make(map[string]*fs.MountpathInfo)

	for i := 1; i < 4; i++ {
		name := fmt.Sprintf("%s/%d", fsCheckerTmpDir, i)
		avail[name] = &fs.MountpathInfo{Path: name}
	}
	unavail[fsCheckerTmpDir+"/4"] = &fs.MountpathInfo{Path: fsCheckerTmpDir + "/4"}

	return &fs.MountedFS{
		Available: avail,
		Disabled:  unavail,
	}
}

func testCheckerConfig() *fshcconf {
	return &fshcconf{
		Enabled:    true,
		ErrorLimit: 2,
	}
}

func testCheckerCleanup() {
	os.RemoveAll(fsCheckerTmpDir)
}

func TestFSCheckerMain(t *testing.T) {
	fshc := newFSHealthChecker(testCheckerMountPaths(), testCheckerConfig(), testTmpFileName)

	if fshc == nil {
		t.Error("Failed to create fshc")
	}

	// intial state = 2 availble FSes - must pass
	if len(fshc.mountpaths.Available) != 3 || len(fshc.mountpaths.Disabled) != 1 {
		t.Errorf("Invalid number of mountpaths at start: %v - %v",
			fshc.mountpaths.Available, fshc.mountpaths.Disabled)
	}

	// inaccessible mountpath
	_, _, exists := fshc.testMountpath(
		fsCheckerTmpDir+"/3/testfile", fsCheckerTmpDir+"/3", 4, 1024)
	if exists {
		t.Error("Testing non-existing mountpath must fail")
	}

	// healthy mountpath
	reads, writes, exists := fshc.testMountpath(
		fsCheckerTmpDir+"/2/testfile", fsCheckerTmpDir+"/2", 4, 1024)
	if !exists {
		t.Error("Testing existing mountpath must detect the mountpath is available")
	}
	if reads != 0 || writes != 0 {
		t.Errorf("Testing existing mountpath must not fail. Read errors: %d, write errors: %d", reads, writes)
	}

	// failed mountpath must be disabled
	failedMpath := fsCheckerTmpDir + "/3"
	fshc.runMpathTest(failedMpath, failedMpath+"/dir/testfile")
	if len(fshc.mountpaths.Available) != 2 || len(fshc.mountpaths.Disabled) != 2 {
		t.Errorf("Failed mountpath %s must be detected: %v - %v",
			failedMpath, fshc.mountpaths.Available, fshc.mountpaths.Disabled)
	}
	if len(fshc.mountpaths.Disabled) == 1 {
		if _, ok := fshc.mountpaths.Disabled[failedMpath]; !ok {
			t.Errorf("Incorrect mountpath was disabled. Failed one: %s, disabled: %v",
				failedMpath, fshc.mountpaths.Disabled)
		}
	}

	// decision making function
	type tstInfo struct {
		title               string
		readErrs, writeErrs int
		avail, result       bool
	}
	testList := []tstInfo{
		{"Inaccessible mountpath", 0, 0, false, false},
		{"Healthy mounpath", 0, 0, true, true},
		{"Unstable but OK mountpath", 1, 1, true, true},
		{"Reads failed", 3, 0, true, false},
		{"Writes failed", 1, 3, true, false},
		{"Reads and writes failed", 3, 3, true, false},
	}

	for _, tst := range testList {
		fmt.Printf("Test: %s.\n", tst.title)
		res := fshc.isTestPassed("/tmp", tst.readErrs, tst.writeErrs, tst.avail)
		if res == tst.result {
			fmt.Printf("    PASSED\n")
		} else {
			fmt.Printf("    FAILED\n")
			t.Errorf("%s failed. %v expected but %v got", tst.title, tst.result, res)
		}
	}

	testCheckerCleanup()
}
