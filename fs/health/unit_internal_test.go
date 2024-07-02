// Package health provides a basic mountpath health monitor.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package health

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/tassert"
)

const (
	fsCheckerTmpDir = "/tmp/fshc"
)

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
	config.FSHC.ErrorLimit = 2
	config.FSHC.TestFileCount = 4
	config.Log.Level = "3"
	cmn.GCO.CommitUpdate(config)
}

func setupTests(t *testing.T) {
	updateTestConfig()
	initMountpaths(t)
}

func TestFSCheckerDecisionFn(t *testing.T) {
	updateTestConfig()

	// Decision making function.
	type tstInfo struct {
		title               string
		readErrs, writeErrs int
		avail, result       bool
	}
	testList := []tstInfo{
		{"Inaccessible mountpath", 0, 0, false, false},
		{"Healthy mountpath", 0, 0, true, true},
		{"Unstable but OK mountpath", 1, 1, true, true},
		{"Reads failed", 3, 0, true, false},
		{"Writes failed", 1, 3, true, false},
		{"Reads and writes failed", 3, 3, true, false},
	}

	for _, tst := range testList {
		t.Run(tst.title, func(t *testing.T) {
			res, err := isTestPassed("/tmp", tst.readErrs, tst.writeErrs, tst.avail)
			if res != tst.result {
				t.Errorf("%s failed: expected %v, got %v (%v)", tst.title, tst.result, res, err)
			}
		})
	}
}

func isTestPassed(mpath string, readErrors, writeErrors int, available bool) (passed bool, err error) {
	config := &cmn.GCO.Get().FSHC
	nlog.Infof("Tested mountpath %s(%v), read: %d of %d, write(size=%d): %d of %d",
		mpath, available,
		readErrors, config.ErrorLimit, fshcFileSize,
		writeErrors, config.ErrorLimit)

	if !available {
		return false, errors.New("mountpath is unavailable")
	}

	passed = readErrors < config.ErrorLimit && writeErrors < config.ErrorLimit
	if !passed {
		err = fmt.Errorf("too many errors: %d read error%s, %d write error%s",
			readErrors, cos.Plural(readErrors), writeErrors, cos.Plural(writeErrors))
	}
	return passed, err
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
