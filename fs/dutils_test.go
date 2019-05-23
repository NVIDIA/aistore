/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"os"
	"testing"

	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/tutils/tassert"

	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

// test file for ios/dutils_linux.go
// placed here because it requires fs to set up the testing environment

func init() {
	Mountpaths = NewMountedFS()
}

func TestLsblk(t *testing.T) {
	var lsblk ios.LsBlk
	rawJSON := jsoniter.RawMessage(
		`{
			"blockdevices": [
			{"name": "sda", "alignment": "0", "min-io": "262144", "opt-io": "262144", "phy-sec": "512", "log-sec": "512", "rota": "1", "sched": "deadline", "rq-size": "4096", "ra": "1024", "wsame": "0B",
			"children": [
			{"name": "sda1", "alignment": "0", "min-io": "262144", "opt-io": "262144", "phy-sec": "512", "log-sec": "512", "rota": "1", "sched": "deadline", "rq-size": "4096", "ra": "1024", "wsame": "0B"},
			{"name": "sda2", "alignment": "0", "min-io": "262144", "opt-io": "262144", "phy-sec": "512", "log-sec": "512", "rota": "1", "sched": "deadline", "rq-size": "4096", "ra": "1024", "wsame": "0B"}
			]
		},
		{"name": "sdb", "alignment": "0", "min-io": "262144", "opt-io": "1048576", "phy-sec": "512", "log-sec": "512", "rota": "1", "sched": "deadline", "rq-size": "4096", "ra": "1024", "wsame": "0B"}]
	}`)
	out, _ := jsoniter.Marshal(&rawJSON)
	jsoniter.Unmarshal(out, &lsblk)
	if len(lsblk.BlockDevices) != 2 {
		t.Fatalf("expected 2 block devices, got %d", len(lsblk.BlockDevices))
	}
	if lsblk.BlockDevices[1].PhySec != "512" {
		t.Fatalf("expected 512 sector, got %s", lsblk.BlockDevices[1].PhySec)
	}
	if lsblk.BlockDevices[0].BlockDevices[1].Name != "sda2" {
		t.Fatalf("expected sda2 device, got %s", lsblk.BlockDevices[0].BlockDevices[1].Name)
	}
	if len(lsblk.BlockDevices[0].BlockDevices) != 2 {
		t.Fatalf("expected 2 block children devices, got %d", len(lsblk.BlockDevices[0].BlockDevices))
	}
}

func TestSearchValidMountPath(t *testing.T) {
	Mountpaths = NewMountedFS()
	oldMPs := setAvailableMountPaths("/tmp")
	mpathInfo, _ := Mountpaths.Path2MpathInfo("/tmp/abc")
	longestPrefix := mpathInfo.Path
	tassert.Errorf(t, longestPrefix == "/tmp", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp")
	setAvailableMountPaths(oldMPs...)
}

func TestSearchInvalidMountPath(t *testing.T) {
	Mountpaths = NewMountedFS()
	oldMPs := setAvailableMountPaths("/tmp")
	mpathInfo, _ := Mountpaths.Path2MpathInfo("xabc")
	tassert.Errorf(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "xabc")
	setAvailableMountPaths(oldMPs...)
}

func TestSearchWithNoMountPath(t *testing.T) {
	Mountpaths = NewMountedFS()
	oldMPs := setAvailableMountPaths("")
	mpathInfo, _ := Mountpaths.Path2MpathInfo("xabc")
	tassert.Errorf(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "xabc")
	setAvailableMountPaths(oldMPs...)
}

func TestSearchWithASuffixToAnotherValue(t *testing.T) {
	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	cmn.GCO.CommitUpdate(config)

	Mountpaths = NewMountedFS()
	dirs := []string{"/tmp/x", "/tmp/xabc", "/tmp/x/abc"}
	createDirs(dirs...)
	defer removeDirs(dirs...)

	oldMPs := setAvailableMountPaths("/tmp", "/tmp/x")

	mpathInfo, _ := Mountpaths.Path2MpathInfo("xabc")
	tassert.Errorf(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "xabc")

	mpathInfo, _ = Mountpaths.Path2MpathInfo("/tmp/xabc")
	longestPrefix := mpathInfo.Path
	tassert.Errorf(t, longestPrefix == "/tmp", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp")

	mpathInfo, _ = Mountpaths.Path2MpathInfo("/tmp/x/abc")
	longestPrefix = mpathInfo.Path
	tassert.Errorf(t, longestPrefix == "/tmp/x", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp/x")
	setAvailableMountPaths(oldMPs...)
}

func TestSimilarCases(t *testing.T) {
	Mountpaths = NewMountedFS()
	dirs := []string{"/tmp/abc", "/tmp/abx"}
	createDirs(dirs...)
	defer removeDirs(dirs...)

	oldMPs := setAvailableMountPaths("/tmp/abc")

	mpathInfo, _ := Mountpaths.Path2MpathInfo("/tmp/abc")
	longestPrefix := mpathInfo.Path
	tassert.Errorf(t, longestPrefix == "/tmp/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp/abc")

	mpathInfo, _ = Mountpaths.Path2MpathInfo("/tmp/abc/")
	longestPrefix = mpathInfo.Path
	tassert.Errorf(t, longestPrefix == "/tmp/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp/abc")

	mpathInfo, _ = Mountpaths.Path2MpathInfo("/abx")
	tassert.Errorf(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "/abx")
	setAvailableMountPaths(oldMPs...)
}

func TestSimilarCasesWithRoot(t *testing.T) {
	// root is an invalid mountpath
	Mountpaths = NewMountedFS()
	oldMPs := setAvailableMountPaths("/tmp", "/")

	mpathInfo, _ := Mountpaths.Path2MpathInfo("/abx")
	tassert.Errorf(t, mpathInfo == nil, "Expected mpathInfo to be nil when no valid matching mountpath")
	setAvailableMountPaths(oldMPs...)
}

func setAvailableMountPaths(paths ...string) []string {
	Mountpaths.DisableFsIDCheck()

	availablePaths, _ := Mountpaths.Get()
	oldPaths := make([]string, 0, len(availablePaths))
	for _, mpathInfo := range availablePaths {
		oldPaths = append(oldPaths, mpathInfo.Path)
	}

	for _, mpathInfo := range availablePaths {
		Mountpaths.Remove(mpathInfo.Path)
	}

	for _, path := range paths {
		if path == "" {
			continue
		}

		Mountpaths.Add(path)
	}

	return oldPaths
}

func createDirs(dirs ...string) error {
	for _, dir := range dirs {
		err := cmn.CreateDir(dir)
		if err != nil {
			return err
		}
	}

	return nil
}

func removeDirs(dirs ...string) {
	for _, dir := range dirs {
		os.RemoveAll(dir)
	}
}
