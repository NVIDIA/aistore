/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package fs

import (
	"os"
	"testing"

	"github.com/NVIDIA/aistore/tutils/tassert"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ios"
	jsoniter "github.com/json-iterator/go"
)

// test file for ios/dutils_linux.go
// placed here because it requires fs to set up the testing environment

func init() {
	Mountpaths = NewMountedFS()
}

func TestGetDiskFromFileSystem(t *testing.T) {
	path := "/tmp"
	fileSystem, err := Fqn2fsAtStartup(path)
	if err != nil {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks := ios.Fs2disks(fileSystem)
	if len(disks) == 0 {
		t.Errorf("Invalid FS disks: [%s]", fileSystem)
	}

	path = "asdasd"
	fileSystem, err = Fqn2fsAtStartup(path)
	if err != nil {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks = ios.Fs2disks(path)
	if len(disks) != 0 {
		t.Errorf("Invalid FS disks: [%s]", fileSystem)
	}
}

func TestMultipleMountPathsOnSameDisk(t *testing.T) {
	rawJSON := jsoniter.RawMessage(
		`{
        "blockdevices": [{
                "name": "xvda",
                "children": [{
                    "name": "xvda1"
                }]
            },
            {
                "name": "xvdb",
                "children": [{
                    "name": "md0"
                }]
            }, {
                "name": "xvdd",
                "children": [{
                    "name": "md0"
                }]
            }
        ]
		}`)
	bytes, err := jsoniter.Marshal(&rawJSON)
	if err != nil {
		t.Errorf("Unable to marshal input json. Error: [%v]", err)
	}
	disks := ios.LsblkOutput2disks(bytes, "md0")
	if len(disks) != 2 {
		t.Errorf("Invalid number of disks returned. Disks: [%v]", disks)
	}
	if _, ok := disks["xvdb"]; !ok {
		t.Errorf("Expected disk [xvdb] not returned. Disks: [%v]", disks)
	}
	if _, ok := disks["xvdd"]; !ok {
		t.Errorf("Expected disk [xvdd] not returned. Disks: [%v]", disks)
	}
	disks = ios.LsblkOutput2disks(bytes, "xvda1")
	if len(disks) != 1 {
		t.Errorf("Invalid number of disks returned. Disks: [%v]", disks)
	}
	if _, ok := disks["xvda"]; !ok {
		t.Errorf("Expected disk [xvda] not returned. Disks: [%v]", disks)
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

func TestLsblk(t *testing.T) {
	out := []byte(`{
		   "blockdevices": [
				{"name": "xvda", "size": "8G", "type": "disk", "mountpoint": null,
					"children": [
						{"name": "xvda1", "size": "8G", "type": "part", "mountpoint": "/"}
					]
				},
				{"name": "xvdf", "size": "1.8T", "type": "disk", "mountpoint": null},
				{"name": "xvdh", "size": "1.8T", "type": "disk", "mountpoint": null},
				{"name": "xvdi", "size": "1.8T", "type": "disk", "mountpoint": null},
				{"name": "xvdl", "size": "100G", "type": "disk", "mountpoint": "/ais/xvdl"},
				{"name": "xvdy", "mountpoint": null, "fstype": "linux_raid_member",
					"children": [
						{"name": "md2", "mountpoint": "/ais/3", "fstype": "xfs"}
					]
				},
				{"name": "xvdz", "mountpoint": null, "fstype": "linux_raid_member",
					"children": [
						{"name": "md2", "mountpoint": "/ais/3", "fstype": "xfs"}
					]
				}
			]
		}
	`)

	type test struct {
		desc      string
		dev       string
		diskCnt   int
		diskNames []string
	}
	testSets := []test{
		{"Single disk (no children)", "/dev/xvdi", 1, []string{"xvdi"}},
		{"Single disk (with children)", "/dev/xvda1", 1, []string{"xvda"}},
		{"Invalid device", "/dev/xvda7", 0, []string{}},
		{"Device with 2 disks", "/dev/md2", 2, []string{"xvdz", "xvdy"}},
	}

	for _, tst := range testSets {
		t.Log(tst.desc)
		disks := ios.LsblkOutput2disks(out, tst.dev)
		if len(disks) != tst.diskCnt {
			t.Errorf("Expected %d disk(s) for %s but found %d (%v)",
				tst.diskCnt, tst.dev, len(disks), disks)
		}
		if tst.diskCnt != 0 {
			for _, disk := range tst.diskNames {
				if _, ok := disks[disk]; !ok {
					t.Errorf("%s is not detected for device %s (%v)", disk, tst.dev, disks)
				}
			}
		}
	}
}
