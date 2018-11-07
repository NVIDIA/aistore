/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/json-iterator/go"
)

func init() {
	ctx.config.CloudBuckets = "cloud"
	ctx.config.LocalBuckets = "local"
}

func TestGetFSUsedPercentage(t *testing.T) {
	percentage, ok := getFSUsedPercentage("/")
	if !ok {
		t.Error("Unable to retrieve FS used percentage!")
	}
	if percentage > 100 {
		t.Errorf("Invalid FS used percentage [%d].", percentage)
	}
}

func TestGetFSDiskUtil(t *testing.T) {
	if err := checkIostatVersion(); err != nil {
		t.Skip("iostat version is not okay.")
	}

	tempRoot := "/tmp"
	fs.Mountpaths.AddMountpath(tempRoot)

	riostat := newIostatRunner()
	go riostat.Run()

	time.Sleep(50 * time.Millisecond)
	percentage, ok := riostat.diskUtilFromFQN(tempRoot + "/test")
	if !ok {
		t.Error("Unable to retrieve disk utilization for File System!")
	}
	if percentage < 0 && percentage > 100 {
		t.Errorf("Invalid FS disk utilization percentage [%f].", percentage)
	}
	glog.Infof("Disk utilization fetched. Value [%f]", percentage)
	riostat.Stop(fmt.Errorf("test"))
}

func TestGetDiskFromFileSystem(t *testing.T) {
	path := "/"
	fileSystem, err := fs.Fqn2fsAtStartup(path)
	if err != nil {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks := fs2disks(fileSystem)
	if len(disks) == 0 {
		t.Errorf("Invalid FS disks: [%s]", fileSystem)
	}

	path = "/tmp"
	fileSystem, err = fs.Fqn2fsAtStartup(path)
	if err != nil {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks = fs2disks(fileSystem)
	if len(disks) == 0 {
		t.Errorf("Invalid FS disks: [%s]", fileSystem)
	}

	path = "/home"
	fileSystem, err = fs.Fqn2fsAtStartup(path)
	if err != nil {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks = fs2disks(fileSystem)
	if len(disks) == 0 {
		t.Errorf("Invalid FS disks: [%s]", fileSystem)
	}

	path = "asdasd"
	fileSystem, err = fs.Fqn2fsAtStartup(path)
	if err != nil {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks = fs2disks(path)
	if len(disks) != 0 {
		t.Errorf("Invalid FS disks: [%s]", fileSystem)
	}
}

func TestMultipleMountPathsOnSameDisk(t *testing.T) {
	rawJson := jsoniter.RawMessage(
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
	bytes, err := jsoniter.Marshal(&rawJson)
	if err != nil {
		t.Errorf("Unable to marshal input json. Error: [%v]", err)
	}
	disks := lsblkOutput2disks(bytes, "md0")
	if len(disks) != 2 {
		t.Errorf("Invalid number of disks returned. Disks: [%v]", disks)
	}
	if _, ok := disks["xvdb"]; !ok {
		t.Errorf("Expected disk [xvdb] not returned. Disks: [%v]", disks)
	}
	if _, ok := disks["xvdd"]; !ok {
		t.Errorf("Expected disk [xvdd] not returned. Disks: [%v]", disks)
	}
	disks = lsblkOutput2disks(bytes, "xvda1")
	if len(disks) != 1 {
		t.Errorf("Invalid number of disks returned. Disks: [%v]", disks)
	}
	if _, ok := disks["xvda"]; !ok {
		t.Errorf("Expected disk [xvda] not returned. Disks: [%v]", disks)
	}
}

func TestGetMaxUtil(t *testing.T) {
	riostat := newIostatRunner()
	riostat.Disk = make(map[string]cmn.SimpleKVs, 2)
	disks := make(cmn.StringSet)
	disk1 := "disk1"
	disks[disk1] = struct{}{}
	riostat.Disk[disk1] = make(cmn.SimpleKVs, 1)
	riostat.Disk[disk1]["%util"] = "23.2"
	util := maxUtilDisks(riostat.Disk, disks)
	if math.Abs(23.2-util) > 0.0001 {
		t.Errorf("Expected: 23.2. Actual: %f", util)
	}
	disk2 := "disk2"
	riostat.Disk[disk2] = make(cmn.SimpleKVs, 1)
	riostat.Disk[disk2]["%util"] = "25.9"
	disks[disk2] = struct{}{}
	util = maxUtilDisks(riostat.Disk, disks)
	if math.Abs(25.9-util) > 0.0001 {
		t.Errorf("Expected: 25.9. Actual: %f", util)
	}
	delete(disks, disk2)
	util = maxUtilDisks(riostat.Disk, disks)
	if math.Abs(23.2-util) > 0.0001 {
		t.Errorf("Expected: 23.2. Actual: %f", util)
	}
}

func TestGetFSDiskUtilizationInvalid(t *testing.T) {
	riostat := newIostatRunner()
	_, ok := riostat.diskUtilFromFQN("test")
	if ok {
		t.Errorf("Expected to fail since no file system.")
	}
	_, ok = riostat.diskUtilFromFQN("/tmp")
	if ok {
		t.Errorf("Expected to fail since no file system to disk mapping.")
	}
}

func TestSearchValidMountPath(t *testing.T) {
	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	oldMPs := setAvailableMountPaths("/")
	mpathInfo, _ := path2mpathInfo("/abc")
	longestPrefix := mpathInfo.Path
	testAssert(t, longestPrefix == "/", "Actual: [%s]. Expected: [%s]", longestPrefix, "/")
	setAvailableMountPaths(oldMPs...)
}

func TestSearchInvalidMountPath(t *testing.T) {
	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	oldMPs := setAvailableMountPaths("/")
	mpathInfo, _ := path2mpathInfo("xabc")
	testAssert(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "xabc")
	setAvailableMountPaths(oldMPs...)
}

func TestSearchWithNoMountPath(t *testing.T) {
	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	oldMPs := setAvailableMountPaths("")
	mpathInfo, _ := path2mpathInfo("xabc")
	testAssert(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "xabc")
	setAvailableMountPaths(oldMPs...)
}

func TestSearchWithASuffixToAnotherValue(t *testing.T) {
	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	dirs := []string{"/tmp/x", "/tmp/xabc", "/tmp/x/abc"}
	createDirs(dirs...)
	defer removeDirs(dirs...)

	oldMPs := setAvailableMountPaths("/tmp", "/tmp/x")

	mpathInfo, _ := path2mpathInfo("xabc")
	testAssert(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "xabc")

	mpathInfo, _ = path2mpathInfo("/tmp/xabc")
	longestPrefix := mpathInfo.Path
	testAssert(t, longestPrefix == "/tmp", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp")

	mpathInfo, _ = path2mpathInfo("/tmp/x/abc")
	longestPrefix = mpathInfo.Path
	testAssert(t, longestPrefix == "/tmp/x", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp/x")
	setAvailableMountPaths(oldMPs...)
}

func TestSimilarCases(t *testing.T) {
	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	dirs := []string{"/tmp/abc", "/tmp/abx"}
	createDirs(dirs...)
	defer removeDirs(dirs...)

	oldMPs := setAvailableMountPaths("/tmp/abc")

	mpathInfo, _ := path2mpathInfo("/tmp/abc")
	longestPrefix := mpathInfo.Path
	testAssert(t, longestPrefix == "/tmp/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp/abc")

	mpathInfo, _ = path2mpathInfo("/tmp/abc/")
	longestPrefix = mpathInfo.Path
	testAssert(t, longestPrefix == "/tmp/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp/abc")

	mpathInfo, _ = path2mpathInfo("/abx")
	testAssert(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "/abx")
	setAvailableMountPaths(oldMPs...)
}

func TestSimilarCasesWithRoot(t *testing.T) {
	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	oldMPs := setAvailableMountPaths("/tmp", "/")

	mpathInfo, _ := path2mpathInfo("/tmp")
	longestPrefix := mpathInfo.Path
	testAssert(t, longestPrefix == "/tmp", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp")

	mpathInfo, _ = path2mpathInfo("/tmp/")
	longestPrefix = mpathInfo.Path
	testAssert(t, longestPrefix == "/tmp", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp")

	mpathInfo, _ = path2mpathInfo("/abx")
	longestPrefix = mpathInfo.Path
	testAssert(t, longestPrefix == "/", "Actual: [%s]. Expected: [%s]", longestPrefix, "/")
	setAvailableMountPaths(oldMPs...)
}

func setAvailableMountPaths(paths ...string) []string {
	fs.Mountpaths.DisableFsIDCheck()

	availablePaths, _ := fs.Mountpaths.Mountpaths()
	oldPaths := make([]string, 0, len(availablePaths))
	for _, mpathInfo := range availablePaths {
		oldPaths = append(oldPaths, mpathInfo.Path)
	}

	for _, mpathInfo := range availablePaths {
		fs.Mountpaths.RemoveMountpath(mpathInfo.Path)
	}

	for _, path := range paths {
		if path == "" {
			continue
		}

		fs.Mountpaths.AddMountpath(path)
	}

	return oldPaths
}

func testAssert(t *testing.T, condition bool, msg string, args ...interface{}) {
	if !condition {
		t.Errorf(fmt.Sprintf(msg, args...))
	}
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
				{"name": "xvdl", "size": "100G", "type": "disk", "mountpoint": "/dfc/xvdl"},
				{"name": "xvdy", "mountpoint": null, "fstype": "linux_raid_member",
					"children": [
						{"name": "md2", "mountpoint": "/dfc/3", "fstype": "xfs"}
					]
				},
				{"name": "xvdz", "mountpoint": null, "fstype": "linux_raid_member",
					"children": [
						{"name": "md2", "mountpoint": "/dfc/3", "fstype": "xfs"}
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
		disks := lsblkOutput2disks(out, tst.dev)
		if len(disks) != tst.diskCnt {
			t.Errorf("Expected %d disk(s) for %s but found %d (%v)",
				tst.diskCnt, tst.dev, len(disks), disks)
		}
		if tst.diskCnt != 0 {
			for _, disk := range tst.diskNames {
				if _, ok := disks[disk]; !ok {
					t.Errorf("Disk %s is not detected for device %s (disk list %v)",
						disk, tst.dev, disks)
				}
			}
		}
	}
}
