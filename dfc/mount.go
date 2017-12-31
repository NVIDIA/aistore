/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/golang/glog"
)

// FIXME: mountPath encapsulates mount specific information on a local node (remove)
type mountPath struct {
	Device  string
	Path    string
	Type    string
	Opts    []string
	Fsid    syscall.Fsid
	errcnt  int
	enabled bool
}

const (
	dfcStoreMntPrefix        = "/mnt/dfcstore"
	dfcSignatureFileName     = "/.dfc.txt"
	expectedNumFieldsPerLine = 6              // num fields per line in /proc/mounts as per the fstab man
	procMountsPath           = "/proc/mounts" // location of the mount file
)

// locate and populate local mount points
func parseProcMounts(filename string) ([]mountPath, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		glog.Fatalf("Failed to read %q, err: %v", filename, err)
	}
	out := []mountPath{}
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		if line == "" {
			// the last split() item is empty string following the last \n
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != expectedNumFieldsPerLine {
			glog.Errorf("Wrong number of fields (expected %d, got %d): %s",
				expectedNumFieldsPerLine, len(fields), line)
			continue
		}
		if checkdfcmntpath(fields[1]) {
			if glog.V(3) {
				glog.Infof("Found mp %s", fields[1])
			}
			mp := mountPath{
				Device:  fields[0],
				Path:    fields[1],
				Type:    fields[2],
				Opts:    strings.Split(fields[3], ","),
				enabled: true,
			}
			statfs := syscall.Statfs_t{}
			if err := syscall.Statfs(mp.Path, &statfs); err != nil {
				glog.Fatalf("Failed to statfs mp %q, err: %v", mp.Path, err)
				return nil, err
			}
			mp.Fsid = statfs.Fsid
			out = append(out, mp)
		}
	}
	return out, nil
}

// emulate mountpath with // local directories.
func emulateCachepathMounts() []mountPath {
	out := []mountPath{}
	for i := 0; i < ctx.config.Cache.CachePathCount; i++ {
		mpath := ctx.config.Cache.CachePath + dfcStoreMntPrefix + strconv.Itoa(i)
		mp := mountPath{
			Path:    mpath,
			enabled: true,
		}
		statfs := syscall.Statfs_t{}
		if err := syscall.Statfs(mp.Path, &statfs); err != nil {
			glog.Fatalf("Failed to statfs mp %q, err: %v", mp.Path, err)
			return nil
		}
		mp.Fsid = statfs.Fsid
		out = append(out, mp)
	}
	return out
}

// can only use prefixed mount points
func checkdfcmntpath(path string) bool {
	return strings.HasPrefix(path, dfcStoreMntPrefix) && checkdfcsignature(path)
}

// check if signature is present
func checkdfcsignature(path string) bool {
	// TODO keep open so that underlying mountpath cannot be unmounted
	filename := path + dfcSignatureFileName
	if _, err := os.Stat(filename); err != nil {
		return false
	}
	return true
}

// FIXME: disabling all mp-s not handled
func setMountPathStatus(path string, status bool) {
	for _, mountpath := range ctx.mountpaths {
		if strings.HasPrefix(path, mountpath.Path) {
			mountpath.enabled = status
			return
		}
	}
}

// Get error count
func getMountPathErrorCount(path string) int {
	for _, mountpath := range ctx.mountpaths {
		if strings.HasPrefix(path, mountpath.Path) {
			return mountpath.errcnt
		}
	}
	return 0
}

// Increment error counter
func incrMountPathErrorCount(path string) {
	for _, mountpath := range ctx.mountpaths {
		if strings.HasPrefix(path, mountpath.Path) {
			mountpath.errcnt++
			return
		}
	}
}
