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

	"github.com/golang/glog"
)

// mountPath encapsulates mount specific information on the local DFC node
// Can only be used if Usable (below) is set to true AND and upon verifying the signature file
type mountPath struct {
	Usable bool
	Device string
	Path   string
	Type   string
	Opts   []string
	errcnt int
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
		glog.Fatalf("Failed to read file %q, err: %v", filename, err)
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
				glog.Infof("Found mount point %s", fields[1])
			}
			mp := mountPath{
				Usable: true,
				Device: fields[0],
				Path:   fields[1],
				Type:   fields[2],
				Opts:   strings.Split(fields[3], ","),
			}
			out = append(out, mp)
		}
	}
	return out, nil
}

// populateCachepathMounts provides functionality to emulate multimountpath support with
// local directories.
func populateCachepathMounts() []mountPath {
	out := []mountPath{}
	for i := 0; i < ctx.config.Cache.CachePathCount; i++ {
		mpath := ctx.config.Cache.CachePath + dfcStoreMntPrefix + strconv.Itoa(i)
		mp := mountPath{
			Usable: true,
			Device: "",
			Path:   mpath,
		}
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
	// TODO keep open so that underlying mountpath cannot be unmounted.
	filename := path + dfcSignatureFileName
	if _, err := os.Stat(filename); err != nil {
		return false
	}
	return true
}

// set mount point usability
func setMountPathStatus(path string, status bool) {
	for _, mountpath := range ctx.mountpaths {
		if strings.HasPrefix(path, mountpath.Path) {
			mountpath.Usable = status
			// FIXME: handle num mount points reduced to zero
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

// Increment error count for underlying mountpath.
func incrMountPathErrorCount(path string) {
	for _, mountpath := range ctx.mountpaths {
		if strings.HasPrefix(path, mountpath.Path) {
			mountpath.errcnt++
			// FIXME: break?
		}
	}
}
