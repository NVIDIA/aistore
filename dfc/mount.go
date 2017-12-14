package dfc

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/golang/glog"
)

// MountPoint structure encapsulates mount specific information on local DFC Node.
type MountPoint struct {

	// Mountpath can be only used for ConsistentHash if Usable is set to  True.
	// It will be set to True after verifying signature file.
	// It will be set to False incase of error(IO error or Non Accessible error).
	Usable bool
	Device string
	Path   string
	Type   string
	Opts   []string
	errcnt int
}

const (
	//
	dfcStoreMntPrefix    = "/mnt/dfcstore"
	dfcSignatureFileName = "/.dfc.txt"
	// Number of fields per line in /proc/mounts as per the fstab man page.
	expectedNumFieldsPerLine = 6
	// Location of the mount file to use
	procMountsPath = "/proc/mounts"
)

// Parse and populate usable locally mounted path on DFC Instance.
func parseProcMounts(filename string) ([]MountPoint, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		glog.Fatalf("Failed to read from file %s err = %v \n", filename, err)
	}
	out := []MountPoint{}
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		if line == "" {
			// the last split() item is empty string following the last \n
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != expectedNumFieldsPerLine {
			glog.Errorf("Wrong number of fields (expected %d, got %d): %s \n",
				expectedNumFieldsPerLine, len(fields), line)
			continue
		}
		if checkdfcmntpath(fields[1]) {
			if glog.V(3) {
				glog.Infof(" Found DFC storage Mountpath = %s \n", fields[1])
			}
			mp := MountPoint{
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
func populateCachepathMounts() []MountPoint {
	out := []MountPoint{}
	for i := 0; i < ctx.config.Cache.CachePathCount; i++ {
		mpath := ctx.config.Cache.CachePath + dfcStoreMntPrefix + strconv.Itoa(i)
		mp := MountPoint{
			Usable: true,
			Device: "",
			Path:   mpath,
		}
		out = append(out, mp)
	}
	return out
}

// DFC can only use mountpaths starting with dfcStoreMntPrefix.
func checkdfcmntpath(path string) bool {

	if strings.HasPrefix(path, dfcStoreMntPrefix) && checkdfcsignature(path) {
		return true
	}
	return false

}

// To Check if signature file is present or not.
func checkdfcsignature(path string) bool {
	//TODO keep handle open on file so that underlying mountpoint cannot be unmounted.
	filename := path + dfcSignatureFileName
	_, err := os.Stat(filename)
	if err != nil {
		return false
	}
	return true
}

// Sets the MountPath status.
func setMountPathStatus(path string, status bool) {
	for _, minfo := range ctx.mntpath {
		if strings.HasPrefix(path, minfo.Path) {
			minfo.Usable = status
		}
	}
}

// Get error count for underlying mountpath.
func getMountPathErrorCount(path string) int {
	for _, minfo := range ctx.mntpath {
		if strings.HasPrefix(path, minfo.Path) {
			return minfo.errcnt
		}
	}
	return 0
}

// Increment error count for underlying mountpath.
func incrMountPathErrorCount(path string) {
	for _, minfo := range ctx.mntpath {
		if strings.HasPrefix(path, minfo.Path) {
			minfo.errcnt++
		}
	}
}
