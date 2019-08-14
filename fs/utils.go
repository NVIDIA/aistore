// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ios"
)

var (
	pid  int64 = 0xDEADBEEF   // pid of the current process
	spid       = "0xDEADBEEF" // string version of the pid

	CSM = &ContentSpecMgr{RegisteredContentTypes: make(map[string]ContentResolver, 8)}
)

func init() {
	pid = int64(os.Getpid())
	spid = strconv.FormatInt(pid, 16)
}

// global init
func InitMountedFS() {
	Mountpaths = NewMountedFS()
	if logLvl, ok := cmn.CheckDebug(pkgName); ok {
		glog.SetV(glog.SmoduleFS, logLvl)
	}
}

// new instance
func NewMountedFS(iostater ...ios.IOStater) *MountedFS {
	mfs := &MountedFS{fsIDs: make(map[syscall.Fsid]string, 10), checkFsID: true}
	if len(iostater) > 0 {
		mfs.ios = iostater[0]
	} else {
		mfs.ios = ios.NewIostatContext()
	}
	return mfs
}

// A simplified version of the filepath.Rel that determines the
// relative path of targ (target) to mpath. Note that mpath and
// targ must be clean (as in: filepath.Clean)
// pathPrefixMatch will only return true if the targ is equivalent to
// mpath + relative path.
func pathPrefixMatch(mpath, targ string) (rel string, match bool) {
	if len(mpath) == len(targ) && mpath == targ {
		return ".", true
	}
	bl := len(mpath)
	tl := len(targ)
	var b0, bi, t0, ti int
	for {
		for bi < bl && mpath[bi] != filepath.Separator {
			bi++
		}
		for ti < tl && targ[ti] != filepath.Separator {
			ti++
		}
		if targ[t0:ti] != mpath[b0:bi] {
			break
		}
		if bi < bl {
			bi++
		}
		if ti < tl {
			ti++
		}
		b0 = bi
		t0 = ti
	}
	if b0 != bl {
		return "", false
	}
	return targ[t0:], true
}
