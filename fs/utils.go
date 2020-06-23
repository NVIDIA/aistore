// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
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

func IsDirEmpty(dir string) (names []string, empty bool, err error) {
	var f *os.File
	f, err = os.Open(dir)
	if err != nil {
		return nil, false, err
	}
	defer func() {
		debug.AssertNoErr(f.Close())
	}()

	// Try listing small number of files/dirs to do a quick emptiness check.
	// If seems empty try a bigger sample to determine if it actually is.
	for _, limit := range []int{10, 100, 1000, -1} {
		names, err = f.Readdirnames(limit)
		if err == io.EOF {
			return nil, true, nil
		}
		if err != nil || len(names) == 0 {
			return nil, true, err
		}
		// Firstly, check if there is any file at this level.
		dirs := names[:0]
		for _, sub := range names {
			subDir := filepath.Join(dir, sub)
			if finfo, erc := os.Stat(subDir); erc == nil {
				if !finfo.IsDir() {
					return names[:cmn.Min(8, len(names))], false, nil
				}
				dirs = append(dirs, subDir)
			}
		}
		// If not, then try to recurse into each directory.
		for _, subDir := range dirs {
			if nestedNames, empty, err := IsDirEmpty(subDir); err != nil {
				return nil, false, err
			} else if !empty {
				return nestedNames, false, nil
			}
		}
		// If we've just listed all the files/dirs then exit.
		if len(names) < limit {
			break
		}
	}
	return nil, true, nil
}
