// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	// prefixes for workfiles created by various services
	WorkfileReplication = "repl"   // replication runner
	WorkfileRemote      = "remote" // getting object from neighbor target while rebalance is running
	WorkfileColdget     = "cold"   // object GET: coldget
	WorkfilePut         = "put"    // object PUT
	WorkfileRebalance   = "reb"    // rebalance
	WorkfileFSHC        = "fshc"   // FSHC test file
)
const sep = string(filepath.Separator)

type ParsedFQN struct {
	MpathInfo   *MountpathInfo
	ContentType string
	Bucket      string
	Objname     string
	Digest      uint64
	IsLocal     bool
}

// mpathInfo, bucket, objname, isLocal, err
func (mfs *MountedFS) FQN2Info(fqn string) (parsed ParsedFQN, err error) {
	var rel string

	parsed.MpathInfo, rel = mfs.FQN2MpathInfo(fqn)
	if parsed.MpathInfo == nil {
		err = fmt.Errorf("fqn %s is invalid", fqn)
		return
	}
	items := strings.SplitN(rel, sep, 4)
	if len(items) < 4 {
		err = fmt.Errorf("fqn %s is invalid: %+v", fqn, items)
	} else if _, ok := CSM.RegisteredContentTypes[items[0]]; !ok {
		err = fmt.Errorf("invalid fqn %s: unrecognized content type", fqn)
	} else if items[2] == "" {
		err = fmt.Errorf("invalid fqn %s: bucket name is empty", fqn)
	} else if items[3] == "" {
		err = fmt.Errorf("invalid fqn %s: object name is empty", fqn)
	} else if items[1] != cmn.LocalBs && items[1] != cmn.CloudBs {
		err = fmt.Errorf("invalid bucket type %q for fqn %s", items[1], fqn)
	} else {
		parsed.ContentType, parsed.IsLocal, parsed.Bucket, parsed.Objname = items[0], (items[1] == cmn.LocalBs), items[2], items[3]
	}
	return
}

func (mfs *MountedFS) FQN2MpathInfo(fqn string) (info *MountpathInfo, relativePath string) {
	var (
		available = (*MPI)(mfs.available.Load())
		max       = 0
		ll        = len(fqn)
	)
	if available == nil {
		return
	}
	availablePaths := *available
	for mpath, mpathInfo := range availablePaths {
		l := len(mpath)
		if ll > l && l > max && fqn[0:l] == mpath && fqn[l] == filepath.Separator {
			info = mpathInfo
			relativePath = fqn[l+1:]
			max = l
		}
	}
	return
}

// Path2MpathInfo takes in any path (fqn or mpath) and returns the mpathInfo of the mpath
// with the longest common prefix and the relative path to this mpath
func (mfs *MountedFS) Path2MpathInfo(path string) (info *MountpathInfo, relativePath string) {
	var (
		availablePaths, _ = mfs.Get()
		cleanedPath       = filepath.Clean(path)
		max               int
	)
	for mpath, mpathInfo := range availablePaths {
		rel, ok := pathPrefixMatch(mpath, cleanedPath)
		if ok && len(mpath) > max {
			info = mpathInfo
			max = len(mpath)
			relativePath = rel
			if relativePath == "." {
				break
			}
		}
	}
	return
}

func (mfs *MountedFS) CreateBucketDir(bucketType string) error {
	if !cmn.StringInSlice(bucketType, []string{cmn.LocalBs, cmn.CloudBs}) {
		cmn.AssertMsg(false, "unknown bucket type: '"+bucketType+"'")
	}
	availablePaths, _ := Mountpaths.Get()
	for contentType := range CSM.RegisteredContentTypes {
		for _, mpathInfo := range availablePaths {
			dir := mpathInfo.MakePath(contentType, bucketType == cmn.LocalBs)
			if _, exists := availablePaths[dir]; exists {
				return fmt.Errorf("local namespace partitioning conflict: %s vs %s", mpathInfo, dir)
			}
			if err := cmn.CreateDir(dir); err != nil {
				return fmt.Errorf("cannot create %s buckets dir %q, err: %v", bucketType, dir, err)
			}
		}
	}
	return nil
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
