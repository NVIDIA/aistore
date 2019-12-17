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

// mountpath/<content-type>/<cloudPath|aisPath>/<bucket-name>/...
const (
	cloudPath = "cloud"
	aisPath   = "local"
)

const (
	// prefixes for workfiles created by various services
	WorkfileRemote  = "remote" // getting object from neighbor target while rebalance is running
	WorkfileColdget = "cold"   // object GET: coldget
	WorkfilePut     = "put"    // object PUT
	WorkfileAppend  = "append" // object APPEND
	WorkfileFSHC    = "fshc"   // FSHC test file
)

type ParsedFQN struct {
	MpathInfo   *MountpathInfo
	ContentType string
	Bucket      string
	Provider    string
	ObjName     string
	Digest      uint64
}

// mpathInfo, bucket, objname, isLocal, err
func (mfs *MountedFS) FQN2Info(fqn string) (parsed ParsedFQN, err error) {
	var rel string

	parsed.MpathInfo, rel, err = mfs.FQN2MpathInfo(fqn)
	if err != nil {
		return
	}
	items := strings.SplitN(rel, string(filepath.Separator), 4)
	if len(items) < 4 {
		err = fmt.Errorf("fqn %s is invalid: %+v", fqn, items)
	} else if _, ok := CSM.RegisteredContentTypes[items[0]]; !ok {
		err = fmt.Errorf("invalid fqn %s: unrecognized content type", fqn)
	} else if items[2] == "" {
		err = fmt.Errorf("invalid fqn %s: bucket name is empty", fqn)
	} else if items[3] == "" {
		err = fmt.Errorf("invalid fqn %s: object name is empty", fqn)
	} else if items[1] != aisPath && items[1] != cloudPath {
		err = fmt.Errorf("invalid bucket type %q for fqn %s", items[1], fqn)
	} else {
		if items[1] == aisPath {
			parsed.Provider = cmn.AIS
		} else {
			parsed.Provider = cmn.Cloud
		}

		parsed.ContentType, parsed.Bucket, parsed.ObjName = items[0], items[2], items[3]
	}
	return
}

func (mfs *MountedFS) FQN2MpathInfo(fqn string) (info *MountpathInfo, relativePath string, err error) {
	var (
		available = (*MPI)(mfs.available.Load())
		max       = 0
		ll        = len(fqn)
	)
	if available == nil {
		err = fmt.Errorf("failed to get mountpaths, fqn: %s", fqn)
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
	if info == nil {
		err = fmt.Errorf("no mountpath match FQN: %s", fqn)
		return
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

func (mfs *MountedFS) CreateBucketDir(provider string) error {
	isLocal := cmn.IsProviderAIS(provider)
	if !isLocal {
		cmn.AssertMsg(cmn.IsProviderCloud(provider), "unknown bucket provider: '"+provider+"'")
	}
	availablePaths, _ := Mountpaths.Get()
	for contentType := range CSM.RegisteredContentTypes {
		for _, mpathInfo := range availablePaths {
			dir := mpathInfo.MakePath(contentType, provider)
			if _, exists := availablePaths[dir]; exists {
				return fmt.Errorf("local namespace partitioning conflict: %s vs %s", mpathInfo, dir)
			}
			if err := cmn.CreateDir(dir); err != nil {
				return fmt.Errorf("cannot create %s buckets dir %q, err: %v", provider, dir, err)
			}
		}
	}
	return nil
}
