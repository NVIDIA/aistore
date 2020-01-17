// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	prefCT       = '~'
	prefProvider = '@'
	// prefNamespace = '$'
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

// ParseFQN splits a provided FQN (created by `MakePathBucketObject`) or reports
// an error.
func (mfs *MountedFS) ParseFQN(fqn string) (parsed ParsedFQN, err error) {
	var (
		rel           string
		itemIdx, prev int
	)

	parsed.MpathInfo, rel, err = mfs.ParseMpathInfo(fqn)
	if err != nil {
		return
	}

	for i := 0; i < len(rel); i++ {
		if rel[i] != filepath.Separator {
			continue
		}

		item := rel[prev:i]
		switch itemIdx {
		case 0: // content type
			if item[0] != prefCT {
				err = fmt.Errorf("invalid fqn %s: expected content type to be prefixed with '%s'", fqn, string(prefCT))
				return
			}
			item = item[1:]
			if _, ok := CSM.RegisteredContentTypes[item]; !ok {
				err = fmt.Errorf("invalid fqn %s: unrecognized content type %q", fqn, item)
				return
			}
			parsed.ContentType = item
		case 1: // cloud provider
			if item[0] != prefProvider {
				err = fmt.Errorf("invalid fqn %s: expected cloud provider to be prefixed with '%v'", fqn, string(prefProvider))
				return
			}
			item = item[1:]
			if !cmn.IsValidProvider(item) {
				err = fmt.Errorf("invalid fqn %s: unrecognized provider %q", fqn, item)
				return
			}
			parsed.Provider = item
		case 2: // bucket and object name
			if item == "" {
				err = fmt.Errorf("invalid fqn %s: bucket name is empty", fqn)
				return
			}
			parsed.Bucket = item

			objName := rel[i+1:]
			if objName == "" {
				err = fmt.Errorf("invalid fqn %s: object name is empty", fqn)
			}
			parsed.ObjName = objName
			return
		}

		itemIdx++
		prev = i + 1
	}

	err = fmt.Errorf("fqn is invalid: %s", fqn)
	return
}

// ParseMpathInfo matches and extracts <mpath> from the FQN and returns the rest of FQN.
func (mfs *MountedFS) ParseMpathInfo(fqn string) (info *MountpathInfo, relativePath string, err error) {
	var (
		available = (*MPI)(mfs.available.Load())
		max       = 0
	)
	if available == nil {
		err = fmt.Errorf("failed to get mountpaths, fqn: %s", fqn)
		return
	}
	availablePaths := *available
	for mpath, mpathInfo := range availablePaths {
		l := len(mpath)
		if len(fqn) > l && l > max && fqn[0:l] == mpath && fqn[l] == filepath.Separator {
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
	cmn.AssertMsg(cmn.IsValidProvider(provider), "unknown cloud provider: '"+provider+"'")

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
