/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/dfcpub/cmn"
)

const (
	BucketLocalType = "local"
	BucketCloudType = "cloud"

	// prefixes for workfiles created by various services
	WorkfileReplication = "repl"   // replication runner
	WorkfileRemote      = "remote" // getting object from neighbor target while rebalance is running
	WorkfileColdget     = "cold"   // object GET: coldget
	WorkfilePut         = "put"    // object PUT
	WorkfileRebalance   = "reb"    // rebalance
	WorkfileFSHC        = "fshc"   // FSHC test file
)

type FQNparsed struct {
	MpathInfo   *MountpathInfo
	ContentType string
	Bucket      string
	Objname     string
	IsLocal     bool
}

// mpathInfo, bucket, objname, isLocal, err
func (mfs *MountedFS) FQN2Info(fqn string) (parsed FQNparsed, err error) {
	var rel string

	parsed.MpathInfo, rel = mfs.Path2MpathInfo(fqn)
	if parsed.MpathInfo == nil {
		err = fmt.Errorf("fqn %s is invalid", fqn)
		return
	}

	sep := string(filepath.Separator)
	items := strings.SplitN(rel, sep, 4)

	if len(items) < 4 {
		err = fmt.Errorf("fqn %s is invalid: %+v", fqn, items)
	} else if _, ok := CSM.RegisteredContentTypes[items[0]]; !ok {
		err = fmt.Errorf("invalid fqn %s: unrecognized content type", fqn)
	} else if items[2] == "" {
		err = fmt.Errorf("invalid fqn %s: bucket name is empty", fqn)
	} else if items[3] == "" {
		err = fmt.Errorf("invalid fqn %s: object name is empty", fqn)
	} else if items[1] != mfs.localBuckets && items[1] != mfs.cloudBuckets {
		err = fmt.Errorf("invalid bucket type %q for fqn %s", items[1], fqn)
	} else {
		parsed.ContentType, parsed.IsLocal, parsed.Bucket, parsed.Objname = items[0], (items[1] == mfs.localBuckets), items[2], items[3]
	}
	return
}

// path2mpathInfo takes in a path (fqn or mpath) and returns the mpathInfo of the mpath with the longest
// common prefix to path. It also returns the relative path to this mpath.
func (mfs *MountedFS) Path2MpathInfo(path string) (info *MountpathInfo, relativePath string) {
	var (
		max               int
		availablePaths, _ = mfs.Get()
		cleanedPath       = filepath.Clean(path)
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
	if !cmn.StringInSlice(bucketType, []string{BucketLocalType, BucketCloudType}) {
		return fmt.Errorf("unknown bucket type: %s", bucketType)
	}
	availablePaths, _ := Mountpaths.Get()

	makeBucket := mfs.MakePathLocal
	if bucketType == BucketCloudType {
		makeBucket = mfs.MakePathCloud
	}

	for contentType := range CSM.RegisteredContentTypes {
		for _, mpathInfo := range availablePaths {
			dir := makeBucket(mpathInfo.Path, contentType)
			if _, exists := availablePaths[dir]; exists {
				return fmt.Errorf("local namespace partitioning conflict: %s vs %s", mpathInfo.Path, dir)
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
