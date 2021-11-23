// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	prefCT       = '%'
	prefProvider = '@'
	prefNsUUID   = cmn.NsUUIDPrefix
	prefNsName   = cmn.NsNamePrefix
)

const (
	// prefixes for workfiles created by various services
	WorkfileRemote       = "remote"         // getting object from neighbor target when rebalancing
	WorkfileColdget      = "cold"           // object GET: coldget
	WorkfilePut          = "put"            // object PUT
	WorkfileAppend       = "append"         // APPEND to object (as file)
	WorkfileAppendToArch = "append-to-arch" // APPEND to existing archive
	WorkfileCreateArch   = "create-arch"    // CREATE multi-object archive
)

type ParsedFQN struct {
	MpathInfo   *MountpathInfo
	ContentType string
	Bck         cmn.Bck
	ObjName     string
	Digest      uint64
}

// ParseFQN splits a provided FQN (created by `MakePathFQN`) or reports
// an error.
func ParseFQN(fqn string) (parsed ParsedFQN, err error) {
	var (
		rel           string
		itemIdx, prev int
	)
	parsed.MpathInfo, rel, err = FQN2Mpath(fqn)
	if err != nil {
		return
	}
	for i := 0; i < len(rel); i++ {
		if rel[i] != filepath.Separator {
			continue
		}

		item := rel[prev:i]
		switch itemIdx {
		case 0: // backend provider
			if item[0] != prefProvider {
				err = fmt.Errorf("invalid fqn %s: bad provider %q", fqn, item)
				return
			}
			provider := item[1:]
			parsed.Bck.Provider = provider
			if !cmn.IsNormalizedProvider(provider) {
				err = fmt.Errorf("invalid fqn %s: unknown provider %q", fqn, provider)
				return
			}
		case 1: // namespace or bucket name
			if item == "" {
				err = fmt.Errorf("invalid fqn %s: bad bucket name (or namespace)", fqn)
				return
			}

			switch item[0] {
			case prefNsName:
				parsed.Bck.Ns = cmn.Ns{
					Name: item[1:],
				}
				itemIdx-- // we must visit this case again
			case prefNsUUID:
				ns := item[1:]
				idx := strings.IndexRune(ns, prefNsName)
				if idx == -1 {
					err = fmt.Errorf("invalid fqn %s: bad namespace %q", fqn, ns)
				}
				parsed.Bck.Ns = cmn.Ns{
					UUID: ns[:idx],
					Name: ns[idx+1:],
				}
				itemIdx-- // we must visit this case again
			default:
				parsed.Bck.Name = item
			}
		case 2: // content type and object name
			if item[0] != prefCT {
				err = fmt.Errorf("invalid fqn %s: bad content type %q", fqn, item)
				return
			}

			item = item[1:]
			if _, ok := CSM.RegisteredContentTypes[item]; !ok {
				err = fmt.Errorf("invalid fqn %s: bad content type %q", fqn, item)
				return
			}
			parsed.ContentType = item

			// Object name
			objName := rel[i+1:]
			if objName == "" {
				err = fmt.Errorf("invalid fqn %s: bad object name", fqn)
			}
			parsed.ObjName = objName
			return
		}

		itemIdx++
		prev = i + 1
	}

	err = fmt.Errorf("fqn %s is invalid", fqn)
	return
}

// FQN2Mpath matches FQN to mountpath and returns the mountpath info and the relative path.
func FQN2Mpath(fqn string) (found *MountpathInfo, relativePath string, err error) {
	availablePaths := GetAvail()
	if len(availablePaths) == 0 {
		err = cmn.ErrNoMountpaths
		return
	}
	for mpath, mi := range availablePaths {
		l := len(mpath)
		if len(fqn) > l && fqn[0:l] == mpath && fqn[l] == filepath.Separator {
			found = mi
			relativePath = fqn[l+1:]
			return
		}
	}
	err = fmt.Errorf("not found mountpath for fqn %s", fqn)

	// make an extra effort to lookup in disabled
	_, disabledPaths := Get()
	for mpath, mi := range disabledPaths {
		l := len(mpath)
		if len(fqn) > l && fqn[0:l] == mpath && fqn[l] == filepath.Separator {
			// TODO -- FIXME: revisit zero-ing out error
			glog.Errorf("mountpath %s for fqn %s exists but is disabled", mi, fqn)
			err = nil
			found = mi
			relativePath = fqn[l+1:]
			return
		}
	}
	return
}

// Path2Mpath takes in any file path (e.g., ../../a/b/c) and returns the matching `mpathInfo`,
// if exists
func Path2Mpath(path string) (found *MountpathInfo, err error) {
	found, _, err = FQN2Mpath(filepath.Clean(path))
	return
}
