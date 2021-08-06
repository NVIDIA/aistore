// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"path/filepath"
	"strings"

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

	parsed.MpathInfo, rel, err = ParseMpathInfo(fqn)
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

	err = fmt.Errorf("fqn is invalid: %s", fqn)
	return
}

// ParseMpathInfo matches and extracts <mpath> from the FQN and returns the rest of FQN.
func ParseMpathInfo(fqn string) (info *MountpathInfo, relativePath string, err error) {
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
func Path2MpathInfo(path string) (info *MountpathInfo, relativePath string) {
	var (
		availablePaths, _ = Get()
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
