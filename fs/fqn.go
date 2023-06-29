// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	prefCT       = '%'
	prefProvider = '@'
	prefNsUUID   = apc.NsUUIDPrefix
	prefNsName   = apc.NsNamePrefix
)

const (
	// prefixes for workfiles created by various services
	WorkfileRemote       = "remote"         // getting object from neighbor target when rebalancing
	WorkfileColdget      = "cold"           // object GET: coldget
	WorkfilePut          = "put"            // object PUT
	WorkfileCopy         = "copy"           // copy object
	WorkfileAppend       = "append"         // APPEND to object (as file)
	WorkfileAppendToArch = "append-to-arch" // APPEND to existing archive
	WorkfileCreateArch   = "create-arch"    // CREATE multi-object archive
)

type ParsedFQN struct {
	Mountpath   *Mountpath
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
	parsed.Mountpath, rel, err = FQN2Mpath(fqn)
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
			if !apc.IsProvider(provider) {
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
			if _, ok := CSM.m[item]; !ok {
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

// FQN2Mpath matches FQN to mountpath and returns the mountpath and the relative path.
func FQN2Mpath(fqn string) (found *Mountpath, relativePath string, err error) {
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

	// make an extra effort to lookup in disabled
	_, disabledPaths := Get()
	for mpath := range disabledPaths {
		l := len(mpath)
		if len(fqn) > l && fqn[0:l] == mpath && fqn[l] == filepath.Separator {
			err = cmn.NewErrMountpathNotFound("" /*mpath*/, fqn, true /*disabled*/)
			return
		}
	}
	err = cmn.NewErrMountpathNotFound("" /*mpath*/, fqn, false /*disabled*/)
	return
}

// Path2Mpath takes in any file path (e.g., ../../a/b/c) and returns the matching `mi`,
// if exists
func Path2Mpath(path string) (found *Mountpath, err error) {
	found, _, err = FQN2Mpath(filepath.Clean(path))
	return
}

// TODO: define fs.PathErr to return "ais://nnn/shard-99.tar not found"
// instead of the current "  no such file or directory"
func CleanPathErr(err error) {
	var (
		pathErr *fs.PathError
		what    string
	)
	if !errors.As(err, &pathErr) {
		return
	}
	parsed, errV := ParseFQN(pathErr.Path)
	if errV != nil {
		return
	}
	pathErr.Path = parsed.Bck.Cname(parsed.ObjName)
	pathErr.Op = "[fs-path]"
	if strings.Contains(pathErr.Err.Error(), "no such file") {
		switch parsed.ContentType {
		case ObjectType:
			what = "object"
		case WorkfileType:
			what = "work file"
		case ECSliceType:
			what = "ec slice"
		case ECMetaType:
			what = "ec metadata"
		default:
			what = "????"
		}
		pathErr.Err = cos.NewErrNotFound(what)
	}
}
