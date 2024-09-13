// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cmn/debug"
)

// for background, see: docs/on_disk_layout.md

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
	ContentType string // enum: { ObjectType, WorkfileType, ECSliceType, ... }
	ObjName     string
	Digest      uint64
	Bck         cmn.Bck
}

///////////////
// ParsedFQN //
///////////////

func (parsed *ParsedFQN) Init(fqn string) (err error) {
	var (
		rel           string
		itemIdx, prev int
	)
	parsed.Mountpath, rel, err = FQN2Mpath(fqn)
	if err != nil {
		return
	}
	for i := range len(rel) {
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

	return fmt.Errorf("fqn %s is invalid", fqn)
}

//
// supporting helpers
//

// match FQN to mountpath and return the former and the relative path
func FQN2Mpath(fqn string) (found *Mountpath, relativePath string, err error) {
	debug.Assert(fqn != "")
	avail := GetAvail()
	if len(avail) == 0 {
		return nil, "", cmn.ErrNoMountpaths
	}
	for mpath, mi := range avail {
		l := len(mpath)
		if len(fqn) > l && fqn[0:l] == mpath && fqn[l] == filepath.Separator {
			return mi, fqn[l+1:], nil
		}
	}

	// make an extra effort to lookup in disabled
	disabled := getDisabled()
	for mpath := range disabled {
		l := len(mpath)
		if len(fqn) > l && fqn[0:l] == mpath && fqn[l] == filepath.Separator {
			return nil, "", cmn.NewErrMpathNotFound(mpath, fqn, true /*disabled*/)
		}
	}
	return nil, "", cmn.NewErrMpathNotFound("" /*mpath*/, fqn, false /*disabled*/)
}

func CleanPathErr(err error) {
	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		return
	}
	if pathErr.Path == "" {
		return
	}

	var parsed ParsedFQN
	if errV := parsed.Init(pathErr.Path); errV != nil {
		return
	}

	// replace
	pathErr.Path = parsed.Bck.Cname(parsed.ObjName)
	pathErr.Op = "[fs-path]"
	if strings.Contains(pathErr.Err.Error(), "no such file") {
		var what string
		switch parsed.ContentType {
		case ObjectType:
			what = "'object'"
		case WorkfileType:
			what = "'work file'"
		case ECSliceType:
			what = "'ec slice'"
		case ECMetaType:
			what = "'ec metadata'"
		default:
			what = fmt.Sprintf("'%s'(?)", parsed.ContentType)
		}
		pathErr.Err = cos.NewErrNotFound(nil, "content type "+what)
	}
}
