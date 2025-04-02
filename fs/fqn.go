// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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
	Bck         cmn.Bck
	ContentType string // enum: { ObjectType, WorkfileType, ECSliceType, ... }
	ObjName     string
	Digest      uint64
}

func LikelyCT(s string) bool { return len(s) == 1+contentTypeLen && s[0] == prefCT }

func ContainsCT(fqn, bname string) bool {
	i := strings.Index(fqn, cos.PathSeparator+bname+cos.PathSeparator)
	if i < 0 {
		return false
	}
	ct := fqn[i+2+len(bname):]
	j := strings.IndexByte(ct, filepath.Separator)
	if j < 0 {
		return false
	}
	return LikelyCT(ct[:j])
}

///////////////
// ParsedFQN //
///////////////

func (parsed *ParsedFQN) Init(fqn string) error {
	const (
		tag = "invalid fqn"
	)
	mi, rel, err := FQN2Mpath(fqn)
	if err != nil {
		return err
	}
	parsed.Mountpath = mi

	var itemIdx, prev int
	for i := range len(rel) {
		if rel[i] != filepath.Separator {
			continue
		}

		item := rel[prev:i]
		switch itemIdx {
		case 0: // backend provider
			if item[0] != prefProvider {
				return fmt.Errorf("%s %s: bad provider %q", tag, fqn, item)
			}
			provider := item[1:]
			parsed.Bck.Provider = provider
			if !apc.IsProvider(provider) {
				return fmt.Errorf("%s %s: unknown provider %q", tag, fqn, provider)
			}
		case 1: // namespace or bucket name
			if item == "" {
				return fmt.Errorf("%s %s: bad bucket name (or namespace)", tag, fqn)
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
					return fmt.Errorf("%s %s: bad namespace %q", tag, fqn, ns)
				}
				parsed.Bck.Ns = cmn.Ns{
					UUID: ns[:idx],
					Name: ns[idx+1:],
				}
				itemIdx-- // revisit
			default:
				parsed.Bck.Name = item
			}
		case 2: // content type and object name
			if item[0] != prefCT {
				return fmt.Errorf("%s %s: bad content type %q", tag, fqn, item)
			}

			item = item[1:]
			if _, ok := CSM.m[item]; !ok {
				return fmt.Errorf("%s %s: bad content type %q", tag, fqn, item)
			}
			parsed.ContentType = item

			// object name
			objName := rel[i+1:]
			if objName == "" {
				return fmt.Errorf("%s %s: bad object name", tag, fqn)
			}
			parsed.ObjName = objName
			return nil
		}

		itemIdx++
		prev = i + 1
	}

	return fmt.Errorf("%s %s (idx %d, prev %d)", tag, fqn, itemIdx, prev)
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
			what = "object"
		case WorkfileType:
			what = "work file"
		case ECSliceType:
			what = "ec slice"
		case ECMetaType:
			what = "ec metadata"
		default:
			what = fmt.Sprintf("content type '%s'(?)", parsed.ContentType)
		}
		pathErr.Err = cos.NewErrNotFound(nil, what)
	}
}
