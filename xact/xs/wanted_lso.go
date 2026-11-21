// Package xs contains most of the supported eXtended actions (xactions) with some
// exceptions that include certain storage services (mirror, EC) and extensions (downloader, lru).
/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
)

func wanted(msg *apc.LsoMsg) (flags cos.BitFlags) {
	debug.Assert(len(allLsoFlags) == len(apc.GetPropsAll)) // (the map is statically initialized - see Tinit)
	for prop, fl := range allLsoFlags {
		if msg.WantProp(prop) {
			flags = flags.Set(fl)
		}
	}
	return
}

func (wi *walkInfo) setWanted(en *cmn.LsoEnt, lom *core.LOM) {
	var (
		checkVchanged = wi.msg.IsFlagSet(apc.LsDiff)
		s3Props       bool
	)
	for name, fl := range allLsoFlags {
		if !wi.wanted.IsSet(fl) {
			continue
		}
		switch name {
		case apc.GetPropsName:
		case apc.GetPropsStatus:
		case apc.GetPropsCached: // (apc.EntryIsCached)

		case apc.GetPropsSize:
			if en.Size > 0 && lom.Lsize() != en.Size {
				en.SetFlag(apc.EntryVerChanged)
			}
			en.Size = lom.Lsize()
		case apc.GetPropsVersion:
			// remote VersionObjMD takes precedence over ais incremental numbering
			if en.Version == "" {
				en.Version = lom.Version()
			}
		case apc.GetPropsChecksum:
			en.Checksum = lom.Checksum().Value()
		case apc.GetPropsAtime:
			// atime vs remote LastModified
			en.Atime = cos.FormatNanoTime(lom.AtimeUnix(), wi.msg.TimeFormat)
		case apc.GetPropsLocation:
			en.Location = lom.Location()
		case apc.GetPropsCopies:
			en.Copies = int16(lom.NumCopies())

		case apc.GetPropsEC:
			// TODO at the risk of significant slow-down

		case apc.GetPropsCustom:
			s3Props = wi.msg.IsFlagSet(apc.LsIsS3)
			if (!s3Props || checkVchanged) && en.Custom == "" {
				if md := lom.GetCustomMD(); len(md) > 0 {
					en.Custom = cmn.CustomMD2S(md)
					checkVchanged = false
				}
			}
		default:
			debug.Assert(false, name)
		}
	}

	// slow path: extensive 'version-changed' check
	if checkVchanged && !en.IsAnyFlagSet(apc.EntryVerChanged|apc.EntryVerRemoved) {
		cmn.S2CustomMD(wi.custom, en.Custom, en.Version)
		if len(wi.custom) > 0 {
			oa := cmn.ObjAttrs{Size: en.Size, CustomMD: wi.custom}
			if lom.CheckEq(&oa) != nil {
				// lom.CheckEq returned err contains the cause
				en.SetFlag(apc.EntryVerChanged)
			}
			clear(wi.custom)
		}
	}
	if s3Props {
		setS3Props(en, lom)
	}
}

// backend-returned attrs take precedence;
// for S3 list-objects, use best-effort metadata (to avoid a stat per object),
// clients that need the object's mtime can use HEAD(object).
func setS3Props(en *cmn.LsoEnt, lom *core.LOM) {
	var etag, modified string
	if en.Custom == "" {
		etag, _ = lom.GetCustomKey(cmn.ETag)
		modified, _ = lom.GetCustomKey(cmn.LsoLastModified)
	} else {
		etag, modified = en.S3Props()
	}
	if en.Custom != "" && etag != "" && modified != "" {
		return
	}
	var mtime time.Time
	if modified == "" {
		modified, mtime = lom.LastModifiedLso()
	}
	if etag == "" {
		etag = lom.ETag(mtime, false /*allow syscall*/)
	}
	en.Custom = cmn.CustomProps2S(cmn.ETag, etag, cmn.LsoLastModified, modified)
}
