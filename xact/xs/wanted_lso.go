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

// `apc.LsoMsg` flags

var (
	allmap map[string]cos.BitFlags
)

func init() {
	allmap = make(map[string]cos.BitFlags, len(apc.GetPropsAll))
	for i, n := range apc.GetPropsAll {
		allmap[n] = cos.BitFlags(1) << i
	}
}

func wanted(msg *apc.LsoMsg) (flags cos.BitFlags) {
	for prop, fl := range allmap {
		if msg.WantProp(prop) {
			flags = flags.Set(fl)
		}
	}
	return
}

func (wi *walkInfo) setWanted(en *cmn.LsoEnt, lom *core.LOM) {
	var (
		checkVchanged = wi.msg.IsFlagSet(apc.LsDiff)
	)
	for name, fl := range allmap {
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
			// en.Custom is set via one of the two alternative flows:
			// - checkRemoteMD => HEAD(obj)
			// - backend.List* api call
			var md cos.StrKVs
			if en.Custom == "" {
				if md = lom.GetCustomMD(); len(md) > 0 {
					en.Custom = cmn.CustomMD2S(md)
					checkVchanged = false
				}
			}

			if !wi.msg.IsFlagSet(apc.LsIsS3) {
				break
			}

			// NOTE:
			// For S3 list-objects, AIS does NOT issue per-object mtime/stat syscalls.
			// This is a deliberate scalability choice. List responses use best-effort
			// metadata only. S3 clients requiring exact values can use HEAD(object) API.
			// A future feature flag may relax this, but the default favors performance.

			var (
				mtimeStr string
				mtime    time.Time
				added    bool
			)
			if md == nil {
				md = make(cos.StrKVs, 4)
				if en.Custom != "" {
					cmn.S2CustomMD(md, en.Custom, en.Version)
				}
			}
			if _, ok := md[cmn.LsoLastModified]; !ok {
				// best-effort; may fall back to atime
				mtimeStr, mtime = lom.LastModifiedLso()
				if mtimeStr != "" {
					md[cmn.LsoLastModified] = mtimeStr
					added = true
				}
			}
			if _, ok := md[cmn.ETag]; !ok {
				if s := lom.ETag(mtime, false /*allow syscall*/); s != "" {
					md[cmn.ETag] = s
					added = true
				}
			}
			if added {
				en.Custom = cmn.CustomMD2S(md)
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
}
