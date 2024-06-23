// Package xs contains most of the supported eXtended actions (xactions) with some
// exceptions that include certain storage services (mirror, EC) and extensions (downloader, lru).
/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"

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

func (wi *walkInfo) setWanted(e *cmn.LsoEnt, lom *core.LOM) {
	var (
		custom  = e.Custom
		version = e.Version
	)
	for name, fl := range allmap {
		if !wi.wanted.IsSet(fl) {
			continue
		}
		switch name {
		case apc.GetPropsName:
		case apc.GetPropsStatus:
		case apc.GetPropsCached: // via obj.SetPresent()

		case apc.GetPropsSize:
			if e.Size > 0 && lom.Lsize() != e.Size {
				e.SetVerChanged()
			}
			e.Size = lom.Lsize()
		case apc.GetPropsVersion:
			e.Version = lom.Version()
		case apc.GetPropsChecksum:
			e.Checksum = lom.Checksum().Value()
		case apc.GetPropsAtime:
			e.Atime = cos.FormatNanoTime(lom.AtimeUnix(), wi.msg.TimeFormat)
		case apc.GetPropsLocation:
			e.Location = lom.Location()
		case apc.GetPropsCopies:
			e.Copies = int16(lom.NumCopies())

		case apc.GetPropsEC:
			// TODO?: risk of significant slow-down loading EC metafiles
		case apc.GetPropsCustom:
			if md := lom.GetCustomMD(); len(md) > 0 {
				e.Custom = fmt.Sprintf("%+v", md)
			}
		default:
			debug.Assert(false, name)
		}
	}
	if wi.msg.IsFlagSet(apc.LsVerChanged) && !e.IsVerChanged() {
		// slow path: extensive version-changed check
		md := cmn.S2CustomMD(custom, version)
		if len(md) > 0 {
			var oa cmn.ObjAttrs
			oa.CustomMD = md
			oa.Size = e.Size
			if !lom.Equal(&oa) {
				e.SetVerChanged()
			}
		}
	}
}
