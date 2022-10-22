// Package xs contains most of the supported eXtended actions (xactions) with some
// exceptions that include certain storage services (mirror, EC) and extensions (downloader, lru).
/*
 * Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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

func setWanted(e *cmn.LsoEntry, lom *cluster.LOM, tmformat string, wanted cos.BitFlags) {
	for name, fl := range allmap {
		if !wanted.IsSet(fl) {
			continue
		}
		switch name {
		case apc.GetPropsName:
		case apc.GetPropsStatus:
		case apc.GetPropsCached: // via obj.SetPresent()

		case apc.GetPropsSize:
			e.Size = lom.SizeBytes()
		case apc.GetPropsVersion:
			e.Version = lom.Version()
		case apc.GetPropsChecksum:
			e.Checksum = lom.Checksum().Value()
		case apc.GetPropsAtime:
			e.Atime = cos.FormatUnixNano(lom.AtimeUnix(), tmformat)
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
}
