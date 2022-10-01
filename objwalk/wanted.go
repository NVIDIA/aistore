// Package objwalk provides common context and helper methods for object listing and
// object querying.
/*
 * Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
 */
package objwalk

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

var (
	allmap map[string]cos.BitFlags
)

func init() {
	allmap = make(map[string]cos.BitFlags, len(apc.GetPropsAll))
	for i, n := range apc.GetPropsAll {
		allmap[n] = cos.BitFlags(1) << i
	}
}

func wanted(msg *apc.ListObjsMsg) (flags cos.BitFlags) {
	for prop, fl := range allmap {
		if msg.WantProp(prop) {
			flags = flags.Set(fl)
		}
	}
	return
}

func setWanted(e *cmn.ObjEntry, lom *cluster.LOM, tmformat string, wanted cos.BitFlags) {
	for name, fl := range allmap {
		if !wanted.IsSet(fl) {
			continue
		}
		switch name {
		case apc.GetPropsName:
		case apc.GetPropsStatus:
		case apc.GetPropsCached:

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

			// TODO -- FIXME: add/support both EC and custom
		case apc.GetPropsEC:
		case apc.GetPropsCustom:
		default:
			debug.Assert(false, name)
		}
	}
}
