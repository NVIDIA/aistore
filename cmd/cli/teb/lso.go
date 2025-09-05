// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// TODO: hardcoded; revisit when adding support for apc.GetPropsChunked
const propChunked = "chunked"

var (
	// ObjectPropsMap matches ObjEntry field
	ObjectPropsMap = map[string]string{
		apc.GetPropsName:     "{{FormatEntryNameDAC $obj.Name $obj.Flags}}",
		apc.GetPropsSize:     "{{FormatBytesSig2 $obj.Size 2 $obj.Flags}}",
		apc.GetPropsChecksum: "{{$obj.Checksum}}",
		apc.GetPropsAtime:    "{{$obj.Atime}}",
		apc.GetPropsVersion:  "{{$obj.Version}}",
		apc.GetPropsLocation: "{{$obj.Location}}",
		apc.GetPropsCustom:   "{{FormatObjCustom $obj.Custom}}",
		apc.GetPropsStatus:   "{{FormatLsObjStatus $obj}}",
		apc.GetPropsCopies:   "{{$obj.Copies}}",
		apc.GetPropsCached:   "{{FormatLsObjIsCached $obj}}",
		//
		propChunked: "{{FormatIsChunked $obj.Flags}}",
	}
)

func LsoTemplate(propsList []string, hideHeader, addCachedCol, addStatusCol, addChunkedCol bool) string {
	var (
		headSb strings.Builder
		bodySb strings.Builder
	)
	bodySb.WriteString("{{range $obj := .}}")
	for _, field := range propsList {
		format, ok := ObjectPropsMap[field]
		if !ok {
			debug.Assert(false, field)
			continue
		}
		if field == apc.GetPropsCached {
			// controlled by `addCachedCol`; goes either last or next to last before status
			continue
		}
		if field == apc.GetPropsStatus {
			addStatusCol = true // always last col
			continue
		}
		columnName := strings.ToUpper(field)
		headSb.WriteString(columnName + "\t ")
		bodySb.WriteString(format + "\t ")
	}
	if addCachedCol {
		columnName := strings.ToUpper(apc.GetPropsCached)
		format := ObjectPropsMap[apc.GetPropsCached]
		headSb.WriteString(columnName + "\t ")
		bodySb.WriteString(format + "\t ")
	}
	if addChunkedCol {
		columnName := strings.ToUpper(propChunked)
		format, ok := ObjectPropsMap[propChunked]
		debug.Assert(ok)
		headSb.WriteString(columnName + "\t ")
		bodySb.WriteString(format + "\t ")
	}
	if addStatusCol {
		columnName := strings.ToUpper(apc.GetPropsStatus)
		format := ObjectPropsMap[apc.GetPropsStatus]
		headSb.WriteString(columnName + "\t ")
		bodySb.WriteString(format + "\t ")
	}

	headSb.WriteString("\n")
	bodySb.WriteString("\n{{end}}")

	if hideHeader {
		return bodySb.String()
	}
	return headSb.String() + bodySb.String()
}

//
// formatting
//

func fmtLsObjStatus(en *cmn.LsoEnt) string {
	switch en.Status() {
	case apc.LocOK:
		if !en.IsPresent() {
			return UnknownStatusVal
		}
		switch {
		case en.IsAnyFlagSet(apc.EntryVerChanged):
			return fcyan("version-changed")
		case en.IsAnyFlagSet(apc.EntryVerRemoved):
			return fblue("deleted") // as in note: deleted
		case en.IsAnyFlagSet(apc.EntryHeadFail):
			return fred("remote-error")
		default:
			return "ok"
		}
	case apc.LocMisplacedNode:
		return "misplaced(cluster)"
	case apc.LocMisplacedMountpath:
		return "misplaced(mountpath)"
	case apc.LocIsCopy:
		return "replica"
	case apc.LocIsCopyMissingObj:
		return "replica(object-is-missing)"
	default:
		debug.Assertf(false, "%#v", en)
		return "invalid"
	}
}

func fmtLsObjIsCached(en *cmn.LsoEnt) string {
	if en.IsAnyFlagSet(apc.EntryIsDir) {
		return ""
	}
	return FmtBool(en.IsPresent())
}
