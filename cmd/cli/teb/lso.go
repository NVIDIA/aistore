// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

var (
	// ObjectPropsMap matches ObjEntry field
	ObjectPropsMap = map[string]string{
		apc.GetPropsName:     "{{FormatNameArch $obj.Name $obj.Flags}}",
		apc.GetPropsSize:     "{{FormatBytesSig $obj.Size 2}}",
		apc.GetPropsChecksum: "{{$obj.Checksum}}",
		apc.GetPropsAtime:    "{{$obj.Atime}}",
		apc.GetPropsVersion:  "{{$obj.Version}}",
		apc.GetPropsLocation: "{{$obj.Location}}",
		apc.GetPropsCustom:   "{{FormatObjCustom $obj.Custom}}",
		apc.GetPropsStatus:   "{{FormatLsObjStatus $obj}}",
		apc.GetPropsCopies:   "{{$obj.Copies}}",
		apc.GetPropsCached:   "{{FormatLsObjIsCached $obj}}",
	}
)

func LsoTemplate(propsList []string, hideHeader, addCachedCol, addStatusCol bool) string {
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
		format, ok := ObjectPropsMap[apc.GetPropsCached]
		debug.Assert(ok)
		headSb.WriteString(columnName + "\t ")
		bodySb.WriteString(format + "\t ")
	}
	if addStatusCol {
		columnName := strings.ToUpper(apc.GetPropsStatus)
		format, ok := ObjectPropsMap[apc.GetPropsStatus]
		debug.Assert(ok)
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

func fmtLsObjStatus(e *cmn.LsoEntry) string {
	switch e.Status() {
	case apc.LocOK:
		if e.IsPresent() {
			if e.IsVerChanged() {
				return "version-changed"
			}
			return "ok"
		}
		return UnknownStatusVal
	case apc.LocMisplacedNode:
		return "misplaced(cluster)"
	case apc.LocMisplacedMountpath:
		return "misplaced(mountpath)"
	case apc.LocIsCopy:
		return "replica"
	case apc.LocIsCopyMissingObj:
		return "replica(object-is-missing)"
	default:
		debug.Assertf(false, "%#v", e)
		return "invalid"
	}
}

func fmtLsObjIsCached(e *cmn.LsoEntry) string {
	return FmtBool(e.IsPresent())
}
