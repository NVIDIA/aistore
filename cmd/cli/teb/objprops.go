// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
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
		apc.GetPropsStatus:   "{{FormatObjStatus $obj}}",
		apc.GetPropsCopies:   "{{$obj.Copies}}",
		apc.GetPropsCached:   "{{FormatObjIsCached $obj}}",
	}
)

func ObjPropsTemplate(propsList []string, hideHeader, addCachedCol bool) string {
	var (
		headSb strings.Builder
		bodySb strings.Builder
	)
	bodySb.WriteString("{{range $obj := .}}")
	for _, field := range propsList {
		format, ok := ObjectPropsMap[field]
		if !ok {
			continue
		}
		if field == apc.GetPropsCached && !addCachedCol {
			continue
		}
		columnName := strings.ToUpper(field)
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
