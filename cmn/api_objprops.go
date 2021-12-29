// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

// ListObjsMsg and HEAD(object) enum
// NOTE: compare with `ObjectProps` below and popular lists of selected props (below as well)
const (
	GetPropsName     = "name"
	GetPropsSize     = "size"
	GetPropsVersion  = "version"
	GetPropsChecksum = "checksum"
	GetPropsAtime    = "atime"
	GetPropsCached   = "cached"
	GetTargetURL     = "target_url"
	GetPropsStatus   = "status"
	GetPropsCopies   = "copies"
	GetPropsEC       = "ec"
	GetPropsCustom   = "custom"
)

// popular lists of selected props (NOTE: update when/if `ObjectProps` type changes)
var (
	// minimal
	GetPropsMinimal = []string{GetPropsName, GetPropsSize}
	// compact
	GetPropsDefault = []string{GetPropsName, GetPropsSize, GetPropsChecksum, GetPropsAtime}
	// all
	GetPropsAll = append(GetPropsDefault,
		GetPropsVersion, GetPropsCached, GetTargetURL, GetPropsStatus, GetPropsCopies, GetPropsEC, GetPropsCustom,
	)
)

// object properties (NOTE: embeds system `ObjAttrs` that in turn includes custom user-defined)
type ObjectProps struct {
	ObjAttrs
	Name      string `json:"name"`
	Bck       Bck    `json:"bucket"`
	NumCopies int    `json:"copies"`
	EC        struct {
		Generation   int64 `json:"ec-generation"`
		DataSlices   int   `json:"ec-data"`
		ParitySlices int   `json:"ec-parity"`
		IsECCopy     bool  `json:"ec-replicated"`
	}
	Present bool `json:"present"`
}
