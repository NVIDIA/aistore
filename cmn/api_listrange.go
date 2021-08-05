// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

// used in multi-object (list|range) operations
type (
	// List of object names, or
	// Prefix, Regex, and Range for a Range Operation
	ListRangeMsg struct {
		ObjNames []string `json:"objnames"`
		Template string   `json:"template"`
	}
	// ArchiveMsg contains parameters for archiving source objects as one of the supported
	// archive cos.ArchExtensions types at the destination
	ArchiveMsg struct {
		ListRangeMsg
		ToBck    Bck    `json:"tobck"`
		ArchName string `json:"archname"` // must have one of the cos.ArchExtensions
		Mime     string `json:"mime"`     // user-specified mime type takes precedence if defined
	}

	// See also: TransCpyListRangeMsg
)

// NOTE: empty ListRangeMsg{} corresponds to (range = entire bucket)

func (lrm *ListRangeMsg) IsList() bool      { return len(lrm.ObjNames) > 0 }
func (lrm *ListRangeMsg) HasTemplate() bool { return lrm.Template != "" }
