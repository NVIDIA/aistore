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
	// ArchiveMsg is used in CreateArchMultiObj operations; the message contains parameters
	// for archiving mutiple (source) objects as one of the supported cos.ArchExtensions types
	// at the specified (bucket) destination
	ArchiveMsg struct {
		ListRangeMsg
		ToBck    Bck    `json:"tobck"`
		ArchName string `json:"archname"` // must have one of the cos.ArchExtensions
		Mime     string `json:"mime"`     // user-specified mime type takes precedence if defined
		// flags
		SkipMisplaced    bool `json:"sm"`    // skip misplaced objects (when rebalancing...)
		IgnoreBackendErr bool `json:"ignbe"` // ignore cold GET and similar errors
		EraseSources     bool `json:"es"`    // remove source objects iff successfully archived
		InclBckName      bool `json:"incbn"` // include source bucket name, e.g. srcbck/obj => dstbck/arch.tar as srcbck/obj
	}

	// See also: TCObjsMsg
)

// NOTE: empty ListRangeMsg{} corresponds to (range = entire bucket)

func (lrm *ListRangeMsg) IsList() bool      { return len(lrm.ObjNames) > 0 }
func (lrm *ListRangeMsg) HasTemplate() bool { return lrm.Template != "" }
