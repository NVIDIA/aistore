// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

// used in multi-object (list|range) operations
type (
	// List of object names _or_ a template specifying { Prefix, Regex, and/or Range }
	ListRangeMsg struct {
		ObjNames []string `json:"objnames"`
		Template string   `json:"template"`
	}

	// ArchiveMsg is used in CreateArchMultiObj operations; the message contains parameters
	// for archiving mutiple (source) objects as one of the supported cos.ArchExtensions types
	// at the specified (bucket) destination
	ArchiveMsg struct {
		TxnUUID     string `json:"-"`
		FromBckName string `json:"-"`
		ListRangeMsg
		ToBck    Bck    `json:"tobck"`
		ArchName string `json:"archname"` // must have one of the cos.ArchExtensions
		Mime     string `json:"mime"`     // user-specified mime type takes precedence if defined
		// flags
		InclSrcBname          bool `json:"isbn"` // include source bucket name into the names of archived objects
		AllowAppendToExisting bool `json:"aate"` // allow adding a list or a range of objects to an existing archive
		ContinueOnError       bool `json:"coer"` // keep running archiving xaction in presence of errors in a any given multi-object transaction
	}

	//  Multi-object copy & transform (see also: TCBMsg)
	TCObjsMsg struct {
		TxnUUID string `json:"-"`
		ListRangeMsg
		TCBMsg
	}
)

// NOTE: empty ListRangeMsg{} corresponds to (range = entire bucket)

func (lrm *ListRangeMsg) IsList() bool      { return len(lrm.ObjNames) > 0 }
func (lrm *ListRangeMsg) HasTemplate() bool { return lrm.Template != "" }
