// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"path/filepath"

	"github.com/NVIDIA/aistore/api/apc"
)

// used in multi-object (list|range) operations
type (
	// List of object names _or_ a template specifying { Prefix, Regex, and/or Range }
	SelectObjsMsg struct {
		Template string   `json:"template"`
		ObjNames []string `json:"objnames"`
	}

	// ArchiveMsg is used in CreateArchMultiObj operations; the message contains parameters
	// for archiving mutiple (source) objects as one of the supported cos.ArchExtensions types
	// at the specified (bucket) destination
	ArchiveMsg struct {
		ToBck       Bck    `json:"tobck"`
		TxnUUID     string `json:"-"`
		FromBckName string `json:"-"`
		ArchName    string `json:"archname"` // must have one of the cos.ArchExtensions
		Mime        string `json:"mime"`     // user-specified mime type takes precedence if defined
		SelectObjsMsg
		InclSrcBname          bool `json:"isbn"` // include source bucket name into the names of archived objects
		AllowAppendToExisting bool `json:"aate"` // allow adding a list or a range of objects to an existing archive
		ContinueOnError       bool `json:"coer"` // keep running archiving xaction in presence of errors in a any given multi-object transaction
	}

	//  Multi-object copy & transform (see also: TCBMsg)
	TCObjsMsg struct {
		ToBck Bck `json:"tobck"`
		SelectObjsMsg
		TxnUUID string `json:"-"`
		apc.TCBMsg
		ContinueOnError bool `json:"coer"` // ditto
	}
)

// NOTE: empty SelectObjsMsg{} corresponds to (range = entire bucket)

func (lrm *SelectObjsMsg) IsList() bool      { return len(lrm.ObjNames) > 0 }
func (lrm *SelectObjsMsg) HasTemplate() bool { return lrm.Template != "" }

////////////////
// ArchiveMsg //
////////////////
func (msg *ArchiveMsg) FullName() string { return filepath.Join(msg.ToBck.Name, msg.ArchName) }
