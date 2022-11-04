// Package apc: API messages and constants
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"errors"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
)

var ErrETLMissingUUID = errors.New("ETL UUID can't be empty")

// copy & (offline) transform bucket to bucket
type (
	CopyBckMsg struct {
		Prefix string `json:"prefix"`  // Prefix added to each resulting object.
		DryRun bool   `json:"dry_run"` // Don't perform any PUT
		Force  bool   `json:"force"`   // Force running in presence of a potential "limited coexistence" type conflict
	}
	TCBMsg struct {
		// Resulting objects names will have this extension. Warning: if in a source bucket exist two objects with the
		// same base name, but different extension, specifying this field might cause object overriding. This is because
		// of resulting name conflict.
		// TODO: this field might not be required when transformation on subset (template) of bucket is supported.
		Ext cos.StrKVs `json:"ext"`

		ID             string       `json:"id,omitempty"`              // optional, ETL only
		RequestTimeout cos.Duration `json:"request_timeout,omitempty"` // optional, ETL only

		CopyBckMsg
	}
)

////////////
// TCBMsg //
////////////

func (msg *TCBMsg) Validate() error {
	if msg.ID == "" {
		return ErrETLMissingUUID
	}
	return nil
}

// Replace extension and add suffix if provided.
func (msg *TCBMsg) ToName(name string) string {
	if msg.Ext != nil {
		if idx := strings.LastIndexByte(name, '.'); idx >= 0 {
			ext := name[idx+1:]
			if replacement, exists := msg.Ext[ext]; exists {
				name = name[:idx+1] + strings.TrimLeft(replacement, ".")
			}
		}
	}
	if msg.Prefix != "" {
		name = msg.Prefix + name
	}
	return name
}
