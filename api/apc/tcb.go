// Package apc: API messages and constants
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"errors"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// copy & (offline) transform bucket to bucket
type (
	CopyBckMsg struct {
		Prefix string `json:"prefix"`  // Prefix added to each resulting object.
		DryRun bool   `json:"dry_run"` // Don't perform any PUT
		Force  bool   `json:"force"`   // Force running in presence of a potential "limited coexistence" type conflict
	}
	Transform struct {
		Name    string       `json:"id,omitempty"`
		Timeout cos.Duration `json:"request_timeout,omitempty"`
	}
	TCBMsg struct {
		// NOTE: resulting object names will have this extension, if specified.
		// NOTE: if source bucket has two (or more) objects with the same base name but different extension,
		// specifying this field might cause unintended override.
		// TODO: this field might not be required when range/list transformation is supported.
		Ext cos.StrKVs `json:"ext"`

		Transform
		CopyBckMsg
	}
)

////////////
// TCBMsg //
////////////

func (msg *TCBMsg) Validate(isEtl bool) (err error) {
	if isEtl && msg.Transform.Name == "" {
		err = errors.New("ETL name can't be empty")
	}
	return
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
