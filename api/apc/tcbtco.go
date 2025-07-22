// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// offline copy/transform: bucket-to-bucket and multi-object

// NOTE: see cmn/api for cmn.TCOMsg that also contains source and destination buckets

// TODO: `ContinueOnError` not implemented for the most part

type (
	// @Description Configuration for bucket-to-bucket copy operations including naming, filtering, and synchronization options.
	CopyBckMsg struct {
		Prepend   string `json:"prepend"`              // destination naming, as in: dest-obj-name = Prepend + source-obj-name
		Prefix    string `json:"prefix"`               // prefix to select matching _source_ objects or virtual directories
		DryRun    bool   `json:"dry_run"`              // visit all source objects, don't make any modifications
		Force     bool   `json:"force"`                // force running in presence of "limited coexistence" type conflicts
		LatestVer bool   `json:"latest-ver"`           // see also: QparamLatestVer, 'versioning.validate_warm_get', PrefetchMsg
		Sync      bool   `json:"synchronize"`          // see also: 'versioning.synchronize'
		NonRecurs bool   `json:"non-recurs,omitempty"` // do not copy contents of nested virtual subdirectories (see also: `apc.LsNoRecursion`, `apc.EvdMsg`)
	}
	// Transform contains ETL transformation parameters.
	// @Description ETL transformation configuration including transform name and timeout settings.
	Transform struct {
		Name    string       `json:"id,omitempty"`
		Timeout cos.Duration `json:"request_timeout,omitempty"`
	}

	// TCBMsg represents parameters for bucket-to-bucket copy and transform operations.
	// @Description Complete configuration for source-to-destination object actions, supporting extension mapping, ETL transformations, and copy options.
	TCBMsg struct {
		// Objname Extension ----------------------------------------------------------------------
		// - resulting object names will have this extension, if specified.
		// - if source bucket has two (or more) objects with the same base name but different extension,
		//   specifying this field might cause unintended override.
		// - this field might not be any longer required
		Ext cos.StrKVs `json:"ext"`

		Transform
		CopyBckMsg

		// user-defined number of concurrent workers:
		// * 0:  number of mountpaths (default)
		// * -1: single thread, serial execution
		NumWorkers int `json:"num-workers,omitempty"`

		ContinueOnError bool `json:"coer,omitempty"`
	}

	// multi-object
	// (cmn.TCOMsg = TCOMsg +  source and destination buckets)
	// @Description Multi-object operation message combining transformation/copy parameters with object selection and transaction tracking
	TCOMsg struct {
		TxnUUID string // (plstcx client; one control message)
		TCBMsg
		ListRange
	}
)

////////////
// TCBMsg //
////////////

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
	if msg.Prepend != "" {
		name = msg.Prepend + name
	}
	return name
}

////////////////
// CopyBckMsg //
////////////////

func (msg *CopyBckMsg) Str(sb *strings.Builder, fromCname, toCname string) {
	sb.WriteString(fromCname)
	sb.WriteString("=>")
	sb.WriteString(toCname)
	if msg.LatestVer {
		sb.WriteString(", latest")
	}
	if msg.Sync {
		sb.WriteString(", sync")
	}
	if msg.NonRecurs {
		sb.WriteString(", non-recurs")
	}
}
