// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
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
	// swagger:model
	CopyBckMsg struct {
		Prepend   string `json:"prepend"`              // destination naming, as in: dest-obj-name = Prepend + source-obj-name
		Prefix    string `json:"prefix"`               // prefix to select matching _source_ objects or virtual directories
		DryRun    bool   `json:"dry_run"`              // visit all source objects, don't make any modifications
		Force     bool   `json:"force"`                // force running in presence of "limited coexistence" type conflicts
		LatestVer bool   `json:"latest-ver"`           // see also: QparamLatestVer, 'versioning.validate_warm_get', PrefetchMsg
		Sync      bool   `json:"synchronize"`          // see also: 'versioning.synchronize'
		NonRecurs bool   `json:"non-recurs,omitempty"` // do not copy contents of nested virtual subdirectories (see also: `apc.LsNoRecursion`, `apc.EvdMsg`)
	}

	// swagger:model
	Transform struct {
		Name     string       `json:"id,omitempty"`
		Pipeline []string     `json:"pipeline,omitempty"`
		Timeout  cos.Duration `json:"request_timeout,omitempty" swaggertype:"primitive,integer"`
	}

	// swagger:model
	TCBMsg struct {
		// Objname Extension ----------------------------------------------------------------------
		// - resulting object names will have this extension, if specified.
		// - if source bucket has two (or more) objects with the same base name but different extension,
		//   specifying this field might cause unintended override.
		// - this field might not be any longer required
		Ext cos.StrKVs `json:"ext"`

		CopyBckMsg
		Transform

		// optional user-defined number of concurrent workers:
		// * 0:  auto-computed (see xs/nwp.go, "media type", load.Advice)
		// * -1:
		//    - TCO: no additional workers (serial execution in the iterating goroutine);
		//    - TCB: no additional workers (one jogger per mountpath)
		NumWorkers int `json:"num-workers,omitempty"`

		ContinueOnError bool `json:"coer,omitempty"`
	}

	// multi-object
	// (cmn.TCOMsg = TCOMsg +  source and destination buckets)
	// swagger:model
	TCOMsg struct {
		TxnUUID string // (plstcx client; one control message)
		ListRange
		TCBMsg
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

func (msg *CopyBckMsg) Str(sb *cos.SB, fromCname, toCname, tag string) {
	sb.WriteString(tag)
	sb.WriteString(fromCname)
	sb.WriteString("=>")
	sb.WriteString(toCname)

	if msg.LatestVer || msg.Sync || msg.NonRecurs {
		sb.WriteString(", flags:")
		first := true
		if msg.LatestVer {
			sb.WriteString("latest")
			first = false
		}
		if msg.Sync {
			if !first {
				sb.WriteUint8(',')
			}
			sb.WriteString("sync")
			first = false
		}
		if msg.NonRecurs {
			if !first {
				sb.WriteUint8(',')
			}
			sb.WriteString("non-recurs")
		}
	}
}
