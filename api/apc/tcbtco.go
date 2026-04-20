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
	// CopyBckMsg is the shared knobs subset used by offline
	// bucket-to-bucket and multi-object copy/transform.
	CopyBckMsg struct {
		// Prefix prepended to destination object names.
		Prepend string `json:"prepend"` // +gen:optional
		// Select only source objects (or virtual subdirectories)
		// whose names start with this prefix.
		Prefix string `json:"prefix"` // +gen:optional
		// Visit all source objects but make no modifications (preview).
		DryRun bool `json:"dry_run"` // +gen:optional
		// Force the operation even when other xactions with limited
		// coexistence are already running.
		Force bool `json:"force"` // +gen:optional
		// For remote sources, revalidate each object's latest version
		// before copying. Overrides the bucket's
		// `versioning.validate_warm_get`.
		LatestVer bool `json:"latest-ver"` // +gen:optional
		// Synchronize destination with source by deleting destination
		// objects no longer present at source. Overrides the bucket's
		// `versioning.synchronize`.
		Sync bool `json:"synchronize"` // +gen:optional
		// Do not recurse into nested virtual subdirectories.
		NonRecurs bool `json:"non-recurs,omitempty"` // +gen:optional
	}

	// Transform selects an ETL transformation (or pipeline) to apply
	// to each object during copy/transform. All fields are interpreted
	// only for the ETL actions (etl-bck, etl-objects, etl-listrange);
	// ignored for the plain copy actions (copy-bck, copy-objects,
	// copy-listrange).
	Transform struct {
		// Name of the ETL to apply as the primary transform.
		Name string `json:"id,omitempty"` // +gen:optional
		// Additional ETL names, applied in order after the primary.
		Pipeline []string `json:"pipeline,omitempty"` // +gen:optional
		// Per-object transform request timeout (Go duration, e.g. `"30s"`).
		Timeout cos.Duration `json:"request_timeout,omitempty"` // +gen:optional
	}

	// TCBMsg parameterizes offline bucket-to-bucket copy and transform
	// (TCB). The multi-object variant is cmn.TCOMsg.
	TCBMsg struct {
		// Destination object extension remap (e.g. `{"jpg": "png"}`).
		// Warning: if source objects share a base name but differ only
		// in extension, remapping may cause overwrites at the
		// destination. May be deprecated in a future release.
		Ext cos.StrKVs `json:"ext"` // +gen:optional

		CopyBckMsg
		Transform

		// Number of concurrent workers:
		//   - `0`: Auto-computed.
		//   - `-1`: Run serially (TCO) or one worker per mountpath (TCB).
		//   - `>0`: Exact worker count.
		NumWorkers int `json:"num-workers,omitempty"` // +gen:optional

		// Soft-error semantics for per-object retrieval or processing
		// failures. Support varies by job.
		ContinueOnError bool `json:"coer,omitempty"` // +gen:optional
	}

	// TCOMsg is the multi-object copy & transform payload. Source
	// objects are selected via ListRange. See cmn.TCOMsg for the full
	// request that wraps this with a destination bucket.
	TCOMsg struct {
		TxnUUID string // Internal plstcx client transaction ID (one control message).
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
