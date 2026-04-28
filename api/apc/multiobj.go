// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"fmt"
	"strconv"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// (common for all multi-object operations)
type (
	// ListRange selects the objects a multi-object operation will act
	// on. Three modes:
	//   - `objnames` set: operate on exactly those named objects
	//   - `template` set: operate on objects matching the range
	//     template (e.g. `"shard-{001..999}.tar"`)
	//   - both empty: operate on all objects in the source bucket
	ListRange struct {
		// Range template selecting objects by name (e.g.
		// `"shard-{001..999}.tar"`).
		Template string `json:"template"` // +gen:optional
		// Explicit list of object names.
		ObjNames []string `json:"objnames"` // +gen:optional
	}
	// EvdMsg parameterizes multi-object delete and evict ("evd")
	// operations. Objects are selected via ListRange. For evict, only
	// the in-cluster (cached) copy is removed; for delete, the object
	// is removed from both the cluster and (when applicable) the
	// remote backend.
	EvdMsg struct {
		ListRange
		// Number of concurrent workers:
		//   - `0`: Auto-computed.
		//   - `-1`: Serial execution in the iterating goroutine.
		//   - `>0`: Exact worker count.
		NumWorkers int `json:"num-workers,omitempty"` // +gen:optional
		// Soft-error semantics for per-object retrieval or processing
		// failures. Support varies by job.
		ContinueOnError bool `json:"coer,omitempty"` // +gen:optional
		// Do not recurse into nested virtual subdirectories.
		NonRecurs bool `json:"non-recurs,omitempty"` // +gen:optional
	}
)

// [NOTE]
// - empty `ListRange{}` implies operating on an entire bucket ("all objects in the source bucket")
// - in re `LatestVer`, see related: `QparamLatestVer`, 'versioning.validate_warm_get'

func (lrm *ListRange) IsList() bool      { return len(lrm.ObjNames) > 0 }
func (lrm *ListRange) HasTemplate() bool { return lrm.Template != "" }

func (lrm *ListRange) Str(sb *cos.SB, isPrefix bool) {
	switch {
	case isPrefix:
		if cos.MatchAll(lrm.Template) {
			return
		}
		sb.WriteString("prefix:")
		sb.WriteString(lrm.Template)
	case lrm.IsList():
		// TODO: ref
		if l := len(lrm.ObjNames); l > 3 {
			s := fmt.Sprintf("list(%d names):[%s, ...]", l, lrm.ObjNames[0])
			sb.WriteString(s)
		} else {
			s := fmt.Sprintf("list:%v", lrm.ObjNames)
			sb.WriteString(s)
		}
	default:
		sb.WriteString("template:")
		sb.WriteString(lrm.Template)
	}
}

// PrefetchMsg parameterizes multi-object prefetch from a remote bucket
// into the cluster. Objects are selected via ListRange. Objects already
// cached in-cluster are skipped unless `latest-ver` is set, in which
// case their version is revalidated against the backend.
type PrefetchMsg struct {
	ListRange
	// Object-size threshold (bytes): objects larger than this are
	// fetched via the blob downloader; smaller objects are fetched as
	// a single cold GET. `0` selects the server default.
	BlobThreshold int64 `json:"blob-threshold"` // +gen:optional
	// Chunk size for blob-downloads started by prefetch
	BlobChunkSize int64 `json:"blob-chunk-size"` // +gen:optional
	// Number of workers for each blob-download started by prefetch; auto-computed when zero
	BlobNumWorkers int `json:"blob-num-workers"` // +gen:optional
	// Number of concurrent workers:
	//   - `0`: Auto-computed.
	//   - `-1`: No additional workers.
	//   - `>0`: Exact worker count.
	NumWorkers int `json:"num-workers"` // +gen:optional
	// Soft-error semantics for per-object retrieval or processing
	// failures. Support varies by job.
	ContinueOnError bool `json:"coer"` // +gen:optional
	// For in-cluster objects, revalidate against the remote backend
	// and refetch if the remote copy was deleted or its version
	// changed. Overrides the bucket's `versioning.validate_warm_get`.
	LatestVer bool `json:"latest-ver"` // +gen:optional
	// Do not recurse into nested virtual subdirectories.
	NonRecurs bool `json:"non-recurs,omitempty"` // +gen:optional
}

// +ctlmsg
func (msg *PrefetchMsg) Str(isPrefix bool) string {
	var sb cos.SB
	sb.Init(80)
	msg.ListRange.Str(&sb, isPrefix)
	if msg.BlobThreshold > 0 {
		msg.delim(&sb)
		sb.WriteString("blob-threshold:")
		sb.WriteString(cos.IEC(msg.BlobThreshold, 0))
	}
	if msg.BlobChunkSize > 0 {
		msg.delim(&sb)
		sb.WriteString("blob-chunk-size:")
		sb.WriteString(cos.IEC(msg.BlobChunkSize, 0))
	}
	if msg.BlobNumWorkers != 0 {
		msg.delim(&sb)
		sb.WriteString("blob-workers:")
		sb.WriteString(strconv.Itoa(msg.BlobNumWorkers))
	}
	if msg.NumWorkers > 0 {
		msg.delim(&sb)
		sb.WriteString("workers:")
		sb.WriteString(strconv.Itoa(msg.NumWorkers))
	}
	if msg.LatestVer {
		msg.delim(&sb)
		sb.WriteString("latest")
	}
	if msg.NonRecurs {
		msg.delim(&sb)
		sb.WriteString("non-recurs")
	}
	return sb.String()
}

func (*PrefetchMsg) delim(sb *cos.SB) {
	if sb.Len() > 0 {
		sb.WriteString(", ")
	}
}

// ArchiveMsg parameterizes archiving multiple objects into a single
// archive ("shard") object - one of `.tar`, `.tgz` / `.tar.gz`, `.zip`,
// or `.tar.lz4`. Source objects are selected via ListRange. See
// cmn.ArchiveMsg for the full request that wraps this with a
// destination bucket.
//
// For the single-object append variant, see api.PutApndArchArgs.
type ArchiveMsg struct {
	TxnUUID     string `json:"-"` // Internal use only
	FromBckName string `json:"-"` // Internal use only
	// Destination archive object name, including a supported archive
	// extension (`.tar`, `.tgz`, `.tar.gz`, `.zip`, `.tar.lz4`).
	ArchName string `json:"archname"`
	// Override the archive MIME type. When set, takes precedence over
	// the extension inferred from `archname`.
	Mime string `json:"mime"` // +gen:optional
	ListRange
	// Store archived entries under each source object's base name
	// only, stripping any virtual directory prefix.
	BaseNameOnly bool `json:"bnonly"` // +gen:optional
	// Prefix archived entry names with the source bucket name.
	InclSrcBname bool `json:"isbn"` // +gen:optional
	// Append to the destination archive if it already exists.
	AppendIfExists bool `json:"aate"` // +gen:optional
	// Soft-error semantics for per-entry retrieval or processing
	// failures. Support varies by job.
	ContinueOnError bool `json:"coer"` // +gen:optional
	// Do not archive contents of nested virtual subdirectories.
	NonRecurs bool `json:"non-recurs,omitempty"` // +gen:optional
}
