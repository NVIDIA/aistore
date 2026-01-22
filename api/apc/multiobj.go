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
	// List of object names _or_ a template specifying { optional Prefix, zero or more Ranges }
	// swagger:model
	ListRange struct {
		Template string   `json:"template"`
		ObjNames []string `json:"objnames"`
	}
	// swagger:model
	EvdMsg struct {
		ListRange
		NumWorkers      int  `json:"num-workers,omitempty"` // number of concurrent workers; 0 - number of mountpaths (default); (-1) none
		ContinueOnError bool `json:"coer,omitempty"`        // ignore non-critical errors, keep going
		NonRecurs       bool `json:"non-recurs,omitempty"`  // do not evict (delete) nested subdirs (see also: `apc.LsNoRecursion`, `apc.CopyBckMsg`)
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

// prefetch
// swagger:model
type PrefetchMsg struct {
	ListRange
	BlobThreshold   int64 `json:"blob-threshold"`       // when greater than threshold prefetch using blob-downloader; otherwise cold GET
	NumWorkers      int   `json:"num-workers"`          // number of concurrent workers; 0 - number of mountpaths (default); (-1) none
	ContinueOnError bool  `json:"coer"`                 // ignore non-critical errors, keep going
	LatestVer       bool  `json:"latest-ver"`           // when true & in-cluster: check with remote whether (deleted | version-changed)
	NonRecurs       bool  `json:"non-recurs,omitempty"` // do not prefetch nested subdirs (see also: `apc.LsNoRecursion`, `apc.CopyBckMsg`, `apc.EvdMsg`)
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

// rechunk
// swagger:model
// Rechunk transforms object storage format (monolithic <-> chunked).
// By default, rechunk operates only on in-cluster (cached) objects - it does not
// fetch objects from remote backends. Use `SyncRemote=true` to also update remote storage.
type RechunkMsg struct {
	ObjSizeLimit int64  `json:"objsize-limit"` // 0: disable chunking (restore existing chunks to monolithic); > 0: chunk objects >= this size
	ChunkSize    int64  `json:"chunk-size"`    // size of each chunk
	Prefix       string `json:"prefix"`        // only rechunk objects with this prefix
	SyncRemote   bool   `json:"sync-remote"`   // if true, also write rechunked objects to remote backend (default: false, local-only)
}

// ArchiveMsg contains the parameters (all except the destination bucket)
// for archiving multiple objects as one of the supported archive.FileExtensions types
// at the specified (bucket) destination.
// See also: api.PutApndArchArgs
// --------------------  terminology   ---------------------
// here and elsewhere "archive" is any (.tar, .tgz/.tar.gz, .zip, .tar.lz4) formatted object.
// [NOTE] see cmn/api for cmn.ArchiveMsg (that also contains ToBck)
// swagger:model
type ArchiveMsg struct {
	TxnUUID     string `json:"-"`        // internal use
	FromBckName string `json:"-"`        // ditto
	ArchName    string `json:"archname"` // one of the archive.FileExtensions
	Mime        string `json:"mime"`     // user-specified mime type (NOTE: takes precedence if defined)
	ListRange
	BaseNameOnly    bool `json:"bnonly"`               // only extract the base name of objects as names of archived objects
	InclSrcBname    bool `json:"isbn"`                 // include source bucket name into the names of archived objects
	AppendIfExists  bool `json:"aate"`                 // adding a list or a range of objects to an existing archive
	ContinueOnError bool `json:"coer"`                 // on err, keep running arc xaction in a any given multi-object transaction
	NonRecurs       bool `json:"non-recurs,omitempty"` // do not archive contents of nested virtual subdirectories (see also: `apc.LsNoRecursion`, `apc.CopyBckMsg`)
}
