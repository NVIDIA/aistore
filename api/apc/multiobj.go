// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// (common for all multi-object operations)
type (
	// List of object names _or_ a template specifying { optional Prefix, zero or more Ranges }
	ListRange struct {
		Template string   `json:"template"`
		ObjNames []string `json:"objnames"`
	}
)

// [NOTE]
// - empty `ListRange{}` implies operating on an entire bucket ("all objects in the source bucket")
// - in re `LatestVer`, see related: `QparamLatestVer`, 'versioning.validate_warm_get'

func (lrm *ListRange) IsList() bool      { return len(lrm.ObjNames) > 0 }
func (lrm *ListRange) HasTemplate() bool { return lrm.Template != "" }

func (lrm *ListRange) Str(sb *strings.Builder, isPrefix bool) {
	switch {
	case isPrefix:
		if cos.MatchAll(lrm.Template) {
			return
		}
		sb.WriteString("prefix: ")
		sb.WriteString(lrm.Template)
	case lrm.IsList():
		// TODO: ref
		if l := len(lrm.ObjNames); l > 3 {
			s := fmt.Sprintf("list(%d names): [%s, ...]", l, lrm.ObjNames[0])
			sb.WriteString(s)
		} else {
			s := fmt.Sprintf("list: %v", lrm.ObjNames)
			sb.WriteString(s)
		}
	default:
		sb.WriteString("template: ")
		sb.WriteString(lrm.Template)
	}
}

// prefetch
type PrefetchMsg struct {
	ListRange
	BlobThreshold   int64 `json:"blob-threshold"` // when greater than threshold prefetch using blob-downloader; otherwise cold GET
	NumWorkers      int   `json:"num-workers"`    // number of concurrent workers; 0 - number of mountpaths (default); (-1) none
	ContinueOnError bool  `json:"coer"`           // ignore non-critical errors, keep going
	LatestVer       bool  `json:"latest-ver"`     // when true & in-cluster: check with remote whether (deleted | version-changed)
}

func (msg *PrefetchMsg) Str(isPrefix bool) string {
	var sb strings.Builder
	sb.Grow(80)
	msg.ListRange.Str(&sb, isPrefix)
	if msg.BlobThreshold > 0 {
		sb.WriteString(", blob-threshold: ")
		sb.WriteString(cos.ToSizeIEC(msg.BlobThreshold, 0))
	}
	if msg.NumWorkers > 0 {
		sb.WriteString(", workers: ")
		sb.WriteString(strconv.Itoa(msg.NumWorkers))
	}
	if msg.LatestVer {
		sb.WriteString(", latest")
	}
	return sb.String()
}

// ArchiveMsg contains the parameters (all except the destination bucket)
// for archiving mutiple objects as one of the supported archive.FileExtensions types
// at the specified (bucket) destination.
// See also: api.PutApndArchArgs
// --------------------  terminology   ---------------------
// here and elsewhere "archive" is any (.tar, .tgz/.tar.gz, .zip, .tar.lz4) formatted object.
// [NOTE] see cmn/api for cmn.ArchiveMsg (that also contains ToBck)
type ArchiveMsg struct {
	TxnUUID     string `json:"-"`        // internal use
	FromBckName string `json:"-"`        // ditto
	ArchName    string `json:"archname"` // one of the archive.FileExtensions
	Mime        string `json:"mime"`     // user-specified mime type (NOTE: takes precedence if defined)
	ListRange
	BaseNameOnly    bool `json:"bnonly"` // only extract the base name of objects as names of archived objects
	InclSrcBname    bool `json:"isbn"`   // include source bucket name into the names of archived objects
	AppendIfExists  bool `json:"aate"`   // adding a list or a range of objects to an existing archive
	ContinueOnError bool `json:"coer"`   // on err, keep running arc xaction in a any given multi-object transaction
}

// multi-object copy & transform
// [NOTE] see cmn/api for cmn.TCOMsg (that also contains ToBck); see also TCBMsg
type TCOMsg struct {
	TxnUUID string // (plstcx client, internal use)
	TCBMsg
	ListRange
	NumWorkers      int  `json:"num-workers"` // user-defined num concurrent workers; 0 - number of mountpaths (default); (-1) none
	ContinueOnError bool `json:"coer"`
}
