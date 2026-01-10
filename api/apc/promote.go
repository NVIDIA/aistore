// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import "github.com/NVIDIA/aistore/cmn/cos"

// common part that's used in `api.PromoteArgs` and `PromoteParams`(server side), both
// swagger:model
type PromoteArgs struct {
	DaemonID  string `json:"tid,omitempty"` // target ID
	SrcFQN    string `json:"src,omitempty"` // source file or directory (must be absolute pathname)
	ObjName   string `json:"obj,omitempty"` // destination object name or prefix
	Recursive bool   `json:"rcr,omitempty"` // recursively promote nested dirs
	// once successfully promoted:
	OverwriteDst bool `json:"ovw,omitempty"` // overwrite destination
	DeleteSrc    bool `json:"dls,omitempty"` // remove source when (and after) successfully promoting
	// explicit request _not_ to treat the source as a potential file share
	// and _not_ to try to auto-detect if it is;
	// (auto-detection takes time, etc.)
	SrcIsNotFshare bool `json:"notshr,omitempty"` // the source is not a file share equally accessible by all targets
}

func (msg *PromoteArgs) Str(sb *cos.SB) {
	sb.WriteString("src:")
	sb.WriteString(msg.SrcFQN)
	sb.WriteString(", dst:")
	sb.WriteString(msg.ObjName)

	if msg.DaemonID != "" {
		sb.WriteString(", node:")
		sb.WriteString(msg.DaemonID)
	}
	sb.WriteString(", flags:")
	if msg.Recursive {
		sb.WriteString("recurs")
	} else {
		sb.WriteString("non-recurs")
	}
	if msg.OverwriteDst {
		sb.WriteUint8(',')
		sb.WriteString("overwrite")
	}
	if msg.DeleteSrc {
		sb.WriteUint8(',')
		sb.WriteString("delete-src")
	}
	if msg.SrcIsNotFshare {
		sb.WriteUint8(',')
		sb.WriteString("not-file-share")
	}
}
