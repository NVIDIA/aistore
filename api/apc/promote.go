// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// PromoteRoot is the only allowed source tree for promote commands.
const PromoteRoot = "/var/lib/ais/promote"

type errInvalidPromoteSource struct {
	src    string
	detail string
}

func (e *errInvalidPromoteSource) Error() string {
	msg := "invalid promote source"
	if e.src != "" {
		msg += fmt.Sprintf(" %q", e.src)
	}
	if e.detail != "" {
		msg += ": " + e.detail
	}
	return msg
}

// common part that's used in `api.PromoteArgs` and `PromoteParams`(server side), both
type PromoteArgs struct {
	DaemonID  string `json:"tid,omitempty"` // target ID
	SrcFQN    string `json:"src,omitempty"` // source file or directory (must be absolute path under PromoteRoot)
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

// ValidatePromoteSource checks that src is an absolute path under PromoteRoot.
// Existing symlinks under PromoteRoot are allowed and resolved by the OS when
// the target reads the source.
func ValidatePromoteSource(src string) error {
	if src == "" {
		return &errInvalidPromoteSource{detail: "pathname is empty"}
	}
	if !filepath.IsAbs(src) {
		return &errInvalidPromoteSource{src: src, detail: "must be an absolute path"}
	}
	if err := cos.ValidateRname(src); err != nil {
		return &errInvalidPromoteSource{src: src, detail: "must not contain '.' or '..' path elements"}
	}
	if src != PromoteRoot && !strings.HasPrefix(src, PromoteRoot+"/") {
		return &errInvalidPromoteSource{src: src, detail: "must be under " + PromoteRoot}
	}
	return nil
}

// ValidatePromote ensures a valid promote request including name, args, and source path.
func ValidatePromote(name string, args *PromoteArgs) (string, error) {
	if args == nil {
		return "", errors.New("missing promote args")
	}
	src := name
	if args.SrcFQN != "" {
		if name != "" && name != args.SrcFQN {
			return "", fmt.Errorf("conflicting promote source: %q vs %q", name, args.SrcFQN)
		}
		if src == "" {
			src = args.SrcFQN
		}
	}
	if err := ValidatePromoteSource(src); err != nil {
		return "", err
	}
	if err := cos.ValidatePrefix("object name or prefix", args.ObjName); err != nil {
		return "", err
	}
	return src, nil
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
