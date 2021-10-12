// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	SieMpathIDMismatch = (1 + iota) * 10
	SieTargetIDMismatch
	SieNotEqVMD
	SieMetaCorrupted
	SieFsDiffers
)

const (
	siePrefix = "storage integrity error: sie#"
	fmterr    = "[%s%d - for troubleshooting, see %s/blob/master/docs/troubleshooting.md]: %s"
)

type (
	ErrStorageIntegrity struct {
		Msg  string
		Code int // see enum above
	}
	ErrMpathNoDisks struct {
		Mi *MountpathInfo
	}
)

var ErrNoMountpaths = errors.New("no mountpaths")

func (e *ErrMpathNoDisks) Error() string { return fmt.Sprintf("%s has no disks", e.Mi) }

func (sie *ErrStorageIntegrity) Error() string {
	return fmt.Sprintf(fmterr, siePrefix, sie.Code, cmn.GitHubHome, sie.Msg)
}
