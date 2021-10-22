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
	SieMpathNotFound
)

const (
	siePrefix = "storage integrity error sie#"
)

type (
	ErrStorageIntegrity struct {
		Msg  string
		Code int // Sie* enum above
	}
	ErrMpathNoDisks struct {
		Mi *MountpathInfo
	}
)

var ErrNoMountpaths = errors.New("no mountpaths")

func (e *ErrMpathNoDisks) Error() string { return fmt.Sprintf("%s has no disks", e.Mi) }

func (sie *ErrStorageIntegrity) Error() string {
	err := fmt.Errorf(cmn.FmtErrIntegrity, siePrefix, sie.Code, cmn.GitHubHome)
	return fmt.Sprintf("%v: %s", err, sie.Msg)
}
