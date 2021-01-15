// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"os"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	FilePermissionBits      os.FileMode = cmn.PermRWR
	DirectoryPermissionBits os.FileMode = cmn.PermRWXRX
)

type Owner struct {
	UID uint32
	GID uint32
}

type ModeBits struct {
	File      os.FileMode
	Directory os.FileMode
}
