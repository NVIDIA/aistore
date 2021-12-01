// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"os"

	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	FilePermissionBits      os.FileMode = cos.PermRWR
	DirectoryPermissionBits os.FileMode = cos.PermRWXRX
)

type Owner struct {
	UID uint32
	GID uint32
}

type ModeBits struct {
	File      os.FileMode
	Directory os.FileMode
}
