// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"os"
)

const (
	FilePermissionBits      os.FileMode = 0644
	DirectoryPermissionBits os.FileMode = 0755
)

type Owner struct {
	UID uint32
	GID uint32
}

type ModeBits struct {
	File      os.FileMode
	Directory os.FileMode
}
