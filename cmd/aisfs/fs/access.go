// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"os"
)

const (
	FilePermissionBits      os.FileMode = 0o644
	DirectoryPermissionBits os.FileMode = 0o755
)

type Owner struct {
	UID uint32
	GID uint32
}

type ModeBits struct {
	File      os.FileMode
	Directory os.FileMode
}
