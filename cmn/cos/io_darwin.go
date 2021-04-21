// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"os"
	"syscall"
)

func fflush(file *os.File) error {
	return syscall.Fsync(int(file.Fd()))
}
