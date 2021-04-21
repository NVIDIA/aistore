// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"os"
	"syscall"
)

// 1) this is slightly cheaper than `fsync` but still not good enough;
// 2) file.Close() is implementation dependent as far as flushing dirty buffers;
// 3) journaling filesystems, such as xfs, generally provide better guarantees but, again, not 100%
// 4) see discussion at https://lwn.net/Articles/788938;
// 5) going forward, some sort of `rename_barrier()` would be a much better alternative
func fflush(file *os.File) error {
	return syscall.Fdatasync(int(file.Fd()))
}
