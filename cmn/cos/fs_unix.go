// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import "syscall"

// syscall to check that path exists (see bench/lstat)
func Stat(path string) error {
	var sys syscall.Stat_t
	return syscall.Stat(path, &sys)
}
