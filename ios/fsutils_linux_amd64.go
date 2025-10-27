// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import "syscall"

const fstatatSyscall = syscall.SYS_NEWFSTATAT
