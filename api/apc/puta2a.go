// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

// api.PutApndArchArgs flags
const (
	ArchAppend = 1 << iota
	ArchAppendIfExist
)
