// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cos

type BitFlags uint64

func (f BitFlags) Set(flags BitFlags) BitFlags {
	return f | flags
}

func (f BitFlags) Clear(flags BitFlags) BitFlags {
	return f &^ flags
}

func (f BitFlags) IsSet(flags BitFlags) bool {
	return f&flags == flags
}

func (f BitFlags) IsAnySet(flags BitFlags) bool {
	return f&flags != 0
}
