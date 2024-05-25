// Package aisloader
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

func _fastLog2(c uint64) uint {
	for i := uint(0); ; {
		if c >>= 1; c == 0 {
			return i
		}
		i++
	}
}

func fastLog2Ceil(c uint64) uint {
	if c == 0 {
		return 0
	}
	return _fastLog2(c-1) + 1
}
