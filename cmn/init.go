// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

var (
	thisNodeName string
	cleanPathErr func(error)
)

func init() {
	_initGCO()
}

func Init(a string, b func(error)) {
	thisNodeName = a
	cleanPathErr = b
}
