// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "sync/atomic"

// TODO: unify (as a struct of callbacks or similar) if and when adding another one
var (
	thisNodeName string
	cleanPathErr func(error)

	// is called at *runtime* - via Rom.Set() upon auth.intra_cluster.request_auth transition (off<->on);
	// note that the startup (LoadConfig => GCO.Put) path intentionally does not call this callback
	onSignVerifyToggle func(enabled bool)

	// sparsely log assorted type codes
	errLog struct {
		nbusy atomic.Int64
	}
)

func init() {
	_initGCO()
}

func Init(thisName string, cleanPath func(error), signVerify func(enabled bool)) {
	thisNodeName = thisName
	cleanPathErr = cleanPath
	onSignVerifyToggle = signVerify
}
