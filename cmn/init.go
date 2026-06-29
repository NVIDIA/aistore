// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

// TODO: unify (as a struct of callbacks or similar) if and when adding another one
var (
	thisNodeName string
	cleanPathErr func(error)

	// is called at *runtime* - via Rom.Set() upon auth.intra_cluster.enabled transition (off<->on);
	// note that the startup (LoadConfig => GCO.Put) path intentionally does not call this callback
	onSignVerifyToggle func(enabled bool)
)

func init() {
	_initGCO()
}

func Init(thisName string, cleanPath func(error), signVerify func(enabled bool)) {
	thisNodeName = thisName
	cleanPathErr = cleanPath
	onSignVerifyToggle = signVerify
}
