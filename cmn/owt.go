// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

// Object Write transaction type
type OWT int

const (
	OwtPut OWT = iota
	OwtMigrate
	OwtFinalize

	// cold (ie., remote) GET variations
	OwtGetTryLock // if !try-lock(exclusive) { return error }
	OwtGetLock    // lock(exclusive)
	OwtGetUpgLock // upgrade(read-lock)
)
