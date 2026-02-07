//go:build !dsort

// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

func dropDsortConfig(c *Config) { c.Dsort = nil }
