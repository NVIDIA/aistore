// Package env contains environment variables
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package env

// See also:
// - cmn/debug/debug_on.go for the list off all package names, including `fs` etc. (comment below)
// - docs/development.md for user guide and examples

const (
	MODE  = "MODE"      // e.g., MODE=debug make deploy ...
	DEBUG = "AIS_DEBUG" // e.g., AIS_DEBUG="fs=4,reb=4,transport=1" make deploy ...
)
