// Package env contains environment variables
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package env

const (
	MODE  = "MODE"      // e.g., MODE=debug make deploy ...
	DEBUG = "AIS_DEBUG" // e.g., AIS_DEBUG="fs=4,reb=4,transport=1" make deploy ...
)
