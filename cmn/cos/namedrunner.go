// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

type (
	Runner interface {
		Name() string
		Run() error
		Stop(error)
	}
	// a strict subset of the core.Xact
	Stopper interface {
		Abort(error) bool
		IsAborted() bool
		IsDone() bool
		Name() string
		ChanAbort() <-chan error
	}
)
