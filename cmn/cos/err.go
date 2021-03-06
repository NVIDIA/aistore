// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
	"syscall"
)

type (
	ErrSignal struct {
		signal syscall.Signal
	}
)

///////////////
// ErrSignal //
///////////////

// https://tldp.org/LDP/abs/html/exitcodes.html
func (e *ErrSignal) ExitCode() int               { return 128 + int(e.signal) }
func NewSignalError(s syscall.Signal) *ErrSignal { return &ErrSignal{signal: s} }
func (e *ErrSignal) Error() string               { return fmt.Sprintf("Signal %d", e.signal) }
