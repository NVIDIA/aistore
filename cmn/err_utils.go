// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"syscall"
)

var (
	ErrSkip           = errors.New("skip")
	ErrStartupTimeout = errors.New("startup timeout")
)

///////////////////////////////////////////
// common error helpers and constructors //
///////////////////////////////////////////

func PathWalkErr(err error) error {
	if IsErrObjNought(err) {
		return nil
	}
	return fmt.Errorf("filepath-walk invoked with err: %v", err)
}

// as of 1.9 net/http does not appear to provide any better way..
func IsErrConnectionRefused(err error) (yes bool) {
	return errors.Is(err, syscall.ECONNREFUSED)
}

// TCP RST
func IsErrConnectionReset(err error) (yes bool) {
	return errors.Is(err, syscall.ECONNRESET) || IsErrBrokenPipe(err)
}

// Check if a given error is a broken-pipe one.
func IsErrBrokenPipe(err error) bool {
	return errors.Is(err, syscall.EPIPE)
}

func IsUnreachable(err error, status int) bool {
	return IsErrConnectionRefused(err) ||
		errors.Is(err, context.DeadlineExceeded) ||
		status == http.StatusRequestTimeout ||
		status == http.StatusServiceUnavailable
}
