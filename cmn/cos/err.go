// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"syscall"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/debug"
)

type (
	ErrSignal struct {
		signal syscall.Signal
	}
	ErrValue struct {
		atomic.Value
		cnt atomic.Int64
	}
)

//////////////
// ErrValue //
//////////////

func (ea *ErrValue) Store(err error) {
	if ea.cnt.Inc() == 1 {
		ea.Value.Store(err)
	}
}

// NOTE: hide atomic.Value.Load() - must use Err() below
func (*ErrValue) Load() interface{} { debug.Assert(false); return nil }

func (ea *ErrValue) _load() (err error) {
	if x := ea.Value.Load(); x != nil {
		err = x.(error)
		debug.Assert(err != nil)
	}
	return
}

func (ea *ErrValue) IsNil() bool { return ea.cnt.Load() == 0 }

func (ea *ErrValue) Err() (err error) {
	err = ea._load()
	if err == nil {
		return
	}
	if cnt := ea.cnt.Load(); cnt > 1 {
		err = fmt.Errorf("%w (cnt=%d)", err, cnt)
	}
	return
}

////////////////////////
// IS-syscall helpers //
////////////////////////

// likely out of socket descriptors
func IsErrConnectionNotAvail(err error) (yes bool) {
	return errors.Is(err, syscall.EADDRNOTAVAIL)
}

// retriable conn errs
func IsErrConnectionRefused(err error) (yes bool) { return errors.Is(err, syscall.ECONNREFUSED) }
func IsErrConnectionReset(err error) (yes bool)   { return errors.Is(err, syscall.ECONNRESET) }
func IsErrBrokenPipe(err error) (yes bool)        { return errors.Is(err, syscall.EPIPE) }

func IsRetriableConnErr(err error) (yes bool) {
	return IsErrConnectionRefused(err) || IsErrConnectionReset(err) || IsErrBrokenPipe(err)
}

func IsErrOOS(err error) bool {
	return errors.Is(err, syscall.ENOSPC)
}

func IsUnreachable(err error, status int) bool {
	return IsErrConnectionRefused(err) ||
		errors.Is(err, context.DeadlineExceeded) ||
		status == http.StatusRequestTimeout ||
		status == http.StatusServiceUnavailable ||
		IsEOF(err) ||
		status == http.StatusBadGateway
}

///////////////
// ErrSignal //
///////////////

// https://tldp.org/LDP/abs/html/exitcodes.html
func (e *ErrSignal) ExitCode() int               { return 128 + int(e.signal) }
func NewSignalError(s syscall.Signal) *ErrSignal { return &ErrSignal{signal: s} }
func (e *ErrSignal) Error() string               { return fmt.Sprintf("Signal %d", e.signal) }

//////////////////////////
// Abnormal Termination //
//////////////////////////

// Exitf writes formatted message to STDERR and exits with non-zero status code.
func Exitf(f string, a ...interface{}) {
	msg := fmt.Sprintf("FATAL ERROR: "+f+"\n", a...)
	fmt.Fprint(os.Stderr, msg)
	os.Exit(1)
}

// ExitLogf is glog + Exitf.
func ExitLogf(f string, a ...interface{}) {
	msg := fmt.Sprintf("FATAL ERROR: "+f+"\n", a...)
	glog.ErrorDepth(1, msg)
	glog.Flush()
	fmt.Fprint(os.Stderr, msg)
	os.Exit(1)
}
