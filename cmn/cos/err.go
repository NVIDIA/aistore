// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"syscall"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/debug"
)

type (
	ErrNotFound struct {
		what string
	}
	ErrSignal struct {
		signal syscall.Signal
	}
	ErrValue struct {
		atomic.Value
		cnt atomic.Int64
	}
)

// ErrNotFound

func NewErrNotFound(format string, a ...any) *ErrNotFound {
	return &ErrNotFound{fmt.Sprintf(format, a...)}
}

func (e *ErrNotFound) Error() string { return e.what + " does not exist" }

func IsErrNotFound(err error) bool {
	_, ok := err.(*ErrNotFound)
	return ok
}

// ErrValue

func (ea *ErrValue) Store(err error) {
	if ea.cnt.Add(1) == 1 {
		ea.Value.Store(err)
	}
}

// NOTE: hide atomic.Value.Load() - must use Err() below
func (*ErrValue) Load() any { debug.Assert(false); return nil }

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

//
// IS-syscall helpers
//

func UnwrapSyscallErr(err error) error {
	if syscallErr, ok := err.(*os.SyscallError); ok {
		return syscallErr.Unwrap()
	}
	return nil
}

func IsErrSyscallTimeout(err error) bool {
	syscallErr, ok := err.(*os.SyscallError)
	return ok && syscallErr.Timeout()
}

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

//
// ErrSignal
//

// https://tldp.org/LDP/abs/html/exitcodes.html
func (e *ErrSignal) ExitCode() int               { return 128 + int(e.signal) }
func NewSignalError(s syscall.Signal) *ErrSignal { return &ErrSignal{signal: s} }
func (e *ErrSignal) Error() string               { return fmt.Sprintf("Signal %d", e.signal) }

//
// Abnormal Termination
//

const fatalPrefix = "FATAL ERROR: "

func Exitf(f string, a ...any) {
	msg := fmt.Sprintf(fatalPrefix+f, a...)
	_exit(msg)
}

// +glog
func ExitLogf(f string, a ...any) {
	msg := fmt.Sprintf(fatalPrefix+f, a...)
	if flag.Parsed() {
		glog.ErrorDepth(1, msg+"\n")
		glog.Flush()
	}
	_exit(msg)
}

func ExitLog(a ...any) {
	msg := fatalPrefix + fmt.Sprint(a...)
	if flag.Parsed() {
		glog.ErrorDepth(1, msg+"\n")
		glog.Flush()
	}
	_exit(msg)
}

func _exit(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

//
// url.Error
//

func Err2ClientURLErr(err error) (uerr *url.Error) {
	if e, ok := err.(*url.Error); ok {
		uerr = e
	}
	return
}

func IsErrClientURLTimeout(err error) bool {
	uerr := Err2ClientURLErr(err)
	return uerr != nil && uerr.Timeout()
}
