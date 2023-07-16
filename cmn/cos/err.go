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
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"syscall"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

type (
	ErrNotFound struct {
		what string
	}
	ErrSignal struct {
		signal syscall.Signal
	}
	Errs struct {
		errs []error
		mu   sync.RWMutex
		cnt  int
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

// Errs
// add Unwrap() if need be

const maxErrs = 4

func (e *Errs) Add(err error) {
	debug.Assert(err != nil)
	e.mu.Lock()
	// first, check for duplication
	for _, added := range e.errs {
		if added.Error() == err.Error() {
			e.mu.Unlock()
			return
		}
	}
	e.cnt++
	if len(e.errs) < maxErrs {
		e.errs = append(e.errs, err)
	}
	e.mu.Unlock()
}

func (e *Errs) Cnt() (cnt int) {
	e.mu.RLock()
	cnt = e.cnt
	e.mu.RUnlock()
	return
}

func (e *Errs) JoinErr() (cnt int, err error) {
	e.mu.RLock()
	err = errors.Join(e.errs...) // up to maxErrs
	cnt = e.cnt
	e.mu.RUnlock()
	return
}

// Errs is an error
func (e *Errs) Error() (s string) {
	e.mu.RLock()
	errs, cnt := e.errs, e.cnt
	e.mu.RUnlock()
	if cnt > 0 {
		err := errs[0]
		if cnt > 1 {
			err = fmt.Errorf("%v (and %d more error%s)", err, cnt-1, Plural(cnt-1))
		}
		s = err.Error()
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

func isErrDNSLookup(err error) bool {
	_, ok := err.(*net.DNSError)
	return ok
}

func IsUnreachable(err error, status int) bool {
	return IsErrConnectionRefused(err) ||
		isErrDNSLookup(err) ||
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

// +log
func ExitLogf(f string, a ...any) {
	msg := fmt.Sprintf(fatalPrefix+f, a...)
	if flag.Parsed() {
		nlog.ErrorDepth(1, msg+"\n")
		nlog.FlushExit()
	}
	_exit(msg)
}

func ExitLog(a ...any) {
	msg := fatalPrefix + fmt.Sprint(a...)
	if flag.Parsed() {
		nlog.ErrorDepth(1, msg+"\n")
		nlog.FlushExit()
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
