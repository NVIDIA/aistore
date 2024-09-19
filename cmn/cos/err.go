// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	ratomic "sync/atomic"
	"syscall"

	"github.com/NVIDIA/aistore/cmn/debug"
)

type (
	ErrNotFound struct {
		where fmt.Stringer
		what  string
	}
	ErrSignal struct {
		signal syscall.Signal
	}
	Errs struct {
		errs []error
		cnt  int64
		mu   sync.Mutex
	}
)

var (
	ErrQuantityUsage   = errors.New("invalid quantity, format should be '81%' or '1GB'")
	ErrQuantityPercent = errors.New("percent must be in the range (0, 100)")
	ErrQuantityBytes   = errors.New("value (bytes) must be non-negative")

	errQuantityNonNegative = errors.New("quantity should not be negative")
)

var ErrWorkChanFull = errors.New("work channel full")

var errBufferUnderrun = errors.New("buffer underrun")

// ErrNotFound

func NewErrNotFound(where fmt.Stringer, what string) *ErrNotFound {
	return &ErrNotFound{where: where, what: what}
}

func (e *ErrNotFound) Error() string {
	s := e.what + " does not exist"
	if e.where == nil {
		return s
	}
	return e.where.String() + ": " + s
}

func IsErrNotFound(err error) bool {
	_, ok := err.(*ErrNotFound)
	return ok
}

//
// gen-purpose not-finding-anything: objects, directories, xactions, nodes, ...
//

// NOTE: compare with cmn.IsErrObjNought() that also includes lmeta-not-found et al.
func IsNotExist(err error, ecode int) bool {
	if ecode == http.StatusNotFound || IsErrNotFound(err) {
		return true
	}
	return os.IsNotExist(err) // unwraps for fs.ErrNotExist
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
	if len(e.errs) < maxErrs {
		e.errs = append(e.errs, err)
		ratomic.StoreInt64(&e.cnt, int64(len(e.errs)))
	}
	e.mu.Unlock()
}

func (e *Errs) Cnt() int { return int(ratomic.LoadInt64(&e.cnt)) }

func (e *Errs) JoinErr() (cnt int, err error) {
	if cnt = e.Cnt(); cnt > 0 {
		e.mu.Lock()
		err = errors.Join(e.errs...) // up to maxErrs
		e.mu.Unlock()
	}
	return
}

// Errs is an error
func (e *Errs) Error() (s string) {
	var (
		err error
		cnt = e.Cnt()
	)
	if cnt == 0 {
		return
	}
	e.mu.Lock()
	if cnt = len(e.errs); cnt > 0 {
		err = e.errs[0]
	}
	e.mu.Unlock()
	if err == nil {
		return // unlikely
	}
	if cnt > 1 {
		err = fmt.Errorf("%v (and %d more error%s)", err, cnt-1, Plural(cnt-1))
	}
	s = err.Error()
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

func IsErrDNSLookup(err error) bool {
	if _, ok := err.(*net.DNSError); ok {
		return ok
	}
	wrapped := &net.DNSError{}
	return errors.As(err, &wrapped)
}

func IsUnreachable(err error, status int) bool {
	return IsErrConnectionRefused(err) ||
		IsErrDNSLookup(err) ||
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
