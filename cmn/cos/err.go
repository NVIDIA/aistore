// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
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
	ErrAlreadyExists struct {
		where fmt.Stringer
		what  string
	}
	ErrSignal struct {
		signal syscall.Signal
	}
	Errs struct {
		errs []error
		cnt  int64
		cap  int
		mu   sync.Mutex
	}

	// background:
	// - normally, keeping objects under their original names
	// - FNTL excepted (see core/lom)
	ErrMv struct {
		// - type 1: mv readme aaa/bbb, where destination aaa/bbb[/ccc/...] is a virtual directory
		// - type 2 (a.k.a. ENOTDIR): mv readme aaa/bbb/ccc, where destination aaa/bbb is (or contains) a file
		ty int
	}
)

var (
	ErrQuantityUsage   = errors.New("invalid quantity, format should be '81%' or '1GB'")
	ErrQuantityPercent = errors.New("percent must be in the range (0, 100)")
	ErrQuantityBytes   = errors.New("value (bytes) must be non-negative")

	errQuantityNonNegative = errors.New("quantity should not be negative")
)

var errBufferUnderrun = errors.New("buffer underrun")

// ErrNotFound

func NewErrNotFound(where fmt.Stringer, what string) *ErrNotFound {
	return &ErrNotFound{where: where, what: what}
}

func (e *ErrNotFound) Error() string {
	s := e.what
	if !strings.Contains(s, "not exist") && !strings.Contains(s, "not found") {
		s += " does not exist"
	}
	if e.where == nil {
		return s
	}
	return e.where.String() + ": " + s
}

func IsErrNotFound(err error) bool {
	_, ok := err.(*ErrNotFound)
	return ok
}

// ErrAlreadyExists

func NewErrAlreadyExists(where fmt.Stringer, what string) *ErrAlreadyExists {
	return &ErrAlreadyExists{where: where, what: what}
}

func (e *ErrAlreadyExists) Error() string {
	s := e.what + " already exists"
	if e.where == nil {
		return s
	}
	return e.where.String() + ": " + s
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

// Errs is a thread-safe collection of errors

const defaultMaxErrs = 8

func NewErrs(maxErrs ...int) Errs {
	capacity := defaultMaxErrs
	if len(maxErrs) > 0 && maxErrs[0] > 0 {
		capacity = maxErrs[0]
	}
	debug.Assert(capacity > 0)
	return Errs{
		errs: make([]error, 0, capacity),
		cap:  capacity,
	}
}

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
	if len(e.errs) < e.cap {
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
func (e *Errs) Error() string {
	var (
		err error
		cnt = e.Cnt()
	)
	if cnt == 0 {
		return ""
	}
	e.mu.Lock()
	debug.Assert(len(e.errs) > 0)
	err = e.errs[0]
	e.mu.Unlock()
	if cnt > 1 {
		err = fmt.Errorf("%v (and %d more error%s)", err, cnt-1, Plural(cnt-1))
	}
	return err.Error()
}

func (e *Errs) Unwrap() []error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.errs) // return a copy to avoid mutation
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

func IsPathErr(err error) (ok bool) {
	pathErr := (*iofs.PathError)(nil)
	if errors.As(err, &pathErr) {
		ok = true
	}
	return ok
}

// "file name too long" errno 0x24 (36); either one of the two possible reasons:
// - len(pathname) > PATH_MAX = 4096
// - len(basename) > 255
func IsErrFntl(err error) bool {
	return strings.Contains(err.Error(), "too long") && errors.Is(err, syscall.ENAMETOOLONG)
}

func IsErrNotDir(err error) bool {
	return strings.Contains(err.Error(), "directory") && errors.Is(err, syscall.ENOTDIR)
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

func IsClientTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
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

func checkMvErr(err error, dst string) error {
	if finfo, errN := os.Stat(dst); errN == nil && finfo.IsDir() {
		return &ErrMv{1}
	}
	if IsErrNotDir(err) {
		return &ErrMv{2}
	}
	return err
}

func IsErrMv(err error) bool {
	_, ok := err.(*ErrMv)
	return ok
}

func (e *ErrMv) Error() string {
	if e.ty == 2 {
		// with underlying `ENOTDIR`
		return "destination contains an object in its path"
	}
	return "destination exists and is a virtual directory"
}
