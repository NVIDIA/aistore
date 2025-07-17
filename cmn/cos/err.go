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
	"path/filepath"
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
)

type (
	errInvalidObjName struct {
		name string
	}
	errInvalidPrefix struct {
		tag    string
		prefix string
	}
	errInvalidArchpath struct {
		path string
	}
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
	if _, ok := err.(*ErrNotFound); ok {
		return true
	}
	return err != nil && strings.Contains(err.Error(), "does not exist")
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

func IsNotExist(err error, ecode ...int) bool {
	if len(ecode) > 0 && ecode[0] == http.StatusNotFound {
		return true
	}
	return IsErrNotFound(err) || os.IsNotExist(err) /*unwraps for fs.ErrNotExist*/
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

//
// ErrMv and related bits
//

const ErrENOTDIR = "destination contains an object in its path"

type ErrMv struct {
	// - type 1: mv readme aaa/bbb, where destination aaa/bbb[/ccc/...] is a virtual directory
	// - type 2 (a.k.a. ENOTDIR): mv readme aaa/bbb/ccc, where destination aaa/bbb is (or contains) a file
	// - see also: ErrMv.Is() below
	ty int
}

func IsErrNotDir(err error) bool {
	return errors.Is(err, syscall.ENOTDIR)
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

// to satisfy errors.Is()
func (e *ErrMv) Is(target error) bool {
	return e.ty == 2 && target == syscall.ENOTDIR
}

func (e *ErrMv) Error() string {
	if e.ty == 2 {
		return ErrENOTDIR
	}
	return "destination exists and is a virtual directory"
}

// errInvalidObjName, errInvalidPrefix

const (
	inv1 = "../"
	inv2 = "~/"
)

func ValidateOname(name string) error {
	if name == "" {
		return &errInvalidObjName{name}
	}
	return ValidOname(name)
}

func ValidOname(name string) error {
	if IsLastB(name, filepath.Separator) {
		return &errInvalidObjName{name}
	}
	if strings.IndexByte(name, inv1[0]) < 0 && strings.IndexByte(name, inv2[0]) < 0 { // most of the time
		return nil
	}
	if strings.Contains(name, inv1) || strings.Contains(name, inv2) {
		return &errInvalidObjName{name}
	}
	return nil
}

func (e *errInvalidObjName) Error() string {
	return fmt.Sprintf("invalid object name %q", e.name)
}

func ValidatePrefix(tag, prefix string) error {
	if prefix == "" {
		return nil
	}
	if strings.IndexByte(prefix, inv1[0]) < 0 && strings.IndexByte(prefix, inv2[0]) < 0 { // ditto
		return nil
	}
	if strings.Contains(prefix, inv1) || strings.Contains(prefix, inv2) {
		return &errInvalidPrefix{tag, prefix}
	}
	return nil
}

func (e *errInvalidPrefix) Error() string {
	return fmt.Sprintf("%s: invalid prefix %q", e.tag, e.prefix)
}

func ValidateArchpath(path string) error {
	if ValidOname(path) != nil {
		return &errInvalidArchpath{path}
	}
	return nil
}

func (e *errInvalidArchpath) Error() string { return "invalid archpath \"" + e.path + "\"" }
