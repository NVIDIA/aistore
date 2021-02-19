// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/debug"
	jsoniter "github.com/json-iterator/go"
)

// This source contains common AIS node inter-module errors -
// the errors that some AIS packages (within a given running AIS node) return
// and other AIS packages handle.

const (
	stackTracePrefix = "stack: ["
	FromNodePrefix   = " from: "
)

type (
	ErrSignal struct {
		signal syscall.Signal
	}
	ErrBucketAlreadyExists      struct{ bck Bck }
	ErrRemoteBucketDoesNotExist struct{ bck Bck }
	ErrRemoteBucketOffline      struct{ bck Bck }
	ErrBucketDoesNotExist       struct{ bck Bck }
	ErrBucketIsBusy             struct{ bck Bck }

	ErrInvalidBucketProvider struct {
		bck Bck
		err error
	}
	ErrCapacityExceeded struct {
		totalBytes     uint64
		totalBytesUsed uint64
		highWM         int64
		usedPct        int32
		oos            bool
	}
	ErrBucketAccessDenied struct{ errAccessDenied }
	ErrObjectAccessDenied struct{ errAccessDenied }
	errAccessDenied       struct {
		entity      string
		operation   string
		accessAttrs AccessAttrs
	}

	ErrInvalidCksum struct {
		expectedHash string
		actualHash   string
	}
	ErrNoMountpath struct {
		mpath string
	}
	InvalidMountpathError struct {
		mpath string
		cause string
	}
	ErrNoNodes struct {
		role string
	}
	ErrXactionNotFound struct {
		cause string
	}
	ObjDefunctErr struct {
		name   string // object's name
		d1, d2 uint64 // lom.md.(bucket-ID) and lom.bck.(bucket-ID), respectively
	}
	ObjMetaErr struct {
		name string // object's name
		err  error  // underlying error
	}
	ErrAborted struct {
		what    string
		details string
		cause   error
	}
	ErrNotFound struct {
		what string
	}
	ErrETL struct {
		Reason string
		ETLErrorContext
	}
	ETLErrorContext struct {
		TID     string
		UUID    string
		ETLName string
		PodName string
		SvcName string
	}
	ErrSoft struct {
		what string
	}
	// Error structure for HTTP errors
	ErrHTTP struct {
		Status     int    `json:"status"`
		Message    string `json:"message"`
		Method     string `json:"method"`
		URLPath    string `json:"url_path"`
		RemoteAddr string `json:"remote_addr"`
		trace      []byte
	}
)

var (
	ErrSkip           = errors.New("skip")
	ErrStartupTimeout = errors.New("startup timeout")
	ErrForwarded      = errors.New("forwarded")

	ErrETLMissingUUID = errors.New("ETL UUID can't be empty")
)

// EOF (to accommodate unsized streaming)
func IsEOF(err error) bool {
	return err == io.ErrUnexpectedEOF || errors.Is(err, io.EOF)
}

func IsErrAborted(err error) bool {
	if _, ok := err.(*ErrAborted); ok {
		return true
	}
	target := &ErrAborted{}
	return errors.As(err, &target)
}

// IsErrorSoft returns true if the error is not critical and can be
// ignored in some cases(e.g, when `--force` is set)
func IsErrSoft(err error) bool {
	if _, ok := err.(*ErrSoft); ok {
		return true
	}
	target := &ErrSoft{}
	return errors.As(err, &target)
}

////////////////////////
// http-error helpers //
////////////////////////

func IsStatusServiceUnavailable(err error) (yes bool) {
	hErr, ok := err.(*ErrHTTP)
	if !ok {
		return false
	}
	return hErr.Status == http.StatusServiceUnavailable
}

func IsStatusNotFound(err error) (yes bool) {
	hErr, ok := err.(*ErrHTTP)
	if !ok {
		return false
	}
	return hErr.Status == http.StatusNotFound
}

func IsStatusBadGateway(err error) (yes bool) {
	hErr, ok := err.(*ErrHTTP)
	if !ok {
		return false
	}
	return hErr.Status == http.StatusBadGateway
}

func IsStatusGone(err error) (yes bool) {
	hErr, ok := err.(*ErrHTTP)
	if !ok {
		return false
	}
	return hErr.Status == http.StatusGone
}

///////////////////////////
// syscall-based helpers //
///////////////////////////

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

func IsErrOOS(err error) bool {
	return errors.Is(err, syscall.ENOSPC)
}

func IsUnreachable(err error, status int) bool {
	return IsErrConnectionRefused(err) ||
		errors.Is(err, context.DeadlineExceeded) ||
		status == http.StatusRequestTimeout ||
		status == http.StatusServiceUnavailable
}

////////////////////////////
// structured error types //
////////////////////////////

func NewSignalError(s syscall.Signal) *ErrSignal {
	return &ErrSignal{signal: s}
}

func (e *ErrSignal) Error() string {
	return fmt.Sprintf("Signal %d", e.signal)
}

// https://tldp.org/LDP/abs/html/exitcodes.html
func (e *ErrSignal) ExitCode() int { return 128 + int(e.signal) }

func NewErrorBucketAlreadyExists(bck Bck) *ErrBucketAlreadyExists {
	return &ErrBucketAlreadyExists{bck: bck}
}

func (e *ErrBucketAlreadyExists) Error() string {
	return fmt.Sprintf("bucket %q already exists", e.bck)
}

func NewErrorRemoteBucketDoesNotExist(bck Bck) *ErrRemoteBucketDoesNotExist {
	return &ErrRemoteBucketDoesNotExist{bck: bck}
}

func (e *ErrRemoteBucketDoesNotExist) Error() string {
	if e.bck.IsCloud() {
		return fmt.Sprintf("cloud bucket %q does not exist", e.bck)
	}
	return fmt.Sprintf("remote ais bucket %q does not exist", e.bck)
}

func NewErrorRemoteBucketOffline(bck Bck) *ErrRemoteBucketOffline {
	return &ErrRemoteBucketOffline{bck: bck}
}

func (e *ErrRemoteBucketOffline) Error() string {
	return fmt.Sprintf("bucket %q is currently unreachable", e.bck)
}

func NewErrorBucketDoesNotExist(bck Bck) *ErrBucketDoesNotExist {
	return &ErrBucketDoesNotExist{bck: bck}
}

func (e *ErrBucketDoesNotExist) Error() string {
	return fmt.Sprintf("bucket %q does not exist", e.bck)
}

func NewErrorInvalidBucketProvider(bck Bck, err error) *ErrInvalidBucketProvider {
	return &ErrInvalidBucketProvider{bck: bck, err: err}
}

func (e *ErrInvalidBucketProvider) Error() string {
	return fmt.Sprintf("%v, bucket %s", e.err, e.bck)
}

func NewErrorBucketIsBusy(bck Bck) *ErrBucketIsBusy {
	return &ErrBucketIsBusy{bck: bck}
}

func (e *ErrBucketIsBusy) Error() string {
	return fmt.Sprintf("bucket %q is currently busy, please retry later", e.bck)
}

func (e *errAccessDenied) String() string {
	return fmt.Sprintf("%s: %s access denied (%#x)", e.entity, e.operation, e.accessAttrs)
}

func (e *ErrBucketAccessDenied) Error() string {
	return "bucket " + e.String()
}

func (e *ErrObjectAccessDenied) Error() string {
	return "object " + e.String()
}

func NewBucketAccessDenied(bucket, oper string, aattrs AccessAttrs) *ErrBucketAccessDenied {
	return &ErrBucketAccessDenied{errAccessDenied{bucket, oper, aattrs}}
}

func NewErrorCapacityExceeded(highWM int64, usedPct int32, totalBytesUsed, totalBytes uint64,
	oos bool) *ErrCapacityExceeded {
	return &ErrCapacityExceeded{
		highWM:         highWM,
		usedPct:        usedPct,
		totalBytes:     totalBytes,
		totalBytesUsed: totalBytesUsed,
		oos:            oos,
	}
}

func (e *ErrCapacityExceeded) Error() string {
	suffix := fmt.Sprintf("total used %s out of %s", B2S(int64(e.totalBytesUsed), 2), B2S(int64(e.totalBytes), 2))
	if e.oos {
		return fmt.Sprintf("out of space: used %d%% of total capacity on at least one of the mountpaths (%s)",
			e.usedPct, suffix)
	}
	return fmt.Sprintf("low on free space: used capacity %d%% exceeded high watermark(%d%%) (%s)",
		e.usedPct, e.highWM, suffix)
}

func (e *ErrInvalidCksum) Error() string {
	return fmt.Sprintf("checksum: expected [%s], actual [%s]", e.expectedHash, e.actualHash)
}

func NewInvalidCksumError(eHash, aHash string) *ErrInvalidCksum {
	return &ErrInvalidCksum{actualHash: aHash, expectedHash: eHash}
}

func (e *ErrInvalidCksum) Expected() string { return e.expectedHash }

func (e *ErrNoMountpath) Error() string {
	return "mountpath [" + e.mpath + "] doesn't exist"
}

func NewNoMountpathError(mpath string) *ErrNoMountpath {
	return &ErrNoMountpath{mpath}
}

func (e *InvalidMountpathError) Error() string {
	return "invalid mountpath [" + e.mpath + "]; " + e.cause
}

func NewInvalidaMountpathError(mpath, cause string) *InvalidMountpathError {
	return &InvalidMountpathError{mpath: mpath, cause: cause}
}

func NewNoNodesError(role string) *ErrNoNodes {
	return &ErrNoNodes{role: role}
}

func (e *ErrNoNodes) Error() string {
	if e.role == Proxy {
		return "no available proxies"
	}
	Assert(e.role == Target)
	return "no available targets"
}

func (e *ErrXactionNotFound) Error() string {
	return "xaction " + e.cause + " not found"
}

func NewXactionNotFoundError(cause string) *ErrXactionNotFound {
	return &ErrXactionNotFound{cause: cause}
}

func (e *ObjDefunctErr) Error() string {
	return fmt.Sprintf("%s is defunct (%d != %d)", e.name, e.d1, e.d2)
}

func NewObjDefunctError(name string, d1, d2 uint64) *ObjDefunctErr {
	return &ObjDefunctErr{name, d1, d2}
}

func (e *ObjMetaErr) Error() string {
	return fmt.Sprintf("object %s failed to load meta: %v", e.name, e.err)
}

func NewObjMetaErr(name string, err error) *ObjMetaErr {
	return &ObjMetaErr{name: name, err: err}
}

func NewAbortedError(what string, details ...string) *ErrAborted {
	var d string
	if len(details) > 0 {
		d = details[0]
	}
	return &ErrAborted{what: what, details: d}
}

func NewAbortedErrorWrapped(what string, cause error) *ErrAborted {
	return &ErrAborted{what: what, cause: cause}
}

func (e *ErrAborted) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s aborted. %s", e.what, e.cause.Error())
	}
	if e.details != "" {
		return fmt.Sprintf("%s aborted. %s", e.what, e.details)
	}
	return fmt.Sprintf("%s aborted", e.what)
}

func (e *ErrAborted) As(target error) bool {
	_, ok := target.(*ErrAborted)
	if e.cause == nil {
		return ok
	}
	return ok || errors.As(e.cause, &target)
}

func NewFailedToCreateHTTPRequest(err error) error {
	return fmt.Errorf("failed to create new HTTP request, err: %v", err)
}

func NewNotFoundError(format string, a ...interface{}) *ErrNotFound {
	return &ErrNotFound{fmt.Sprintf(format, a...)}
}

func (e *ErrNotFound) Error() string { return e.what + " not found" }

func NewETLError(ctx *ETLErrorContext, format string, a ...interface{}) *ErrETL {
	e := &ErrETL{
		Reason: fmt.Sprintf(format, a...),
	}
	return e.WithContext(ctx)
}

func (e *ErrETL) Error() string {
	s := make([]string, 0, 3)
	if e.TID != "" {
		s = append(s, fmt.Sprintf("t[%s]", e.TID))
	}
	if e.UUID != "" {
		s = append(s, fmt.Sprintf("uuid=%q", e.UUID))
	}
	if e.ETLName != "" {
		s = append(s, fmt.Sprintf("etl=%q", e.ETLName))
	}
	if e.PodName != "" {
		s = append(s, fmt.Sprintf("pod=%q", e.PodName))
	}
	if e.SvcName != "" {
		s = append(s, fmt.Sprintf("service=%q", e.SvcName))
	}
	return fmt.Sprintf("[%s] %s", strings.Join(s, ","), e.Reason)
}

func (e *ErrETL) withUUID(uuid string) *ErrETL {
	if uuid != "" {
		e.UUID = uuid
	}
	return e
}

func (e *ErrETL) withTarget(tid string) *ErrETL {
	if tid != "" {
		e.TID = tid
	}
	return e
}

func (e *ErrETL) withETLName(name string) *ErrETL {
	if name != "" {
		e.ETLName = name
	}
	return e
}

func (e *ErrETL) withSvcName(name string) *ErrETL {
	if name != "" {
		e.SvcName = name
	}
	return e
}

func (e *ErrETL) WithPodName(name string) *ErrETL {
	if name != "" {
		e.PodName = name
	}
	return e
}

func (e *ErrETL) WithContext(ctx *ETLErrorContext) *ErrETL {
	if ctx == nil {
		return e
	}
	return e.
		withTarget(ctx.TID).
		withUUID(ctx.UUID).
		WithPodName(ctx.PodName).
		withETLName(ctx.ETLName).
		withSvcName(ctx.SvcName)
}

func NewSoftError(what string) *ErrSoft {
	return &ErrSoft{what}
}

func (e *ErrSoft) Error() string {
	return e.what
}

////////////////////////////
// error grouping helpers //
////////////////////////////

// nought: not a thing
func IsErrBucketNought(err error) bool {
	if _, ok := err.(*ErrBucketDoesNotExist); ok {
		return true
	}
	if _, ok := err.(*ErrRemoteBucketDoesNotExist); ok {
		return true
	}
	_, ok := err.(*ErrRemoteBucketOffline)
	return ok
}

func IsErrObjNought(err error) bool {
	if IsObjNotExist(err) {
		return true
	}
	if _, ok := err.(*ObjMetaErr); ok {
		return true
	}
	if _, ok := err.(*ObjDefunctErr); ok {
		return true
	}
	return false
}

func IsObjNotExist(err error) bool {
	if os.IsNotExist(err) {
		return true
	}
	_, ok := err.(*ErrNotFound)
	return ok
}

func IsErrBucketLevel(err error) bool { return IsErrBucketNought(err) }
func IsErrObjLevel(err error) bool    { return IsErrObjNought(err) }

///////////////
// ErrHTTP //
///////////////

func NewHTTPErr(r *http.Request, msg string, errCode ...int) (e *ErrHTTP) {
	status := http.StatusBadRequest
	if len(errCode) > 0 && errCode[0] > status {
		status = errCode[0]
	}
	e = allocHTTPErr()
	e.Status, e.Message = status, msg
	if r != nil {
		e.Method, e.URLPath, e.RemoteAddr = r.Method, r.URL.Path, r.RemoteAddr
	}
	return
}

func S2HTTPErr(r *http.Request, msg string, status int) *ErrHTTP {
	if msg != "" {
		var httpErr ErrHTTP
		if err := jsoniter.UnmarshalFromString(msg, &httpErr); err == nil {
			return &httpErr
		}
	}
	return NewHTTPErr(r, msg, status)
}

func Err2HTTPErr(err error) (httpErr *ErrHTTP) {
	var ok bool
	if httpErr, ok = err.(*ErrHTTP); ok {
		return
	}
	httpErr = &ErrHTTP{}
	if !errors.As(err, &httpErr) {
		httpErr = nil
	}
	return
}

// E.g.: Bad Request: Bucket abc does not appear to be local or does not exist:
// DELETE /v1/buckets/abc from 127.0.0.1:54064 (stack: [httpcommon.go:840 <- proxy.go:484 <- proxy.go:264])
func (e *ErrHTTP) String() (s string) {
	s = http.StatusText(e.Status) + ": " + e.Message
	if e.Method != "" || e.URLPath != "" {
		s += ":"
		if e.Method != "" {
			s += " " + e.Method
		}
		if e.URLPath != "" {
			s += " " + e.URLPath
		}
	}
	if e.RemoteAddr != "" && !strings.Contains(e.Message, FromNodePrefix) {
		s += " from " + e.RemoteAddr
	}
	return s + " (" + string(e.trace) + ")"
}

func (e *ErrHTTP) Error() string {
	// Stop from escaping <, > ,and &
	buf := new(bytes.Buffer)
	enc := jsoniter.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(e); err != nil {
		return err.Error()
	}
	return buf.String()
}

func (e *ErrHTTP) write(w http.ResponseWriter, r *http.Request, silent bool) {
	msg := e.Error()
	if len(msg) > 0 {
		// error strings should not be capitalized (lint)
		if c := msg[0]; c >= 'A' && c <= 'Z' {
			msg = string(c+'a'-'A') + msg[1:]
		}
	}
	if !strings.Contains(msg, stackTracePrefix) {
		e.stackTrace()
	}
	if !silent {
		glog.Errorln(e.String())
	}
	// make sure that the caller is aware that we return JSON error
	w.Header().Set(HeaderContentType, ContentJSON)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.Method == http.MethodHead {
		w.Header().Set(HeaderError, e.Error())
		w.WriteHeader(e.Status)
	} else {
		w.WriteHeader(e.Status)
		fmt.Fprintln(w, e.Error())
	}
}

func (e *ErrHTTP) stackTrace() {
	debug.Assert(len(e.trace) == 0)
	buffer := bytes.NewBuffer(e.trace)
	fmt.Fprint(buffer, stackTracePrefix)
	for i := 1; i < 9; i++ {
		if _, file, line, ok := runtime.Caller(i); !ok {
			break
		} else {
			if !strings.Contains(file, "aistore") {
				break
			}
			f := filepath.Base(file)
			if f == "err.go" {
				continue
			}
			if buffer.Len() > len(stackTracePrefix) {
				buffer.WriteString(" <- ")
			}
			fmt.Fprintf(buffer, "%s:%d", f, line)
		}
	}
	fmt.Fprint(buffer, "]")
	e.trace = buffer.Bytes()
}

//////////////////////////////
// invalid message handlers //
//////////////////////////////

// write error into http response
func WriteErr(w http.ResponseWriter, r *http.Request, err error, opts ...int) {
	status := http.StatusBadRequest
	if len(opts) > 0 && opts[0] > status {
		status = opts[0]
	}
	if httpErr := Err2HTTPErr(err); httpErr != nil {
		if status > http.StatusBadRequest || httpErr.Status == 0 {
			httpErr.Status = status
		}
		httpErr.write(w, r, len(opts) > 1 /*silent*/)
	} else {
		WriteErrMsg(w, r, err.Error(), opts...)
	}
}

// create ErrHTTP (based on `msg` and `opts`) and write it into http response
func WriteErrMsg(w http.ResponseWriter, r *http.Request, msg string, opts ...int) {
	httpErr := NewHTTPErr(r, msg, opts...)
	httpErr.write(w, r, len(opts) > 1 /*silent*/)
	FreeHTTPErr(httpErr)
}

// 405 Method Not Allowed - see https://tools.ietf.org/html/rfc2616#section-10.4.6
func WriteErr405(w http.ResponseWriter, r *http.Request, methods ...string) {
	allowed := strings.Join(methods, ", ")
	w.Header().Set("Allow", allowed)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

/////////////
// errPool //
/////////////

var (
	errPool sync.Pool
	err0    ErrHTTP
)

func allocHTTPErr() (a *ErrHTTP) {
	if v := errPool.Get(); v != nil {
		a = v.(*ErrHTTP)
		return
	}
	return &ErrHTTP{}
}

func FreeHTTPErr(a *ErrHTTP) {
	trace := a.trace
	*a = err0
	if trace != nil {
		a.trace = trace[:0]
	}
	errPool.Put(a)
}
