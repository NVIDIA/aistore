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
	"syscall"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	jsoniter "github.com/json-iterator/go"
)

// This source contains common AIS node inter-module errors -
// the errors that some AIS packages (within a given running AIS node) return
// and other AIS packages handle.

const (
	stackTracePrefix = "stack: ["
	FromNodePrefix   = " from: ["
)

type (
	SignalError struct {
		signal syscall.Signal
	}

	ErrorBucketAlreadyExists      struct{ bck Bck }
	ErrorRemoteBucketDoesNotExist struct{ bck Bck }
	ErrorRemoteBucketOffline      struct{ bck Bck }
	ErrorBucketDoesNotExist       struct{ bck Bck }
	ErrorBucketIsBusy             struct{ bck Bck }

	ErrorInvalidBucketProvider struct {
		bck Bck
		err error
	}
	ErrorCapacityExceeded struct {
		totalBytes     uint64
		totalBytesUsed uint64
		highWM         int64
		usedPct        int32
		oos            bool
	}

	BucketAccessDenied struct{ errAccessDenied }
	ObjectAccessDenied struct{ errAccessDenied }
	errAccessDenied    struct {
		entity      string
		operation   string
		accessAttrs AccessAttrs
	}

	InvalidCksumError struct {
		expectedHash string
		actualHash   string
	}
	NoMountpathError struct {
		mpath string
	}
	InvalidMountpathError struct {
		mpath string
		cause string
	}
	NoNodesError struct {
		role string
	}
	XactionNotFoundError struct {
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
	AbortedError struct {
		what    string
		details string
		cause   error
	}
	NotFoundError struct {
		what string
	}
	ETLError struct {
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
	SoftError struct {
		what string
	}

	// Error structure for HTTP errors
	HTTPError struct {
		Status     int    `json:"status"`
		Message    string `json:"message"`
		Method     string `json:"method"`
		URLPath    string `json:"url_path"`
		RemoteAddr string `json:"remote_addr"`
		trace      string
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
	if _, ok := err.(*AbortedError); ok {
		return true
	}
	return errors.As(err, &AbortedError{})
}

// IsErrorSoft returns true if the error is not critical and can be
// ignored in some cases(e.g, when `--force` is set)
func IsErrSoft(err error) bool {
	if _, ok := err.(*SoftError); ok {
		return true
	}
	return errors.As(err, &SoftError{})
}

////////////////////////
// http-error helpers //
////////////////////////

func IsStatusServiceUnavailable(err error) (yes bool) {
	hErr, ok := err.(*HTTPError)
	if !ok {
		return false
	}
	return hErr.Status == http.StatusServiceUnavailable
}

func IsStatusNotFound(err error) (yes bool) {
	hErr, ok := err.(*HTTPError)
	if !ok {
		return false
	}
	return hErr.Status == http.StatusNotFound
}

func IsStatusBadGateway(err error) (yes bool) {
	hErr, ok := err.(*HTTPError)
	if !ok {
		return false
	}
	return hErr.Status == http.StatusBadGateway
}

func IsStatusGone(err error) (yes bool) {
	hErr, ok := err.(*HTTPError)
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

func NewSignalError(s syscall.Signal) *SignalError { return &SignalError{signal: s} }
func (e *SignalError) Error() string               { return fmt.Sprintf("Signal %d", e.signal) }

// https://tldp.org/LDP/abs/html/exitcodes.html
func (e *SignalError) ExitCode() int { return 128 + int(e.signal) }

func NewErrorBucketAlreadyExists(bck Bck) *ErrorBucketAlreadyExists {
	return &ErrorBucketAlreadyExists{bck: bck}
}

func (e *ErrorBucketAlreadyExists) Error() string {
	return fmt.Sprintf("bucket %q already exists", e.bck)
}

func NewErrorRemoteBucketDoesNotExist(bck Bck) *ErrorRemoteBucketDoesNotExist {
	return &ErrorRemoteBucketDoesNotExist{bck: bck}
}

func (e *ErrorRemoteBucketDoesNotExist) Error() string {
	if e.bck.IsCloud() {
		return fmt.Sprintf("cloud bucket %q does not exist", e.bck)
	}
	return fmt.Sprintf("remote ais bucket %q does not exist", e.bck)
}

func NewErrorRemoteBucketOffline(bck Bck) *ErrorRemoteBucketOffline {
	return &ErrorRemoteBucketOffline{bck: bck}
}

func (e *ErrorRemoteBucketOffline) Error() string {
	return fmt.Sprintf("bucket %q is currently unreachable", e.bck)
}

func NewErrorBucketDoesNotExist(bck Bck) *ErrorBucketDoesNotExist {
	return &ErrorBucketDoesNotExist{bck: bck}
}

func (e *ErrorBucketDoesNotExist) Error() string {
	return fmt.Sprintf("bucket %q does not exist", e.bck)
}

func NewErrorInvalidBucketProvider(bck Bck, err error) *ErrorInvalidBucketProvider {
	return &ErrorInvalidBucketProvider{bck: bck, err: err}
}

func (e *ErrorInvalidBucketProvider) Error() string {
	return fmt.Sprintf("%v, bucket %s", e.err, e.bck)
}

func NewErrorBucketIsBusy(bck Bck) *ErrorBucketIsBusy {
	return &ErrorBucketIsBusy{bck: bck}
}

func (e *ErrorBucketIsBusy) Error() string {
	return fmt.Sprintf("bucket %q is currently busy, please retry later", e.bck)
}

func (e *errAccessDenied) String() string {
	return fmt.Sprintf("%s: %s access denied (%#x)", e.entity, e.operation, e.accessAttrs)
}
func (e *BucketAccessDenied) Error() string { return "bucket " + e.String() }
func (e *ObjectAccessDenied) Error() string { return "object " + e.String() }

func NewBucketAccessDenied(bucket, oper string, aattrs AccessAttrs) *BucketAccessDenied {
	return &BucketAccessDenied{errAccessDenied{bucket, oper, aattrs}}
}

func NewErrorCapacityExceeded(highWM int64, usedPct int32, totalBytesUsed, totalBytes uint64, oos bool) *ErrorCapacityExceeded {
	return &ErrorCapacityExceeded{
		highWM:         highWM,
		usedPct:        usedPct,
		totalBytes:     totalBytes,
		totalBytesUsed: totalBytesUsed,
		oos:            oos,
	}
}

func (e *ErrorCapacityExceeded) Error() string {
	suffix := fmt.Sprintf("total used %s out of %s", B2S(int64(e.totalBytesUsed), 2), B2S(int64(e.totalBytes), 2))
	if e.oos {
		return fmt.Sprintf("out of space: used %d%% of total capacity on at least one of the mountpaths (%s)", e.usedPct, suffix)
	}
	return fmt.Sprintf("low on free space: used capacity %d%% exceeded high watermark(%d%%) (%s)", e.usedPct, e.highWM, suffix)
}

func (e InvalidCksumError) Error() string {
	return fmt.Sprintf("checksum: expected [%s], actual [%s]", e.expectedHash, e.actualHash)
}

func NewInvalidCksumError(eHash, aHash string) InvalidCksumError {
	return InvalidCksumError{actualHash: aHash, expectedHash: eHash}
}
func (e InvalidCksumError) Expected() string { return e.expectedHash }

func (e NoMountpathError) Error() string                { return "mountpath [" + e.mpath + "] doesn't exist" }
func NewNoMountpathError(mpath string) NoMountpathError { return NoMountpathError{mpath} }

func (e InvalidMountpathError) Error() string {
	return "invalid mountpath [" + e.mpath + "]; " + e.cause
}

func NewInvalidaMountpathError(mpath, cause string) InvalidMountpathError {
	return InvalidMountpathError{mpath: mpath, cause: cause}
}

func NewNoNodesError(role string) *NoNodesError {
	return &NoNodesError{role: role}
}

func (e *NoNodesError) Error() string {
	if e.role == Proxy {
		return "no available proxies"
	}
	Assert(e.role == Target)
	return "no available targets"
}

func (e XactionNotFoundError) Error() string { return "xaction '" + e.cause + "' not found" }
func NewXactionNotFoundError(cause string) XactionNotFoundError {
	return XactionNotFoundError{cause: cause}
}

func (e ObjDefunctErr) Error() string {
	return fmt.Sprintf("%s is defunct (%d != %d)", e.name, e.d1, e.d2)
}
func NewObjDefunctError(name string, d1, d2 uint64) ObjDefunctErr { return ObjDefunctErr{name, d1, d2} }

func (e ObjMetaErr) Error() string {
	return fmt.Sprintf("object %s failed to load meta: %v", e.name, e.err)
}

func NewObjMetaErr(name string, err error) ObjMetaErr { return ObjMetaErr{name: name, err: err} }

func NewAbortedError(what string) AbortedError {
	return AbortedError{what: what, details: ""}
}

func NewAbortedErrorDetails(what, details string) AbortedError {
	return AbortedError{what: what, details: details}
}

func NewAbortedErrorWrapped(what string, cause error) AbortedError {
	return AbortedError{what: what, cause: cause}
}

func (e AbortedError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s aborted. %s", e.what, e.cause.Error())
	}
	if e.details != "" {
		return fmt.Sprintf("%s aborted. %s", e.what, e.details)
	}
	return fmt.Sprintf("%s aborted", e.what)
}

func (e *AbortedError) As(target error) bool {
	_, ok := target.(*AbortedError)
	if e.cause == nil {
		return ok
	}
	return ok || errors.As(e.cause, &target)
}

func NewFailedToCreateHTTPRequest(err error) error {
	return fmt.Errorf("failed to create new HTTP request, err: %v", err)
}

func NewNotFoundError(format string, a ...interface{}) *NotFoundError {
	return &NotFoundError{fmt.Sprintf(format, a...)}
}

func (e *NotFoundError) Error() string { return e.what + " not found" }

func NewETLError(ctx *ETLErrorContext, format string, a ...interface{}) *ETLError {
	e := &ETLError{
		Reason: fmt.Sprintf(format, a...),
	}
	return e.WithContext(ctx)
}

func (e *ETLError) Error() string {
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

func (e *ETLError) withUUID(uuid string) *ETLError {
	if uuid != "" {
		e.UUID = uuid
	}
	return e
}

func (e *ETLError) withTarget(tid string) *ETLError {
	if tid != "" {
		e.TID = tid
	}
	return e
}

func (e *ETLError) withETLName(name string) *ETLError {
	if name != "" {
		e.ETLName = name
	}
	return e
}

func (e *ETLError) withSvcName(name string) *ETLError {
	if name != "" {
		e.SvcName = name
	}
	return e
}

func (e *ETLError) WithPodName(name string) *ETLError {
	if name != "" {
		e.PodName = name
	}
	return e
}

func (e *ETLError) WithContext(ctx *ETLErrorContext) *ETLError {
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

func NewSoftError(what string) SoftError { return SoftError{what} }
func (e SoftError) Error() string        { return e.what }

////////////////////////////
// error grouping helpers //
////////////////////////////

// nought: not a thing
func IsErrBucketNought(err error) bool {
	if _, ok := err.(*ErrorBucketDoesNotExist); ok {
		return true
	}
	if _, ok := err.(*ErrorRemoteBucketDoesNotExist); ok {
		return true
	}
	_, ok := err.(*ErrorRemoteBucketOffline)
	return ok
}

func IsErrObjNought(err error) bool {
	if IsObjNotExist(err) {
		return true
	}
	if _, ok := err.(ObjMetaErr); ok {
		return true
	}
	if _, ok := err.(ObjDefunctErr); ok {
		return true
	}
	return false
}

func IsObjNotExist(err error) bool {
	if os.IsNotExist(err) {
		return true
	}
	_, ok := err.(*NotFoundError)
	return ok
}
func IsErrBucketLevel(err error) bool { return IsErrBucketNought(err) }
func IsErrObjLevel(err error) bool    { return IsErrObjNought(err) }

///////////////
// HTTPError //
///////////////

func NewHTTPErr(r *http.Request, msg string, errCode ...int) (e *HTTPError) {
	status := http.StatusBadRequest
	if len(errCode) > 0 {
		status = errCode[0]
	}
	e = &HTTPError{Status: status, Message: msg}
	if r != nil {
		e.Method, e.URLPath, e.RemoteAddr = r.Method, r.URL.Path, r.RemoteAddr
	}
	return
}

func S2HTTPErr(r *http.Request, msg string, status int) *HTTPError {
	if msg != "" {
		var httpErr HTTPError
		if err := jsoniter.UnmarshalFromString(msg, &httpErr); err == nil {
			return &httpErr
		}
	}
	return NewHTTPErr(r, msg, status)
}

func Err2HTTPErr(err error) (httpErr *HTTPError) {
	var ok bool
	if httpErr, ok = err.(*HTTPError); ok {
		return
	}
	httpErr = &HTTPError{}
	if !errors.As(err, &httpErr) {
		httpErr = nil
	}
	return
}

// E.g.: Bad Request: Bucket abc does not appear to be local or does not exist:
// DELETE /v1/buckets/abc from 127.0.0.1:54064 (stack: [httpcommon.go:840 <- proxy.go:484 <- proxy.go:264])
func (e *HTTPError) String() (s string) {
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
	return s + " (" + e.trace + ")"
}

func (e *HTTPError) Error() string {
	// Stop from escaping <, > ,and &
	buf := new(bytes.Buffer)
	enc := jsoniter.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(e); err != nil {
		return err.Error()
	}
	return buf.String()
}

func (e *HTTPError) write(w http.ResponseWriter, r *http.Request, silent bool) {
	msg := e.Error()
	if !strings.Contains(msg, stackTracePrefix) {
		e.trace = appendStack()
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

//////////////////////////////
// invalid message handlers //
//////////////////////////////

// with stack trace
func InvalidHandlerDetailed(w http.ResponseWriter, r *http.Request, err error, opts ...int) {
	status := http.StatusBadRequest
	if len(opts) > 0 && opts[0] > http.StatusBadRequest {
		status = opts[0]
	}
	if httpErr := Err2HTTPErr(err); httpErr != nil {
		if status > http.StatusBadRequest || httpErr.Status == 0 {
			httpErr.Status = status
		}
		httpErr.write(w, r, len(opts) > 1 /*silent*/)
	} else {
		InvalidHandlerWithMsg(w, r, err.Error(), opts...)
	}
}

func InvalidHandlerWithMsg(w http.ResponseWriter, r *http.Request, msg string, opts ...int) {
	httpErr := NewHTTPErr(r, msg, opts...)
	httpErr.write(w, r, len(opts) > 1 /*silent*/)
}

func appendStack() string {
	var errMsg bytes.Buffer
	fmt.Fprint(&errMsg, stackTracePrefix)
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
			if errMsg.Len() > len(stackTracePrefix) {
				errMsg.WriteString(" <- ")
			}
			fmt.Fprintf(&errMsg, "%s:%d", f, line)
		}
	}
	fmt.Fprint(&errMsg, "]")
	return errMsg.String()
}
