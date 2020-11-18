// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"syscall"
)

// This source contains common AIS node inter-module errors -
// the errors that some AIS packages (within a given running AIS node) return
// and other AIS packages handle.

type (
	SignalError struct {
		signal syscall.Signal
	}

	nodeBckPair struct {
		node string
		bck  Bck
	}
	ErrorBucketAlreadyExists      nodeBckPair
	ErrorRemoteBucketDoesNotExist nodeBckPair
	ErrorCloudBucketOffline       nodeBckPair
	ErrorBucketDoesNotExist       nodeBckPair
	ErrorInvalidBucketProvider    nodeBckPair
	ErrorBucketIsBusy             nodeBckPair

	ErrorCapacityExceeded struct {
		high int64
		used int32
		oos  bool
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
)

var (
	ErrSkip           = errors.New("skip")
	ErrStartupTimeout = errors.New("startup timeout")
	ErrForwarded      = errors.New("forwarded")
)

func _errBucket(msg, node string) string {
	if node != "" {
		return node + ": " + msg
	}
	return msg
}

///////////////////////////
// http-error helpers   //
//////////////////////////

func IsStatusServiceUnavailable(err error) (yes bool) {
	hErr, ok := err.(*HTTPError)
	if !ok {
		return false
	}
	return hErr.Status == http.StatusServiceUnavailable
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

func IsReqCanceled(err error) bool {
	// TODO: find a better alternative
	// Currently net/http does not have an exported error type for this
	return strings.Contains(err.Error(), "net/http: request canceled")
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

func NewErrorBucketAlreadyExists(bck Bck, node string) *ErrorBucketAlreadyExists {
	return &ErrorBucketAlreadyExists{node: node, bck: bck}
}

func (e *ErrorBucketAlreadyExists) Error() string {
	return _errBucket(fmt.Sprintf("bucket %s already exists", e.bck), e.node)
}

func NewErrorRemoteBucketDoesNotExist(bck Bck, node string) *ErrorRemoteBucketDoesNotExist {
	return &ErrorRemoteBucketDoesNotExist{node: node, bck: bck}
}

func (e *ErrorRemoteBucketDoesNotExist) Error() string {
	if e.bck.IsCloud() {
		return _errBucket(fmt.Sprintf("cloud bucket %s does not exist", e.bck), e.node)
	}
	return _errBucket(fmt.Sprintf("remote ais bucket %s does not exist", e.bck), e.node)
}

func NewErrorCloudBucketOffline(bck Bck, node string) *ErrorCloudBucketOffline {
	return &ErrorCloudBucketOffline{node: node, bck: bck}
}

func (e *ErrorCloudBucketOffline) Error() string {
	return _errBucket(fmt.Sprintf("bucket %s is currently unreachable", e.bck), e.node)
}

func NewErrorBucketDoesNotExist(bck Bck, node string) *ErrorBucketDoesNotExist {
	return &ErrorBucketDoesNotExist{node: node, bck: bck}
}

func (e *ErrorBucketDoesNotExist) Error() string {
	return _errBucket(fmt.Sprintf("bucket %s does not exist", e.bck), e.node)
}

func NewErrorInvalidBucketProvider(bck Bck, node string) *ErrorInvalidBucketProvider {
	return &ErrorInvalidBucketProvider{node: node, bck: bck}
}

func (e *ErrorInvalidBucketProvider) Error() string {
	return _errBucket(fmt.Sprintf("invalid provider %q of bucket %s", e.bck.Provider, e.bck), e.node)
}

func NewErrorBucketIsBusy(bck Bck, node string) *ErrorBucketIsBusy {
	return &ErrorBucketIsBusy{node: node, bck: bck}
}

func (e *ErrorBucketIsBusy) Error() string {
	return _errBucket(fmt.Sprintf("bucket %s is currently busy, please retry later", e.bck), e.node)
}

func (e *errAccessDenied) String() string {
	return fmt.Sprintf("%s: %s access denied (%#x)", e.entity, e.operation, e.accessAttrs)
}
func (e *BucketAccessDenied) Error() string { return "bucket " + e.String() }
func (e *ObjectAccessDenied) Error() string { return "object " + e.String() }

func NewBucketAccessDenied(bucket, oper string, aattrs AccessAttrs) *BucketAccessDenied {
	return &BucketAccessDenied{errAccessDenied{bucket, oper, aattrs}}
}

func NewErrorCapacityExceeded(high int64, used int32, oos bool) *ErrorCapacityExceeded {
	return &ErrorCapacityExceeded{high: high, used: used, oos: oos}
}

func (e *ErrorCapacityExceeded) Error() string {
	if e.oos {
		return fmt.Sprintf("out of space: used %d%% of total capacity on at least one of the mountpaths",
			e.used)
	}
	return fmt.Sprintf("low on free space: used capacity %d%% exceeded high watermark(%d%%)", e.used, e.high)
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
	_, ok := err.(*ErrorCloudBucketOffline)
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
