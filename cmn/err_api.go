// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"errors"
	"fmt"
	"os"
)

///////////////////////////////////////////////////////
// API errors - the errors that API calls may return //
///////////////////////////////////////////////////////

type (
	nodeBckPair struct {
		node string
		bck  Bck
	}
	ErrorBucketAlreadyExists     nodeBckPair
	ErrorCloudBucketDoesNotExist nodeBckPair
	ErrorCloudBucketOffline      nodeBckPair
	ErrorBucketDoesNotExist      nodeBckPair
	ErrorBucketIsBusy            nodeBckPair

	ErrorCapacityExceeded struct {
		prefix string
		high   int64
		used   int32
		oos    bool
	}

	BucketAccessDenied struct{ errAccessDenied }
	ObjectAccessDenied struct{ errAccessDenied }
	errAccessDenied    struct {
		entity      string
		operation   string
		accessAttrs uint64
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
)

func _errBucket(msg, node string) string {
	if node != "" {
		return node + ": " + msg
	}
	return msg
}

func NewErrorBucketAlreadyExists(bck Bck, node string) *ErrorBucketAlreadyExists {
	return &ErrorBucketAlreadyExists{node: node, bck: bck}
}
func (e *ErrorBucketAlreadyExists) Error() string {
	return _errBucket(fmt.Sprintf("bucket %s already exists", e.bck), e.node)
}

func NewErrorCloudBucketDoesNotExist(bck Bck, node string) *ErrorCloudBucketDoesNotExist {
	return &ErrorCloudBucketDoesNotExist{node: node, bck: bck}
}
func (e *ErrorCloudBucketDoesNotExist) Error() string {
	return _errBucket(fmt.Sprintf("cloud bucket %s does not exist", e.bck), e.node)
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

func NewBucketAccessDenied(bucket, oper string, aattrs uint64) *BucketAccessDenied {
	return &BucketAccessDenied{errAccessDenied{bucket, oper, aattrs}}
}

func NewErrorCapacityExceeded(prefix string, high int64, used int32, oos bool) *ErrorCapacityExceeded {
	return &ErrorCapacityExceeded{prefix: prefix, high: high, used: used, oos: oos}
}

func (e *ErrorCapacityExceeded) Error() string {
	if e.oos {
		return fmt.Sprintf("%s: OUT OF SPACE (used %d%% of total available capacity)", e.prefix, e.used)
	}
	return fmt.Sprintf("%s: used capacity %d%% exceeded high watermark %d%%", e.prefix, e.used, e.high)
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
	return AbortedError{
		what:    what,
		details: "",
	}
}

func NewAbortedErrorDetails(what, details string) AbortedError {
	return AbortedError{
		what:    what,
		details: details,
	}
}

func NewAbortedErrorWrapped(what string, cause error) AbortedError {
	return AbortedError{
		what:  what,
		cause: cause,
	}
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

//////////////////////////////////
// error grouping, error levels //
//////////////////////////////////

// nought: not a thing

func IsErrBucketNought(err error) bool {
	if _, ok := err.(*ErrorBucketDoesNotExist); ok {
		return true
	}
	if _, ok := err.(*ErrorCloudBucketDoesNotExist); ok {
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

func IsObjNotExist(err error) bool    { return os.IsNotExist(err) }
func IsErrBucketLevel(err error) bool { return IsErrBucketNought(err) }
func IsErrObjLevel(err error) bool    { return IsErrObjNought(err) }
