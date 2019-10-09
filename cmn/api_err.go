// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
)

// Common errors

type ErrorBucketAlreadyExists struct {
	bucket string
}

func NewErrorBucketAlreadyExists(bucket string) *ErrorBucketAlreadyExists {
	return &ErrorBucketAlreadyExists{bucket: bucket}
}

func (e *ErrorBucketAlreadyExists) Error() string {
	return fmt.Sprintf("bucket %q already exists", e.bucket)
}

type ErrorCloudBucketDoesNotExist struct {
	bucket string
}

func NewErrorCloudBucketDoesNotExist(bucket string) *ErrorCloudBucketDoesNotExist {
	return &ErrorCloudBucketDoesNotExist{bucket: bucket}
}

func (e *ErrorCloudBucketDoesNotExist) Error() string {
	return fmt.Sprintf("cloud bucket %q does not exist", e.bucket)
}

type ErrorBucketDoesNotExist struct {
	bucket string
}

func NewErrorBucketDoesNotExist(bucket string) *ErrorBucketDoesNotExist {
	return &ErrorBucketDoesNotExist{bucket: bucket}
}

func (e *ErrorBucketDoesNotExist) Error() string {
	return fmt.Sprintf("%q does not appear to be an ais bucket or does not exist", e.bucket)
}

type errAccessDenied struct {
	entity      string
	operation   string
	accessAttrs uint64
}

func (e *errAccessDenied) String() string {
	return fmt.Sprintf("%s: %s access denied (%#x)", e.entity, e.operation, e.accessAttrs)
}

type BucketAccessDenied struct{ errAccessDenied }
type ObjectAccessDenied struct{ errAccessDenied }

func (e *BucketAccessDenied) Error() string { return "bucket " + e.String() }
func (e *ObjectAccessDenied) Error() string { return "object " + e.String() }

func NewBucketAccessDenied(bucket, oper string, aattrs uint64) *BucketAccessDenied {
	return &BucketAccessDenied{errAccessDenied{bucket, oper, aattrs}}
}
func NewObjectAccessDenied(name, oper string, aattrs uint64) *ObjectAccessDenied {
	return &ObjectAccessDenied{errAccessDenied{name, oper, aattrs}}
}

func IsErrBucketDoesNotExist(err error) bool {
	if _, ok := err.(*ErrorBucketDoesNotExist); ok {
		return true
	}
	_, ok := err.(*ErrorCloudBucketDoesNotExist)
	return ok
}

type ErrorCapacityExceeded struct {
	prefix string
	high   int64
	used   int32
	oos    bool
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
