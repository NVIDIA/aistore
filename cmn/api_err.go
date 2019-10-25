// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"os"
)

type (
	ErrorBucketAlreadyExists     struct{ bucket string }
	ErrorCloudBucketDoesNotExist struct{ bucket string }
	ErrorCloudBucketOffline      struct{ bucket, provider string }
	ErrorBucketDoesNotExist      struct{ bucket string }

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
)

func NewErrorBucketAlreadyExists(bucket string) *ErrorBucketAlreadyExists {
	return &ErrorBucketAlreadyExists{bucket: bucket}
}
func (e *ErrorBucketAlreadyExists) Error() string {
	return fmt.Sprintf("bucket %q already exists", e.bucket)
}

func NewErrorCloudBucketDoesNotExist(bucket string) *ErrorCloudBucketDoesNotExist {
	return &ErrorCloudBucketDoesNotExist{bucket: bucket}
}
func (e *ErrorCloudBucketDoesNotExist) Error() string {
	return fmt.Sprintf("cloud bucket %q does not exist", e.bucket)
}

func NewErrorCloudBucketOffline(bucket, provider string) *ErrorCloudBucketOffline {
	return &ErrorCloudBucketOffline{bucket: bucket, provider: provider}
}
func (e *ErrorCloudBucketOffline) Error() string {
	return fmt.Sprintf("%s bucket %q is currently unreachable", e.provider, e.bucket)
}

func NewErrorBucketDoesNotExist(bucket string) *ErrorBucketDoesNotExist {
	return &ErrorBucketDoesNotExist{bucket: bucket}
}
func (e *ErrorBucketDoesNotExist) Error() string {
	return fmt.Sprintf("%q does not appear to be an ais bucket or does not exist", e.bucket)
}

func IsErrBucketUnreachable(err error) bool {
	if _, ok := err.(*ErrorBucketDoesNotExist); ok {
		return true
	}
	if _, ok := err.(*ErrorCloudBucketDoesNotExist); ok {
		return true
	}
	_, ok := err.(*ErrorCloudBucketOffline)
	return ok
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

// This function aggregates all bucket errors which can ever occur.
// Extend if necessary.
func IsErrBucketLevel(err error) bool {
	return IsErrBucketUnreachable(err)
}

// This function aggregates all lom errors which can ever occur.
// Extend if necessary.
func IsErrLOMLevel(err error) bool {
	return os.IsNotExist(err)
}
