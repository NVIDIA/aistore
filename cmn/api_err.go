// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "fmt"

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

type ErrorLocalBucketDoesNotExist struct {
	bucket string
}

func NewErrorLocalBucketDoesNotExist(bucket string) *ErrorLocalBucketDoesNotExist {
	return &ErrorLocalBucketDoesNotExist{bucket: bucket}
}

func (e *ErrorLocalBucketDoesNotExist) Error() string {
	return fmt.Sprintf("bucket %q does not appear to be local or does not exist", e.bucket)
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
