// Package ais implements an AIStore client.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
)

//////////
// ERRORS
//////////

const (
	IOErrorKindBucket = "bucket"
	IOErrorKindObject = "object"
)

type IOError struct {
	Kind   string
	Op     string
	Object string
	Err    error
}

func (e *IOError) Error() string {
	return fmt.Sprintf("IOError: %s op %s %s: %v",
		e.Kind, e.Op, e.Object, e.Err)
}

func newIOError(err error, kind string, op string, object string) error {
	return &IOError{
		Kind:   kind,
		Op:     op,
		Object: object,
		Err:    err,
	}
}

func newBucketIOError(err error, op string, object ...string) error {
	if len(object) > 0 {
		return newIOError(err, IOErrorKindBucket, op, object[0])
	}
	return newIOError(err, IOErrorKindBucket, op, "")
}

func newObjectIOError(err error, op string, object string) error {
	return newIOError(err, IOErrorKindObject, op, object)
}
