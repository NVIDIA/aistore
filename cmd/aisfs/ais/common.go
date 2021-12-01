// Package ais implements an AIStore client.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
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

type ErrIO struct {
	Kind   string
	Op     string
	Object string
	Err    error
}

func (e *ErrIO) Error() string {
	return fmt.Sprintf("ErrIO: %s op %s %s: %v", e.Kind, e.Op, e.Object, e.Err)
}

func newIOError(err error, kind, op, object string) error {
	return &ErrIO{
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

func newObjectIOError(err error, op, object string) error {
	return newIOError(err, IOErrorKindObject, op, object)
}
