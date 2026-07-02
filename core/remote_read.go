// Package core provides core metadata and in-cluster API.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"fmt"
)

type (
	RemoteSource struct {
		Provider string
		Bucket   string
		Object   string
	}

	RemoteSourceError struct {
		RemoteSource
		Operation  string
		StatusCode int
		Err        error
	}
)

func (src *RemoteSource) WrapErr(operation string, statusCode int, err error) error {
	if src == nil || err == nil {
		return err
	}
	return &RemoteSourceError{
		RemoteSource: *src,
		Operation:    operation,
		StatusCode:   statusCode,
		Err:          err,
	}
}

func (e *RemoteSourceError) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("remote source %s %s failed for %s/%s (status=%d): %v",
			e.Provider, e.Operation, e.Bucket, e.Object, e.StatusCode, e.Err)
	}
	return fmt.Sprintf("remote source %s %s failed for %s/%s: %v",
		e.Provider, e.Operation, e.Bucket, e.Object, e.Err)
}

func (e *RemoteSourceError) Unwrap() error { return e.Err }
