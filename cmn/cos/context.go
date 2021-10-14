// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"io"
)

type (
	// Declare a new type for Context field names.
	contextID string

	ReadWrapperFunc func(r io.ReadCloser) io.ReadCloser
	SetSizeFunc     func(size int64)
)

const (
	CtxReadWrapper contextID = "readWrapper" // context key for ReadWrapperFunc
	CtxSetSize     contextID = "setSize"     // context key for SetSizeFunc
	CtxOriginalURL contextID = "origURL"     // context key for OriginalURL for HTTP cloud
)
