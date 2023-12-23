// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/cmn/cos"
)

const TarBlockSize = 512 // Size of each block in a tar stream

// assorted errors
type (
	ErrUnknownMime struct{ detail string }

	ErrUnknownFileExt struct {
		filename string
		detail   string
	}
)

var ErrTarIsEmpty = errors.New("tar is empty")

func NewErrUnknownMime(d string) *ErrUnknownMime { return &ErrUnknownMime{d} }
func (e *ErrUnknownMime) Error() string          { return "unknown mime type \"" + e.detail + "\"" }

func IsErrUnknownMime(err error) bool {
	_, ok := err.(*ErrUnknownMime)
	return ok
}

func NewErrUnknownFileExt(filename, detail string) (e *ErrUnknownFileExt) {
	e = &ErrUnknownFileExt{filename: filename, detail: detail}
	return
}

func (e *ErrUnknownFileExt) Error() (s string) {
	s = fmt.Sprintf("unknown filename extension (%q, %q)", e.filename, cos.Ext(e.filename))
	if e.detail != "" {
		s += " - " + e.detail
	}
	return
}

func IsErrUnknownFileExt(err error) bool {
	_, ok := err.(*ErrUnknownFileExt)
	return ok
}
