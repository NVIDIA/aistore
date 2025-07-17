// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/cmn/cos"
)

const TarBlockSize = 512 // Size of each block in a tar stream

const fmtErrTooShort = "%s file is too short, should have at least %d size"

type (
	errUnknownMime struct{ detail string }

	errUnknownFileExt struct {
		filename string
		detail   string
	}
)

var ErrTarIsEmpty = errors.New("tar is empty")

func newErrUnknownMime(d string) error  { return &errUnknownMime{d} }
func (e *errUnknownMime) Error() string { return "unknown mime type \"" + e.detail + "\"" }

func IsErrUnknownMime(err error) bool {
	_, ok := err.(*errUnknownMime)
	return ok
}

func newErrUnknownFileExt(filename, detail string) error {
	return &errUnknownFileExt{filename: filename, detail: detail}
}

func (e *errUnknownFileExt) Error() (s string) {
	s = fmt.Sprintf("unknown filename extension (%q, %q)", e.filename, cos.Ext(e.filename))
	if e.detail != "" {
		s += " - " + e.detail
	}
	return
}

func IsErrUnknownFileExt(err error) bool {
	_, ok := err.(*errUnknownFileExt)
	return ok
}
