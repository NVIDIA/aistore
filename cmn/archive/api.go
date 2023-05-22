// Package archive
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import "errors"

// supported archive types (file extensions)
const (
	ExtTar    = ".tar"
	ExtTgz    = ".tgz"
	ExtTarTgz = ".tar.gz"
	ExtZip    = ".zip"
	ExtTarLz4 = ".tar.lz4"
)

// NOTE: when adding/removing check `allMagic` as well
var FileExtensions = []string{ExtTar, ExtTgz, ExtTarTgz, ExtZip, ExtTarLz4}

// assorted errors
type (
	ErrUnknownMime    struct{ detail string }
	ErrUnknownFileExt struct{ detail string }
)

var ErrTarIsEmpty = errors.New("tar is empty")

func NewErrUnknownMime(d string) *ErrUnknownMime { return &ErrUnknownMime{d} }
func (e *ErrUnknownMime) Error() string          { return "unknown mime type \"" + e.detail + "\"" }

func IsErrUnknownMime(err error) bool {
	_, ok := err.(*ErrUnknownMime)
	return ok
}

func NewErrUnknownFileExt(d string) *ErrUnknownFileExt { return &ErrUnknownFileExt{d} }
func (e *ErrUnknownFileExt) Error() string             { return "unknown file extension \"" + e.detail + "\"" }

func IsErrUnknownFileExt(err error) bool {
	_, ok := err.(*ErrUnknownFileExt)
	return ok
}
