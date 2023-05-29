// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"errors"

	"github.com/NVIDIA/aistore/cmn/feat"
)

const TarBlockSize = 512 // Size of each block in a tar stream

// supported archive types (file extensions); ref cmd/cli/cli/const.go archExts
const (
	ExtTar    = ".tar"
	ExtTgz    = ".tgz"
	ExtTarTgz = ".tar.gz"
	ExtZip    = ".zip"
	ExtTarLz4 = ".tar.lz4"
)

// NOTE: when adding/removing check `allMagic` as well
var FileExtensions = []string{ExtTar, ExtTgz, ExtTarTgz, ExtZip, ExtTarLz4}

// package-local copy of config feature flags (a change requires restart)
var features feat.Flags

func Init(f feat.Flags) { features = f }

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
