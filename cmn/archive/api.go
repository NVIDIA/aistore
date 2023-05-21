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

	// msgpack doesn't have a "common extension", see for instance:
	// * https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types
	// however, there seems to be a de-facto agreement wrt Content-Type
	// * application/msgpack
	// * application/x-msgpack (<<< recommended)
	// * application/*+msgpack
	// AIS uses the following single constant for both the default file extension
	// and for the Content-Type (the latter with offset [1:])
	ExtMsgpack = ".msgpack"
)

type (
	ErrUnknownMime    struct{ detail string }
	ErrUnknownFileExt struct{ detail string }
)

var (
	FileExtensions = []string{ExtTar, ExtTgz, ExtTarTgz, ExtZip, ExtMsgpack}
	ErrTarIsEmpty  = errors.New("tar is empty")
)

func NewErrUnknownMime(d string) *ErrUnknownMime { return &ErrUnknownMime{d} }
func (e *ErrUnknownMime) Error() string          { return "unknown mime type \"" + e.detail + "\"" }

func IsErrUnknownMime(err error) bool {
	_, ok := err.(*ErrUnknownMime)
	return ok
}

func NewErrUnknownFileExt(d string) *ErrUnknownFileExt { return &ErrUnknownFileExt{d} }
func (e *ErrUnknownFileExt) Error() string             { return "unknown file extension \"" + e.detail + "\"" }
