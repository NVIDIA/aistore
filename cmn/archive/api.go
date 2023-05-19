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

const TarBlockSize = 512 // Size of each block in a tar stream

type ErrUnknownMime struct{ detail string }

var (
	ArchExtensions = []string{ExtTar, ExtTgz, ExtTarTgz, ExtZip, ExtMsgpack}
	ErrTarIsEmpty  = errors.New("tar is empty")
)

func NewUnknownMimeError(d string) *ErrUnknownMime { return &ErrUnknownMime{d} }
func (e *ErrUnknownMime) Error() string            { return "unknown mime type \"" + e.detail + "\"" }
