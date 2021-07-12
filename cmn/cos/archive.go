// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import "strings"

// supported archive types (file extensions)
const (
	ExtTar    = ".tar"
	ExtTgz    = ".tgz"
	ExtTarTgz = ".tar.gz"
	ExtZip    = ".zip"
)

var ArchExtensions = []string{ExtTar, ExtTgz, ExtTarTgz, ExtZip}

func IsGzipped(filename string) bool {
	return strings.HasSuffix(filename, ExtTgz) || strings.HasSuffix(filename, ExtTarTgz)
}

type (
	ErrUnknownMime struct {
		detail string
	}
)

func (e *ErrUnknownMime) Error() string            { return "unknown mime type \"" + e.detail + "\"" }
func NewUnknownMimeError(d string) *ErrUnknownMime { return &ErrUnknownMime{d} }

// Map user-specified mime type OR the filename's extension to one of the supported ArchExtensions
func Mime(mime, filename string) (ext string, err error) {
	// user-specified (intended) format takes precedence
	if mime != "" {
		if strings.Contains(mime, ExtTarTgz[1:]) { // ExtTarTgz contains ExtTar
			return ExtTarTgz, nil
		}
		for _, ext := range ArchExtensions {
			if strings.Contains(mime, ext[1:]) {
				return ext, nil
			}
		}
		err = NewUnknownMimeError(mime)
		return
	}
	// otherwise, by extension
	for _, ext := range ArchExtensions {
		if strings.HasSuffix(filename, ext) {
			return ext, nil
		}
	}
	err = NewUnknownMimeError(filename)
	return
}
