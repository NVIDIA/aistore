// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import "fmt"

type (
	ErrBadSignature struct {
		tag      string
		got      string
		expected string
	}
	ErrVersion struct {
		tag      string
		got      uint32
		expected uint32
	}
	ErrJspCompatibleVersion struct {
		ErrVersion
	}
	ErrUnsupportedMetaVersion struct {
		ErrVersion
	}
)

func (e *ErrBadSignature) Error() string {
	return fmt.Sprintf("bad signature %q: got %s, expected %s", e.tag, e.got, e.expected)
}

func newErrVersion(tag string, got, expected uint32, compatibles ...uint32) error {
	err := &ErrVersion{tag, got, expected}
	for _, v := range compatibles {
		if got == v {
			return &ErrJspCompatibleVersion{*err}
		}
	}
	return &ErrUnsupportedMetaVersion{*err}
}

func (e *ErrVersion) Version() uint32 { return e.got }

func (e *ErrUnsupportedMetaVersion) Error() string {
	return fmt.Sprintf("unsupported meta-version %q: got %d, expected %d", e.tag, e.got, e.expected)
}

func (e *ErrJspCompatibleVersion) Error() string {
	return fmt.Sprintf("older but still compatible meta-version %q: %d (the current meta-version is %d)",
		e.tag, e.got, e.expected)
}
