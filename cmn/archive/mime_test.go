// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package archive_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn/archive"
)

func TestMimeContentType(t *testing.T) {
	tests := []struct {
		mime string
		ext  string
	}{
		// IANA and de-facto content types
		{"application/x-tar", archive.ExtTar},
		{"application/gzip", archive.ExtTgz},
		{"application/x-gzip", archive.ExtTgz},
		{"application/zip", archive.ExtZip},
		{"application/x-lz4", archive.ExtTarLz4},
		// "application/x-" + extension (see ais/test/archive_test.go)
		{"application/x-tgz", archive.ExtTgz},
		{"application/x-tar.gz", archive.ExtTarGz},
		{"application/x-zip", archive.ExtZip},
		{"application/x-tar.lz4", archive.ExtTarLz4},
		// extensions, with and without leading '.'
		{archive.ExtTar, archive.ExtTar},
		{archive.ExtTgz, archive.ExtTgz},
		{archive.ExtTarGz, archive.ExtTarGz},
		{archive.ExtZip, archive.ExtZip},
		{archive.ExtTarLz4, archive.ExtTarLz4},
		{"tgz", archive.ExtTgz},
		{"zip", archive.ExtZip},
	}
	for _, test := range tests {
		m, err := archive.Mime(test.mime, "")
		if err != nil {
			t.Errorf("Mime(%q): unexpected error: %v", test.mime, err)
			continue
		}
		if !archive.EqExt(m, test.ext) {
			t.Errorf("Mime(%q) = %q, expected %q", test.mime, m, test.ext)
		}
	}

	// not archive formats
	for _, mime := range []string{".gz", ".lz4", "lz4", "application/octet-stream"} {
		if m, err := archive.Mime(mime, ""); err == nil {
			t.Errorf("Mime(%q) = %q, expected error", mime, m)
		}
	}

	// round-trip
	for _, ext := range archive.FileExtensions {
		ct := archive.ContentTypeFromExt(ext)
		m, err := archive.Mime(ct, "")
		if err != nil {
			t.Errorf("Mime(%q) [from %q]: unexpected error: %v", ct, ext, err)
			continue
		}
		if !archive.EqExt(m, ext) {
			t.Errorf("Mime(%q) [from %q] = %q", ct, ext, m)
		}
	}
}
