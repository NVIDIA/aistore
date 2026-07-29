// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"archive/tar"
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestExtractRejectsUnsafePaths(t *testing.T) {
	for _, tc := range []struct {
		name, entry string
	}{
		{"parent", "../outside"},
		{"embedded parent", "dir/../outside"},
		{"absolute", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			archive := filepath.Join(dir, "archive.tar")
			entry := tc.entry
			if entry == "" {
				entry = filepath.Join(dir, "outside")
			}
			size := writeTestTar(t, archive, entry, "attack")
			_, err := doExtract("archive.tar", archive, size)
			tassert.Fatalf(t, err != nil, "expected extraction of %q to fail", entry)
			tassert.Fatalf(t, !fileExists(filepath.Join(dir, "outside")), "entry escaped destination")
		})
	}
}

func TestExtractRejectsSymlinkEscape(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "archive.tar")
	dstDir, outside := filepath.Join(dir, "archive"), filepath.Join(dir, "outside")
	tassert.CheckFatal(t, os.MkdirAll(dstDir, 0o750))
	tassert.CheckFatal(t, os.MkdirAll(outside, 0o750))
	if err := os.Symlink(outside, filepath.Join(dstDir, "link")); err != nil {
		t.Skipf("cannot create symlink: %v", err)
	}

	size := writeTestTar(t, archive, "link/escaped", "attack")
	_, err := doExtract("archive.tar", archive, size)
	tassert.Fatalf(t, err != nil, "expected symlink escape to fail")
	tassert.Fatalf(t, !fileExists(filepath.Join(outside, "escaped")), "entry escaped through symlink")
}

func TestExtractDoesNotClobber(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "archive.tar")
	existing := filepath.Join(dir, "archive", "existing")
	tassert.CheckFatal(t, os.MkdirAll(filepath.Dir(existing), 0o750))
	tassert.CheckFatal(t, os.WriteFile(existing, []byte("original"), 0o600))

	size := writeTestTar(t, archive, "existing", "replacement")
	_, err := doExtract("archive.tar", archive, size)
	tassert.Fatalf(t, errors.Is(err, os.ErrExist), "expected existing-file error, got %v", err)
	data, err := os.ReadFile(existing)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, string(data) == "original", "existing file was overwritten")
}

func TestExtractNestedFile(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "archive.tar")
	size := writeTestTar(t, archive, "nested/file", "content")
	_, err := doExtract("archive.tar", archive, size)
	tassert.CheckFatal(t, err)
	data, err := os.ReadFile(filepath.Join(dir, "archive", "nested", "file"))
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, string(data) == "content", "unexpected extracted content %q", data)
}

func writeTestTar(t *testing.T, fqn, name, content string) int64 {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	tassert.CheckFatal(t, tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(content))}))
	_, err := tw.Write([]byte(content))
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, tw.Close())
	tassert.CheckFatal(t, os.WriteFile(fqn, buf.Bytes(), 0o600))
	return int64(buf.Len())
}

func fileExists(fqn string) bool {
	_, err := os.Stat(fqn)
	return err == nil
}
