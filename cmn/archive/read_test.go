// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package archive_test

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"testing"

	"github.com/NVIDIA/aistore/cmn/archive"
)

func TestReadOneEmptyArchiveName(t *testing.T) {
	tests := []struct {
		name string
		mime string
	}{
		{"tar", archive.ExtTar},
		{"zip", archive.ExtZip},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			switch test.mime {
			case archive.ExtTar:
				tw := tar.NewWriter(&buf)
				hdr := &tar.Header{Name: "", Mode: 0o600, Size: 3, Typeflag: tar.TypeReg}
				if err := tw.WriteHeader(hdr); err != nil {
					t.Fatal(err)
				}
				if _, err := tw.Write([]byte("abc")); err != nil {
					t.Fatal(err)
				}
				if err := tw.Close(); err != nil {
					t.Fatal(err)
				}
			case archive.ExtZip:
				zw := zip.NewWriter(&buf)
				w, err := zw.Create("")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := w.Write([]byte("abc")); err != nil {
					t.Fatal(err)
				}
				if err := zw.Close(); err != nil {
					t.Fatal(err)
				}
			}

			ar, err := archive.NewReader(test.mime, bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			if err != nil {
				t.Fatal(err)
			}
			r, err := ar.ReadOne("anything")
			if err != nil {
				t.Fatal(err)
			}
			if r != nil {
				_ = r.Close()
				t.Fatal("unexpected match for empty archived filename")
			}
		})
	}
}
