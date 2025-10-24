// Package readers provides implementation for common reader types
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package readers_test

import (
	"archive/tar"
	"io"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
)

const mmName = "readers_test"

//
// older unit tests - plain objects
//

func TestFileReader(t *testing.T) {
	r, err := readers.New(&readers.Arg{
		Type:      readers.File,
		Path:      "/tmp",
		Name:      "seek",
		Size:      10240,
		CksumType: cos.ChecksumNone,
	})
	if err != nil {
		t.Fatal("Failed to create file reader", err)
	}
	tassert.CheckFatal(t, r.Close())
}

func testReaderBasic(t *testing.T, r readers.Reader, size int64) {
	_, err := r.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal("Failed to seek", err)
	}

	data := make([]byte, size)
	n, err := r.Read(data)
	if err != nil || int64(n) != size {
		t.Fatal("Failed to read all data", n, err)
	}

	{
		// Seek from start and read should return the correct data
		n, err := r.Seek(100, io.SeekStart)
		if err != nil || n != 100 {
			t.Fatal("Failed to seek", n, err)
		}

		buf := make([]byte, 20)
		m, err := r.Read(buf)
		if err != nil || m != 20 {
			t.Fatal("Failed to read after seek", n, err)
		}

		if !reflect.DeepEqual(buf, data[100:120]) {
			t.Fatal("Failed to match data after seek and read", buf, data[100:120])
		}

		r.Seek(0, io.SeekStart)
	}

	{
		// Seek from end and read should return the correct data
		_, err := r.Seek(-40, io.SeekEnd)
		if err != nil {
			t.Fatal("Failed to seek", err)
		}

		buf := make([]byte, 20)
		m, err := r.Read(buf)
		if err != nil || m != 20 {
			t.Fatal("Failed to read after seek", n, err)
		}

		if !reflect.DeepEqual(buf, data[size-40:size-20]) {
			t.Fatal("Failed to match data after seek and read", buf, data[size-40:size-20])
		}

		r.Seek(0, io.SeekStart)
	}

	{
		// Seek from end and read should return the correct data
		_, err := r.Seek(-40, io.SeekEnd)
		if err != nil {
			t.Fatal("Failed to seek", err)
		}

		buf := make([]byte, 20)
		m, err := r.Read(buf)
		if err != nil || m != 20 {
			t.Fatal("Failed to read after seek", n, err)
		}

		if !reflect.DeepEqual(buf, data[size-40:size-20]) {
			t.Fatal("Failed to match data after seek and read", buf, data[size-40:size-20])
		}

		r.Seek(0, io.SeekStart)
	}
}

// NOTE: These are testcases that fail when running on SGReader.
func testReaderAdv(t *testing.T, r readers.Reader, size int64) {
	buf := make([]byte, size)
	_, err := r.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal("Failed to seek", err)
	}

	data := make([]byte, size)
	n, err := r.Read(data)
	if err != nil || int64(n) != size {
		t.Fatal("Failed to read all data", n, err)
	}

	{
		// Seek pass EOF
		_, err := r.Seek(size+10, io.SeekStart)
		if err != nil {
			t.Fatal("Failed to seek pass EOF", err)
		}

		buf := make([]byte, 20)
		_, err = r.Read(buf)
		if err == nil {
			t.Fatal("Failed to return error while reading pass EOF")
		}

		r.Seek(0, io.SeekStart)
	}

	{
		// Seek relative and read should return the correct data
		_, err := r.Seek(size-40, io.SeekStart)
		if err != nil {
			t.Fatal("Failed to seek", err)
		}

		n, err := r.Seek(-20, io.SeekCurrent)
		if err != nil || n != size-60 {
			t.Fatal("Failed to seek", n, err)
		}

		buf := make([]byte, 20)
		m, err := r.Read(buf)
		if err != nil || m != 20 {
			t.Fatal("Failed to read after seek", n, err)
		}

		if !reflect.DeepEqual(buf, data[size-60:size-40]) {
			t.Fatal("Failed to match data after seek and read", buf, data[size-60:size-40])
		}

		r.Seek(0, io.SeekStart)
	}

	{
		// Read return the correct number of data when there are enough data
		m, err := r.Seek(0, io.SeekStart)
		if err != nil {
			t.Fatal("Failed to seek", err)
		}

		if m != 0 {
			t.Fatal("Failed to seek to begin", m)
		}

		n, err := r.Read(buf[:size-20])
		if err != nil {
			t.Fatal("Failed to seek", err)
		}

		if int64(n) != size-20 {
			t.Fatalf("Failed to seek, expected %d, actual %d", size-20, n)
		}

		n, err = r.Read(buf[:8])
		if err != nil || n != 8 {
			t.Fatal("Failed to read", n, err)
		}

		n, err = r.Read(buf[:8])
		if err != nil || n != 8 {
			t.Fatal("Failed to read", n, err)
		}

		n, err = r.Read(buf[:8])
		if err != nil || n != 4 {
			t.Fatal("Failed to read when there is less data", n, err)
		}

		_, err = r.Read(buf)
		if err != io.EOF {
			t.Fatal("Failed to read when it is EOF", err)
		}
	}

	{
		// Read return the correct number of data when there are enough data
		o, err := r.Seek(-20, io.SeekEnd)
		if err != nil {
			t.Fatal("Failed to seek", err)
		}
		if o != size-20 {
			t.Fatalf("Failed to seek, offset expected %d, actual %d", size-20, o)
		}

		buf := make([]byte, 8)
		n, err := r.Read(buf)
		if err != nil || n != 8 {
			t.Fatal("Failed to read", n, err)
		}

		n, err = r.Read(buf)
		if err != nil || n != 8 {
			t.Fatal("Failed to read", n, err)
		}

		n, err = r.Read(buf)
		if err != nil || n != 4 {
			t.Fatal("Failed to read when there is less data", n, err)
		}

		_, err = r.Read(buf)
		if err != io.EOF {
			t.Fatal("Failed to read when it is EOF", err)
		}
	}
}

func TestRandReader(t *testing.T) {
	size := int64(1024)
	r, err := readers.New(&readers.Arg{
		Type:      readers.Rand,
		Size:      size,
		CksumType: cos.ChecksumCesXxh,
	})
	if err != nil {
		t.Fatal(err)
	}
	testReaderBasic(t, r, size)
	testReaderAdv(t, r, size)
	r.Close()
}

func TestSGReader(t *testing.T) {
	mmsa := memsys.NewMMSA(mmName, false)
	defer mmsa.Terminate(false)
	{
		// Basic read
		size := max(rand.Int64N(cos.MiB), cos.KiB+rand.Int64N(cos.KiB))
		sgl := mmsa.NewSGL(size)
		defer sgl.Free()

		r, err := readers.New(&readers.Arg{
			Type:      readers.SG,
			SGL:       sgl,
			Size:      size,
			CksumType: cos.ChecksumCesXxh,
		})
		if err != nil {
			t.Fatal(err)
		}

		lenHeader := rand.IntN(cos.KiB)
		buf := make([]byte, size)
		n, err := r.Read(buf[:lenHeader])
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}

		if n != lenHeader {
			t.Fatalf("Read returned wrong number of bytes, expected = %d, actual = %d", lenHeader, n)
		}

		// read the rest
		n, err = r.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}

		if n != int(size)-lenHeader {
			t.Fatalf("Read returned wrong number of bytes, expected = %d, actual = %d", int(size)-lenHeader, n)
		}

		r.Close()
	}

	{
		size := int64(1024)
		sgl := mmsa.NewSGL(size)
		defer sgl.Free()

		r, err := readers.New(&readers.Arg{
			Type:      readers.SG,
			SGL:       sgl,
			Size:      size,
			CksumType: cos.ChecksumCesXxh,
		})
		if err != nil {
			t.Fatal(err)
		}
		testReaderBasic(t, r, size)
		r.Close()
	}
}

//
// assorted readers have the capability to provide archived content
//

func TestArchReader(t *testing.T) {
	mmsa := memsys.NewMMSA(mmName, false)
	defer mmsa.Terminate(false)

	tests := []struct {
		name        string
		arch        *readers.Arch
		wantErr     bool
		errContains string
	}{
		{
			name: "basic_tar",
			arch: &readers.Arch{
				Mime:    archive.ExtTar,
				Num:     5,
				MinSize: cos.KiB,
				MaxSize: cos.KiB,
				Seed:    123,
			},
		},
		{
			name: "with_extension",
			arch: &readers.Arch{
				Mime:    archive.ExtTgz,
				Num:     3,
				MinSize: 100,
				MaxSize: 200,
				RecExt:  ".json",
				Seed:    456,
			},
		},
		{
			name: "extension_no_dot",
			arch: &readers.Arch{
				Mime:    archive.ExtTar,
				Num:     2,
				MinSize: 50,
				MaxSize: 50,
				RecExt:  "txt", // Should normalize to ".txt"
				Seed:    789,
			},
		},
		{
			name: "with_prefix",
			arch: &readers.Arch{
				Mime:    archive.ExtZip,
				Num:     4,
				MinSize: cos.KiB,
				MaxSize: 2 * cos.KiB,
				Prefix:  "data/trunk-",
				Seed:    111,
			},
		},
		{
			name: "random_names",
			arch: &readers.Arch{
				Mime:      archive.ExtTar,
				Num:       3,
				MinSize:   100,
				MaxSize:   100,
				RandNames: true,
				Seed:      222,
			},
		},
		{
			name: "random_dirs",
			arch: &readers.Arch{
				Mime:      archive.ExtTgz,
				Num:       5,
				MinSize:   500,
				MaxSize:   1000,
				RandDir:   true,
				MaxLayers: 3,
				Seed:      333,
			},
		},
		{
			name: "explicit_names",
			arch: &readers.Arch{
				Mime:    archive.ExtTar,
				Num:     3,
				MinSize: 100,
				MaxSize: 100,
				Names:   []string{"file1.txt", "file2.json", "file3.csv"},
				Seed:    444,
			},
		},
		{
			name: "tar_format_override",
			arch: &readers.Arch{
				Mime:      archive.ExtTar,
				TarFormat: tar.FormatGNU,
				Num:       2,
				MinSize:   cos.KiB,
				MaxSize:   cos.KiB,
				Seed:      555,
			},
		},
		{
			name: "deterministic_seed",
			arch: &readers.Arch{
				Mime:    archive.ExtTar,
				Num:     3,
				MinSize: 100,
				MaxSize: 100,
				Seed:    999, // Same seed should produce same output
			},
		},
		// Error cases
		{
			name: "invalid_min_negative",
			arch: &readers.Arch{
				Mime:    archive.ExtTar,
				Num:     1,
				MinSize: -1,
				MaxSize: 100,
			},
			wantErr:     true,
			errContains: "MinSize must be non-negative",
		},
		{
			name: "invalid_max_less_than_min",
			arch: &readers.Arch{
				Mime:    archive.ExtTar,
				Num:     1,
				MinSize: 1000,
				MaxSize: 100,
			},
			wantErr:     true,
			errContains: "MaxSize",
		},
		{
			name: "invalid_names_length",
			arch: &readers.Arch{
				Mime:    archive.ExtTar,
				Num:     3,
				MinSize: 100,
				MaxSize: 100,
				Names:   []string{"only_one.txt"}, // Should be 3
			},
			wantErr:     true,
			errContains: "Names length",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sgl := mmsa.NewSGL(0)
			defer sgl.Free()

			r, err := readers.New(&readers.Arg{
				Type:      readers.SG,
				SGL:       sgl,
				CksumType: cos.ChecksumCesXxh,
				Arch:      tt.arch,
			})

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errContains)
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("expected error containing %q, got %q", tt.errContains, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer r.Close()

			// Verify we can read the archive
			buf := make([]byte, sgl.Size())
			n, err := r.Read(buf)
			if err != nil && err != io.EOF {
				t.Fatalf("read error: %v", err)
			}
			if n != int(sgl.Size()) {
				t.Fatalf("read size mismatch: got %d, want %d", n, sgl.Size())
			}

			// Verify checksum is set
			if r.Cksum() == nil {
				t.Fatal("expected checksum, got nil")
			}
		})
	}
}

func TestArchFileReader(t *testing.T) {
	tmpDir := t.TempDir()

	arch := &readers.Arch{
		Mime:    archive.ExtTgz,
		Num:     3,
		MinSize: cos.KiB,
		MaxSize: 2 * cos.KiB,
		Seed:    777,
	}

	r, err := readers.New(&readers.Arg{
		Type:      readers.File,
		Path:      tmpDir,
		Name:      "test.tgz",
		CksumType: cos.ChecksumCesXxh,
		Arch:      arch,
	})
	if err != nil {
		t.Fatalf("failed to create file reader: %v", err)
	}
	defer r.Close()

	// Verify file was created
	filePath := filepath.Join(tmpDir, "test.tgz")
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("archive file not created: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("archive file is empty")
	}

	// Verify we can read it
	buf := make([]byte, info.Size())
	n, err := r.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("read error: %v", err)
	}
	if int64(n) != info.Size() {
		t.Fatalf("read size mismatch: got %d, want %d", n, info.Size())
	}
}

func TestRandReaderRejectsArch(t *testing.T) {
	_, err := readers.New(&readers.Arg{
		Type: readers.Rand,
		Size: cos.KiB,
		Arch: &readers.Arch{
			Mime:    archive.ExtTar,
			Num:     1,
			MinSize: 100,
			MaxSize: 100,
		},
	})
	if err == nil {
		t.Fatal("expected error when using Arch with Rand reader")
	}
	if !strings.Contains(err.Error(), "does not support archival content") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

//
// micro-benchmarks
//

func BenchmarkFileReaderCreateWithHash1M(b *testing.B) {
	filepath := "/tmp"
	fn := "reader-test"

	for b.Loop() {
		r, err := readers.New(&readers.Arg{
			Type:      readers.File,
			Path:      filepath,
			Name:      fn,
			Size:      cos.MiB,
			CksumType: cos.ChecksumCesXxh,
		})
		if err != nil {
			os.Remove(path.Join(filepath, fn))
			b.Fatal(err)
		}
		if err := r.Close(); err != nil {
			os.Remove(path.Join(filepath, fn))
			b.Fatal(err)
		}
		os.Remove(path.Join(filepath, fn))
	}
}

func BenchmarkRandReaderCreateWithHash1M(b *testing.B) {
	for b.Loop() {
		r, err := readers.New(&readers.Arg{
			Type:      readers.Rand,
			Size:      cos.MiB,
			CksumType: cos.ChecksumCesXxh,
		})
		r.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSGReaderCreateWithHash1M(b *testing.B) {
	mmsa := memsys.NewMMSA(mmName, false)
	sgl := mmsa.NewSGL(cos.MiB)
	defer func() {
		sgl.Free()
		mmsa.Terminate(false)
	}()

	for b.Loop() {
		sgl.Reset()
		r, err := readers.New(&readers.Arg{
			Type:      readers.SG,
			SGL:       sgl,
			Size:      cos.MiB,
			CksumType: cos.ChecksumCesXxh,
		})
		r.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileReaderCreateNoHash1M(b *testing.B) {
	filepath := "/tmp"
	fn := "reader-test"

	for b.Loop() {
		r, err := readers.New(&readers.Arg{
			Type:      readers.File,
			Path:      filepath,
			Name:      fn,
			Size:      cos.MiB,
			CksumType: cos.ChecksumNone,
		})
		if err != nil {
			os.Remove(path.Join(filepath, fn))
			b.Fatal(err)
		}
		if err := r.Close(); err != nil {
			os.Remove(path.Join(filepath, fn))
			b.Fatal(err)
		}
		os.Remove(path.Join(filepath, fn))
	}
}

func BenchmarkRandReaderCreateNoHash1M(b *testing.B) {
	for b.Loop() {
		r, err := readers.New(&readers.Arg{
			Type:      readers.Rand,
			Size:      cos.MiB,
			CksumType: cos.ChecksumNone,
		})
		r.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSGReaderCreateNoHash1M(b *testing.B) {
	mmsa := memsys.NewMMSA(mmName, false)
	sgl := mmsa.NewSGL(cos.MiB)
	defer func() {
		sgl.Free()
		mmsa.Terminate(false)
	}()

	for b.Loop() {
		sgl.Reset()
		r, err := readers.New(&readers.Arg{
			Type:      readers.SG,
			SGL:       sgl,
			Size:      cos.MiB,
			CksumType: cos.ChecksumNone,
		})
		r.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}
