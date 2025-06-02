// Package readers provides implementation for common reader types
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package readers_test

import (
	"io"
	"math/rand/v2"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
)

const mmName = "readers_test"

func TestFileReader(t *testing.T) {
	r, err := readers.NewRandFile("/tmp", "seek", 10240, cos.ChecksumNone)
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
	r, err := readers.NewRand(size, cos.ChecksumCesXxh)
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

		r, err := readers.NewSG(sgl, size, cos.ChecksumCesXxh)
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

		r, err := readers.NewSG(sgl, size, cos.ChecksumCesXxh)
		if err != nil {
			t.Fatal(err)
		}
		testReaderBasic(t, r, size)
		r.Close()
	}
}

func BenchmarkFileReaderCreateWithHash1M(b *testing.B) {
	filepath := "/tmp"
	fn := "reader-test"

	for b.Loop() {
		r, err := readers.NewRandFile(filepath, fn, cos.MiB, cos.ChecksumCesXxh)
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
		r, err := readers.NewRand(cos.MiB, cos.ChecksumCesXxh)
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
		r, err := readers.NewSG(sgl, cos.MiB, cos.ChecksumCesXxh)
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
		r, err := readers.NewRandFile(filepath, fn, cos.MiB, cos.ChecksumNone)
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
		r, err := readers.NewRand(cos.MiB, cos.ChecksumNone)
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
		r, err := readers.NewSG(sgl, cos.MiB, cos.ChecksumNone)
		r.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}
