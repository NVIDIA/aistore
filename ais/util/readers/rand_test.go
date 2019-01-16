/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package readers

import (
	"io"
	"testing"
)

func TestRandReaderEmptyFile(t *testing.T) {
	var (
		chunkSize = 36 * 1024
		buf       = make([]byte, chunkSize)
		err       error
		n         int
	)

	emptyReader := NewRandReader(0)
	n, err = emptyReader.Read(buf)
	if n != 0 {
		t.Errorf("Empty file - read %d bytes", n)
	}
	if err != io.EOF {
		t.Errorf("Empty file returned incorrect error: %v", err)
	}
}

// read a file by chunks - a file size is greater than chunk size
func TestRandReaderBigFile(t *testing.T) {
	var (
		chunkSize = 36 * 1024
		buf       = make([]byte, chunkSize)
		err       error
		n         int
		fileSize  = 64 * 1024
	)

	readerChunks := NewRandReader(int64(fileSize))
	n, err = readerChunks.Read(buf)
	if err != nil {
		t.Errorf("First read failed: %v", err)
	}
	if n != chunkSize {
		t.Errorf("Incorrect number of bytes read: %d bytes, expected %d bytes", n, fileSize)
	}
	n, err = readerChunks.Read(buf)
	if err != nil && err != io.EOF {
		t.Errorf("Second read failed: %v", err)
	}
	expected := fileSize - chunkSize
	if n != expected {
		t.Errorf("Second read %d bytes, expected %d", n, expected)
	}
}

// read a chunk bigger than the file
func TestRandReaderSmallFile(t *testing.T) {
	var (
		chunkSize = 36 * 1024
		buf       = make([]byte, chunkSize)
		err       error
		n         int
		fileSize  = 16 * 1024
	)

	readerChunks := NewRandReader(int64(fileSize))
	n, err = readerChunks.Read(buf)
	if err != nil {
		t.Errorf("Read failed: %v", err)
	}
	if n != fileSize {
		t.Errorf("Incorrect number of bytes read: %d bytes, expected %d bytes", n, fileSize)
	}
	n, err = readerChunks.Read(buf)
	if n != 0 || err != io.EOF {
		t.Errorf("Reading beyond file end must fail (read %d bytes, error: %v)", n, err)
	}
}

// read the file with a size of a chunk
func TestRandReaderExactSize(t *testing.T) {
	var (
		chunkSize = 36 * 1024
		buf       = make([]byte, chunkSize)
		err       error
		n         int
		fileSize  = chunkSize
	)

	readerChunks := NewRandReader(int64(fileSize))
	n, err = readerChunks.Read(buf)
	if err != nil {
		t.Errorf("Read failed: %v", err)
	}
	if n != fileSize {
		t.Errorf("Incorrect number of bytes read: %d bytes, expected %d bytes", n, fileSize)
	}
	n, err = readerChunks.Read(buf)
	if n != 0 || err != io.EOF {
		t.Errorf("Reading beyond file end must fail (read %d bytes, error: %v)", n, err)
	}
}
