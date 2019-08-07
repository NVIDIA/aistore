// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"io"
	"io/ioutil"
)

var (
	_ io.Reader = &nopReader{}
)

type (
	nopReader struct {
		size   int
		offset int
	}
)

func NopReader(size int64) io.Reader {
	return &nopReader{
		size:   int(size),
		offset: 0,
	}
}

func (r *nopReader) Read(b []byte) (int, error) {
	left := r.size - r.offset
	if left == 0 {
		return 0, io.EOF
	}

	toRead := Min(len(b), left)
	r.offset += toRead
	return toRead, nil
}

// DrainReader reads and discards all the data from a reader.
func DrainReader(r io.Reader) error {
	_, err := io.Copy(ioutil.Discard, r)
	return err
}
