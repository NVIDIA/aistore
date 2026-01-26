// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import "io"

type multiReader struct {
	mr  io.Reader       // standard io.multiReader
	rcs []io.ReadCloser // to close
}

func newMultiReader(rcs ...io.ReadCloser) *multiReader {
	mrWrapper := &multiReader{
		rcs: rcs,
	}
	readers := make([]io.Reader, len(rcs))
	for i, rc := range rcs {
		readers[i] = io.Reader(rc)
	}
	mrWrapper.mr = io.MultiReader(readers...)
	return mrWrapper
}

func (mr *multiReader) Read(p []byte) (int, error) { return mr.mr.Read(p) }

func (mr *multiReader) Close() (err error) {
	for _, r := range mr.rcs {
		err = r.Close()
	}
	return err
}
