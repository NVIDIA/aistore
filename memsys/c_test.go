// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package memsys_test

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	objsize = 100
	objects = 10000
	workers = 1000
)

// concurrently: write buffer => SGL, copy SGL => SGL, read back and compare
func TestSGLStressN(t *testing.T) {
	mem := &memsys.MMSA{Name: "cmem", MinPctFree: 50}
	mem.Init(0)
	defer mem.Terminate(false)
	num := objects
	if testing.Short() {
		num = objects / 10
	}
	wg := &sync.WaitGroup{}
	fn := func() {
		defer wg.Done()
		bufR := make([]byte, objsize)
		for i := range num {
			for j := range objsize {
				bufR[j] = byte('A') + byte(i%26)
			}
			if err := sglCopyCmp(mem, bufR); err != nil {
				t.Errorf("step %d: %v", i, err)
				return
			}
		}
	}
	for range workers {
		wg.Add(1)
		go fn()
	}
	wg.Wait()
}

func sglCopyCmp(mem *memsys.MMSA, bufR []byte) error {
	sglR := mem.NewSGL(128)
	defer sglR.Free()
	sglW := mem.NewSGL(128)
	defer sglW.Free()

	if _, err := io.Copy(sglR, bytes.NewReader(bufR)); err != nil {
		return err
	}
	if _, err := io.Copy(sglW, memsys.NewReader(sglR)); err != nil {
		return err
	}
	bufW, err := cos.ReadAll(memsys.NewReader(sglW))
	if err != nil {
		return err
	}
	if !bytes.Equal(bufW, bufR) {
		return fmt.Errorf("IN: %q, OUT: %q", bufR, bufW)
	}
	return nil
}
