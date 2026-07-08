// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package memsys_test

import (
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools/tassert"
)

// GuardReader reads are guarded vs Free; reads-after-Free return EOF; see memsys.GuardReader
func TestGuardReaderFree(t *testing.T) {
	mem := &memsys.MMSA{Name: "guardr.test", MinPctFree: 50}
	mem.Init(0)
	defer mem.Terminate(false)

	sgl := mem.NewSGL(128)
	sgl.Write(make([]byte, 3000))
	rdr := memsys.NewGuardReader(sgl)

	view, err := rdr.Open()
	tassert.CheckFatal(t, err)

	n, err := view.Read(make([]byte, 1000))
	tassert.Fatalf(t, n == 1000 && err == nil, "read before Free: (%d, %v)", n, err)
	tassert.Fatalf(t, !sgl.IsNil(), "SGL freed before Free")

	rdr.Free()
	rdr.Free() // idempotent
	tassert.Fatalf(t, sgl.IsNil(), "SGL must be freed upon GuardReader.Free")

	// lingering transport reads after Free: clean EOF, no panic
	n, err = view.Read(make([]byte, 1000))
	tassert.Fatalf(t, n == 0 && err == io.EOF, "read after Free: expected (0, EOF), got (%d, %v)", n, err)
}

// concurrent reads racing Free: no panic, no unexpected error, no SGL leak
func TestGuardReaderStress(t *testing.T) {
	mem := &memsys.MMSA{Name: "guardr.stress", MinPctFree: 50}
	mem.Init(0)
	defer mem.Terminate(false)

	const (
		numReaders = 4
		rounds     = 100
	)
	for range rounds {
		sgl := mem.NewSGL(128)
		sgl.Write(make([]byte, 8192))
		rdr := memsys.NewGuardReader(sgl)

		var (
			wg      sync.WaitGroup
			errCh   = make(chan error, numReaders)
			started = make(chan struct{}, numReaders)
		)
		for range numReaders {
			view, err := rdr.Open()
			tassert.CheckFatal(t, err)
			wg.Add(1)
			go func() {
				defer wg.Done()
				started <- struct{}{}
				buf := make([]byte, 512)
				for {
					_, err := view.Read(buf)
					if err == io.EOF {
						return
					}
					if err != nil {
						errCh <- fmt.Errorf("unexpected read error: %v", err)
						return
					}
				}
			}()
		}
		for range numReaders {
			<-started
		}
		rdr.Free() // concurrently with the readers
		rdr.Free() // idempotent, ditto
		wg.Wait()
		close(errCh)
		for err := range errCh {
			tassert.CheckFatal(t, err)
		}
		tassert.Fatalf(t, sgl.IsNil(), "SGL leaked with concurrent readers racing Free")
	}
}

// deterministic overlap: reader completes one read, signals, and keeps reading
// while main frees mid-stream; the reader must then observe clean reads or EOF
func TestGuardReaderFreeMidStream(t *testing.T) {
	mem := &memsys.MMSA{Name: "guardr.midstream", MinPctFree: 50}
	mem.Init(0)
	defer mem.Terminate(false)

	sgl := mem.NewSGL(128)
	sgl.Write(make([]byte, 8192))
	rdr := memsys.NewGuardReader(sgl)

	view, err := rdr.Open()
	tassert.CheckFatal(t, err)

	var (
		firstRead = make(chan struct{})
		done      = make(chan error, 1)
	)
	go func() {
		buf := make([]byte, 512)
		n, err := view.Read(buf)
		if n != len(buf) || err != nil {
			done <- fmt.Errorf("first read: (%d, %v)", n, err)
			return
		}
		close(firstRead) // signal main to Free
		for {
			_, err := view.Read(buf) // keep reading, racing Free
			if err == io.EOF {
				done <- nil
				return
			}
			if err != nil {
				done <- fmt.Errorf("unexpected read error: %v", err)
				return
			}
		}
	}()

	<-firstRead
	rdr.Free()
	tassert.CheckFatal(t, <-done)
	tassert.Fatalf(t, sgl.IsNil(), "SGL must be freed after mid-stream Free")
}
