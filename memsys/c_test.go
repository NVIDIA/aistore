// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package memsys_test

// E.g. running this specific test:
//
// go test -v -run=SGLS -verbose=true -logtostderr=true
//
// For more examples, see the README in this directory
//

import (
	"bytes"
	"io"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	objsize = 100
	objects = 10000
	workers = 1000
)

// creates 2 SGL, put some data to one of them and them copy from SGL to SGL
func TestSGLStressN(t *testing.T) {
	mem := &memsys.MMSA{MinPctFree: 50, Name: "cmem"}
	err := mem.Init(false /*panic env err*/, false /*panic insuff mem err*/)
	defer mem.Terminate()
	if err != nil {
		t.Fatal(err)
	}
	num := objects
	if testing.Short() {
		num = objects / 10
	}
	wg := &sync.WaitGroup{}
	fn := func() {
		defer wg.Done()
		for i := 0; i < num; i++ {
			sglR := mem.NewSGL(128)
			sglW := mem.NewSGL(128)
			bufR := make([]byte, objsize)

			// fill buffer with "unique content"
			for j := 0; j < objsize; j++ {
				bufR[j] = byte('A') + byte(i%26)
			}

			// save buffer to SGL
			br := bytes.NewReader(bufR)
			_, err := io.Copy(sglR, br)
			tassert.CheckFatal(t, err)

			// copy SGL to SGL
			rr := memsys.NewReader(sglR)
			_, err = io.Copy(sglW, rr)
			tassert.CheckFatal(t, err)

			// read SGL from destination and compare with the original
			var bufW []byte
			bufW, err = io.ReadAll(memsys.NewReader(sglW))
			tassert.CheckFatal(t, err)
			for j := 0; j < objsize; j++ {
				if bufW[j] != bufR[j] {
					tlog.Logf("IN : %s\nOUT: %s\n", string(bufR), string(bufW))
					t.Errorf("Step %d failed", i)
					return
				}
			}
			sglR.Free() // removing these two lines fixes the test
			sglW.Free()
		}
	}
	for n := 0; n < workers; n++ {
		wg.Add(1)
		go fn()
	}
	wg.Wait()
}
