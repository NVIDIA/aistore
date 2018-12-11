// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package memsys_test

// E.g. running this specific test:
//
// go test -v -run=SGLS -verbose=true -logtostderr=true
//
// For more examples, see other tests in this directory
//

import (
	"bytes"
	"io"
	"io/ioutil"
	"sync"
	"testing"

	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/tutils"
)

const (
	objsize = 100
	objects = 1000
	workers = 1000
)

// creates 2 SGL, put some data to one of them and them copy from SGL to SGL
func TestSGLStressN(t *testing.T) {
	mem := &memsys.Mem2{MinPctFree: 50, Name: "cmem", Debug: verbose}
	err := mem.Init(true /* ignore errors */)
	defer mem.Stop(nil)
	if err != nil {
		t.Fatal(err)
	}
	wg := &sync.WaitGroup{}
	fn := func(id int) {
		defer wg.Done()
		for i := 0; i < objects; i++ {
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
			tutils.CheckFatal(err, t)

			// copy SGL to SGL
			rr := memsys.NewReader(sglR)
			_, err = io.Copy(sglW, rr)
			tutils.CheckFatal(err, t)

			// read SGL from destination and compare with the original
			var bufW []byte
			bufW, err = ioutil.ReadAll(memsys.NewReader(sglW))
			tutils.CheckFatal(err, t)
			for j := 0; j < objsize; j++ {
				if bufW[j] != bufR[j] {
					tutils.Logf("IN : %s\nOUT: %s\n", string(bufR), string(bufW))
					t.Fatalf("Step %d failed", i)
				}
			}
			sglR.Free() // removing these two lines fixes the test
			sglW.Free()
		}
		tutils.Progress(id, 100)
	}
	for n := 0; n < workers; n++ {
		wg.Add(1)
		go fn(n)
	}
	wg.Wait()
}
