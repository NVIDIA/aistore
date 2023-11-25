// Package tools provides common tools and utilities for all unit and integration tests
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package tools_test

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/trand"
)

var bp api.BaseParams

func TestPutFile(t *testing.T) {
	err := putFile(1024, cos.ChecksumXXHash)
	if err != nil {
		t.Fatal("Put file failed", err)
	}
}

func TestPutSG(t *testing.T) {
	size := int64(10)
	mmsa := memsys.ByteMM()
	sgl := mmsa.NewSGL(size)
	defer sgl.Free()

	err := putSG(sgl, size, cos.ChecksumXXHash)
	if err != nil {
		t.Fatal(err)
	}
}

func putFile(size int64, cksumType string) error {
	fn := "ais-client-test-" + trand.String(32)
	dir := "/tmp"
	r, err := readers.NewRandFile(dir, fn, size, cksumType)
	if err != nil {
		return err
	}
	putArgs := api.PutArgs{
		BaseParams: bp,
		Bck:        cmn.Bck{Name: "bucket", Provider: apc.AIS},
		ObjName:    "key",
		Cksum:      r.Cksum(),
		Reader:     r,
	}
	_, err = api.PutObject(&putArgs)
	os.Remove(path.Join(dir, fn))
	return err
}

func putRand(size int64, cksumType string) error {
	r, err := readers.NewRand(size, cksumType)
	if err != nil {
		return err
	}
	putArgs := api.PutArgs{
		BaseParams: bp,
		Bck:        cmn.Bck{Name: "bucket", Provider: apc.AIS},
		ObjName:    "key",
		Cksum:      r.Cksum(),
		Reader:     r,
	}
	_, err = api.PutObject(&putArgs)
	return err
}

func putSG(sgl *memsys.SGL, size int64, cksumType string) error {
	sgl.Reset()
	r, err := readers.NewSG(sgl, size, cksumType)
	if err != nil {
		return err
	}
	putArgs := api.PutArgs{
		BaseParams: bp,
		Bck:        cmn.Bck{Name: "bucket", Provider: apc.AIS},
		ObjName:    "key",
		Cksum:      r.Cksum(),
		Reader:     r,
	}
	_, err = api.PutObject(&putArgs)
	return err
}

func BenchmarkPutFileWithHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := putFile(1024*1024, cos.ChecksumXXHash)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutRandWithHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := putRand(1024*1024, cos.ChecksumXXHash)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutSGWithHash1M(b *testing.B) {
	mmsa := memsys.PageMM()
	sgl := mmsa.NewSGL(cos.MiB)
	defer sgl.Free()

	for i := 0; i < b.N; i++ {
		err := putSG(sgl, 1024*1024, cos.ChecksumXXHash)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutFileNoHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := putFile(1024*1024, cos.ChecksumNone)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutRandNoHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := putRand(1024*1024, cos.ChecksumNone)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutSGNoHash1M(b *testing.B) {
	mmsa := memsys.PageMM()
	sgl := mmsa.NewSGL(cos.MiB)
	defer sgl.Free()

	for i := 0; i < b.N; i++ {
		err := putSG(sgl, 1024*1024, cos.ChecksumNone)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutFileWithHash1MParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := putFile(1024*1024, cos.ChecksumXXHash)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPutRandWithHash1MParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := putRand(1024*1024, cos.ChecksumXXHash)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPutSGWithHash1MParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		mmsa, _ := memsys.NewMMSA("dev-tools", false)
		sgl := mmsa.NewSGL(cos.MiB)
		defer func() {
			mmsa.Terminate(false)
		}()

		for pb.Next() {
			err := putSG(sgl, 1024*1024, cos.ChecksumXXHash)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestMain(m *testing.M) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			io.Copy(io.Discard, bufio.NewReader(r.Body))
			r.Body.Close()
		}()

		errCb := func(statusCode int, f string, a ...any) {
			w.WriteHeader(statusCode)
			fmt.Fprintf(w, f, a...)
		}

		// Verify checksum.
		var (
			cksumType  = r.Header.Get(apc.HdrObjCksumType)
			cksumValue = r.Header.Get(apc.HdrObjCksumVal)
		)
		_, cksum, err := cos.CopyAndChecksum(io.Discard, r.Body, nil, cksumType)
		if err != nil {
			errCb(http.StatusBadRequest, "server failed to read, error %v", err)
			return
		}
		if cksum != nil && cksum.Value() != cksumValue {
			errCb(http.StatusNotAcceptable, "cksum mismatch got: %q, expected: %q", cksum.Value(), cksumValue)
		}
	}))
	bp = tools.BaseAPIParams(srv.URL)

	m.Run()
	srv.Close()
}
