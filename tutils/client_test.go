// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package tutils_test

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/OneOfOne/xxhash"
)

var (
	server     *httptest.Server
	baseParams api.BaseParams
)

func TestPutFile(t *testing.T) {
	err := putFile(1024, cmn.ChecksumXXHash)
	if err != nil {
		t.Fatal("Put file failed", err)
	}
}

func TestPutSG(t *testing.T) {
	size := int64(10)
	sgl := tutils.MMSA.NewSGL(size)
	defer sgl.Free()
	err := putSG(sgl, size, cmn.ChecksumXXHash)
	if err != nil {
		t.Fatal(err)
	}
}

func putFile(size int64, cksumType string) error {
	fn := "ais-client-test-" + tutils.GenRandomString(32)
	dir := "/tmp"
	r, err := readers.NewFileReader(dir, fn, size, cksumType)
	if err != nil {
		return err
	}
	putArgs := api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        cmn.Bck{Name: "bucket", Provider: cmn.ProviderAIS},
		Object:     "key",
		Cksum:      r.Cksum(),
		Reader:     r,
	}
	err = api.PutObject(putArgs)
	os.Remove(path.Join(dir, fn))
	return err
}

func putRand(size int64, cksumType string) error {
	r, err := readers.NewRandReader(size, cksumType)
	if err != nil {
		return err
	}
	putArgs := api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        cmn.Bck{Name: "bucket", Provider: cmn.ProviderAIS},
		Object:     "key",
		Cksum:      r.Cksum(),
		Reader:     r,
	}
	return api.PutObject(putArgs)
}

func putSG(sgl *memsys.SGL, size int64, cksumType string) error {
	sgl.Reset()
	r, err := readers.NewSGReader(sgl, size, cksumType)
	if err != nil {
		return err
	}
	putArgs := api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        cmn.Bck{Name: "bucket", Provider: cmn.ProviderAIS},
		Object:     "key",
		Cksum:      r.Cksum(),
		Reader:     r,
	}
	return api.PutObject(putArgs)
}

func BenchmarkPutFileWithHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := putFile(1024*1024, cmn.ChecksumXXHash)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutRandWithHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := putRand(1024*1024, cmn.ChecksumXXHash)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutSGWithHash1M(b *testing.B) {
	sgl := tutils.MMSA.NewSGL(cmn.MiB)
	defer sgl.Free()

	for i := 0; i < b.N; i++ {
		err := putSG(sgl, 1024*1024, cmn.ChecksumXXHash)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutFileNoHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := putFile(1024*1024, cmn.ChecksumNone)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutRandNoHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := putRand(1024*1024, cmn.ChecksumNone)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutSGNoHash1M(b *testing.B) {
	sgl := tutils.MMSA.NewSGL(cmn.MiB)
	defer sgl.Free()

	for i := 0; i < b.N; i++ {
		err := putSG(sgl, 1024*1024, cmn.ChecksumNone)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutFileWithHash1MParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := putFile(1024*1024, cmn.ChecksumXXHash)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPutRandWithHash1MParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := putRand(1024*1024, cmn.ChecksumXXHash)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPutSGWithHash1MParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		sgl := tutils.MMSA.NewSGL(cmn.MiB)
		defer sgl.Free()

		for pb.Next() {
			err := putSG(sgl, 1024*1024, cmn.ChecksumXXHash)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestMain(m *testing.M) {
	verifyHash := flag.Bool("verifyhash", true, "True if verify hash when a packet is received")
	flag.Parse()

	// HTTP server; verifies xxhash (if configured)
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			io.Copy(ioutil.Discard, bufio.NewReader(r.Body))
			r.Body.Close()
		}()

		errf := func(s int, msg string) {
			w.WriteHeader(s)
			w.Write([]byte(msg))
		}

		if !*verifyHash {
			return
		}

		// Verify hash
		t := r.Header.Get(cmn.HeaderObjCksumType)
		if t != cmn.ChecksumXXHash {
			errf(http.StatusBadRequest, fmt.Sprintf("Do not know how to handle hash type %s", t))
			return
		}

		hasher := xxhash.New64()
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			errf(http.StatusBadRequest, fmt.Sprintf("Server failed to read, error %v", err))
			return
		}

		hasher.Write(data)
		cksum := cmn.HashToStr(hasher)
		headerCksum := r.Header.Get(cmn.HeaderObjCksumVal)
		if cksum != headerCksum {
			errf(http.StatusNotAcceptable, fmt.Sprintf("Hash mismatch expected = %s, actual = %s", headerCksum, cksum))
		}
	}))
	baseParams = tutils.BaseAPIParams(server.URL)

	exitCode := m.Run()
	server.Close()
	os.Exit(exitCode)
}
