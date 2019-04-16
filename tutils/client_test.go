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
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/OneOfOne/xxhash"
)

var (
	server     *httptest.Server
	baseParams *api.BaseParams
)

func TestPutFile(t *testing.T) {
	err := putFile(1024, true /* withHash */)
	if err != nil {
		t.Fatal("Put file failed", err)
	}
}

func TestPutSG(t *testing.T) {
	size := int64(10)
	sgl := tutils.Mem2.NewSGL(size)
	defer sgl.Free()
	err := putSG(sgl, size, true /* withHash */)
	if err != nil {
		t.Fatal(err)
	}
}

func putFile(size int64, withHash bool) error {
	fn := "ais-client-test-" + tutils.FastRandomFilename(rand.New(rand.NewSource(time.Now().UnixNano())), 32)
	dir := "/tmp"
	r, err := tutils.NewFileReader(dir, fn, size, withHash)
	if err != nil {
		return err
	}
	putArgs := api.PutObjectArgs{
		BaseParams: baseParams,
		Bucket:     "bucket",
		Object:     "key",
		Hash:       r.XXHash(),
		Reader:     r,
	}
	err = api.PutObject(putArgs)
	r.Close()
	os.Remove(path.Join(dir, fn))
	return err
}

func putRand(size int64, withHash bool) error {
	r, err := tutils.NewRandReader(size, withHash)
	if err != nil {
		return err
	}
	defer r.Close()
	putArgs := api.PutObjectArgs{
		BaseParams: baseParams,
		Bucket:     "bucket",
		Object:     "key",
		Hash:       r.XXHash(),
		Reader:     r,
	}
	return api.PutObject(putArgs)
}

func putSG(sgl *memsys.SGL, size int64, withHash bool) error {
	sgl.Reset()
	r, err := tutils.NewSGReader(sgl, size, withHash)
	if err != nil {
		return err
	}
	defer r.Close()
	putArgs := api.PutObjectArgs{
		BaseParams: baseParams,
		Bucket:     "bucket",
		Object:     "key",
		Hash:       r.XXHash(),
		Reader:     r,
	}
	return api.PutObject(putArgs)
}

func BenchmarkPutFileWithHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := putFile(1024*1024, true /* withHash */)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutRandWithHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := putRand(1024*1024, true /* withHash */)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutSGWithHash1M(b *testing.B) {
	sgl := tutils.Mem2.NewSGL(cmn.MiB)
	defer sgl.Free()

	for i := 0; i < b.N; i++ {
		err := putSG(sgl, 1024*1024, true /* withHash */)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutFileNoHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := putFile(1024*1024, false /* withHash */)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutRandNoHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := putRand(1024*1024, false /* withHash */)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutSGNoHash1M(b *testing.B) {
	sgl := tutils.Mem2.NewSGL(cmn.MiB)
	defer sgl.Free()

	for i := 0; i < b.N; i++ {
		err := putSG(sgl, 1024*1024, false /* withHash */)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutFileWithHash1MParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := putFile(1024*1024, true /* withHash */)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPutRandWithHash1MParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := putRand(1024*1024, true /* withHash */)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPutSGWithHash1MParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		sgl := tutils.Mem2.NewSGL(cmn.MiB)
		defer sgl.Free()

		for pb.Next() {
			err := putSG(sgl, 1024*1024, true /* withHash */)
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
	defer server.Close()
	baseParams = tutils.BaseAPIParams(server.URL)

	os.Exit(m.Run())
}
