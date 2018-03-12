package client_test

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
	"github.com/NVIDIA/dfcpub/pkg/client/readers"
	"github.com/OneOfOne/xxhash"
)

var server *httptest.Server

func TestPutFile(t *testing.T) {
	err := putFile(1024, true /* withHash */)
	if err != nil {
		t.Fatal("Put file failed", err)
	}
}

func putFile(size int64, withHash bool) error {
	dir, fn := "/tmp", "dfc-client-test"
	r, err := readers.NewFileReader(dir, fn, size, withHash)
	if err != nil {
		return err
	}

	err = client.PutWithReader(server.URL, r, "bucket", "key", nil /* errch */, true /* silent */)

	r.Close()
	os.Remove(dir + "/" + fn)
	return err
}

func putInMem(size int64, withHash bool) error {
	r, err := readers.NewInMemReader(size, withHash)
	if err != nil {
		return err
	}
	defer r.Close()

	return client.PutWithReader(server.URL, r, "bucket", "key", nil /* errch */, true /* silent */)
}

func putRand(size int64, withHash bool) error {
	r, err := readers.NewRandReader(size, withHash)
	if err != nil {
		return err
	}
	defer r.Close()

	return client.PutWithReader(server.URL, r, "bucket", "key", nil /* errch */, true /* silent */)
}

func BenchmarkPutFileWithHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := putFile(1024*1024, true /* withHash */)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutInMemWithHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := putInMem(1024*1024, true /* withHash */)
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

func BenchmarkPutFileNoHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := putFile(1024*1024, false /* withHash */)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutInMemNoHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := putInMem(1024*1024, false /* withHash */)
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

		if *verifyHash == false {
			return
		}

		// Verify hash
		t := r.Header.Get(dfc.HeaderDfcChecksumType)
		if t != dfc.ChecksumXXHash {
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
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(hasher.Sum64()))
		hash := hex.EncodeToString(b)
		v := r.Header.Get(dfc.HeaderDfcChecksumVal)
		if hash != v {
			errf(http.StatusNotAcceptable, fmt.Sprintf("Hash mismatch expected = %s, actual = %s", v, hash))
		}
	}))
	defer server.Close()

	os.Exit(m.Run())
}
