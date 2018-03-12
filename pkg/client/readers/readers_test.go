package readers_test

import (
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/NVIDIA/dfcpub/pkg/client/readers"
)

func TestFileReader(t *testing.T) {
	r, err := readers.NewFileReader("/tmp", "seek", 10240, false /* withHash */)
	if err != nil {
		t.Fatal("Failed to create file reader", err)
	}

	_, err = r.Seek(0, 0)
	if err != nil {
		t.Fatal("Failed to seek", err)
	}
}

func TestRandReader(t *testing.T) {
	size := int64(1024)
	r, err := readers.NewRandReader(size, true /* withHash */)
	data := make([]byte, size)
	n, err := r.Read(data)
	if err != nil || int64(n) != size {
		t.Fatal("Failed to read all data", n, err)
	}

	{
		// Read return the correct number of data when there are enough data
		o, err := r.Seek(-20, io.SeekEnd)
		if err != nil || o != size-20 {
			t.Fatal("Failed to seek", o, err)
		}

		buf := make([]byte, 8)
		n, err := r.Read(buf)
		if err != nil || n != 8 {
			t.Fatal("Failed to read", n, err)
		}

		n, err = r.Read(buf)
		if err != nil || n != 8 {
			t.Fatal("Failed to read", n, err)
		}

		n, err = r.Read(buf)
		if err != nil || n != 4 {
			t.Fatal("Failed to read when there is less data", n, err)
		}

		_, err = r.Read(buf)
		if err != io.EOF {
			t.Fatal("Failed to read when it is EOF", err)
		}
	}

	{
		// Seek from start and read should return the correct data
		n, err := r.Seek(100, io.SeekStart)
		if err != nil || n != 100 {
			t.Fatal("Failed to seek", n, err)
		}

		buf := make([]byte, 20)
		m, err := r.Read(buf)
		if err != nil || m != 20 {
			t.Fatal("Failed to read after seek", n, err)
		}

		if reflect.DeepEqual(buf, data[100:120]) != true {
			t.Fatal("Failed to match data after seek and read", buf, data[100:120])
		}

		r.Seek(0, io.SeekStart)
	}

	{
		// Seek relative and read should return the correct data
		_, err := r.Seek(size-40, io.SeekStart)
		if err != nil {
			t.Fatal("Failed to seek", err)
		}

		n, err := r.Seek(-20, io.SeekCurrent)
		if err != nil || n != size-60 {
			t.Fatal("Failed to seek", n, err)
		}

		buf := make([]byte, 20)
		m, err := r.Read(buf)
		if err != nil || m != 20 {
			t.Fatal("Failed to read after seek", n, err)
		}

		if reflect.DeepEqual(buf, data[size-60:size-40]) != true {
			t.Fatal("Failed to match data after seek and read", buf, data[size-60:size-40])
		}

		r.Seek(0, io.SeekStart)
	}

	{
		// Seek from end and read should return the correct data
		_, err := r.Seek(-40, io.SeekEnd)
		if err != nil {
			t.Fatal("Failed to seek", err)
		}

		buf := make([]byte, 20)
		m, err := r.Read(buf)
		if err != nil || m != 20 {
			t.Fatal("Failed to read after seek", n, err)
		}

		if reflect.DeepEqual(buf, data[size-40:size-20]) != true {
			t.Fatal("Failed to match data after seek and read", buf, data[size-40:size-20])
		}

		r.Seek(0, io.SeekStart)
	}

	{
		// Seek pass EOF
		_, err := r.Seek(size+10, io.SeekStart)
		if err != nil {
			t.Fatal("Failed to seek pass EOF", err)
		}

		buf := make([]byte, 20)
		_, err = r.Read(buf)
		if err == nil {
			t.Fatal("Failed to return error while reading pass EOF")
		}

		r.Seek(0, io.SeekStart)
	}
}

func BenchmarkFileReaderCreateWithHash1M(b *testing.B) {
	path := "/tmp"
	fn := "reader-test"

	for i := 0; i < b.N; i++ {
		r, err := readers.NewFileReader(path, fn, 1024*1024, true /* withHash */)
		r.Close()
		os.Remove(path + "/" + fn)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInMemReaderCreateWithHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		r, err := readers.NewInMemReader(1024*1024, true /* withHash */)
		r.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRandReaderCreateWithHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		r, err := readers.NewRandReader(1024*1024, true /* withHash */)
		r.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileReaderCreateNoHash1M(b *testing.B) {
	path := "/tmp"
	fn := "reader-test"

	for i := 0; i < b.N; i++ {
		r, err := readers.NewFileReader(path, fn, 1024*1024, false /* withHash */)
		r.Close()
		os.Remove(path + "/" + fn)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInMemReaderCreateNoHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		r, err := readers.NewInMemReader(1024*1024, false /* withHash */)
		r.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRandReaderCreateNoHash1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		r, err := readers.NewRandReader(1024*1024, false /* withHash */)
		r.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}
