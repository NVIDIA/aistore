package util

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/tutils"
)

var (
	buf    = make([]byte, 1024*1024*1024)
	random = rand.New(rand.NewSource(time.Now().UnixNano()))
	rbuf   = make([]byte, 1024*1024*1024)

	mu sync.Mutex
)

func init() {
	if _, err := rand.Read(rbuf); err != nil {
		fmt.Print(err)
		return
	}
	fmt.Println("initalized random buffer")
}

func CreateTar(w io.Writer, start, end, size, digits int) {
	uid := os.Getuid()
	gid := os.Getgid()
	tw := tar.NewWriter(w)
	for fileNum := start; fileNum < end; fileNum++ {
		fileNumStr := fmt.Sprintf("%0*d", digits, fileNum)
		mu.Lock()
		h := &tar.Header{
			Typeflag: tar.TypeReg,
			Size:     int64(size),
			Name:     fmt.Sprintf("%s-%s.test", tutils.FastRandomFilename(random, 5), fileNumStr),
			Uid:      uid,
			Gid:      gid,
			Mode:     0664,
		}
		mu.Unlock()
		if err := tw.WriteHeader(h); err != nil {
			fmt.Print(err)
			return
		}
		if _, err := io.CopyBuffer(tw, bytes.NewReader(rbuf[:size]), buf); err != nil {
			fmt.Print(err)
			return
		}
	}
	if err := tw.Close(); err != nil {
		fmt.Print(err)
	}
}
