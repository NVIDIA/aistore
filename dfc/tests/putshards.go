package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/iosgl"
	"github.com/NVIDIA/dfcpub/pkg/client"
)

const (
	proxyURL = "http://localhost:8080"
	bucket   = "super-test"
)

var (
	// buf    = make([]byte, 1024*1024*1024)
	random = rand.New(rand.NewSource(time.Now().UnixNano()))
	rbuf   = make([]byte, 1024*1024*1024)
)

func init() {
	if _, err := rand.Read(rbuf); err != nil {
		fmt.Print(err)
		return
	}
	fmt.Println("initalized random buffer")
}

func main() {
	flag.Parse()
	exists, err := client.DoesLocalBucketExist(proxyURL, bucket)
	if err != nil {
		glog.Fatalf("local bucket %s does not exist: %v", bucket, err)
	}

	if !exists {
		if err = client.CreateLocalBucket(proxyURL, bucket); err != nil {
			glog.Fatal("failed to create local bucket")
		}
	}

	wg := &sync.WaitGroup{}
	sema := make(chan struct{}, 32) // FIXME #1
	for i := 0; i < 10000; i++ {
		sema <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer func() {
				<-sema
				wg.Done()
			}()
			shardNumStr := fmt.Sprintf("%0*d", 10, i)
			name := fmt.Sprintf("shard-%s.tar", shardNumStr)
			sgl := iosgl.NewSGL(uint64(1024 * 300))
			slab := sgl.Slab()
			buf1 := slab.Alloc()
			if _, err := io.CopyBuffer(sgl, bytes.NewReader(rbuf[:1024*400]), buf1); err != nil {
				fmt.Print(err)
				return
			}
			reader, _ := sgl.Open()
			clientReader := reader.(client.Reader) // FIXME: hack to satisfy client.Put
			if err := client.Put(proxyURL, clientReader, bucket, name, false); err != nil {
				glog.Error(err)
			}
			slab.Free(buf1)
		}(i)
	}
	wg.Wait()
}
