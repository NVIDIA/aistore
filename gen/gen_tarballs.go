package main

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"sync"
)

const (
	dir                   = "data"
	ext                   = ".tar"
	totalDatasetSizeBytes = 100 * 1024 * 1024 * 1024 // 100 GB
	fileSize              = 1024 * 1024              // 1 MB
	numShards             = 10 * 1000
	shardSize             = totalDatasetSizeBytes / numShards
	numFiles              = shardSize / fileSize
)

func main() {
	fmt.Printf("expected shard size: %d bytes\n", shardSize)
	os.Mkdir(dir, 0755)
	uid := os.Getuid()
	gid := os.Getgid()
	numShardsDigits := len(strconv.Itoa(numShards))
	numFilesDigits := len(strconv.Itoa(numFiles))
	doneCh := make(chan struct{}, numShards)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for i := 1; i <= numShards; i++ {
			<-doneCh
			if i%500 == 0 {
				fmt.Printf("created %d/%d shards...\n", i, numShards)
			}
		}
		wg.Done()
		fmt.Println("done")
	}()
	for shardNum := 0; shardNum < numShards; shardNum++ {
		go func(i int) {
			defer func() {
				doneCh <- struct{}{}
			}()
			shardNumStr := fmt.Sprintf("%0*d", numShardsDigits, i)
			tarball, err := os.OpenFile(
				fmt.Sprintf("%s/shard-%s%s", dir, shardNumStr, ext), os.O_CREATE|os.O_RDWR, 0664)
			if err != nil {
				fmt.Print(err)
				return
			}
			tw := tar.NewWriter(tarball)
			for fileNum := i * numFiles; fileNum < (i+1)*numFiles; fileNum++ {
				fileNumStr := fmt.Sprintf("%0*d", numFilesDigits, fileNum)
				h := &tar.Header{
					Typeflag: tar.TypeReg,
					Size:     int64(fileSize),
					Name:     fmt.Sprintf("file-%s.test", fileNumStr),
					Uid:      uid,
					Gid:      gid,
					Mode:     0664,
				}
				if err = tw.WriteHeader(h); err != nil {
					fmt.Print(err)
					return
				}
				b := make([]byte, fileSize)
				if _, err = rand.Read(b); err != nil {
					fmt.Print(err)
					return
				}
				if _, err := io.Copy(tw, bytes.NewReader(b)); err != nil {
					fmt.Print(err)
					return
				}
			}
			if err = tw.Close(); err != nil {
				fmt.Print(err)
			}
			if err = tarball.Close(); err != nil {
				fmt.Print(err)
			}
		}(shardNum)
	}
	wg.Wait()
}
