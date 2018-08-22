package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/NVIDIA/dfcpub/dsort/playground/gen/util"
)

const (
	concurrencyLimit = 500

	fileInTarballSize = 1024
	fileInTarballCnt  = 100
	tarballSize       = fileInTarballCnt * fileInTarballSize
)

var (
	dir        string
	ext        string
	tarballCnt int
)

func init() {
	flag.StringVar(&dir, "dir", "data", "directory where the data will be created")
	flag.StringVar(&ext, "ext", ".tar", "extension for tarballs")
	flag.IntVar(&tarballCnt, "shards", 0, "number of shards (tarballs) to create")
	flag.Parse()
}

func main() {
	fmt.Printf("expected shard size: %d bytes\n", tarballSize)
	if err := os.Mkdir(dir, 0750); err != nil && !os.IsExist(err) {
		fmt.Printf("%v\n", err)
		return
	}

	numShardsDigits := len(strconv.Itoa(tarballCnt))
	numFilesDigits := len(strconv.Itoa(fileInTarballCnt))
	doneCh := make(chan struct{}, tarballCnt)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for i := 1; i <= tarballCnt; i++ {
			<-doneCh
			if i%500 == 0 {
				fmt.Printf("created %d/%d shards...\n", i, tarballCnt)
			}
		}
		wg.Done()
		fmt.Println("done")
	}()

	concurrencyCh := make(chan struct{}, concurrencyLimit)
	for i := 0; i < concurrencyLimit; i++ {
		concurrencyCh <- struct{}{}
	}

	for shardNum := 0; shardNum < tarballCnt; shardNum++ {
		<-concurrencyCh
		go func(i int) {
			defer func() {
				doneCh <- struct{}{}
				concurrencyCh <- struct{}{}
			}()
			shardNumStr := fmt.Sprintf("%0*d", numShardsDigits, i)
			tarball, err := os.OpenFile(fmt.Sprintf("%s/shard-%s%s", dir, shardNumStr, ext), os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				fmt.Print(err)
				return
			}
			util.CreateTar(tarball, i*fileInTarballCnt, (i+1)*fileInTarballCnt, fileInTarballSize, numFilesDigits)
			if err = tarball.Close(); err != nil {
				fmt.Print(err)
			}
		}(shardNum)
	}
	wg.Wait()
}
