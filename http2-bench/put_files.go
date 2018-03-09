package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
)

const (
	blocksize = 1048576
	baseseed  = 1062984096
	megabytes = uint64(1024 * 1024)
	smokeDir  = "/tmp/dfc/smoke"        // smoke test dir
	ProxyURL  = "http://localhost:8080" // assuming local proxy is listening on 8080
)

var (
	numfiles   int
	numworkers int
	filesize   uint64
	clibucket  string
	proxyurl   string
)

func init() {
	flag.StringVar(&proxyurl, "proxyurl", ProxyURL, "Proxy URL")
	flag.StringVar(&clibucket, "bucket", "localbkt", "AWS or GCP bucket")
	flag.IntVar(&numfiles, "files", 10, "Number of files to put")
	flag.IntVar(&numworkers, "workers", 10, "Number of workers")
	flag.Uint64Var(&filesize, "filesize", 1, "Size of files to put in MB")
}

func worker(id int, jobs <-chan func()) {
	for j := range jobs {
		j()
	}
}

func main() {
	flag.Parse()
	jobs := make(chan func(), numfiles)

	for w := 0; w < numworkers; w++ {
		go worker(w, jobs)
	}

	filesput := make(chan string, numfiles)
	err := putSpecificFiles(0, int64(baseseed), filesize*megabytes, numfiles, clibucket, jobs, filesput)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	close(filesput)
	for f := range filesput {
		err = os.Remove(smokeDir + "/" + f)
		if err != nil {
			fmt.Printf("Error removing file: %v\n", err)
			return
		}
	}
}

func putSpecificFiles(id int, seed int64, fileSize uint64, numPuts int, bucket string,
	pool chan func(), filesput chan string) error {
	var (
		src    = rand.NewSource(seed)
		random = rand.New(src)
		buffer = make([]byte, blocksize)
		errch  = make(chan error, numfiles)
		wg     = &sync.WaitGroup{}
	)

	dfc.CreateDir(smokeDir)

	for i := 1; i < numPuts+1; i++ {
		fname := fmt.Sprintf("l%d", i)
		if _, err := client.WriteRandomData(smokeDir+"/"+fname, buffer, int(fileSize), blocksize, random); err != nil {
			return err
		}
		wg.Add(1)
		pool <- func() {
			client.Put(proxyurl, smokeDir+"/"+fname, bucket, "__bench/"+fname, "", wg, errch, false)
		}

		filesput <- fname
	}
	close(pool)
	wg.Wait()
	select {
	case err := <-errch:
		return err
	default:
		return nil
	}
}
