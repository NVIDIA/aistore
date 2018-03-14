package main

import (
	"flag"
	"fmt"
	"sync"

	"github.com/NVIDIA/dfcpub/pkg/client/readers"

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

	err := putSpecificFiles(0, int64(baseseed), filesize*megabytes, numfiles, clibucket, jobs)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
}

func putSpecificFiles(id int, seed int64, fileSize uint64, numPuts int, bucket string, pool chan func()) error {
	var (
		errch = make(chan error, numfiles)
		wg    = &sync.WaitGroup{}
	)

	dfc.CreateDir(smokeDir)

	for i := 1; i < numPuts+1; i++ {
		r, err := readers.NewRandReader(int64(fileSize), true /* withHash */)
		if err != nil {
			return err
		}

		fname := fmt.Sprintf("l%d", i)
		wg.Add(1)
		pool <- func() {
			client.PutAsync(wg, proxyurl, r, bucket, "__bench/"+fname, errch, false)
		}
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
