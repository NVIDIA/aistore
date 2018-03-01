package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"sync"

	"github.com/NVIDIA/dfcpub/dfc"
)

const (
	proxyURL        = "http://localhost:8080" // assuming local proxy is listening on 8080
	restAPIProxyPut = proxyURL + "/v1/files"  // version = 1, resource = files
	blocksize       = 1048576
	baseseed        = 1062984096
	megabytes       = uint64(1024 * 1024)
	smokeDir        = "/tmp/dfc/smoke" // smoke test dir
)

var (
	numfiles   int
	numworkers int
	filesize   uint64
	clibucket  string
)

func init() {
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

func put(fname string, bucket string, keyname string, wg *sync.WaitGroup, errch chan error) {
	if wg != nil {
		defer wg.Done()
	}
	proxyurl := restAPIProxyPut + "/" + bucket + "/" + keyname
	client := &http.Client{}
	fmt.Fprintf(os.Stdout, "PUT: %s fname %s\n", keyname, fname)
	file, err := os.Open(fname)
	if err != nil {
		fmt.Printf("Failed to open file %s, err: %v", fname, err)
		if errch != nil {
			fmt.Fprintf(os.Stdout, "Failed to open file %s, err: %v\n", fname, err)
			errch <- fmt.Errorf("Failed to open file %s, err: %v", fname, err)
		}
		return
	}
	defer file.Close()
	req, err := http.NewRequest(http.MethodPut, proxyurl, file)
	if err != nil {
		if errch != nil {
			errch <- fmt.Errorf("Failed to create new http request, err: %v", err)
		}
		return
	}
	// The HTTP package doesn't automatically set this for files, so it has to be done manually
	// If it wasn't set, we would need to deal with the redirect manually.
	req.GetBody = func() (io.ReadCloser, error) {
		return os.Open(fname)
	}

	md5 := md5.New()
	// FIXME: the client must compute xxhash not md5
	md5hash, errstr := dfc.ComputeFileMD5(file, nil, md5)
	if errstr != "" {
		if errch != nil {
			errch <- fmt.Errorf("Failed to compute md5 for file %s, err: %s", fname, errstr)
		}
		return
	}
	req.Header.Set("Content-HASH", md5hash)
	_, err = file.Seek(0, 0)
	if err != nil {
		if errch != nil {
			errch <- fmt.Errorf("Failed to seek file %s, err: %v", fname, err)
		}
		return
	}
	r, err := client.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	if r != nil {
		if r.StatusCode >= http.StatusBadRequest {
			if errch != nil {
				errch <- fmt.Errorf("Bad status code: http status %d", r.StatusCode)
			}
			return
		}
		_, err = ioutil.ReadAll(r.Body)
		if err != nil {
			if errch != nil {
				errch <- fmt.Errorf("Failed to read response content, err %v", err)
			}
			return
		}
	} else {
		if errch != nil {
			errch <- fmt.Errorf("Failed to get proxy put response, err %v", err)
		}
		return
	}
}

func writeRandomData(fname string, bytes []byte, filesize int, random *rand.Rand) (int, error) {
	f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE, 0666) //wr-wr-wr-
	if err != nil {
		return 0, err
	}
	nblocks := filesize / blocksize
	tot := 0
	var r int
	for i := 0; i <= nblocks; i++ {
		if blocksize < filesize-tot {
			r = blocksize
		} else {
			r = filesize - tot
		}
		random.Read(bytes[0:r])
		n, err := f.Write(bytes[0:r])
		if err != nil {
			return tot, err
		} else if n < r {
			return tot, io.ErrShortWrite
		}
		tot += n
	}
	return tot, f.Close()
}
func putSpecificFiles(id int, seed int64, fileSize uint64, numPuts int, bucket string,
	pool chan func(), filesput chan string) error {
	var (
		src    = rand.NewSource(seed)
		random = rand.New(src)
		buffer = make([]byte, blocksize)
		errch  = make(chan error)
		wg     = &sync.WaitGroup{}
	)
	for i := 1; i < numPuts+1; i++ {
		fname := fmt.Sprintf("l%d", i)
		if _, err := writeRandomData(smokeDir+"/"+fname, buffer, int(fileSize), random); err != nil {
			return err
		}
		wg.Add(1)
		pool <- func() {
			put(smokeDir+"/"+fname, bucket, "__bench/"+fname, wg, errch)
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
