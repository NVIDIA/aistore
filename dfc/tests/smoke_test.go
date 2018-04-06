/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/NVIDIA/dfcpub/pkg/client/readers"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
)

const (
	SmokeDir        = "/tmp/dfc/smoke" // smoke test dir
	SmokeStr        = "smoke"
	blocksize       = 1048576
	defaultbaseseed = 1062984096
)

var (
	numops    int
	fnlen     int
	baseseed  int64
	filesizes = [3]int{128 * 1024, 1024 * 1024, 4 * 1024 * 1024} // 128 KiB, 1MiB, 4 MiB
	ratios    = [6]float32{0, 0.1, 0.25, 0.5, 0.75, 0.9}         // #gets / #puts
)

func init() {
	flag.IntVar(&numops, "numops", 4, "Number of PUT/GET per worker")
	flag.IntVar(&fnlen, "fnlen", 20, "Length of randomly generated filenames")
	// When running multiple tests at the same time on different threads, ensure that
	// They are given different seeds, as the tests are completely deterministic based on
	// choice of seed, so they will interfere with each other.
	flag.Int64Var(&baseseed, "seed", defaultbaseseed, "Seed to use for random number generators")
}

func Test_smoke(t *testing.T) {
	parse()

	if err := client.Tcping(proxyurl); err != nil {
		tlogf("%s: %v\n", proxyurl, err)
		os.Exit(1)
	}

	if err := dfc.CreateDir(LocalDestDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", LocalDestDir, err)
	}

	if err := dfc.CreateDir(SmokeDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", SmokeDir, err)
	}

	fp := make(chan string, len(filesizes)*len(ratios)*numops*numworkers)
	bs := int64(baseseed)
	for _, fs := range filesizes {
		for _, r := range ratios {
			t.Run(fmt.Sprintf("Filesize:%dB,Ratio:%.3f%%", fs, r*100), func(t *testing.T) { oneSmoke(t, fs, r, bs, fp) })
			bs += int64(numworkers + 1)
		}
	}

	close(fp)

	// Clean up all the files from the test
	wg := &sync.WaitGroup{}
	errch := make(chan error, len(filesizes)*len(ratios)*numops*numworkers)
	for file := range fp {
		if usingFile {
			err := os.Remove(SmokeDir + "/" + file)
			if err != nil {
				t.Error(err)
			}
		}
		wg.Add(1)
		go client.Del(proxyurl, clibucket, "smoke/"+file, wg, errch, false)
	}
	wg.Wait()
	select {
	case err := <-errch:
		t.Error(err)
	default:
	}
}

func oneSmoke(t *testing.T, filesize int, ratio float32, bseed int64, filesput chan string) {
	// Start the worker pools
	errch := make(chan error, 100)
	var wg = &sync.WaitGroup{}
	// Decide the number of each type
	var (
		nGet = int(float32(numworkers) * ratio)
		nPut = numworkers - nGet
		sgls = make([]*dfc.SGLIO, numworkers, numworkers)
	)

	// Get the workers started
	if usingSG {
		for i := 0; i < numworkers; i++ {
			sgls[i] = dfc.NewSGLIO(uint64(filesize))
		}
		defer func() {
			for _, sgl := range sgls {
				sgl.Free()
			}
		}()
	}

	for i := 0; i < numworkers; i++ {
		if (i%2 == 0 && nPut > 0) || nGet == 0 {
			wg.Add(1)
			go func(i int) {
				var sgl *dfc.SGLIO
				if usingSG {
					sgl = sgls[i]
				}

				putRandomFiles(i, bseed+int64(i), uint64(filesize), numops, clibucket, t, nil, errch, filesput,
					SmokeDir, SmokeStr, "", false, sgl)
				wg.Done()
			}(i)
			nPut--
		} else {
			wg.Add(1)
			go func(i int) {
				getRandomFiles(i, bseed+int64(i), numops, clibucket, t, nil, errch)
				wg.Done()
			}(i)
			nGet--
		}
	}
	wg.Wait()
	select {
	case err := <-errch:
		t.Error(err)
	default:
	}
}

func getRandomFiles(id int, seed int64, numGets int, bucket string, t *testing.T, wg *sync.WaitGroup, errch chan error) {
	if wg != nil {
		defer wg.Done()
	}
	src := rand.NewSource(seed)
	random := rand.New(src)
	getsGroup := &sync.WaitGroup{}
	var msg = &dfc.GetMsg{}
	for i := 0; i < numGets; i++ {
		items, cerr := client.ListBucket(proxyurl, bucket, msg)
		if testfail(cerr, "List files with prefix failed", nil, errch, t) {
			return
		}

		if items == nil {
			errch <- fmt.Errorf("listbucket %s: is empty - no entries", bucket)
			return
		}
		files := make([]string, 0)
		for _, it := range items.Entries {
			// directories show up as files with '/' endings - filter them out
			if it.Name[len(it.Name)-1] != '/' {
				files = append(files, it.Name)
			}
		}
		if len(files) == 0 {
			errch <- fmt.Errorf("Cannot retrieve from an empty bucket %s", bucket)
			return
		}
		keyname := files[random.Intn(len(files))]
		tlogln("GET: " + keyname)
		getsGroup.Add(1)
		go client.Get(proxyurl, bucket, keyname, getsGroup, errch, false, false)
	}
	getsGroup.Wait()
}

func fillWithRandomData(seed int64, fileSize uint64, objList []string, bucket string,
	t *testing.T, errch chan error, filesput chan string,
	dir, keystr string, silent bool, sgl *dfc.SGLIO) {
	src := rand.NewSource(seed)
	random := rand.New(src)
	for _, fname := range objList {
		size := fileSize
		if size == 0 {
			size = uint64(random.Intn(1024)+1) * 1024
		}

		var (
			r   client.Reader
			err error
		)
		if sgl != nil {
			sgl.Reset()
			r, err = readers.NewSGReader(sgl, int64(size), true /* with Hash */)
		} else {
			r, err = readers.NewReader(readers.ParamReader{
				Type: readerType,
				SGL:  nil,
				Path: dir,
				Name: fname,
				Size: int64(size),
			})
		}

		if err != nil {
			t.Error(err)
			fmt.Fprintf(os.Stderr, "Failed to generate random file %s, err: %v\n", dir+"/"+fname, err)
			if errch != nil {
				errch <- err
			}
			return
		}

		// We could PUT while creating files, but that makes it
		// begin all the puts immediately (because creating random files is fast
		// compared to the listbucket call that getRandomFiles does)
		err = client.Put(proxyurl, r, bucket, keystr+"/"+fname, silent)
		if err != nil {
			if errch == nil {
				fmt.Println("Error channel is nil, do not know how to report error")
			}
			errch <- err
		}
		filesput <- fname
	}
}

func putRandomFiles(id int, seed int64, fileSize uint64, numPuts int, bucket string,
	t *testing.T, wg *sync.WaitGroup, errch chan error, filesput chan string,
	dir, keystr, htype string, silent bool, sgl *dfc.SGLIO) {
	if wg != nil {
		defer wg.Done()
	}

	src := rand.NewSource(seed)
	random := rand.New(src)
	fileList := make([]string, 0, numPuts)
	for i := 0; i < numPuts; i++ {
		fname := client.FastRandomFilename(random, fnlen)
		fileList = append(fileList, fname)
	}

	fillWithRandomData(seed, fileSize, fileList, bucket, t, errch, filesput, dir, keystr, silent, sgl)
}
