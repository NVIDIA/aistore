/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/common"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/pkg/client"
	"github.com/NVIDIA/dfcpub/tutils"
)

var (
	filesizes = [3]int64{128 * common.KiB, 192 * common.KiB, 256 * common.KiB}
	ratios    = [5]float32{0, 0.25, 0.50, 0.75, 1} // #gets / #puts
)

func Test_smoke(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	proxyURL := getPrimaryURL(t, proxyURLRO)
	if err := common.CreateDir(LocalDestDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", LocalDestDir, err)
	}

	if err := common.CreateDir(SmokeDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", SmokeDir, err)
	}

	created := createLocalBucketIfNotExists(t, proxyURL, clibucket)
	fp := make(chan string, len(filesizes)*len(ratios)*numops*numworkers)
	bs := int64(baseseed)
	for _, fs := range filesizes {
		for _, r := range ratios {
			s := fmt.Sprintf("size:%s,GET/PUT:%.0f%%", common.B2S(fs, 0), r*100)
			t.Run(s, func(t *testing.T) { oneSmoke(t, proxyURL, fs, r, bs, fp) })
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
		go client.Del(proxyURL, clibucket, "smoke/"+file, wg, errch, true)
	}
	wg.Wait()
	select {
	case err := <-errch:
		t.Error(err)
	default:
	}

	if created {
		destroyLocalBucket(t, proxyURL, clibucket)
	}
}

func oneSmoke(t *testing.T, proxyURL string, filesize int64, ratio float32, bseed int64, filesput chan string) {
	// Start the worker pools
	errch := make(chan error, 100)
	var wg = &sync.WaitGroup{}
	// Decide the number of each type
	var (
		nGet = int(float32(numworkers) * ratio)
		nPut = numworkers - nGet
		sgls = make([]*memsys.SGL, numworkers, numworkers)
	)

	// Get the workers started
	if usingSG {
		for i := 0; i < numworkers; i++ {
			sgls[i] = client.Mem2.NewSGL(filesize)
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
				var sgl *memsys.SGL
				if usingSG {
					sgl = sgls[i]
				}

				putRandomFiles(proxyURL, bseed+int64(i), uint64(filesize), numops, clibucket, t, nil, errch, filesput,
					SmokeDir, SmokeStr, true, sgl)
				wg.Done()
			}(i)
			nPut--
		} else {
			wg.Add(1)
			go func(i int) {
				getRandomFiles(proxyURL, bseed+int64(i), numops, clibucket, SmokeStr+"/", t, nil, errch)
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

func getRandomFiles(proxyURL string, seed int64, numGets int, bucket, prefix string, t *testing.T, wg *sync.WaitGroup, errch chan error) {
	if wg != nil {
		defer wg.Done()
	}

	src := rand.NewSource(seed)
	random := rand.New(src)
	getsGroup := &sync.WaitGroup{}
	var msg = &api.GetMsg{GetPrefix: prefix, GetPageSize: int(pagesize)}
	for i := 0; i < numGets; i++ {
		items, err := client.ListBucket(proxyURL, bucket, msg, 0)
		if err != nil {
			errch <- err
			t.Error(err)
			return
		}

		if items == nil {
			errch <- fmt.Errorf("listbucket %s: is empty - no entries", bucket)
			// not considered as a failure, just can't do the test
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
			// not considered as a failure, just can't do the test
			return
		}

		keyname := files[random.Intn(len(files))]
		getsGroup.Add(1)
		go client.Get(proxyURL, bucket, keyname, getsGroup, errch, !testing.Verbose(), false /* validate */)
	}

	getsGroup.Wait()
}

func fillWithRandomData(proxyURL string, seed int64, fileSize uint64, objList []string, bucket string,
	t *testing.T, errch chan error, filesput chan string,
	dir, keystr string, silent bool, sgl *memsys.SGL) {
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
			r, err = client.NewSGReader(sgl, int64(size), true /* with Hash */)
		} else {
			r, err = client.NewReader(client.ParamReader{
				Type: readerType,
				SGL:  nil,
				Path: dir,
				Name: fname,
				Size: int64(size),
			})
		}

		if err != nil {
			tutils.Logf("Failed to generate random file %s, err: %v\n", dir+"/"+fname, err)
			t.Error(err)
			if errch != nil {
				errch <- err
			}
			return
		}

		// We could PUT while creating files, but that makes it
		// begin all the puts immediately (because creating random files is fast
		// compared to the listbucket call that getRandomFiles does)
		err = client.Put(proxyURL, r, bucket, keystr+"/"+fname, silent)
		if err != nil {
			if errch == nil {
				fmt.Println("Error channel is nil, do not know how to report error")
			}
			errch <- err
		}
		filesput <- fname
	}
}

func putRandomFiles(proxyURL string, seed int64, fileSize uint64, numPuts int, bucket string,
	t *testing.T, wg *sync.WaitGroup, errch chan error, filesput chan string,
	dir, keystr string, silent bool, sgl *memsys.SGL) {
	if wg != nil {
		defer wg.Done()
	}

	src := rand.NewSource(seed)
	random := rand.New(src)
	fileList := make([]string, 0, numPuts)
	for i := 0; i < numPuts; i++ {
		fname := tutils.FastRandomFilename(random, fnlen)
		fileList = append(fileList, fname)
	}

	fillWithRandomData(proxyURL, seed, fileSize, fileList, bucket, t, errch, filesput, dir, keystr, silent, sgl)
}
