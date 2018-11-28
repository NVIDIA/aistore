/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/tutils"
)

var (
	objSizes = [3]int64{128 * cmn.KiB, 192 * cmn.KiB, 256 * cmn.KiB}
	ratios   = [5]float32{0, 0.25, 0.50, 0.75, 1} // #gets / #puts
)

func Test_smoke(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	proxyURL := getPrimaryURL(t, proxyURLRO)
	if err := cmn.CreateDir(LocalDestDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", LocalDestDir, err)
	}

	if err := cmn.CreateDir(SmokeDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", SmokeDir, err)
	}

	created := createLocalBucketIfNotExists(t, proxyURL, clibucket)
	fp := make(chan string, len(objSizes)*len(ratios)*numops*numworkers)
	bs := int64(baseseed)
	for _, fs := range objSizes {
		for _, r := range ratios {
			s := fmt.Sprintf("size:%s,GET/PUT:%.0f%%", cmn.B2S(fs, 0), r*100)
			t.Run(s, func(t *testing.T) { oneSmoke(t, proxyURL, fs, r, bs, fp) })
			bs += int64(numworkers + 1)
		}
	}

	close(fp)

	// Clean up all the files from the test
	wg := &sync.WaitGroup{}
	errCh := make(chan error, len(objSizes)*len(ratios)*numops*numworkers)
	for file := range fp {
		if usingFile {
			err := os.Remove(path.Join(SmokeDir, file))
			if err != nil {
				t.Error(err)
			}
		}
		wg.Add(1)
		go tutils.Del(proxyURL, clibucket, "smoke/"+file, wg, errCh, true)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		t.Error(err)
	default:
	}

	if created {
		destroyLocalBucket(t, proxyURL, clibucket)
	}
}

func oneSmoke(t *testing.T, proxyURL string, objSize int64, ratio float32, bseed int64, filesPutCh chan string) {
	// Start the worker pools
	errCh := make(chan error, 100)
	var wg = &sync.WaitGroup{}
	// Decide the number of each type
	var (
		nGet = int(float32(numworkers) * ratio)
		nPut = numworkers - nGet
		sgls = make([]*memsys.SGL, numworkers)
	)

	// Get the workers started
	if usingSG {
		for i := 0; i < numworkers; i++ {
			sgls[i] = tutils.Mem2.NewSGL(objSize)
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

				tutils.PutRandObjs(proxyURL, clibucket, SmokeDir, readerType, SmokeStr, uint64(objSize), numops, errCh, filesPutCh, sgl)
				wg.Done()
			}(i)
			nPut--
		} else {
			wg.Add(1)
			go func(i int) {
				getRandomFiles(proxyURL, numops, clibucket, SmokeStr+"/", t, nil, errCh)
				wg.Done()
			}(i)
			nGet--
		}
	}
	wg.Wait()
	select {
	case err := <-errCh:
		t.Error(err)
	default:
	}
}

func getRandomFiles(proxyURL string, numGets int, bucket, prefix string, t *testing.T, wg *sync.WaitGroup, errCh chan error) {
	if wg != nil {
		defer wg.Done()
	}

	var (
		src        = rand.NewSource(time.Now().UnixNano())
		random     = rand.New(src)
		getsGroup  = &sync.WaitGroup{}
		msg        = &cmn.GetMsg{GetPrefix: prefix, GetPageSize: int(pagesize)}
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	for i := 0; i < numGets; i++ {
		items, err := api.ListBucket(baseParams, bucket, msg, 0)
		if err != nil {
			errCh <- err
			t.Error(err)
			return
		}

		if items == nil {
			errCh <- fmt.Errorf("listbucket %s: is empty - no entries", bucket)
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
			errCh <- fmt.Errorf("Cannot retrieve from an empty bucket %s", bucket)
			// not considered as a failure, just can't do the test
			return
		}

		keyname := files[random.Intn(len(files))]
		getsGroup.Add(1)
		go func() {
			baseParams := tutils.BaseAPIParams(proxyURL)
			_, err := api.GetObject(baseParams, bucket, keyname)
			if err != nil {
				errCh <- err
			}
			getsGroup.Done()
		}()
	}

	getsGroup.Wait()
}
