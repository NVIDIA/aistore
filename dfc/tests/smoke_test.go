/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/tutils"
)

var (
	filesizes = [3]int64{128 * cmn.KiB, 192 * cmn.KiB, 256 * cmn.KiB}
	ratios    = [5]float32{0, 0.25, 0.50, 0.75, 1} // #gets / #puts
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
	fp := make(chan string, len(filesizes)*len(ratios)*numops*numworkers)
	bs := int64(baseseed)
	for _, fs := range filesizes {
		for _, r := range ratios {
			s := fmt.Sprintf("size:%s,GET/PUT:%.0f%%", cmn.B2S(fs, 0), r*100)
			t.Run(s, func(t *testing.T) { oneSmoke(t, proxyURL, fs, r, bs, fp) })
			bs += int64(numworkers + 1)
		}
	}

	close(fp)

	// Clean up all the files from the test
	wg := &sync.WaitGroup{}
	errCh := make(chan error, len(filesizes)*len(ratios)*numops*numworkers)
	for file := range fp {
		if usingFile {
			err := os.Remove(SmokeDir + "/" + file)
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

func oneSmoke(t *testing.T, proxyURL string, filesize int64, ratio float32, bseed int64, filesPutCh chan string) {
	// Start the worker pools
	errCh := make(chan error, 100)
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
			sgls[i] = tutils.Mem2.NewSGL(filesize)
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

				putRandObjs(proxyURL, bseed+int64(i), uint64(filesize), numops, clibucket, errCh, filesPutCh,
					SmokeDir, SmokeStr, true, sgl)
				wg.Done()
			}(i)
			nPut--
		} else {
			wg.Add(1)
			go func(i int) {
				getRandomFiles(proxyURL, bseed+int64(i), numops, clibucket, SmokeStr+"/", t, nil, errCh)
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

func getRandomFiles(proxyURL string, seed int64, numGets int, bucket, prefix string, t *testing.T, wg *sync.WaitGroup, errCh chan error) {
	if wg != nil {
		defer wg.Done()
	}

	src := rand.NewSource(seed)
	random := rand.New(src)
	getsGroup := &sync.WaitGroup{}
	var msg = &cmn.GetMsg{GetPrefix: prefix, GetPageSize: int(pagesize)}
	for i := 0; i < numGets; i++ {
		items, err := tutils.ListBucket(proxyURL, bucket, msg, 0)
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
		go tutils.Get(proxyURL, bucket, keyname, getsGroup, errCh, !testing.Verbose(), false /* validate */)
	}

	getsGroup.Wait()
}

func putObjsWithRandData(proxyURL, bucket, dir, keystr string, random *rand.Rand, fileSize uint64, silent bool,
	sgl *memsys.SGL, errCh chan error, objCh, filesPutCh chan string) {
	for {
		fname := <-objCh
		if fname == "" {
			return
		}
		size := fileSize
		if size == 0 {
			size = uint64(random.Intn(1024)+1) * 1024
		}
		var (
			reader tutils.Reader
			err    error
		)
		if sgl != nil {
			sgl.Reset()
			reader, err = tutils.NewSGReader(sgl, int64(size), true /* with Hash */)
		} else {
			reader, err = tutils.NewReader(tutils.ParamReader{
				Type: readerType,
				SGL:  nil,
				Path: dir,
				Name: fname,
				Size: int64(size),
			})
		}

		if err != nil {
			tutils.Logf("Failed to generate random file %s, err: %v\n", dir+"/"+fname, err)
			if errCh != nil {
				errCh <- err
			}
			return
		}

		objname := filepath.Join(keystr, fname)
		// We could PUT while creating files, but that makes it
		// begin all the puts immediately (because creating random files is fast
		// compared to the listbucket call that getRandomFiles does)
		err = tutils.Put(proxyURL, reader, bucket, objname, silent)
		if err != nil {
			if errCh == nil {
				tutils.Logf("Error performing PUT of object with random data, provided error channel is nil")
			} else {
				errCh <- err
			}
		}
		filesPutCh <- fname
	}
}

func putRandObjsFromList(proxyURL string, seed int64, fileSize uint64, objList []string, bucket string,
	errCh chan error, filesPutCh chan string, dir, keystr string, silent bool, sgl *memsys.SGL) {
	var (
		random = rand.New(rand.NewSource(seed))
		wg     = &sync.WaitGroup{}
		objCh  = make(chan string, len(objList))
	)
	// if len(objList) < numworkers, only need as many workers as there are objects to be PUT
	numworkers := cmn.Min(numworkers, len(objList))
	sgls := make([]*memsys.SGL, numworkers, numworkers)

	// need an SGL for each worker with its size being that of the original SGL
	if usingSG {
		slabSize := sgl.Slab().Size()
		for i := 0; i < numworkers; i++ {
			sgls[i] = tutils.Mem2.NewSGL(slabSize)
		}
		defer func() {
			for _, sgl := range sgls {
				sgl.Free()
			}
		}()
	}

	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		if usingSG {
			sgl = sgls[i]
		}
		go func(sgl *memsys.SGL) {
			putObjsWithRandData(proxyURL, bucket, dir, keystr, random, fileSize, silent, sgl, errCh, objCh, filesPutCh)
			wg.Done()
		}(sgl)
	}

	for _, objName := range objList {
		objCh <- objName
	}
	close(objCh)

	wg.Wait()
}

func putRandObjs(proxyURL string, seed int64, fileSize uint64, numPuts int, bucket string,
	errCh chan error, filesPutCh chan string,
	dir, keystr string, silent bool, sgl *memsys.SGL) {

	src := rand.NewSource(seed)
	random := rand.New(src)
	fileList := make([]string, 0, numPuts)
	for i := 0; i < numPuts; i++ {
		fname := tutils.FastRandomFilename(random, fnlen)
		fileList = append(fileList, fname)
	}

	putRandObjsFromList(proxyURL, seed, fileSize, fileList, bucket, errCh, filesPutCh, dir, keystr, silent, sgl)
}
