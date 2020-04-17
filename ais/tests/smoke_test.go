// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
)

var (
	objSizes = [3]int64{128 * cmn.KiB, 192 * cmn.KiB, 256 * cmn.KiB}
	ratios   = [5]float32{0, 0.25, 0.50, 0.75, 1} // #gets / #puts
)

func TestSmoke(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		bck      = cmn.Bck{Name: clibucket}
		proxyURL = tutils.GetPrimaryURL()
	)

	created := createBucketIfNotCloud(t, proxyURL, &bck)
	fp := make(chan string, len(objSizes)*len(ratios)*numops*numworkers)
	for _, fs := range objSizes {
		for _, r := range ratios {
			s := fmt.Sprintf("size:%s,GET/PUT:%.0f%%", cmn.B2S(fs, 0), r*100)
			t.Run(s, func(t *testing.T) { oneSmoke(t, proxyURL, bck, fs, r, fp) })
		}
	}

	close(fp)

	// Clean up all the files from the test
	wg := &sync.WaitGroup{}
	errCh := make(chan error, len(objSizes)*len(ratios)*numops*numworkers)
	for file := range fp {
		wg.Add(1)
		go tutils.Del(proxyURL, bck, "smoke/"+file, wg, errCh, true)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		t.Error(err)
	default:
	}

	if created {
		tutils.DestroyBucket(t, proxyURL, bck)
	}
}

func oneSmoke(t *testing.T, proxyURL string, bck cmn.Bck, objSize int64, ratio float32, filesPutCh chan string) {
	var (
		nGet  = int(float32(numworkers) * ratio)
		nPut  = numworkers - nGet
		errCh = make(chan error, 100)
		wg    = &sync.WaitGroup{}
	)

	for i := 0; i < numworkers; i++ {
		if (i%2 == 0 && nPut > 0) || nGet == 0 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				tutils.PutRandObjs(proxyURL, bck, SmokeStr, uint64(objSize), numops, errCh, filesPutCh)
			}()
			nPut--
		} else {
			wg.Add(1)
			go func() {
				defer wg.Done()
				getRandomFiles(proxyURL, bck, numops, SmokeStr+"/", t, errCh)
			}()
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

func getRandomFiles(proxyURL string, bck cmn.Bck, numGets int, prefix string, t *testing.T, errCh chan error) {
	var (
		src        = rand.NewSource(time.Now().UnixNano())
		random     = rand.New(src)
		getsGroup  = &sync.WaitGroup{}
		msg        = &cmn.SelectMsg{Prefix: prefix, PageSize: int(pagesize)}
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	items, err := api.ListObjects(baseParams, bck, msg, 0)
	if err != nil {
		errCh <- err
		t.Error(err)
		return
	}
	if len(items.Entries) == 0 {
		errCh <- fmt.Errorf("list_objects %s: is empty - no entries", bck)
		// not considered a failure
		return
	}
	files := make([]string, 0)
	for _, it := range items.Entries {
		files = append(files, it.Name)
	}

	for i := 0; i < numGets; i++ {
		keyname := files[random.Intn(len(files))]
		getsGroup.Add(1)
		go func() {
			defer getsGroup.Done()

			baseParams := tutils.BaseAPIParams(proxyURL)
			_, err := api.GetObject(baseParams, bck, keyname)
			if err != nil {
				errCh <- err
			}
		}()
	}

	getsGroup.Wait()
}
