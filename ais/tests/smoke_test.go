// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/go-tfdata/test/tassert"
	"golang.org/x/sync/errgroup"
)

var (
	objSizes = [3]int64{3 * cmn.KiB, 19 * cmn.KiB, 77 * cmn.KiB}
	ratios   = [5]float32{0, 0.25, 0.50, 0.75, 1} // #gets / #puts
)

func TestSmoke(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	const objPrefix = "smoke"
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		var (
			cnt      = len(objSizes) * len(ratios) * 40 * workerCnt
			fp       = make(chan string, cnt)
			proxyURL = tutils.GetPrimaryURL()
		)
		if bck.IsCloud() && bck.RemoteBck().Provider == cmn.ProviderGoogle {
			t.Skip("GCP fails intermittently when overloaded with requests - skipping")
		}
		for _, fs := range objSizes {
			for _, r := range ratios {
				s := fmt.Sprintf("size:%s,GET/PUT:%.0f%%", cmn.B2S(fs, 0), r*100)
				t.Run(s, func(t *testing.T) {
					oneSmoke(t, proxyURL, bck.Bck, objPrefix, fs, r, bck.Props.Cksum.Type, fp)
				})
			}
		}

		close(fp)

		// Clean up all the files from the test
		wg := &sync.WaitGroup{}
		errCh := make(chan error, cnt)
		for file := range fp {
			wg.Add(1)
			go tutils.Del(proxyURL, bck.Bck, file, wg, errCh, true)
		}
		wg.Wait()
		select {
		case err := <-errCh:
			t.Error(err)
		default:
		}
	})
}

func oneSmoke(t *testing.T, proxyURL string, bck cmn.Bck, objPrefix string, objSize int64, ratio float32, cksumType string, objNameCh chan string) {
	var (
		objCnt = 40
		nGet   = int(float32(workerCnt) * ratio)
		nPut   = workerCnt - nGet
		wg     = &errgroup.Group{}
	)

	for i := 0; i < workerCnt; i++ {
		if (i%2 == 0 && nPut > 0) || nGet == 0 {
			wg.Go(func() error {
				objNames, _, err := tutils.PutRandObjs(tutils.PutObjectsArgs{
					ProxyURL:  proxyURL,
					Bck:       bck,
					ObjPath:   objPrefix,
					ObjCnt:    objCnt,
					ObjSize:   uint64(objSize),
					CksumType: cksumType,
				})
				for _, objName := range objNames {
					objNameCh <- objName
				}
				return err
			})
			nPut--
		} else {
			wg.Go(func() error {
				return getRandomFiles(proxyURL, bck, objCnt, objPrefix+"/")
			})
			nGet--
		}
	}
	err := wg.Wait()
	tassert.CheckError(t, err)
}

func getRandomFiles(proxyURL string, bck cmn.Bck, numGets int, prefix string) error {
	var (
		src        = rand.NewSource(time.Now().UnixNano())
		random     = rand.New(src)
		wg         = &errgroup.Group{}
		msg        = &cmn.SelectMsg{Prefix: prefix}
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	items, err := api.ListObjects(baseParams, bck, msg, 0)
	if err != nil {
		return err
	}
	if len(items.Entries) == 0 {
		return fmt.Errorf("list_objects %s: is empty - no entries", bck)
	}
	files := make([]string, 0)
	for _, it := range items.Entries {
		files = append(files, it.Name)
	}

	for i := 0; i < numGets; i++ {
		keyname := files[random.Intn(len(files))]
		wg.Go(func() error {
			baseParams := tutils.BaseAPIParams(proxyURL)
			_, err := api.GetObject(baseParams, bck, keyname)
			return err
		})
	}
	return wg.Wait()
}
