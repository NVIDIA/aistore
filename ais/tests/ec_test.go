// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tutils"
)

const (
	ecSliceDir = "/" + ec.SliceType + "/"
	ecMetaDir  = "/" + ec.MetaType + "/"
	ecDataDir  = "/" + fs.ObjectType + "/"
	ecWorkDir  = "/" + fs.WorkfileType + "/"
	ecLogDir   = "/log/"
	ecLocalDir = "/local/"
	ecTestDir  = "ec-test/"

	ECPutTimeOut = time.Minute * 4 // maximum wait time after PUT to be sure that the object is EC'ed/replicated

	ecObjLimit     = 256 * cmn.KiB
	ecMinSmallSize = 32 * cmn.KiB
	ecSmallDelta   = 200 * cmn.KiB
	ecMinBigSize   = ecObjLimit * 2
	ecBigDelta     = 10 * cmn.MiB
)

var (
	ecSliceCnt  = 2
	ecParityCnt = 2
)

func ecSliceNumInit(smap cluster.Smap) error {
	tCnt := len(smap.Tmap)
	if tCnt < 4 {
		return fmt.Errorf("test requires at least 4 targets")
	}

	if tCnt == 4 {
		ecSliceCnt = 1
		ecParityCnt = 1
		return nil
	} else if tCnt == 5 {
		ecSliceCnt = 1
		ecParityCnt = 1
		return nil
	}

	ecSliceCnt = 2
	ecParityCnt = 2
	return nil
}

// Since all replicas are equal, it is hard to tell main one from others. Main
// replica is the replica that is on the target chosen by proxy using HrwTarget
// algorithm on GET request from a client.
// The function uses heuristics to detect the main one: it should be the oldest
func ecGetAllLocalSlices(objDir, objName string) (map[string]int64, string) {
	foundParts := make(map[string]int64)
	oldest := time.Now()
	main := ""

	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return nil
		}
		if info.IsDir() ||
			strings.Contains(path, ecLogDir) ||
			!strings.Contains(path, ecLocalDir) ||
			strings.Contains(path, ecWorkDir) {
			return nil
		}

		if strings.Contains(path, objName) {
			foundParts[path] = info.Size()
			if strings.Contains(path, ecDataDir) && oldest.After(info.ModTime()) {
				main = path
				oldest = info.ModTime()
			}
		}

		return nil
	}
	filepath.Walk(rootDir, fsWalkFunc)
	return foundParts, main
}

func filterObjListOK(lst []*cmn.BucketEntry) []*cmn.BucketEntry {
	i := 0
	curr := 0

	for i < len(lst) {
		if lst[i].Status != cmn.ObjStatusOK {
			i++
			continue
		}

		if i != curr {
			lst[curr] = lst[i]
		}
		i++
		curr++
	}

	if i != curr {
		return lst[:curr]
	}

	return lst
}

func ecCheckSlices(t *testing.T, sliceList map[string]int64,
	objFullPath string, objSize, sliceSize int64,
	totalCnt, parityCnt int) (mainObjPath string) {
	if len(sliceList) != totalCnt {
		t.Errorf("Expected number of objects for %q: %d, found: %d\n%+v",
			objFullPath, totalCnt, len(sliceList), sliceList)
	}
	metaCnt := 0
	for k, v := range sliceList {
		if strings.Contains(k, ecMetaDir) {
			metaCnt++
			if v > 512 {
				t.Errorf("Metafile %q size is too big: %d", k, v)
			}
		} else if strings.Contains(k, ecSliceDir) {
			if v != sliceSize {
				t.Errorf("Slice %q size mismatch: %d, expected %d", k, v, sliceSize)
			}
		} else {
			if !strings.HasSuffix(k, objFullPath) {
				t.Errorf("Invalid object name: %s [expected '../%s']", k, objFullPath)
			}
			if v != objSize {
				t.Errorf("File %q size mismatch: got %d, expected %d", k, v, objSize)
			}
			mainObjPath = k
		}
	}

	metaCntMust := totalCnt / 2
	if metaCnt != metaCntMust {
		t.Errorf("Number of metafiles mismatch: %d, expected %d", metaCnt, metaCntMust)
	}

	return
}

func waitForECFinishes(totalCnt int, objSize, sliceSize int64, doEC bool, fullPath, objName string) (foundParts map[string]int64, mainObjPath string) {
	deadLine := time.Now().Add(ECPutTimeOut)
	for time.Now().Before(deadLine) {
		foundParts, mainObjPath = ecGetAllLocalSlices(fullPath, objName)
		if len(foundParts) == totalCnt {
			same := true
			for nm, sz := range foundParts {
				if doEC {
					if strings.Contains(nm, ecSliceDir) {
						if sz != sliceSize {
							same = false
							break
						}
					}
				} else {
					if strings.Contains(nm, ecDataDir) {
						if sz != objSize {
							same = false
							break
						}
					}
				}
			}

			if same {
				break
			}
		}
		time.Sleep(time.Millisecond * 20)
	}

	return
}

// Generates a random size for object
// Returns:
// - how many files should be generated (including metafiles)
// - generated object size
// - an object slice size (it equals object size for replicated objects)
// - whether to encode(true) or to replicate(false) the object
func randObjectSize(rnd *rand.Rand, n, every int) (
	totalCnt int, objSize, sliceSize int64, doEC bool) {
	// Big object case
	// full object copy+meta: 1+1
	// number of metafiles: parity+slices
	// number of slices: slices+parity
	totalCnt = 2 + (ecSliceCnt+ecParityCnt)*2
	objSize = int64(ecMinBigSize + rnd.Intn(ecBigDelta))
	sliceSize = ec.SliceSize(objSize, ecSliceCnt)
	if (n+1)%every == 0 {
		// Small object case
		// full object copy+meta: 1+1
		// number of metafiles: parity
		// number of slices: parity
		totalCnt = 2 + ecParityCnt*2
		objSize = int64(ecMinSmallSize + rnd.Intn(ecSmallDelta))
		sliceSize = objSize
	}
	doEC = objSize >= ecObjLimit
	return
}

func calculateSlicesCount(slices map[string]int64) map[string]int {
	calc := make(map[string]int, len(slices))
	for k := range slices {
		o := filepath.Base(k)
		if n, ok := calc[o]; ok {
			calc[o] = n + 1
		} else {
			calc[o] = 1
		}
	}
	return calc
}

func compareSlicesCount(t *testing.T, orig, found map[string]int) {
	for k, v := range orig {
		if fnd, ok := found[k]; !ok {
			t.Errorf("%s - no files found", k)
		} else if fnd != v {
			t.Errorf("Object %s must have %d files, found %d", k, v, fnd)
		}
	}
	for k, v := range found {
		if _, ok := orig[k]; !ok {
			t.Errorf("%s - should not exist (%d files found)", k, v)
			continue
		}
	}
}

// Short test to make sure that EC options cannot be changed after
// EC is enabled
func TestECPropsChange(t *testing.T) {
	var (
		proxyURL    = getPrimaryURL(t, proxyURLReadOnly)
		bucketProps cmn.BucketProps
	)

	tutils.CreateFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer tutils.DestroyLocalBucket(t, proxyURL, TestLocalBucketName)

	bucketProps.CksumConf.Checksum = "inherit"
	bucketProps.ECConf = cmn.ECConf{
		ECEnabled:      true,
		ECObjSizeLimit: ecObjLimit,
		DataSlices:     1,
		ParitySlices:   1,
	}
	baseParams := tutils.BaseAPIParams(proxyURL)

	tutils.Logln("Resetting bucket properties")
	err := api.ResetBucketProps(baseParams, TestLocalBucketName)
	tutils.CheckFatal(err, t)

	tutils.Logln("Trying to set too many slices")
	bucketProps.DataSlices = 25
	bucketProps.ParitySlices = 25
	err = api.SetBucketProps(baseParams, TestLocalBucketName, bucketProps)
	if err == nil {
		t.Error("Enabling EC must fail in case of the number of targets fewer than the number of slices")
	}

	tutils.Logln("Enabling EC")
	bucketProps.DataSlices = 1
	bucketProps.ParitySlices = 1
	err = api.SetBucketProps(baseParams, TestLocalBucketName, bucketProps)
	tutils.CheckFatal(err, t)

	tutils.Logln("Trying to set EC options to the same values")
	err = api.SetBucketProps(baseParams, TestLocalBucketName, bucketProps)
	tutils.CheckFatal(err, t)

	tutils.Logln("Trying to disable EC")
	bucketProps.ECEnabled = false
	err = api.SetBucketProps(baseParams, TestLocalBucketName, bucketProps)
	if err != nil {
		t.Errorf("Disabling EC failed: %v", err)
	}

	tutils.Logln("Trying to re-enable EC")
	bucketProps.ECEnabled = true
	err = api.SetBucketProps(baseParams, TestLocalBucketName, bucketProps)
	if err != nil {
		t.Errorf("Enabling EC failed: %v", err)
	}

	tutils.Logln("Trying to modify EC options when EC is enabled")
	bucketProps.ECEnabled = true
	bucketProps.ECObjSizeLimit = 300000
	err = api.SetBucketProps(baseParams, TestLocalBucketName, bucketProps)
	if err == nil {
		t.Error("Modifiying EC properties must fail")
	}

	tutils.Logln("Resetting bucket properties")
	err = api.ResetBucketProps(baseParams, TestLocalBucketName)
	if err == nil {
		t.Error("Resetting properties when EC is enabled must fail")
	}
}

// Quick check that EC can restore a damaged object and a missing slice
//  - PUTs an object to the bucket
//  - filepath.Walk checks that the number of metafiles and slices are correct
//  - Either original object or original object and a random slice are deleted
//  - GET should detect that original file has gone
//  - The target restores the original object from slices and missing slices
func TestECRestoreObjAndSlice(t *testing.T) {
	const (
		objPatt          = "obj-rest-%04d"
		numFiles         = 50
		semaCnt          = 8
		smallEvery       = 7               // Every N-th object is small
		sliceDelPct      = 50              // %% of objects that have damaged body and a slice
		sleepRestoreTime = time.Second * 5 // wait time after GET restores slices
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	var (
		sgl         *memsys.SGL
		proxyURL    = getPrimaryURL(t, proxyURLReadOnly)
		bucketProps cmn.BucketProps
		sema        = make(chan struct{}, semaCnt)
	)

	smap := getClusterMap(t, proxyURL)
	if err := ecSliceNumInit(smap); err != nil {
		t.Fatal(err)
	}

	if usingSG {
		sgl = tutils.Mem2.NewSGL(0)
		defer sgl.Free()
	}

	fullPath := fmt.Sprintf("local/%s/%s", TestLocalBucketName, ecTestDir)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	tutils.CreateFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer tutils.DestroyLocalBucket(t, proxyURL, TestLocalBucketName)

	bucketProps.CksumConf.Checksum = "inherit"
	bucketProps.ECConf = cmn.ECConf{
		ECEnabled:      true,
		ECObjSizeLimit: ecObjLimit,
		DataSlices:     ecSliceCnt,
		ParitySlices:   ecParityCnt,
	}
	baseParams := tutils.BaseAPIParams(proxyURL)
	err := api.SetBucketProps(baseParams, TestLocalBucketName, bucketProps)
	tutils.CheckFatal(err, t)

	wg := sync.WaitGroup{}

	oneObj := func(objName string, idx int) {
		sema <- struct{}{}
		defer func() {
			wg.Done()
			<-sema
		}()

		delSlice := false // delete only main object
		deletedFiles := 1
		if ecSliceCnt+ecParityCnt > 2 && rnd.Intn(100) < sliceDelPct {
			// delete a random slice, too
			delSlice = true
			deletedFiles = 2
		}

		totalCnt, objSize, sliceSize, doEC := randObjectSize(rnd, idx, smallEvery)
		objPath := ecTestDir + objName
		ecStr, delStr := "-", "obj"
		if doEC {
			ecStr = "EC"
		}
		if delSlice {
			delStr = "obj+slice"
		}
		tutils.Logf("Creating %s, size %8d [%2s] [%s]\n", objPath, objSize, ecStr, delStr)
		r, err := tutils.NewRandReader(int64(objSize), false)
		tutils.CheckFatal(err, t)
		defer r.Close()

		putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: TestLocalBucketName, Object: objPath, Reader: r}
		err = api.PutObject(putArgs)
		tutils.CheckFatal(err, t)

		tutils.Logf("Waiting for %s\n", objPath)
		foundParts, mainObjPath := waitForECFinishes(totalCnt, objSize, sliceSize, doEC, fullPath, objName)

		ecCheckSlices(t, foundParts, fullPath+objName,
			objSize, sliceSize, totalCnt, ecParityCnt)
		if mainObjPath == "" {
			t.Errorf("Full copy is not found")
			return
		}

		tutils.Logf("Damaging %s [removing %s]\n", objPath, mainObjPath)
		tutils.CheckFatal(os.Remove(mainObjPath), t)
		metafile := strings.Replace(mainObjPath, ecDataDir, ecMetaDir, -1)
		tutils.Logf("Damaging %s [removing %s]\n", objPath, metafile)
		tutils.CheckFatal(os.Remove(metafile), t)
		if delSlice {
			sliceToDel := ""
			for k := range foundParts {
				if k != mainObjPath && strings.Contains(k, ecSliceDir) && doEC {
					sliceToDel = k
					break
				} else if k != mainObjPath && strings.Contains(k, ecDataDir) && !doEC {
					sliceToDel = k
					break
				}
			}
			if sliceToDel == "" {
				t.Errorf("Failed to select random slice for %s", objName)
				return
			}
			tutils.Logf("Removing slice/replica: %s\n", sliceToDel)
			tutils.CheckFatal(os.Remove(sliceToDel), t)
			if doEC {
				metafile := strings.Replace(sliceToDel, ecSliceDir, ecMetaDir, -1)
				tutils.Logf("Removing slice meta %s\n", metafile)
				tutils.CheckFatal(os.Remove(metafile), t)
			} else {
				metafile := strings.Replace(sliceToDel, ecDataDir, ecMetaDir, -1)
				tutils.Logf("Removing replica meta %s\n", metafile)
				tutils.CheckFatal(os.Remove(metafile), t)
			}
		}

		partsAfterRemove, _ := ecGetAllLocalSlices(fullPath, objName)
		_, ok := partsAfterRemove[mainObjPath]
		if ok || len(partsAfterRemove) != len(foundParts)-deletedFiles*2 {
			t.Errorf("Files are not deleted [%d - %d]: %#v", len(foundParts), len(partsAfterRemove), partsAfterRemove)
			return
		}

		tutils.Logf("Restoring %s\n", objPath)
		_, err = api.GetObject(baseParams, TestLocalBucketName, objPath)
		tutils.CheckFatal(err, t)

		if doEC {
			deadline := time.Now().Add(sleepRestoreTime)
			var partsAfterRestore map[string]int64
			for time.Now().Before(deadline) {
				time.Sleep(time.Millisecond * 250)
				partsAfterRestore, _ = ecGetAllLocalSlices(fullPath, objName)
				if len(partsAfterRestore) == totalCnt {
					break
				}
			}
			ecCheckSlices(t, partsAfterRestore, fullPath+objName,
				objSize, sliceSize, totalCnt, ecParityCnt)
		}
	}

	wg.Add(numFiles)
	for i := 0; i < numFiles; i++ {
		objName := fmt.Sprintf(objPatt, i)
		go oneObj(objName, i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	var msg = &cmn.GetMsg{GetPageSize: int(pagesize), GetProps: "size,status"}
	reslist, err := api.ListBucket(baseParams, TestLocalBucketName, msg, 0)
	tutils.CheckFatal(err, t)
	reslist.Entries = filterObjListOK(reslist.Entries)

	if len(reslist.Entries) != numFiles {
		t.Fatalf("Invalid number of objects: %d, expected %d", len(reslist.Entries), 1)
	}

	tutils.Logln("Deleting objects...")
	wg.Add(numFiles)
	for idx := 0; idx < numFiles; idx++ {
		go func(i int) {
			defer wg.Done()
			objName := fmt.Sprintf(objPatt, i)
			objPath := ecTestDir + objName
			err = tutils.Del(proxyURL, TestLocalBucketName, objPath, "", nil, nil, true)
			tutils.CheckFatal(err, t)

			deadline := time.Now().Add(time.Second * 10)
			var partsAfterDelete map[string]int64
			for time.Now().Before(deadline) {
				time.Sleep(time.Millisecond * 250)
				partsAfterDelete, _ := ecGetAllLocalSlices(fullPath, objName)
				if len(partsAfterDelete) == 0 {
					break
				}
			}
			if len(partsAfterDelete) != 0 {
				t.Errorf("Some slices were not cleaned up after DEL: %#v", partsAfterDelete)
			}
		}(idx)
	}
	wg.Wait()
}

// Stress test to check that EC works as expected.
//  - Changes bucket props to use EC
//  - Generates `objCount` objects, size between `ecObjMinSize` and `ecObjMinSize`+ecObjMaxSize`
//  - Objects smaller `ecObjLimit` must be copies, while others must be EC'ed
//  - PUTs objects to the bucket
//  - filepath.Walk checks that the number of metafiles and slices are correct
//  - The original object is deleted
//  - GET should detect that original file has gone and that there are EC slices
//  - The target restores the original object from slices/copies and returns it
//  - No errors must occur
func TestECStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Long run only")
	}
	const (
		objPatt    = "obj-%04d"
		objCount   = 400
		concurr    = 12
		smallEvery = 10 // Every N-th object is small
	)

	var (
		sgl         *memsys.SGL
		proxyURL    = getPrimaryURL(t, proxyURLReadOnly)
		bucketProps cmn.BucketProps
		semaphore   = make(chan struct{}, concurr) // concurrent EC jobs at a time

		sizes = make(chan int64, objCount)
		times = make(chan time.Duration, objCount)
	)

	smap := getClusterMap(t, proxyURL)
	if err := ecSliceNumInit(smap); err != nil {
		t.Fatal(err)
	}

	if usingSG {
		sgl = tutils.Mem2.NewSGL(0)
		defer sgl.Free()
	}

	tutils.CreateFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer tutils.DestroyLocalBucket(t, proxyURL, TestLocalBucketName)

	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))

	bucketProps.CksumConf.Checksum = "inherit"
	bucketProps.ECConf = cmn.ECConf{
		ECEnabled:      true,
		ECObjSizeLimit: ecObjLimit,
		DataSlices:     ecSliceCnt,
		ParitySlices:   ecParityCnt,
	}
	tutils.Logf("Changing EC %d:%d [ seed = %d ], concurrent: %d\n",
		ecSliceCnt, ecParityCnt, seed, concurr)
	baseParams := tutils.BaseAPIParams(proxyURL)
	err := api.SetBucketProps(baseParams, TestLocalBucketName, bucketProps)
	tutils.CheckFatal(err, t)

	wg := &sync.WaitGroup{}
	for idx := 0; idx < objCount; idx++ {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(i int) {
			totalCnt, objSize, sliceSize, doEC := randObjectSize(rnd, i, smallEvery)
			sizes <- objSize
			objName := fmt.Sprintf(objPatt, i)
			objPath := ecTestDir + objName
			if doEC {
				tutils.Logf("Object %s, size %9d[%9d]\n", objName, objSize, sliceSize)
			} else {
				tutils.Logf("Object %s, size %9d[%9s]\n", objName, objSize, "-")
			}
			r, err := tutils.NewRandReader(int64(objSize), false)
			defer func() {
				r.Close()
				<-semaphore
				wg.Done()
			}()
			tutils.CheckFatal(err, t)
			putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: TestLocalBucketName, Object: objPath, Reader: r}
			err = api.PutObject(putArgs)
			tutils.CheckFatal(err, t)

			fullPath := fmt.Sprintf("local/%s/%s", TestLocalBucketName, ecTestDir)
			foundParts, _ := waitForECFinishes(totalCnt, objSize, sliceSize, doEC, fullPath, objName)
			mainObjPath := ""
			if len(foundParts) != totalCnt {
				t.Errorf("Expected number of files %s: %d, found: %d\n%+v",
					objName, totalCnt, len(foundParts), foundParts)
				return
			}
			metaCnt, sliceCnt, replCnt := 0, 0, 0
			for k, v := range foundParts {
				if strings.Contains(k, ecMetaDir) {
					metaCnt++
					if v > 512 {
						t.Errorf("Metafile %q size is too big: %d", k, v)
					}
				} else if strings.Contains(k, ecSliceDir) {
					sliceCnt++
					if v != sliceSize && doEC {
						t.Errorf("Slice %q size mismatch: %d, expected %d", k, v, sliceSize)
					}
					if v != objSize && !doEC {
						t.Errorf("Copy %q size mismatch: %d, expected %d", k, v, objSize)
					}
				} else {
					objFullPath := fullPath + objName
					if !strings.HasSuffix(k, objFullPath) {
						t.Errorf("Invalid object name: %s [expected '..%s']", k, objFullPath)
					}
					if v != objSize {
						t.Errorf("File %q size mismatch: got %d, expected %d", k, v, objSize)
					}
					mainObjPath = k
					replCnt++
				}
			}

			metaCntMust := ecParityCnt + 1
			if doEC {
				metaCntMust = ecParityCnt + ecSliceCnt + 1
			}
			if metaCnt != metaCntMust {
				t.Errorf("Number of metafiles mismatch: %d, expected %d", metaCnt, ecParityCnt+1)
			}
			if doEC {
				if sliceCnt != ecParityCnt+ecSliceCnt {
					t.Errorf("Number of chunks mismatch: %d, expected %d", sliceCnt, ecParityCnt+ecSliceCnt)
				}
			} else {
				if replCnt != ecParityCnt+1 {
					t.Errorf("Number replicas mismatch: %d, expected %d", replCnt, ecParityCnt)
				}
			}

			if mainObjPath == "" {
				t.Errorf("Full copy is not found")
				return
			}

			tutils.CheckFatal(os.Remove(mainObjPath), t)
			partsAfterRemove, _ := ecGetAllLocalSlices(fullPath, objName)
			_, ok := partsAfterRemove[mainObjPath]
			if ok || len(partsAfterRemove) >= len(foundParts) {
				t.Errorf("File is not deleted: %#v", partsAfterRemove)
				return
			}

			_, err = api.GetObject(baseParams, TestLocalBucketName, objPath)
			tutils.CheckFatal(err, t)

			if doEC {
				partsAfterRestore, _ := ecGetAllLocalSlices(fullPath, objName)
				restoredSize, ok := partsAfterRestore[mainObjPath]
				if !ok || len(partsAfterRestore) != len(foundParts) {
					t.Errorf("File is not restored: %#v", partsAfterRestore)
					return
				}

				if restoredSize != objSize {
					t.Errorf("File is restored incorrectly, size mismatches: %d, expected %d", restoredSize, objSize)
					return
				}
			}
		}(idx)
	}

	wg.Wait()
	close(sizes)
	close(times)

	var msg = &cmn.GetMsg{GetPageSize: int(pagesize), GetProps: "size,status"}
	reslist, err := api.ListBucket(baseParams, TestLocalBucketName, msg, 0)
	tutils.CheckFatal(err, t)
	reslist.Entries = filterObjListOK(reslist.Entries)

	if len(reslist.Entries) != objCount {
		t.Fatalf("Invalid number of objects: %d, expected %d", len(reslist.Entries), objCount)
	}

	szTotal := int64(0)
	tmTotal := time.Duration(0)
	szLen := len(sizes)
	tmLen := len(times)
	tmMin, tmMax := time.Duration(time.Hour), time.Duration(0)
	for sz := range sizes {
		szTotal += sz
	}
	for tm := range times {
		tmTotal += tm
		if tm < tmMin {
			tmMin = tm
		}
		if tm > tmMax {
			tmMax = tm
		}
	}
	if szLen != 0 {
		t.Logf("Average size: %s\n", cmn.B2S(szTotal/int64(szLen), 1))
	}
	if tmLen != 0 {
		t.Logf("Average time: %v [%v - %v]\n", time.Duration(int64(tmTotal)/int64(tmLen)), tmMin, tmMax)
	}
}

// ExtraStress test to check that EC works as expected
//  - Changes bucket props to use EC
//  - Generates `objCount` objects, size between `ecObjMinSize` and `ecObjMinSize`+ecObjMaxSize`
//  - Objects smaller `ecObjLimit` must be copies, while others must be EC'ed
//  - PUTs ALL objects to the bucket stressing both EC and transport
//  - filepath.Walk checks that the number of metafiles at the end is correct
//  - No errors must occur
func TestECExtraStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Long run only")
	}
	const (
		objStart   = "obj-extra-"
		objPatt    = objStart + "%04d"
		objCount   = 400
		concurr    = 12
		smallEvery = 7
	)

	var (
		sgl         *memsys.SGL
		proxyURL    = getPrimaryURL(t, proxyURLReadOnly)
		bucketProps cmn.BucketProps
		waitAllTime = time.Minute * 4              // should be enough for all object to complete EC
		semaphore   = make(chan struct{}, concurr) // concurrent EC jobs at a time

		totalSlices = int64(0)
	)

	smap := getClusterMap(t, proxyURL)
	if err := ecSliceNumInit(smap); err != nil {
		t.Fatal(err)
	}

	if usingSG {
		sgl = tutils.Mem2.NewSGL(0)
		defer sgl.Free()
	}

	tutils.CreateFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer tutils.DestroyLocalBucket(t, proxyURL, TestLocalBucketName)

	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))

	bucketProps.CksumConf.Checksum = "inherit"
	bucketProps.ECConf = cmn.ECConf{
		ECEnabled:      true,
		ECObjSizeLimit: ecObjLimit,
		DataSlices:     ecSliceCnt,
		ParitySlices:   ecParityCnt,
	}
	tutils.Logf("Changing EC %d:%d [ seed = %d ], concurrent: %d\n", ecSliceCnt, ecParityCnt, seed, concurr)
	baseParams := tutils.BaseAPIParams(proxyURL)
	err := api.SetBucketProps(baseParams, TestLocalBucketName, bucketProps)
	tutils.CheckFatal(err, t)
	started := time.Now()

	type sCnt struct {
		obj string
		cnt int
	}
	cntCh := make(chan sCnt, objCount)
	wg := &sync.WaitGroup{}
	wg.Add(objCount)
	for idx := 0; idx < objCount; idx++ {
		semaphore <- struct{}{}

		go func(i int) {
			objName := fmt.Sprintf(objPatt, i)
			totalCnt, objSize, sliceSize, doEC := randObjectSize(rnd, i, smallEvery)
			objPath := ecTestDir + objName
			if doEC {
				tutils.Logf("Object %s, size %9d[%9d]\n", objName, objSize, sliceSize)
			} else {
				tutils.Logf("Object %s, size %9d[%9s]\n", objName, objSize, "-")
			}
			r, err := tutils.NewRandReader(int64(objSize), false)
			if err != nil {
				t.Errorf("Failed to create reader: %v", err)
			}
			putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: TestLocalBucketName, Object: objPath, Reader: r}
			err = api.PutObject(putArgs)
			if err != nil {
				t.Errorf("PUT failed: %v", err)
			}

			atomic.AddInt64(&totalSlices, int64(totalCnt))
			cntCh <- sCnt{obj: objName, cnt: totalCnt}

			<-semaphore
			wg.Done()
		}(idx)
	}

	wg.Wait()
	close(cntCh)

	var foundParts map[string]int64
	fullPath := fmt.Sprintf("local/%s/%s", TestLocalBucketName, ecTestDir)
	startedWaiting := time.Now()
	deadLine := startedWaiting.Add(waitAllTime)
	for time.Now().Before(deadLine) {
		foundParts, _ = ecGetAllLocalSlices(fullPath, objStart)
		if len(foundParts) == int(totalSlices) {
			delta := time.Since(startedWaiting)
			t.Logf("Waiting for EC completed after all object are PUT %v\n", delta)
			break
		}
		time.Sleep(time.Millisecond * 30)
	}
	if len(foundParts) != int(totalSlices) {
		slices := make(map[string]int, objCount)
		for sl := range cntCh {
			slices[sl.obj] = sl.cnt
		}
		fndSlices := calculateSlicesCount(foundParts)
		compareSlicesCount(t, slices, fndSlices)

		t.Fatalf("Expected total number of files: %d, found: %d\n",
			totalSlices, len(foundParts))
	}
	delta := time.Since(started)
	t.Logf("Total test time %v\n", delta)

	var msg = &cmn.GetMsg{GetPageSize: int(pagesize), GetProps: "size,status"}
	reslist, err := api.ListBucket(baseParams, TestLocalBucketName, msg, 0)
	tutils.CheckFatal(err, t)
	reslist.Entries = filterObjListOK(reslist.Entries)

	if len(reslist.Entries) != objCount {
		t.Fatalf("Invalid number of objects: %d, expected %d", len(reslist.Entries), objCount)
	}
}

// Quick check that EC keeps xattrs:
// - enable EC and versioning for the bucket
// - put/damage/restore one by one a few object two times to increase their versions
// - get the list of the objects at the end and check that they all have the correct versions
func TestECXattrs(t *testing.T) {
	const (
		objPatt          = "obj-xattr-%04d"
		numFiles         = 30
		sliceDelPct      = 50              // %% of objects that have damaged body and a slice
		sleepRestoreTime = time.Second * 5 // wait time after GET restores slices
		finalVersion     = "2"
		smallEvery       = 4
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	var (
		sgl         *memsys.SGL
		proxyURL    = getPrimaryURL(t, proxyURLReadOnly)
		bucketProps cmn.BucketProps
	)

	smap := getClusterMap(t, proxyURL)
	if err := ecSliceNumInit(smap); err != nil {
		t.Fatal(err)
	}

	if usingSG {
		sgl = tutils.Mem2.NewSGL(0)
		defer sgl.Free()
	}

	fullPath := fmt.Sprintf("local/%s/%s", TestLocalBucketName, ecTestDir)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	tutils.CreateFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer tutils.DestroyLocalBucket(t, proxyURL, TestLocalBucketName)

	bucketProps.CksumConf.Checksum = "inherit"
	bucketProps.Versioning = "all"
	bucketProps.ECConf = cmn.ECConf{
		ECEnabled:      true,
		ECObjSizeLimit: ecObjLimit,
		DataSlices:     ecSliceCnt,
		ParitySlices:   ecParityCnt,
	}
	baseParams := tutils.BaseAPIParams(proxyURL)
	err := api.SetBucketProps(baseParams, TestLocalBucketName, bucketProps)
	tutils.CheckFatal(err, t)

	oneObj := func(idx int, objName string) {
		delSlice := false // delete only main object
		deletedFiles := 1
		if ecSliceCnt+ecParityCnt > 2 && rnd.Intn(100) < sliceDelPct {
			// delete a random slice, too
			delSlice = true
			deletedFiles = 2
		}

		totalCnt, objSize, sliceSize, doEC := randObjectSize(rnd, idx, smallEvery)
		objPath := ecTestDir + objName
		ecStr, delStr := "-", "obj"
		if doEC {
			ecStr = "EC"
		}
		if delSlice {
			delStr = fmt.Sprintf("obj+slice [%d]", deletedFiles)
		}
		tutils.Logf("Creating %s, size %8d [%2s] [%s]\n", objPath, objSize, ecStr, delStr)
		r, err := tutils.NewRandReader(int64(objSize), false)
		tutils.CheckFatal(err, t)
		defer r.Close()
		putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: TestLocalBucketName, Object: objPath, Reader: r}
		err = api.PutObject(putArgs)
		tutils.CheckFatal(err, t)

		tutils.Logf("Waiting for %s\n", objPath)
		foundParts, mainObjPath := waitForECFinishes(totalCnt, objSize, sliceSize, doEC, fullPath, objName)

		ecCheckSlices(t, foundParts, fullPath+objName,
			objSize, sliceSize, totalCnt, ecParityCnt)
		if mainObjPath == "" {
			t.Fatalf("Full copy is not found")
		}

		tutils.Logf("Damaging %s [removing %s]\n", objPath, mainObjPath)
		tutils.CheckFatal(os.Remove(mainObjPath), t)
		metafile := strings.Replace(mainObjPath, ecDataDir, ecMetaDir, -1)
		tutils.Logf("Damaging %s [removing %s]\n", objPath, metafile)
		tutils.CheckFatal(os.Remove(metafile), t)
		if delSlice {
			sliceToDel := ""
			for k := range foundParts {
				if k != mainObjPath && strings.Contains(k, ecSliceDir) && doEC {
					sliceToDel = k
					break
				} else if k != mainObjPath && strings.Contains(k, ecDataDir) && !doEC {
					sliceToDel = k
					break
				}
			}
			if sliceToDel == "" {
				t.Fatalf("Failed to select random slice for %s", objName)
			}
			tutils.Logf("Removing slice/replica: %s\n", sliceToDel)
			tutils.CheckFatal(os.Remove(sliceToDel), t)
			if doEC {
				metafile := strings.Replace(sliceToDel, ecSliceDir, ecMetaDir, -1)
				tutils.Logf("Removing slice meta %s\n", metafile)
				tutils.CheckFatal(os.Remove(metafile), t)
			} else {
				metafile := strings.Replace(sliceToDel, ecDataDir, ecMetaDir, -1)
				tutils.Logf("Removing replica meta %s\n", metafile)
				tutils.CheckFatal(os.Remove(metafile), t)
			}
		}

		partsAfterRemove, _ := ecGetAllLocalSlices(fullPath, objName)
		_, ok := partsAfterRemove[mainObjPath]
		if ok || len(partsAfterRemove) != len(foundParts)-deletedFiles*2 {
			t.Fatalf("Files are not deleted [%d - %d]: %#v", len(foundParts), len(partsAfterRemove), partsAfterRemove)
		}

		tutils.Logf("Restoring %s\n", objPath)
		_, err = api.GetObject(baseParams, TestLocalBucketName, objPath)
		tutils.CheckFatal(err, t)

		if doEC {
			deadline := time.Now().Add(sleepRestoreTime)
			var partsAfterRestore map[string]int64
			for time.Now().Before(deadline) {
				time.Sleep(time.Millisecond * 250)
				partsAfterRestore, _ = ecGetAllLocalSlices(fullPath, objName)
				if len(partsAfterRestore) == totalCnt {
					break
				}
			}
			ecCheckSlices(t, partsAfterRestore, fullPath+objName,
				objSize, sliceSize, totalCnt, ecParityCnt)
		}
	}

	// PUT objects twice to make their version 2
	for j := 0; j < 2; j++ {
		for i := 0; i < numFiles; i++ {
			objName := fmt.Sprintf(objPatt, i)
			oneObj(i, objName)
		}
	}

	var msg = &cmn.GetMsg{GetPageSize: int(pagesize), GetProps: "size,status,version"}
	reslist, err := api.ListBucket(baseParams, TestLocalBucketName, msg, 0)
	tutils.CheckFatal(err, t)

	// check that all returned objects and their repicas have the same version
	for _, e := range reslist.Entries {
		if e.Version != finalVersion {
			t.Errorf("%s[status=%s] must have version %s but it is %s\n", e.Name, e.Status, finalVersion, e.Version)
		}
	}

	reslist.Entries = filterObjListOK(reslist.Entries)
	if len(reslist.Entries) != numFiles {
		t.Fatalf("Invalid number of objects: %d, expected %d", len(reslist.Entries), 1)
	}
}

// Lost target test:
// - puts some objects
// - kills a random target
// - gets all objects
// - nothing must fail
// - register the target back
func TestECEmergencyTarget(t *testing.T) {
	const (
		objPatt    = "obj-emt-%04d"
		numFiles   = 100
		smallEvery = 4
		concurr    = 12
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	var (
		sgl         *memsys.SGL
		proxyURL    = getPrimaryURL(t, proxyURLReadOnly)
		bucketProps cmn.BucketProps
		semaphore   = make(chan struct{}, concurr) // concurrent EC jobs at a time
	)

	smap := getClusterMap(t, proxyURL)
	if err := ecSliceNumInit(smap); err != nil {
		t.Fatal(err)
	}

	if usingSG {
		sgl = tutils.Mem2.NewSGL(0)
		defer sgl.Free()
	}

	setClusterConfig(t, proxyURL, "rebalancing_enabled", false)
	defer setClusterConfig(t, proxyURL, "rebalancing_enabled", true)

	fullPath := fmt.Sprintf("local/%s/%s", TestLocalBucketName, ecTestDir)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	tutils.CreateFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer tutils.DestroyLocalBucket(t, proxyURL, TestLocalBucketName)

	bucketProps.CksumConf.Checksum = "inherit"
	bucketProps.ECConf = cmn.ECConf{
		ECEnabled:      true,
		ECObjSizeLimit: ecObjLimit,
		DataSlices:     ecSliceCnt,
		ParitySlices:   ecParityCnt,
	}
	baseParams := tutils.BaseAPIParams(proxyURL)
	err := api.SetBucketProps(baseParams, TestLocalBucketName, bucketProps)
	tutils.CheckFatal(err, t)

	wg := &sync.WaitGroup{}

	// 1. PUT objects
	putOneObj := func(idx int, objName string) {
		defer func() {
			wg.Done()
			<-semaphore
		}()

		start := time.Now()
		totalCnt, objSize, sliceSize, doEC := randObjectSize(rnd, idx, smallEvery)
		objPath := ecTestDir + objName
		ecStr := "-"
		if doEC {
			ecStr = "EC"
		}
		tutils.Logf("Creating %s, size %8d [%2s]\n", objPath, objSize, ecStr)
		r, err := tutils.NewRandReader(int64(objSize), false)
		tutils.CheckFatal(err, t)
		defer r.Close()
		putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: TestLocalBucketName, Object: objPath, Reader: r}
		err = api.PutObject(putArgs)
		tutils.CheckFatal(err, t)
		t.Logf("Object %s put in %v", objName, time.Since(start))
		start = time.Now()

		foundParts, mainObjPath := waitForECFinishes(totalCnt, objSize, sliceSize, doEC, fullPath, objName)

		ecCheckSlices(t, foundParts, fullPath+objName,
			objSize, sliceSize, totalCnt, ecParityCnt)
		if mainObjPath == "" {
			t.Errorf("Full copy is not found")
			return
		}
		t.Logf("Object %s EC in %v", objName, time.Since(start))
	}

	wg.Add(numFiles)
	for i := 0; i < numFiles; i++ {
		objName := fmt.Sprintf(objPatt, i)
		semaphore <- struct{}{}
		go putOneObj(i, objName)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	// 2. Kill a random target
	removeTarget := extractTargetNodes(smap)[0]
	tutils.Logf("Removing a target: %s\n", removeTarget.DaemonID)
	err = tutils.UnregisterTarget(proxyURL, removeTarget.DaemonID)
	tutils.CheckFatal(err, t)
	smap, err = waitForPrimaryProxy(
		proxyURL,
		"target is gone",
		smap.Version, testing.Verbose(),
		len(smap.Pmap),
		len(smap.Tmap)-1,
	)
	tutils.CheckFatal(err, t)

	defer func() {
		// Restore target
		tutils.Logf("Reregistering target...\n")
		err = tutils.RegisterTarget(proxyURL, removeTarget, smap)
		tutils.CheckFatal(err, t)
		smap, err = waitForPrimaryProxy(
			proxyURL,
			"to join target back",
			smap.Version, testing.Verbose(),
			len(smap.Pmap),
			len(smap.Tmap)+1,
		)
		tutils.CheckFatal(err, t)
	}()

	// 3. Read objects
	tutils.Logln("Reading all objects...")
	getOneObj := func(idx int, objName string) {
		defer func() {
			wg.Done()
			<-semaphore
		}()
		start := time.Now()
		objPath := ecTestDir + objName
		_, err = api.GetObject(baseParams, TestLocalBucketName, objPath)
		if err != nil {
			t.Error(err)
		}
		t.Logf("Object %s get in %v", objName, time.Since(start))
	}

	wg.Add(numFiles)
	for i := 0; i < numFiles; i++ {
		objName := fmt.Sprintf(objPatt, i)
		semaphore <- struct{}{}
		go getOneObj(i, objName)
	}
	wg.Wait()

	// 4. Check that ListBucket returns correct number of items
	tutils.Logln("DONE\nReading bucket list...")
	var msg = &cmn.GetMsg{GetPageSize: int(pagesize), GetProps: "size,status,version"}
	reslist, err := api.ListBucket(baseParams, TestLocalBucketName, msg, 0)
	if err != nil {
		t.Error(err)
	}

	reslist.Entries = filterObjListOK(reslist.Entries)
	if len(reslist.Entries) != numFiles {
		t.Errorf("Invalid number of objects: %d, expected %d", len(reslist.Entries), numFiles)
	}
}

// Lost mountpah test:
// - puts some objects
// - disable a random mountpath
// - gets all objects
// - nothing must fail
// - enable the mountpath back
func TestECEmergencyMpath(t *testing.T) {
	const (
		objPatt    = "obj-emm-%04d"
		numFiles   = 400
		smallEvery = 4
		concurr    = 24
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	var (
		sgl         *memsys.SGL
		proxyURL    = getPrimaryURL(t, proxyURLReadOnly)
		bucketProps cmn.BucketProps
		baseParams  = tutils.BaseAPIParams(proxyURL)
		semaphore   = make(chan struct{}, concurr) // concurrent EC jobs at a time
	)

	smap := getClusterMap(t, proxyURL)
	if err := ecSliceNumInit(smap); err != nil {
		t.Fatal(err)
	}

	removeTarget := extractTargetNodes(smap)[0]
	tgtParams := tutils.BaseAPIParams(removeTarget.URL(cmn.NetworkPublic))

	mpathList, err := api.GetMountpaths(tgtParams)
	tutils.CheckFatal(err, t)
	if len(mpathList.Available) < 2 {
		t.Fatal("Test requires 2 or more mountpaths")
	}

	if usingSG {
		sgl = tutils.Mem2.NewSGL(0)
		defer sgl.Free()
	}

	setClusterConfig(t, proxyURL, "rebalancing_enabled", false)
	defer setClusterConfig(t, proxyURL, "rebalancing_enabled", true)

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	fullPath := fmt.Sprintf("local/%s/%s", TestLocalBucketName, ecTestDir)

	tutils.CreateFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer tutils.DestroyLocalBucket(t, proxyURL, TestLocalBucketName)

	bucketProps.CksumConf.Checksum = "inherit"
	bucketProps.ECConf = cmn.ECConf{
		ECEnabled:      true,
		ECObjSizeLimit: ecObjLimit,
		DataSlices:     ecSliceCnt,
		ParitySlices:   ecParityCnt,
	}
	err = api.SetBucketProps(baseParams, TestLocalBucketName, bucketProps)
	tutils.CheckFatal(err, t)

	wg := &sync.WaitGroup{}

	// 1. PUT objects
	putOneObj := func(idx int, objName string) {
		defer func() {
			wg.Done()
			<-semaphore
		}()

		totalCnt, objSize, sliceSize, doEC := randObjectSize(rnd, idx, smallEvery)
		objPath := ecTestDir + objName
		ecStr := "-"
		if doEC {
			ecStr = "EC"
		}
		tutils.Logf("Creating %s, size %8d [%2s]\n", objPath, objSize, ecStr)
		r, err := tutils.NewRandReader(int64(objSize), false)
		tutils.CheckFatal(err, t)
		defer r.Close()
		putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: TestLocalBucketName, Object: objPath, Reader: r}
		err = api.PutObject(putArgs)
		tutils.CheckFatal(err, t)

		foundParts, mainObjPath := waitForECFinishes(totalCnt, objSize, sliceSize, doEC, fullPath, objName)

		ecCheckSlices(t, foundParts, fullPath+objName,
			objSize, sliceSize, totalCnt, ecParityCnt)
		if mainObjPath == "" {
			t.Errorf("Full copy is not found")
			return
		}
	}

	wg.Add(numFiles)
	for i := 0; i < numFiles; i++ {
		objName := fmt.Sprintf(objPatt, i)
		semaphore <- struct{}{}
		go putOneObj(i, objName)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	// 2. Disable a random mountpath
	mpathID := rnd.Intn(len(mpathList.Available))
	removeMpath := mpathList.Available[mpathID]
	tutils.Logf("Disabling a mountpath %s at target: %s\n", removeMpath, removeTarget.DaemonID)
	err = api.DisableMountpath(tgtParams, removeMpath)
	tutils.CheckFatal(err, t)
	defer func() {
		// Enable mountpah
		tutils.Logf("Enabling mountpath %s at target %s...\n", removeMpath, removeTarget.DaemonID)
		err = api.EnableMountpath(tgtParams, removeMpath)
		tutils.CheckFatal(err, t)
	}()

	// 3. Read objects
	getOneObj := func(idx int, objName string) {
		defer wg.Done()
		objPath := ecTestDir + objName
		_, err = api.GetObject(baseParams, TestLocalBucketName, objPath)
		tutils.CheckFatal(err, t)
	}

	tutils.Logln("Reading all objects...")
	wg.Add(numFiles)
	for i := 0; i < numFiles; i++ {
		objName := fmt.Sprintf(objPatt, i)
		go getOneObj(i, objName)
	}
	wg.Wait()

	// 4. Check that ListBucket returns correct number of items
	tutils.Logf("DONE\nReading bucket list...\n")
	var msg = &cmn.GetMsg{GetPageSize: int(pagesize), GetProps: "size,status,version"}
	reslist, err := api.ListBucket(baseParams, TestLocalBucketName, msg, 0)
	tutils.CheckFatal(err, t)

	reslist.Entries = filterObjListOK(reslist.Entries)
	if len(reslist.Entries) != numFiles {
		t.Fatalf("Invalid number of objects: %d, expected %d", len(reslist.Entries), numFiles)
	}
}
