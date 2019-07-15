// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/containers"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
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

type ecSliceMD struct {
	size int64
	hash string
}

var (
	ecDataSliceCnt   = 2
	ecParitySliceCnt = 2
)

func defaultECBckProps() cmn.BucketProps {
	return cmn.BucketProps{
		Cksum: cmn.CksumConf{Type: "inherit"},
		EC: cmn.ECConf{
			Enabled:      true,
			ObjSizeLimit: ecObjLimit,
			DataSlices:   ecDataSliceCnt,
			ParitySlices: ecParitySliceCnt,
		},
	}
}

func ecSliceNumInit(t *testing.T, smap cluster.Smap) error {
	tCnt := len(smap.Tmap)
	if tCnt < 4 {
		return fmt.Errorf("%s requires at least 4 targets", t.Name())
	}

	if tCnt == 4 {
		ecDataSliceCnt = 1
		ecParitySliceCnt = 1
		return nil
	} else if tCnt == 5 {
		ecDataSliceCnt = 1
		ecParitySliceCnt = 2
		return nil
	}

	ecDataSliceCnt = 2
	ecParitySliceCnt = 2
	return nil
}

// Since all replicas are equal, it is hard to tell main one from others. Main
// replica is the replica that is on the target chosen by proxy using HrwTarget
// algorithm on GET request from a client.
// The function uses heuristics to detect the main one: it should be the oldest
func ecGetAllLocalSlices(t *testing.T, objName, bucketName string) (map[string]ecSliceMD, string) {
	tMock := cluster.NewTargetMock(cluster.NewBaseBownerMock(bucketName))
	foundParts := make(map[string]ecSliceMD)
	oldest := time.Now()
	main := ""
	noObjCnt := 0

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
			sliceLom, errstr := cluster.LOM{FQN: path, T: tMock}.Init(cmn.LocalBs)
			tassert.Fatalf(t, errstr == "", errstr)

			_, errstr = sliceLom.Load(false)

			var cksmVal string
			if strings.Contains(path, ecMetaDir) && errstr != "" {
				// meta file of the original object on the main target doesn't have meta saved on a disk
				noObjCnt++
			} else if !strings.Contains(path, ecMetaDir) {
				// meta files contain checksum inside, but dont have a checksum itself in a lom
				if sliceLom.Cksum() != nil {
					_, cksmVal = sliceLom.Cksum().Get()
				}
			}

			foundParts[path] = ecSliceMD{info.Size(), cksmVal}
			if strings.Contains(path, ecDataDir) && oldest.After(info.ModTime()) {
				main = path
				oldest = info.ModTime()
			}
		}

		return nil
	}

	filepath.Walk(rootDir, fsWalkFunc)

	if noObjCnt > 1 {
		tutils.Logf("meta not found for %v files matching object name %s\n", noObjCnt, objName)
	}

	return foundParts, main
}

func filterObjListOK(lst []*cmn.BucketEntry) []*cmn.BucketEntry {
	i := 0
	curr := 0

	for i < len(lst) {
		if !lst[i].IsStatusOK() {
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

func ecCheckSlices(t *testing.T, sliceList map[string]ecSliceMD,
	objFullPath string, objSize, sliceSize int64,
	totalCnt int) (mainObjPath string) {
	tassert.Errorf(t, len(sliceList) == totalCnt, "Expected number of objects for %q: %d, found: %d\n%+v",
		objFullPath, totalCnt, len(sliceList), sliceList)

	sliced := sliceSize < objSize
	hashes := make(map[string]bool)
	metaCnt := 0
	for k, md := range sliceList {
		if strings.Contains(k, ecMetaDir) {
			metaCnt++
			tassert.Errorf(t, md.size <= 512, "Metafile %q size is too big: %d", k, md.size)
		} else if strings.Contains(k, ecSliceDir) {
			tassert.Errorf(t, md.size == sliceSize, "Slice %q size mismatch: %d, expected %d", k, md.size, sliceSize)
			if sliced {
				if _, ok := hashes[md.hash]; ok {
					t.Errorf("Duplicated slice hash(slice) %q: %s\n", objFullPath, md.hash)
				}
				hashes[md.hash] = true
			}
		} else {
			tassert.Errorf(t, strings.HasSuffix(k, objFullPath), "Invalid object name: %s [expected '../%s']", k, objFullPath)
			tassert.Errorf(t, md.size == objSize, "File %q size mismatch: got %d, expected %d", k, md.size, objSize)
			mainObjPath = k
			if sliced {
				if _, ok := hashes[md.hash]; ok {
					t.Errorf("Duplicated slice hash(main) %q: %s\n", objFullPath, md.hash)
				}
				hashes[md.hash] = true
			}
		}
	}

	metaCntMust := totalCnt / 2
	tassert.Errorf(t, metaCnt == metaCntMust, "Number of metafiles mismatch: %d, expected %d", metaCnt, metaCntMust)

	return
}

func waitForECFinishes(t *testing.T, totalCnt int, objSize, sliceSize int64, doEC bool, objName, bckName string) (foundParts map[string]ecSliceMD, mainObjPath string) {
	deadLine := time.Now().Add(ECPutTimeOut)
	for time.Now().Before(deadLine) {
		foundParts, mainObjPath = ecGetAllLocalSlices(t, objName, bckName)
		if len(foundParts) == totalCnt {
			same := true
			for nm, md := range foundParts {
				if doEC {
					if strings.Contains(nm, ecSliceDir) {
						if md.size != sliceSize {
							same = false
							break
						}
					}
				} else {
					if strings.Contains(nm, ecDataDir) {
						if md.size != objSize {
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
	totalCnt = 2 + (ecDataSliceCnt+ecParitySliceCnt)*2
	objSize = int64(ecMinBigSize + rnd.Intn(ecBigDelta))
	sliceSize = ec.SliceSize(objSize, ecDataSliceCnt)
	if (n+1)%every == 0 {
		// Small object case
		// full object copy+meta: 1+1
		// number of metafiles: parity
		// number of slices: parity
		totalCnt = 2 + ecParitySliceCnt*2
		objSize = int64(ecMinSmallSize + rnd.Intn(ecSmallDelta))
		sliceSize = objSize
	}
	doEC = objSize >= ecObjLimit
	return
}

func calculateSlicesCount(slices map[string]ecSliceMD) map[string]int {
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

func doECPutsAndCheck(t *testing.T, bckName string, seed int64, baseParams *api.BaseParams, concurr int, objCount int) {
	const (
		smallEvery = 10 // Every N-th object is small
		objPatt    = "obj-%s-%04d"
	)

	wg := &sync.WaitGroup{}

	rnd := rand.New(rand.NewSource(seed))
	semaphore := make(chan struct{}, concurr) // concurrent EC jobs at a time
	sizes := make(chan int64, objCount)

	for idx := 0; idx < objCount; idx++ {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(i int) {
			totalCnt, objSize, sliceSize, doEC := randObjectSize(rnd, i, smallEvery)
			sizes <- objSize
			objName := fmt.Sprintf(objPatt, bckName, i)
			objPath := ecTestDir + objName

			if i%10 == 0 {
				if doEC {
					tutils.Logf("Object %s, size %9d[%9d]\n", objName, objSize, sliceSize)
				} else {
					tutils.Logf("Object %s, size %9d[%9s]\n", objName, objSize, "-")
				}
			}

			r, err := tutils.NewRandReader(objSize, false)
			defer func() {
				r.Close()
				<-semaphore
				wg.Done()
			}()
			tassert.CheckFatal(t, err)
			putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: bckName, Object: objPath, Reader: r}
			err = api.PutObject(putArgs)
			tassert.CheckFatal(t, err)

			fullPath := fmt.Sprintf("local/%s/%s", bckName, ecTestDir)
			foundParts, _ := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, objName, bckName)
			mainObjPath := ""
			if len(foundParts) != totalCnt {
				t.Errorf("Expected number of files %s: %d, found: %d\n%+v",
					objName, totalCnt, len(foundParts), foundParts)
				return
			}
			metaCnt, sliceCnt, replCnt := 0, 0, 0
			for k, md := range foundParts {
				if strings.Contains(k, ecMetaDir) {
					metaCnt++
					tassert.Errorf(t, md.size <= 512, "Metafile %q size is too big: %d", k, md.size)
				} else if strings.Contains(k, ecSliceDir) {
					sliceCnt++
					if md.size != sliceSize && doEC {
						t.Errorf("Slice %q size mismatch: %d, expected %d", k, md.size, sliceSize)
					}
					if md.size != objSize && !doEC {
						t.Errorf("Copy %q size mismatch: %d, expected %d", k, md.size, objSize)
					}
				} else {
					objFullPath := fullPath + objName
					tassert.Errorf(t, strings.HasSuffix(k, objFullPath), "Invalid object name: %s [expected '..%s']", k, objFullPath)
					tassert.Errorf(t, md.size == objSize, "File %q size mismatch: got %d, expected %d", k, md.size, objSize)
					mainObjPath = k
					replCnt++
				}
			}

			metaCntMust := ecParitySliceCnt + 1
			if doEC {
				metaCntMust = ecParitySliceCnt + ecDataSliceCnt + 1
			}
			tassert.Errorf(t, metaCnt == metaCntMust, "Number of metafiles mismatch: %d, expected %d", metaCnt, ecParitySliceCnt+1)
			if doEC {
				tassert.Errorf(t, sliceCnt == ecParitySliceCnt+ecDataSliceCnt, "Number of chunks mismatch: %d, expected %d", sliceCnt, ecParitySliceCnt+ecDataSliceCnt)
			} else {
				tassert.Errorf(t, replCnt == ecParitySliceCnt+1, "Number replicas mismatch: %d, expected %d", replCnt, ecParitySliceCnt)
			}

			if mainObjPath == "" {
				t.Errorf("Full copy is not found")
				return
			}

			tassert.CheckFatal(t, os.Remove(mainObjPath))
			partsAfterRemove, _ := ecGetAllLocalSlices(t, objName, bckName)
			_, ok := partsAfterRemove[mainObjPath]
			if ok || len(partsAfterRemove) >= len(foundParts) {
				t.Errorf("File is not deleted: %#v", partsAfterRemove)
				return
			}

			_, err = api.GetObject(baseParams, bckName, objPath)
			tassert.CheckFatal(t, err)

			if doEC {
				partsAfterRestore, _ := ecGetAllLocalSlices(t, objName, bckName)
				md, ok := partsAfterRestore[mainObjPath]
				if !ok || len(partsAfterRestore) != len(foundParts) {
					t.Errorf("File is not restored: %#v", partsAfterRestore)
					return
				}

				if md.size != objSize {
					t.Errorf("File is restored incorrectly, size mismatches: %d, expected %d", md.size, objSize)
					return
				}
			}
		}(idx)
	}

	wg.Wait()
	close(sizes)

	szTotal := int64(0)
	szLen := len(sizes)
	for sz := range sizes {
		szTotal += sz
	}
	if szLen != 0 {
		t.Logf("Average size on bucket %s: %s\n", bckName, cmn.B2S(szTotal/int64(szLen), 1))
	}
}

func assertBucketSize(t *testing.T, baseParams *api.BaseParams, bckName string, numFiles int) {
	bckObjectsCnt := bucketSize(t, baseParams, bckName)
	tassert.Fatalf(t, bckObjectsCnt == numFiles, "Invalid number of objects: %d, expected %d", bckObjectsCnt, numFiles)
}

func bucketSize(t *testing.T, baseParams *api.BaseParams, bckName string) int {
	var msg = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status"}
	reslist, err := api.ListBucket(baseParams, bckName, msg, 0)
	tassert.CheckFatal(t, err)
	reslist.Entries = filterObjListOK(reslist.Entries)

	return len(reslist.Entries)
}

func putRandomFile(t *testing.T, baseParams *api.BaseParams, bckName string, objPath string, size int) {
	r, err := tutils.NewRandReader(int64(size), false)
	tassert.CheckFatal(t, err)
	defer r.Close()

	putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: bckName, Object: objPath, Reader: r}
	err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)
}

func newLocalBckWithProps(t *testing.T, name string, bckProps cmn.BucketProps, seed int64, concurr int, baseParams *api.BaseParams) {
	tutils.CreateFreshLocalBucket(t, proxyURLReadOnly, name)

	tutils.Logf("Changing EC %d:%d [ seed = %d ], concurrent: %d\n",
		ecDataSliceCnt, ecParitySliceCnt, seed, concurr)
	err := api.SetBucketPropsMsg(baseParams, name, bckProps)

	if err != nil {
		tutils.DestroyLocalBucket(t, proxyURLReadOnly, name)
	}
	tassert.CheckFatal(t, err)
}

func clearAllECObjects(t *testing.T, numFiles int, bucket, objPatt string, failOnDelErr bool) {
	tutils.Logln("Deleting objects...")
	wg := sync.WaitGroup{}

	wg.Add(numFiles)
	for idx := 0; idx < numFiles; idx++ {
		go func(i int) {
			defer wg.Done()
			objName := fmt.Sprintf(objPatt, i)
			objPath := ecTestDir + objName
			err := tutils.Del(proxyURLReadOnly, bucket, objPath, "", nil, nil, true)
			if failOnDelErr {
				tassert.CheckFatal(t, err)
			} else if err != nil {
				t.Log(err.Error())
			}

			deadline := time.Now().Add(time.Second * 10)
			var partsAfterDelete map[string]int64
			for time.Now().Before(deadline) {
				time.Sleep(time.Millisecond * 250)
				partsAfterDelete, _ := ecGetAllLocalSlices(t, objName, bucket)
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

func objectsExist(t *testing.T, baseParams *api.BaseParams, bckName, objPatt string, numFiles int) {
	wg := &sync.WaitGroup{}
	getOneObj := func(objName string) {
		defer wg.Done()
		objPath := ecTestDir + objName
		_, err := api.GetObject(baseParams, bckName, objPath)
		tassert.CheckFatal(t, err)
	}

	tutils.Logln("Reading all objects...")
	wg.Add(numFiles)
	for i := 0; i < numFiles; i++ {
		objName := fmt.Sprintf(objPatt, i)
		go getOneObj(objName)
	}
	wg.Wait()
}

// Short test to make sure that EC options cannot be changed after
// EC is enabled
func TestECChange(t *testing.T) {
	var (
		bucket      = TestLocalBucketName
		proxyURL    = getPrimaryURL(t, proxyURLReadOnly)
		bucketProps cmn.BucketProps
	)

	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	bucketProps.Cksum.Type = "inherit"
	bucketProps.EC = cmn.ECConf{
		Enabled:      true,
		ObjSizeLimit: ecObjLimit,
		DataSlices:   1,
		ParitySlices: 1,
	}
	baseParams := tutils.BaseAPIParams(proxyURL)

	tutils.Logln("Resetting bucket properties")
	err := api.ResetBucketProps(baseParams, bucket)
	tassert.CheckFatal(t, err)

	tutils.Logln("Trying to set too many slices")
	bucketProps.EC.DataSlices = 25
	bucketProps.EC.ParitySlices = 25
	err = api.SetBucketPropsMsg(baseParams, bucket, bucketProps)
	tassert.Errorf(t, err != nil, "Enabling EC must fail in case of the number of targets fewer than the number of slices")

	tutils.Logln("Enabling EC")
	bucketProps.EC.DataSlices = 1
	bucketProps.EC.ParitySlices = 1
	err = api.SetBucketPropsMsg(baseParams, bucket, bucketProps)
	tassert.CheckFatal(t, err)

	tutils.Logln("Trying to set EC options to the same values")
	err = api.SetBucketPropsMsg(baseParams, bucket, bucketProps)
	tassert.CheckFatal(t, err)

	tutils.Logln("Trying to disable EC")
	bucketProps.EC.Enabled = false
	err = api.SetBucketPropsMsg(baseParams, bucket, bucketProps)
	tassert.Errorf(t, err == nil, "Disabling EC failed: %v", err)

	tutils.Logln("Trying to re-enable EC")
	bucketProps.EC.Enabled = true
	err = api.SetBucketPropsMsg(baseParams, bucket, bucketProps)
	tassert.Errorf(t, err == nil, "Enabling EC failed: %v", err)

	tutils.Logln("Trying to modify EC options when EC is enabled")
	bucketProps.EC.Enabled = true
	bucketProps.EC.ObjSizeLimit = 300000
	err = api.SetBucketPropsMsg(baseParams, bucket, bucketProps)
	tassert.Errorf(t, err != nil, "Modifiying EC properties must fail")

	tutils.Logln("Resetting bucket properties")
	err = api.ResetBucketProps(baseParams, bucket)
	tassert.Errorf(t, err == nil, "Resetting properties should work")
}

func createECReplicas(t *testing.T, baseParams *api.BaseParams, bucket, objName string, sema chan struct{}, rnd *rand.Rand, fullPath string) {
	sema <- struct{}{}
	defer func() {
		<-sema
	}()

	totalCnt := 2 + ecParitySliceCnt*2
	objSize := int64(ecMinSmallSize + rnd.Intn(ecSmallDelta))
	sliceSize := objSize

	objPath := ecTestDir + objName

	tutils.Logf("Creating %s, size %8d\n", objPath, objSize)
	r, err := tutils.NewRandReader(objSize, false)
	tassert.CheckFatal(t, err)
	defer r.Close()

	putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: bucket, Object: objPath, Reader: r}
	err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)

	tutils.Logf("Waiting for %s\n", objPath)
	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, false, objName, bucket)

	ecCheckSlices(t, foundParts, fullPath+objName, objSize, sliceSize, totalCnt)
	tassert.Errorf(t, mainObjPath != "", "Full copy is not found")
}

func createDamageRestoreECFile(t *testing.T, baseParams *api.BaseParams, bucket, objName string, idx int, sema chan struct{}, rnd *rand.Rand, fullPath string) {
	const (
		sleepRestoreTime = 5 * time.Second // wait time after GET restores slices
		smallEvery       = 7               // Every N-th object is small
		sliceDelPct      = 50              // %% of objects that have damaged body and a slice
	)

	sema <- struct{}{}
	defer func() {
		<-sema
	}()

	delSlice := false // delete only main object
	deletedFiles := 1
	if ecDataSliceCnt+ecParitySliceCnt > 2 && rnd.Intn(100) < sliceDelPct {
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
	r, err := tutils.NewRandReader(objSize, false)
	tassert.CheckFatal(t, err)
	defer r.Close()

	putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: bucket, Object: objPath, Reader: r}
	err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)

	tutils.Logf("Waiting for %s\n", objPath)
	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, objName, bucket)

	ecCheckSlices(t, foundParts, fullPath+objName, objSize, sliceSize, totalCnt)
	if mainObjPath == "" {
		t.Errorf("Full copy is not found")
		return
	}

	tutils.Logf("Damaging %s [removing %s]\n", objPath, mainObjPath)
	tassert.CheckFatal(t, os.Remove(mainObjPath))
	metafile := strings.Replace(mainObjPath, ecDataDir, ecMetaDir, -1)
	tutils.Logf("Damaging %s [removing %s]\n", objPath, metafile)
	tassert.CheckFatal(t, os.Remove(metafile))
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
		tassert.CheckFatal(t, os.Remove(sliceToDel))
		if doEC {
			metafile := strings.Replace(sliceToDel, ecSliceDir, ecMetaDir, -1)
			tutils.Logf("Removing slice meta %s\n", metafile)
			tassert.CheckFatal(t, os.Remove(metafile))
		} else {
			metafile := strings.Replace(sliceToDel, ecDataDir, ecMetaDir, -1)
			tutils.Logf("Removing replica meta %s\n", metafile)
			tassert.CheckFatal(t, os.Remove(metafile))
		}
	}

	partsAfterRemove, _ := ecGetAllLocalSlices(t, objName, bucket)
	_, ok := partsAfterRemove[mainObjPath]
	if ok || len(partsAfterRemove) != len(foundParts)-deletedFiles*2 {
		t.Errorf("Files are not deleted [%d - %d]: %#v", len(foundParts), len(partsAfterRemove), partsAfterRemove)
		return
	}

	tutils.Logf("Restoring %s\n", objPath)
	_, err = api.GetObject(baseParams, bucket, objPath)
	tassert.CheckFatal(t, err)

	if doEC {
		deadline := time.Now().Add(sleepRestoreTime)
		var partsAfterRestore map[string]ecSliceMD
		for time.Now().Before(deadline) {
			time.Sleep(time.Millisecond * 250)
			partsAfterRestore, _ = ecGetAllLocalSlices(t, objName, bucket)
			if len(partsAfterRestore) == totalCnt {
				break
			}
		}
		ecCheckSlices(t, partsAfterRestore, fullPath+objName,
			objSize, sliceSize, totalCnt)
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
		objPatt  = "obj-rest-%04d"
		numFiles = 50
		semaCnt  = 8
	)

	var (
		bucket   = TestLocalBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		sema     = make(chan struct{}, semaCnt)
	)

	smap := getClusterMap(t, proxyURL)
	err := ecSliceNumInit(t, smap)
	tassert.CheckFatal(t, err)

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	fullPath := fmt.Sprintf("local/%s/%s", bucket, ecTestDir)

	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	baseParams := tutils.BaseAPIParams(proxyURL)

	newLocalBckWithProps(t, bucket, defaultECBckProps(), seed, 0, baseParams)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	wg := sync.WaitGroup{}

	wg.Add(numFiles)
	for i := 0; i < numFiles; i++ {
		objName := fmt.Sprintf(objPatt, i)
		go func(i int) {
			createDamageRestoreECFile(t, baseParams, bucket, objName, i, sema, rnd, fullPath)
			wg.Done()
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	assertBucketSize(t, baseParams, bucket, numFiles)
	clearAllECObjects(t, numFiles, bucket, objPatt, true)
}

func putECFile(baseParams *api.BaseParams, bucket, objName string) error {
	objSize := int64(ecMinBigSize * 2)
	objPath := ecTestDir + objName

	r, err := tutils.NewRandReader(objSize, false)
	if err != nil {
		return err
	}
	defer r.Close()

	putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: bucket, Object: objPath, Reader: r}
	return api.PutObject(putArgs)
}

// Returns path to main object and map of all object's slices and ioContext
func createECFile(t *testing.T, bucket, objName, fullPath string, baseParams *api.BaseParams) (map[string]ecSliceMD, string) {
	totalCnt := 2 + (ecDataSliceCnt+ecParitySliceCnt)*2
	objSize := int64(ecMinBigSize * 2)
	sliceSize := ec.SliceSize(objSize, ecDataSliceCnt)

	err := putECFile(baseParams, bucket, objName)
	tassert.CheckFatal(t, err)

	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, true, objName, bucket)
	tassert.Fatalf(t, mainObjPath != "", "Full copy %s was not found", mainObjPath)

	ecCheckSlices(t, foundParts, fullPath+objName, objSize, sliceSize, totalCnt)

	return foundParts, mainObjPath
}

// Creates 2 EC files and then corrupts their slices
// Checks that after corrupting one slice it is still possible to recover a file
// Checks that after corrupting all slices it is not possible to recover a file
func TestECChecksum(t *testing.T) {
	const (
		objPatt = "obj-cksum-%04d"
	)

	if containers.DockerRunning() {
		t.Skip(fmt.Sprintf("test %q requires Xattributes to be set, doesn't work with docker", t.Name()))
	}

	var (
		bucket   = TestLocalBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		tMock    = cluster.NewTargetMock(cluster.NewBaseBownerMock(TestLocalBucketName))
	)

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap))

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()
	fullPath := fmt.Sprintf("local/%s/%s", bucket, ecTestDir)

	seed := time.Now().UnixNano()
	baseParams := tutils.BaseAPIParams(proxyURL)

	newLocalBckWithProps(t, bucket, defaultECBckProps(), seed, 0, baseParams)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	objName1 := fmt.Sprintf(objPatt, 1)
	objPath1 := ecTestDir + objName1
	foundParts1, mainObjPath1 := createECFile(t, bucket, objName1, fullPath, baseParams)

	objName2 := fmt.Sprintf(objPatt, 2)
	objPath2 := ecTestDir + objName2
	foundParts2, mainObjPath2 := createECFile(t, bucket, objName2, fullPath, baseParams)

	tutils.Logf("Removing main object %s\n", mainObjPath1)
	tassert.CheckFatal(t, os.Remove(mainObjPath1))

	// Corrupt just one slice, EC should be able to restore the original object
	for k := range foundParts1 {
		if k != mainObjPath1 && strings.Contains(k, ecSliceDir) {
			err := tutils.SetXattrCksm(k, cmn.NewCksum(cmn.ChecksumXXHash, "01234"), tMock)
			tassert.CheckFatal(t, err)
			break
		}
	}

	_, err := api.GetObject(baseParams, bucket, objPath1)
	tassert.CheckFatal(t, err)

	tutils.Logf("Removing main object %s\n", mainObjPath2)
	tassert.CheckFatal(t, os.Remove(mainObjPath2))

	// Corrupt all slices, EC should not be able to restore
	for k := range foundParts2 {
		if k != mainObjPath2 && strings.Contains(k, ecSliceDir) {
			err := tutils.SetXattrCksm(k, cmn.NewCksum(cmn.ChecksumXXHash, "01234"), tMock)
			tassert.CheckFatal(t, err)
		}
	}

	_, err = api.GetObject(baseParams, bucket, objPath2)
	tassert.Fatalf(t, err != nil, "Object should not be restored when checksums are wrong")
}

func TestECEnabledDisabledEnabled(t *testing.T) {
	const (
		objPatt  = "obj-rest-%04d"
		numFiles = 25
		semaCnt  = 8
	)
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		bucket   = TestLocalBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		sema     = make(chan struct{}, semaCnt)
	)

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap))

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	fullPath := fmt.Sprintf("local/%s/%s", bucket, ecTestDir)

	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	baseParams := tutils.BaseAPIParams(proxyURL)

	newLocalBckWithProps(t, bucket, defaultECBckProps(), seed, 0, baseParams)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	// End of preparation, create files with EC enabled, check if are restored properly

	wg := sync.WaitGroup{}
	wg.Add(numFiles)
	for i := 0; i < numFiles; i++ {
		objName := fmt.Sprintf(objPatt, i)
		go func(i int) {
			createDamageRestoreECFile(t, baseParams, bucket, objName, i, sema, rnd, fullPath)
			wg.Done()
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	assertBucketSize(t, baseParams, bucket, numFiles)

	// Disable EC, put normal files, check if were created properly
	err := api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "false"})
	tassert.CheckError(t, err)

	wg.Add(numFiles)
	for i := numFiles; i < 2*numFiles; i++ {
		objName := fmt.Sprintf(objPatt, i)
		go func() {
			putRandomFile(t, baseParams, bucket, objName, cmn.MiB)
			wg.Done()
		}()
	}

	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	assertBucketSize(t, baseParams, bucket, numFiles*2)

	// Enable EC again, check if EC was started properly and creates files with EC correctly
	err = api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "true"})
	tassert.CheckError(t, err)

	wg.Add(numFiles)
	for i := 2 * numFiles; i < 3*numFiles; i++ {
		objName := fmt.Sprintf(objPatt, i)
		go func(i int) {
			createDamageRestoreECFile(t, baseParams, bucket, objName, i, sema, rnd, fullPath)
			wg.Done()
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	assertBucketSize(t, baseParams, bucket, numFiles*3)
}

func TestECDisableEnableDuringLoad(t *testing.T) {
	const (
		objPatt  = "obj-disable-enable-load-%04d"
		numFiles = 5
		semaCnt  = 8
	)
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		bucket   = TestLocalBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		sema     = make(chan struct{}, semaCnt)
	)

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap))

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	fullPath := fmt.Sprintf("local/%s/%s", bucket, ecTestDir)

	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	baseParams := tutils.BaseAPIParams(proxyURL)

	newLocalBckWithProps(t, bucket, defaultECBckProps(), seed, 0, baseParams)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	// End of preparation, create files with EC enabled, check if are restored properly

	wg := &sync.WaitGroup{}
	wg.Add(numFiles)
	for i := 0; i < numFiles; i++ {
		objName := fmt.Sprintf(objPatt, i)
		go func(i int) {
			createDamageRestoreECFile(t, baseParams, bucket, objName, i, sema, rnd, fullPath)
			wg.Done()
		}(i)
	}
	wg.Wait()

	assertBucketSize(t, baseParams, bucket, numFiles)

	abortCh := make(chan struct{}, 1)
	numCreated := 0

	go func() {
		ticker := time.NewTicker(3 * time.Millisecond)
		for {
			select {
			case <-abortCh:
				ticker.Stop()
				return
			case <-ticker.C:
				objName := fmt.Sprintf(objPatt, numFiles+numCreated)
				go putRandomFile(t, baseParams, bucket, objName, cmn.KiB)
				numCreated++
			}
		}
	}()

	time.Sleep(time.Second)

	// Create different disable/enable actions to test if system persists behaving well
	go func() {
		err := api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "false"})
		tassert.CheckError(t, err)
	}()

	time.Sleep(5 * time.Millisecond)

	go func() {
		err := api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "true"})
		tassert.CheckError(t, err)
	}()

	time.Sleep(5 * time.Millisecond)

	go func() {
		err := api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "false"})
		tassert.CheckError(t, err)
	}()

	time.Sleep(5 * time.Millisecond)

	go func() {
		err := api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "false"})
		tassert.CheckError(t, err)
	}()

	time.Sleep(300 * time.Millisecond)

	go func() {
		err := api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "true"})
		tassert.CheckError(t, err)
	}()

	close(abortCh)
	time.Sleep(1 * time.Second) // wait for everything to settle down

	assertBucketSize(t, baseParams, bucket, numFiles+numCreated)

	if t.Failed() {
		t.FailNow()
	}

	wg = &sync.WaitGroup{}
	wg.Add(numFiles)
	for i := 0; i < numFiles; i++ {
		j := i + numFiles + numCreated
		objName := fmt.Sprintf(objPatt, j)

		go func(i int) {
			createDamageRestoreECFile(t, baseParams, bucket, objName, i, sema, rnd, fullPath)
			wg.Done()
		}(j)
	}

	wg.Wait()

	// Disabling and enabling EC should not result in put's failing
	assertBucketSize(t, baseParams, bucket, 2*numFiles+numCreated)
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
		objCount = 400
		concurr  = 12
	)

	var (
		bucket   = TestLocalBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap))

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	seed := time.Now().UnixNano()
	baseParams := tutils.BaseAPIParams(proxyURL)
	newLocalBckWithProps(t, bucket, defaultECBckProps(), seed, concurr, baseParams)

	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	doECPutsAndCheck(t, bucket, seed, baseParams, concurr, objCount)

	var msg = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status"}
	reslist, err := api.ListBucket(baseParams, bucket, msg, 0)
	tassert.CheckFatal(t, err)
	reslist.Entries = filterObjListOK(reslist.Entries)

	tassert.Fatalf(t, len(reslist.Entries) == objCount, "Invalid number of objects: %d, expected %d", len(reslist.Entries), objCount)
}

// Stress 2 buckets at the same time
func TestECStressManyBuckets(t *testing.T) {
	if testing.Short() {
		t.Skip("Long run only")
	}

	const (
		objCount = 200
		concurr  = 12
		bck1Name = TestLocalBucketName + "1"
		bck2Name = TestLocalBucketName + "2"
	)

	var proxyURL = getPrimaryURL(t, proxyURLReadOnly)

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap))

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	seed1 := time.Now().UnixNano()
	seed2 := time.Now().UnixNano()

	baseParams := tutils.BaseAPIParams(proxyURL)
	newLocalBckWithProps(t, bck1Name, defaultECBckProps(), seed1, concurr, baseParams)
	newLocalBckWithProps(t, bck2Name, defaultECBckProps(), seed2, concurr, baseParams)
	defer tutils.DestroyLocalBucket(t, proxyURL, bck1Name)
	defer tutils.DestroyLocalBucket(t, proxyURL, bck2Name)

	// Run EC on different buckets concurrently
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		doECPutsAndCheck(t, bck1Name, seed1, baseParams, concurr, objCount)
		wg.Done()
	}()
	go func() {
		doECPutsAndCheck(t, bck2Name, seed2, baseParams, concurr, objCount)
		wg.Done()
	}()
	wg.Wait()

	var msg = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status"}
	reslist, err := api.ListBucket(baseParams, bck1Name, msg, 0)
	tassert.CheckFatal(t, err)
	reslist.Entries = filterObjListOK(reslist.Entries)

	tassert.Fatalf(t, len(reslist.Entries) == objCount, "Bucket %s: Invalid number of objects: %d, expected %d", bck1Name, len(reslist.Entries), objCount)

	msg = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status"}
	reslist, err = api.ListBucket(baseParams, bck2Name, msg, 0)
	tassert.CheckFatal(t, err)
	reslist.Entries = filterObjListOK(reslist.Entries)

	tassert.Fatalf(t, len(reslist.Entries) == objCount, "Bucket %s: Invalid number of objects: %d, expected %d", bck2Name, len(reslist.Entries), objCount)
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
		bucket      = TestLocalBucketName
		proxyURL    = getPrimaryURL(t, proxyURLReadOnly)
		waitAllTime = time.Minute * 4              // should be enough for all object to complete EC
		semaphore   = make(chan struct{}, concurr) // concurrent EC jobs at a time

		totalSlices atomic.Int64
	)

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap))

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	baseParams := tutils.BaseAPIParams(proxyURL)

	newLocalBckWithProps(t, bucket, defaultECBckProps(), seed, concurr, baseParams)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

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
			r, err := tutils.NewRandReader(objSize, false)
			tassert.Errorf(t, err == nil, "Failed to create reader: %v", err)
			putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: bucket, Object: objPath, Reader: r}
			err = api.PutObject(putArgs)
			tassert.Errorf(t, err == nil, "PUT failed: %v", err)

			totalSlices.Add(int64(totalCnt))
			cntCh <- sCnt{obj: objName, cnt: totalCnt}

			<-semaphore
			wg.Done()
		}(idx)
	}

	wg.Wait()
	close(cntCh)

	var foundParts map[string]ecSliceMD
	startedWaiting := time.Now()
	deadLine := startedWaiting.Add(waitAllTime)
	for time.Now().Before(deadLine) {
		foundParts, _ = ecGetAllLocalSlices(t, objStart, bucket)
		if len(foundParts) == int(totalSlices.Load()) {
			delta := time.Since(startedWaiting)
			t.Logf("Waiting for EC completed after all object are PUT %v\n", delta)
			break
		}
		time.Sleep(time.Millisecond * 30)
	}
	if len(foundParts) != int(totalSlices.Load()) {
		slices := make(map[string]int, objCount)
		for sl := range cntCh {
			slices[sl.obj] = sl.cnt
		}
		fndSlices := calculateSlicesCount(foundParts)
		compareSlicesCount(t, slices, fndSlices)

		t.Fatalf("Expected total number of files: %d, found: %d\n",
			totalSlices.Load(), len(foundParts))
	}
	delta := time.Since(started)
	t.Logf("Total test time %v\n", delta)

	var msg = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status"}
	reslist, err := api.ListBucket(baseParams, bucket, msg, 0)
	tassert.CheckFatal(t, err)
	reslist.Entries = filterObjListOK(reslist.Entries)

	tassert.Fatalf(t, len(reslist.Entries) == objCount, "Invalid number of objects: %d, expected %d", len(reslist.Entries), objCount)
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
		t.Skip(tutils.SkipMsg)
	}

	var (
		bucket   = TestLocalBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap))

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()
	fullPath := fmt.Sprintf("local/%s/%s", bucket, ecTestDir)

	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	baseParams := tutils.BaseAPIParams(proxyURL)
	bckProps := defaultECBckProps()
	bckProps.Versioning.Type = cmn.PropOwn
	bckProps.Versioning.Enabled = true

	newLocalBckWithProps(t, bucket, bckProps, seed, 0, baseParams)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	oneObj := func(idx int, objName string) {
		delSlice := false // delete only main object
		deletedFiles := 1
		if ecDataSliceCnt+ecParitySliceCnt > 2 && rnd.Intn(100) < sliceDelPct {
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
		r, err := tutils.NewRandReader(objSize, false)
		tassert.CheckFatal(t, err)
		defer r.Close()
		putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: bucket, Object: objPath, Reader: r}
		err = api.PutObject(putArgs)
		tassert.CheckFatal(t, err)

		tutils.Logf("Waiting for %s\n", objPath)
		foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, objName, bucket)

		ecCheckSlices(t, foundParts, fullPath+objName,
			objSize, sliceSize, totalCnt)
		if mainObjPath == "" {
			t.Fatalf("Full copy is not found")
		}

		tutils.Logf("Damaging %s [removing %s]\n", objPath, mainObjPath)
		tassert.CheckFatal(t, os.Remove(mainObjPath))
		metafile := strings.Replace(mainObjPath, ecDataDir, ecMetaDir, -1)
		tutils.Logf("Damaging %s [removing %s]\n", objPath, metafile)
		tassert.CheckFatal(t, os.Remove(metafile))
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
			tassert.CheckFatal(t, os.Remove(sliceToDel))
			if doEC {
				metafile := strings.Replace(sliceToDel, ecSliceDir, ecMetaDir, -1)
				tutils.Logf("Removing slice meta %s\n", metafile)
				tassert.CheckFatal(t, os.Remove(metafile))
			} else {
				metafile := strings.Replace(sliceToDel, ecDataDir, ecMetaDir, -1)
				tutils.Logf("Removing replica meta %s\n", metafile)
				tassert.CheckFatal(t, os.Remove(metafile))
			}
		}

		partsAfterRemove, _ := ecGetAllLocalSlices(t, objName, bucket)
		_, ok := partsAfterRemove[mainObjPath]
		if ok || len(partsAfterRemove) != len(foundParts)-deletedFiles*2 {
			t.Fatalf("Files are not deleted [%d - %d]: %#v", len(foundParts), len(partsAfterRemove), partsAfterRemove)
		}

		tutils.Logf("Restoring %s\n", objPath)
		_, err = api.GetObject(baseParams, bucket, objPath)
		tassert.CheckFatal(t, err)

		if doEC {
			deadline := time.Now().Add(sleepRestoreTime)
			var partsAfterRestore map[string]ecSliceMD
			for time.Now().Before(deadline) {
				time.Sleep(time.Millisecond * 250)
				partsAfterRestore, _ = ecGetAllLocalSlices(t, objName, bucket)
				if len(partsAfterRestore) == totalCnt {
					break
				}
			}
			ecCheckSlices(t, partsAfterRestore, fullPath+objName,
				objSize, sliceSize, totalCnt)
		}
	}

	// PUT objects twice to make their version 2
	for j := 0; j < 2; j++ {
		for i := 0; i < numFiles; i++ {
			objName := fmt.Sprintf(objPatt, i)
			oneObj(i, objName)
		}
	}

	var msg = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status,version"}
	reslist, err := api.ListBucket(baseParams, bucket, msg, 0)
	tassert.CheckFatal(t, err)

	// check that all returned objects and their repicas have the same version
	for _, e := range reslist.Entries {
		if e.Version != finalVersion {
			t.Errorf("%s[status=%d] must have version %s but it is %s\n", e.Name, e.Flags, finalVersion, e.Version)
		}
	}

	reslist.Entries = filterObjListOK(reslist.Entries)
	if len(reslist.Entries) != numFiles {
		t.Fatalf("Invalid number of objects: %d, expected %d", len(reslist.Entries), 1)
	}

	clearAllECObjects(t, numFiles, bucket, objPatt, true)
}

// 1. start putting EC files into the cluster
// 2. in the middle of puts destroy bucket
// 3. wait for puts to finish
// 4. create bucket with the same name
// 5. check that EC is working properly for this bucket
func TestECDestroyBucket(t *testing.T) {
	const (
		objPatt  = "obj-destroy-bck-%04d"
		numFiles = 100
		concurr  = 10
	)

	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		bucket    = TestLocalBucketName + "-DESTROY"
		proxyURL  = getPrimaryURL(t, proxyURLReadOnly)
		semaphore = make(chan struct{}, concurr) // concurrent EC jobs at a time
	)

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap))

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	seed := time.Now().UnixNano()
	baseParams := tutils.BaseAPIParams(proxyURL)

	bckProps := defaultECBckProps()
	newLocalBckWithProps(t, bucket, bckProps, seed, concurr, baseParams)

	wg := &sync.WaitGroup{}
	wg.Add(numFiles)
	errCnt := atomic.NewInt64(0)
	sucCnt := atomic.NewInt64(0)

	for i := 0; i < numFiles; i++ {
		objName := fmt.Sprintf(objPatt, i)
		go func(i int) {
			semaphore <- struct{}{}
			if i%10 == 0 {
				tutils.Logf("ec file %s into bucket %s\n", objName, bucket)
			}
			if putECFile(baseParams, bucket, objName) != nil {
				errCnt.Inc()
			} else {
				sucCnt.Inc()
			}
			<-semaphore
			wg.Done()
		}(i)

		if i == 4*numFiles/5 {
			// DestroyLocalBucket when put requests are still executing
			go func() {
				semaphore <- struct{}{}
				tutils.Logf("Destroying bucket %s\n", bucket)
				tutils.DestroyLocalBucket(t, proxyURL, bucket)
				<-semaphore
			}()
		}
	}

	wg.Wait()
	tutils.Logf("EC put files resulted in error in %d out of %d files\n", errCnt.Load(), numFiles)

	// create bucket with the same name and check if puts are successful
	newLocalBckWithProps(t, bucket, bckProps, seed, concurr, baseParams)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)
	doECPutsAndCheck(t, bucket, seed, baseParams, concurr, numFiles)

	// check if get requests are successful
	var msg = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status,version"}
	reslist, err := api.ListBucket(baseParams, bucket, msg, 0)
	tassert.CheckError(t, err)

	reslist.Entries = filterObjListOK(reslist.Entries)
	tassert.Errorf(t, len(reslist.Entries) == numFiles, "Invalid number of objects: %d, expected %d", len(reslist.Entries), numFiles)
}

// Lost target test:
// - puts some objects
// - kills a random target
// - gets all objects
// - nothing must fail
// - register the target back
func TestECEmergencyTargetForSlices(t *testing.T) {
	const (
		objPatt    = "obj-emt-%04d"
		numFiles   = 100
		smallEvery = 4
		concurr    = 12
	)

	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		bucket    = TestLocalBucketName
		proxyURL  = getPrimaryURL(t, proxyURLReadOnly)
		semaphore = make(chan struct{}, concurr) // concurrent EC jobs at a time
	)

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap))

	// Increase number of EC data slices, now there's just enough targets to handle EC requests
	// Encoding will fail if even one is missing, restoring should still work
	ecDataSliceCnt++

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	fullPath := fmt.Sprintf("local/%s/%s", bucket, ecTestDir)
	seed := time.Now().UnixNano()
	baseParams := tutils.BaseAPIParams(proxyURL)
	bckProps := defaultECBckProps()
	newLocalBckWithProps(t, bucket, bckProps, seed, concurr, baseParams)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	wg := &sync.WaitGroup{}

	// 1. PUT objects
	putOneObj := func(idx int, objName string) {
		defer func() {
			wg.Done()
			<-semaphore
		}()

		start := time.Now()
		totalCnt, objSize, sliceSize, doEC := randObjectSize(cmn.NowRand(), idx, smallEvery)
		objPath := ecTestDir + objName
		ecStr := "-"
		if doEC {
			ecStr = "EC"
		}
		tutils.Logf("Creating %s, size %8d [%2s]\n", objPath, objSize, ecStr)
		r, err := tutils.NewRandReader(objSize, false)
		tassert.CheckFatal(t, err)
		defer r.Close()
		putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: bucket, Object: objPath, Reader: r}
		err = api.PutObject(putArgs)
		tassert.CheckFatal(t, err)
		t.Logf("Object %s put in %v", objName, time.Since(start))
		start = time.Now()

		foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, objName, bucket)

		ecCheckSlices(t, foundParts, fullPath+objName,
			objSize, sliceSize, totalCnt)
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
	var removedTarget *cluster.Snode
	smap, removedTarget = tutils.RemoveTarget(t, proxyURL, smap)

	defer func() {
		smap = tutils.RestoreTarget(t, proxyURL, smap, removedTarget)
		waitForRebalanceToComplete(t, baseParams, rebalanceTimeout)
	}()

	// 3. Read objects
	objectsExist(t, baseParams, bucket, objPatt, numFiles)

	// 4. Check that ListBucket returns correct number of items
	tutils.Logln("DONE\nReading bucket list...")
	var msg = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status,version"}
	reslist, err := api.ListBucket(baseParams, bucket, msg, 0)
	tassert.CheckError(t, err)

	reslist.Entries = filterObjListOK(reslist.Entries)
	tassert.Errorf(t, len(reslist.Entries) == numFiles, "Invalid number of objects: %d, expected %d", len(reslist.Entries), numFiles)
}

func TestECEmergencyTargetForReplica(t *testing.T) {
	const (
		objPatt  = "obj-rest-%04d"
		numFiles = 50
		semaCnt  = 8
	)

	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		bucket   = TestLocalBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		sema     = make(chan struct{}, semaCnt)
	)

	initialSmap := getClusterMap(t, proxyURL)

	if len(initialSmap.Tmap) > 10 {
		// Reason: calculating main obj directory based on DeamonID
		// see getOneObj, 'HACK' annotation
		t.Skip("Test requires at most 10 targets")
	}

	tassert.CheckFatal(t, ecSliceNumInit(t, initialSmap))

	for _, target := range initialSmap.Tmap {
		target.Digest()
	}

	// Increase number of EC data slices, now there's just enough targets to handle EC requests
	// Encoding will fail if even one is missing, restoring should still work
	ecDataSliceCnt++

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	fullPath := fmt.Sprintf("local/%s/%s", bucket, ecTestDir)

	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	baseParams := tutils.BaseAPIParams(proxyURL)

	bckProps := defaultECBckProps()
	newLocalBckWithProps(t, bucket, bckProps, seed, 0, baseParams)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	wg := sync.WaitGroup{}

	// put a file
	wg.Add(numFiles)
	for i := 0; i < numFiles; i++ {
		objName := fmt.Sprintf(objPatt, i)
		go func() {
			createECReplicas(t, baseParams, bucket, objName, sema, rnd, fullPath)
			wg.Done()
		}()
	}

	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	// kill #dataslices of targets, normal EC restore won't be possible
	// 2. Kill a random target
	removedTargets := make([]*cluster.Snode, 0, ecDataSliceCnt)
	smap := getClusterMap(t, proxyURL)

	for i := ecDataSliceCnt - 1; i >= 0; i-- {
		var removedTarget *cluster.Snode
		smap, removedTarget = tutils.RemoveTarget(t, proxyURL, smap)
		removedTargets = append(removedTargets, removedTarget)
	}

	defer func() {
		// Restore targets
		smap = getClusterMap(t, proxyURL)
		for _, target := range removedTargets {
			smap = tutils.RestoreTarget(t, proxyURL, smap, target)
		}
		waitForRebalanceToComplete(t, baseParams, rebalanceTimeout)
	}()

	hasTarget := func(targets []*cluster.Snode, target *cluster.Snode) bool {
		for _, tr := range targets {
			if tr.DaemonID == target.DaemonID {
				return true
			}
		}
		return false
	}

	getOneObj := func(objName string) {
		defer wg.Done()

		// hack: calculate which targets stored a replica
		targets, errstr := cluster.HrwTargetList(bucket, ecTestDir+objName, &initialSmap, ecParitySliceCnt+1)
		tassert.Errorf(t, errstr == "", errstr)

		mainTarget := targets[0]
		targets = targets[1:]

		replicas, _ := ecGetAllLocalSlices(t, objName, bucket)
		// HACK: this tells directory of target based on last number of it's port
		// This is usually true, but undefined if target has > 9 nodes
		// as the last digit becomes ambiguous
		targetDir := mainTarget.DaemonID[len(mainTarget.DaemonID)-1]

		for p := range replicas {
			if strings.Contains(p, path.Join(rootDir, string(targetDir))) {
				// Delete the actual main object
				// NOTE: this might fail if the targetDir is not calculated correctly
				tassert.CheckFatal(t, os.Remove(p))
				break
			}
		}

		for _, target := range targets {
			if !hasTarget(removedTargets, target) {
				// there exists a target which was not killed and stores replica
				objPath := ecTestDir + objName
				_, err := api.GetObject(baseParams, bucket, objPath)
				tassert.CheckFatal(t, err)
				return
			}
		}
	}

	tutils.Logln("Reading all objects...")
	wg.Add(numFiles)
	for i := 0; i < numFiles; i++ {
		objName := fmt.Sprintf(objPatt, i)
		go getOneObj(objName)
	}
	wg.Wait()

	// it is OK to have some Del failed with "object not found" because
	// some targets are still dead at this point
	clearAllECObjects(t, numFiles, bucket, objPatt, false)
}

// Lost mountpah test:
// - puts some objects
// - disable a random mountpath
// - gets all objects
// - nothing must fail
// - enable the mountpath back
func TestECEmergencyMpath(t *testing.T) {
	const (
		objPatt    = "obj-em-mpath-%04d"
		numFiles   = 400
		smallEvery = 4
		concurr    = 24
	)

	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		bucket     = TestLocalBucketName
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.BaseAPIParams(proxyURL)
		semaphore  = make(chan struct{}, concurr) // concurrent EC jobs at a time
	)

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap))

	removeTarget := tutils.ExtractTargetNodes(smap)[0]
	tgtParams := tutils.BaseAPIParams(removeTarget.URL(cmn.NetworkPublic))

	mpathList, err := api.GetMountpaths(tgtParams)
	tassert.CheckFatal(t, err)
	if len(mpathList.Available) < 2 {
		t.Fatalf("%s requires 2 or more mountpaths", t.Name())
	}

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	fullPath := fmt.Sprintf("local/%s/%s", bucket, ecTestDir)
	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))

	bckProps := defaultECBckProps()
	newLocalBckWithProps(t, bucket, bckProps, seed, concurr, baseParams)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

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
		r, err := tutils.NewRandReader(objSize, false)
		tassert.CheckFatal(t, err)
		defer r.Close()
		putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: bucket, Object: objPath, Reader: r}
		err = api.PutObject(putArgs)
		tassert.CheckFatal(t, err)

		foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, objName, bucket)

		ecCheckSlices(t, foundParts, fullPath+objName,
			objSize, sliceSize, totalCnt)
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
	tassert.CheckFatal(t, err)
	defer func() {
		// Enable mountpah
		tutils.Logf("Enabling mountpath %s at target %s...\n", removeMpath, removeTarget.DaemonID)
		err = api.EnableMountpath(tgtParams, removeMpath)
		tassert.CheckFatal(t, err)
	}()

	// 3. Read objects
	objectsExist(t, baseParams, bucket, objPatt, numFiles)

	// 4. Check that ListBucket returns correct number of items
	tutils.Logf("DONE\nReading bucket list...\n")
	var msg = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status,version"}
	reslist, err := api.ListBucket(baseParams, bucket, msg, 0)
	tassert.CheckFatal(t, err)

	reslist.Entries = filterObjListOK(reslist.Entries)
	if len(reslist.Entries) != numFiles {
		t.Fatalf("Invalid number of objects: %d, expected %d", len(reslist.Entries), numFiles)
	}
}

func init() {
	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	cmn.GCO.CommitUpdate(config)

	fs.InitMountedFS()
	fs.Mountpaths.DisableFsIDCheck()
	targetDirs, _ := ioutil.ReadDir(rootDir)

	for _, tDir := range targetDirs {
		dir := path.Join(rootDir, tDir.Name())
		mpDirs, _ := ioutil.ReadDir(dir)

		for _, mpDir := range mpDirs {
			if mpDir.Name() != "log" {
				_ = fs.Mountpaths.Add(path.Join(dir, mpDir.Name()))
			}
		}
	}

	_ = fs.CSM.RegisterFileType(fs.ObjectType, &fs.ObjectContentResolver{})
	_ = fs.CSM.RegisterFileType(fs.WorkfileType, &fs.WorkfileContentResolver{})
	_ = fs.CSM.RegisterFileType(ec.SliceType, &ec.SliceSpec{})
	_ = fs.CSM.RegisterFileType(ec.MetaType, &ec.MetaSpec{})
}
