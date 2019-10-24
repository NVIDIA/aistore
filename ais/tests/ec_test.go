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
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

const (
	ecSliceDir = "/" + ec.SliceType + "/"
	ecMetaDir  = "/" + ec.MetaType + "/"
	ecDataDir  = "/" + fs.ObjectType + "/"
	ecWorkDir  = "/" + fs.WorkfileType + "/"
	ecLogDir   = "/log/"   // TODO -- FIXME: hardcoding is illegal, use fs.MakePath*
	ecLocalDir = "/local/" // TODO -- FIXME: ditto
	ecCloudDir = "/cloud/" // TODO -- FIXME: ditto
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

type ecOptions struct {
	seed      int64
	objSize   int64
	concurr   int
	objCount  int
	dataCnt   int
	parityCnt int
	pattern   string
	sema      chan struct{}
	isAIS     bool
	silent    bool
	rnd       *rand.Rand
}

func (o ecOptions) init() *ecOptions {
	if o.concurr > 0 {
		o.sema = make(chan struct{}, o.concurr)
	}
	o.seed = time.Now().UnixNano()
	o.rnd = rand.New(rand.NewSource(o.seed))
	return &o
}
func (o *ecOptions) sliceTotal() int {
	return o.dataCnt + o.parityCnt
}

// var (
// 	ecDataSliceCnt   = 2
// 	ecParitySliceCnt = 2
// )

func defaultECBckProps(o *ecOptions) cmn.BucketProps {
	return cmn.BucketProps{
		Cksum: cmn.CksumConf{Type: "inherit"},
		EC: cmn.ECConf{
			Enabled:      true,
			ObjSizeLimit: ecObjLimit,
			DataSlices:   o.dataCnt,
			ParitySlices: o.parityCnt,
		},
	}
}

func ecSliceNumInit(t *testing.T, smap *cluster.Smap, o *ecOptions) error {
	tCnt := smap.CountTargets()
	if tCnt < 4 {
		return fmt.Errorf("%s requires at least 4 targets", t.Name())
	}

	if tCnt == 4 {
		o.dataCnt = 1
		o.parityCnt = 1
		return nil
	} else if tCnt == 5 {
		o.dataCnt = 1
		o.parityCnt = 2
		return nil
	}

	o.dataCnt = 2
	o.parityCnt = 2
	return nil
}

// Since all replicas are identical, it is difficult to differentiate main one from others.
// The main replica is the replica that is on the target chosen by proxy using HrwTarget
// algorithm on GET request from a client.
// The function uses heuristics to detect the main one: it should be the oldest
func ecGetAllSlices(t *testing.T, objName, bucketName string, o *ecOptions) (map[string]ecSliceMD, string) {
	tMock := cluster.NewTargetMock(cluster.NewBaseBownerMock(bucketName))
	foundParts := make(map[string]ecSliceMD)
	oldest := time.Now().Add(time.Hour)
	main := ""
	noObjCnt := 0
	bckTypeDir := ecLocalDir
	bckType := cmn.AIS

	if !o.isAIS {
		bckTypeDir = ecCloudDir
		bckType = cmn.Cloud
	}

	//
	// FIXME -- TODO: must be redone using fs.MakePath and friends
	//
	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return nil
		}
		if info.IsDir() ||
			strings.Contains(path, ecLogDir) ||
			!strings.Contains(path, bckTypeDir) ||
			strings.Contains(path, ecWorkDir) {
			return nil
		}

		if strings.Contains(path, objName) {
			sliceLom := &cluster.LOM{T: tMock, FQN: path}
			err := sliceLom.Init("", bckType)
			if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); !ok {
				tassert.CheckFatal(t, err)
			}
			err = sliceLom.Load(false)

			var cksumVal string
			if strings.Contains(path, ecMetaDir) && err != nil {
				// metafile of the original object on the main target doesn't have meta saved on disk
				noObjCnt++
			} else if !strings.Contains(path, ecMetaDir) {
				// metafiles contain checksum inside but don't have a checksum in a lom
				if sliceLom.Cksum() != nil {
					_, cksumVal = sliceLom.Cksum().Get()
				}
			}

			foundParts[path] = ecSliceMD{info.Size(), cksumVal}
			if strings.Contains(path, ecDataDir) && oldest.After(info.ModTime()) {
				main = path
				oldest = info.ModTime()
			}
		}

		return nil
	}

	filepath.Walk(rootDir, fsWalkFunc)

	if noObjCnt > 1 {
		tutils.Logf("meta not found for %d files matching object name %s\n", noObjCnt, objName)
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
	totalCnt int) {
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
			tassert.Errorf(t, md.size == objSize, "%q size mismatch: got %d, expected %d", k, md.size, objSize)
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
}

func waitForECFinishes(t *testing.T, totalCnt int, objSize, sliceSize int64, doEC bool,
	objName, bckName string, o *ecOptions) (
	foundParts map[string]ecSliceMD, mainObjPath string) {
	deadLine := time.Now().Add(ECPutTimeOut)
	for time.Now().Before(deadLine) {
		foundParts, mainObjPath = ecGetAllSlices(t, objName, bckName, o)
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
func randObjectSize(n, every int, o *ecOptions) (
	totalCnt int, objSize, sliceSize int64, doEC bool) {
	if o.objSize != 0 {
		doEC = o.objSize >= ecObjLimit
		objSize = o.objSize
		if doEC {
			totalCnt = 2 + (o.sliceTotal())*2
			sliceSize = ec.SliceSize(objSize, o.dataCnt)
		} else {
			totalCnt = 2 + o.parityCnt*2
			sliceSize = objSize
		}
		return
	}

	// Big object case
	// full object copy+meta: 1+1
	// number of metafiles: parity+slices
	// number of slices: slices+parity
	totalCnt = 2 + (o.sliceTotal())*2
	objSize = int64(ecMinBigSize + o.rnd.Intn(ecBigDelta))
	sliceSize = ec.SliceSize(objSize, o.dataCnt)
	if (n+1)%every == 0 {
		// Small object case
		// full object copy+meta: 1+1
		// number of metafiles: parity
		// number of slices: parity
		totalCnt = 2 + o.parityCnt*2
		objSize = int64(ecMinSmallSize + o.rnd.Intn(ecSmallDelta))
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

func doECPutsAndCheck(t *testing.T, bckName string, baseParams *api.BaseParams, o *ecOptions) {
	const (
		smallEvery = 10 // Every N-th object is small
		objPatt    = "obj-%s-%04d"
	)

	wg := &sync.WaitGroup{}
	sizes := make(chan int64, o.objCount)

	for idx := 0; idx < o.objCount; idx++ {
		wg.Add(1)
		o.sema <- struct{}{}

		go func(i int) {
			totalCnt, objSize, sliceSize, doEC := randObjectSize(i, smallEvery, o)
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
				<-o.sema
				wg.Done()
			}()
			tassert.CheckFatal(t, err)
			putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: bckName, Object: objPath, Reader: r}
			err = api.PutObject(putArgs)
			tassert.CheckFatal(t, err)

			fullPath := fmt.Sprintf("local/%s/%s", bckName, ecTestDir)
			foundParts, _ := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, objName, bckName, o)
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
					tassert.Errorf(t, md.size == objSize, "%q size mismatch: got %d, expected %d", k, md.size, objSize)
					mainObjPath = k
					replCnt++
				}
			}

			metaCntMust := o.parityCnt + 1
			if doEC {
				metaCntMust = o.sliceTotal() + 1
			}
			tassert.Errorf(t, metaCnt == metaCntMust, "Number of metafiles mismatch: %d, expected %d", metaCnt, o.parityCnt+1)
			if doEC {
				tassert.Errorf(t, sliceCnt == o.sliceTotal(), "Number of chunks mismatch: %d, expected %d", sliceCnt, o.sliceTotal())
			} else {
				tassert.Errorf(t, replCnt == o.parityCnt+1, "Number replicas mismatch: %d, expected %d", replCnt, o.parityCnt)
			}

			if mainObjPath == "" {
				t.Errorf("Full copy is not found")
				return
			}

			tassert.CheckFatal(t, os.Remove(mainObjPath))
			partsAfterRemove, _ := ecGetAllSlices(t, objName, bckName, o)
			_, ok := partsAfterRemove[mainObjPath]
			if ok || len(partsAfterRemove) >= len(foundParts) {
				t.Errorf("Object is not deleted: %#v", partsAfterRemove)
				return
			}

			_, err = api.GetObject(baseParams, bckName, objPath)
			tassert.CheckFatal(t, err)

			if doEC {
				partsAfterRestore, _ := ecGetAllSlices(t, objName, bckName, o)
				md, ok := partsAfterRestore[mainObjPath]
				if !ok || len(partsAfterRestore) != len(foundParts) {
					t.Errorf("Object is not restored: %#v", partsAfterRestore)
					return
				}

				if md.size != objSize {
					t.Errorf("Object is restored incorrectly, size mismatches: %d, expected %d", md.size, objSize)
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
		t.Logf("Average size of the bucket %s: %s\n", bckName, cmn.B2S(szTotal/int64(szLen), 1))
	}
}

func assertBucketSize(t *testing.T, baseParams *api.BaseParams, bckName string, objCount int) {
	bckObjectsCnt := bucketSize(t, baseParams, bckName)
	tassert.Fatalf(t, bckObjectsCnt == objCount, "Invalid number of objects: %d, expected %d", bckObjectsCnt, objCount)
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

func newLocalBckWithProps(t *testing.T, name string, bckProps cmn.BucketProps, baseParams *api.BaseParams, o *ecOptions) {
	tutils.CreateFreshBucket(t, proxyURLReadOnly, name)

	tutils.Logf("Changing EC %d:%d [ seed = %d ], concurrent: %d\n",
		o.dataCnt, o.parityCnt, o.seed, o.concurr)
	err := api.SetBucketPropsMsg(baseParams, name, bckProps)

	if err != nil {
		tutils.DestroyBucket(t, proxyURLReadOnly, name)
	}
	tassert.CheckFatal(t, err)
}

func setBucketECProps(t *testing.T, name string, bckProps cmn.BucketProps, baseParams *api.BaseParams) {
	tutils.Logf("Changing EC %d:%d\n", bckProps.EC.DataSlices, bckProps.EC.ParitySlices)
	err := api.SetBucketPropsMsg(baseParams, name, bckProps)
	tassert.CheckFatal(t, err)
}

func clearAllECObjects(t *testing.T, bucket string, failOnDelErr bool, o *ecOptions) {
	tutils.Logln("Deleting objects...")
	wg := sync.WaitGroup{}

	wg.Add(o.objCount)
	for idx := 0; idx < o.objCount; idx++ {
		go func(i int) {
			defer wg.Done()
			objName := fmt.Sprintf(o.pattern, i)
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
				partsAfterDelete, _ := ecGetAllSlices(t, objName, bucket, o)
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

func objectsExist(t *testing.T, baseParams *api.BaseParams, bckName, objPatt string, objCount int) {
	wg := &sync.WaitGroup{}
	getOneObj := func(objName string) {
		defer wg.Done()
		objPath := ecTestDir + objName
		_, err := api.GetObject(baseParams, bckName, objPath)
		tassert.CheckFatal(t, err)
	}

	tutils.Logln("Reading all objects...")
	wg.Add(objCount)
	for i := 0; i < objCount; i++ {
		objName := fmt.Sprintf(objPatt, i)
		go getOneObj(objName)
	}
	wg.Wait()
}

// Short test to make sure that EC options cannot be changed after
// EC is enabled
func TestECChange(t *testing.T) {
	var (
		bucket      = TestBucketName
		proxyURL    = getPrimaryURL(t, proxyURLReadOnly)
		bucketProps cmn.BucketProps
	)

	tutils.CreateFreshBucket(t, proxyURL, bucket)
	defer tutils.DestroyBucket(t, proxyURL, bucket)

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

func createECReplicas(t *testing.T, baseParams *api.BaseParams, bucket, objName string, fullPath string, o *ecOptions) {
	o.sema <- struct{}{}
	defer func() {
		<-o.sema
	}()

	totalCnt := 2 + o.parityCnt*2
	objSize := int64(ecMinSmallSize + o.rnd.Intn(ecSmallDelta))
	sliceSize := objSize

	objPath := ecTestDir + objName

	tutils.Logf("Creating %s, size %8d\n", objPath, objSize)
	r, err := tutils.NewRandReader(objSize, false)
	tassert.CheckFatal(t, err)
	defer r.Close()

	putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: bucket, Object: objPath, Reader: r}
	err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)

	tutils.Logf("waiting for %s\n", objPath)
	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, false, objName, bucket, o)

	ecCheckSlices(t, foundParts, fullPath+objName, objSize, sliceSize, totalCnt)
	tassert.Errorf(t, mainObjPath != "", "Full copy is not found")
}

func createECObject(t *testing.T, baseParams *api.BaseParams, bucket, objName string, idx int, fullPath string, o *ecOptions) int {
	const (
		smallEvery = 7 // Every N-th object is small
	)

	o.sema <- struct{}{}
	defer func() {
		<-o.sema
	}()

	totalCnt, objSize, sliceSize, doEC := randObjectSize(idx, smallEvery, o)
	objPath := ecTestDir + objName
	ecStr := "-"
	if doEC {
		ecStr = "EC"
	}
	if !o.silent {
		tutils.Logf("Creating %s, size %8d [%2s]\n", objPath, objSize, ecStr)
	}
	r, err := tutils.NewRandReader(objSize, false)
	tassert.CheckFatal(t, err)
	defer r.Close()

	putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: bucket, Object: objPath, Reader: r}
	err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)

	if !o.silent {
		tutils.Logf("waiting for %s\n", objPath)
	}
	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, objName, bucket, o)

	ecCheckSlices(t, foundParts, fullPath+objName, objSize, sliceSize, totalCnt)
	if mainObjPath == "" {
		t.Errorf("Full copy is not found")
	}
	return totalCnt
}

func createDamageRestoreECFile(t *testing.T, baseParams *api.BaseParams, bucket, objName string, idx int, fullPath string, o *ecOptions) {
	const (
		sleepRestoreTime = 5 * time.Second // wait time after GET restores slices
		smallEvery       = 7               // Every N-th object is small
		sliceDelPct      = 50              // %% of objects that have damaged body and a slice
	)

	o.sema <- struct{}{}
	defer func() {
		<-o.sema
	}()

	delSlice := false // delete only main object
	deletedFiles := 1
	if o.dataCnt+o.parityCnt > 2 && o.rnd.Intn(100) < sliceDelPct {
		// delete a random slice, too
		delSlice = true
		deletedFiles = 2
	}

	totalCnt, objSize, sliceSize, doEC := randObjectSize(idx, smallEvery, o)
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

	tutils.Logf("waiting for %s\n", objPath)
	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, objName, bucket, o)

	ecCheckSlices(t, foundParts, fullPath+objName, objSize, sliceSize, totalCnt)
	if mainObjPath == "" {
		t.Errorf("Full copy is not found")
		return
	}

	tutils.Logf("Damaging %s [removing %s]\n", objPath, mainObjPath)
	tassert.CheckFatal(t, os.Remove(mainObjPath))
	metafile := strings.ReplaceAll(mainObjPath, ecDataDir, ecMetaDir)
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
			metafile := strings.ReplaceAll(sliceToDel, ecSliceDir, ecMetaDir)
			tutils.Logf("Removing slice meta %s\n", metafile)
			tassert.CheckFatal(t, os.Remove(metafile))
		} else {
			metafile := strings.ReplaceAll(sliceToDel, ecDataDir, ecMetaDir)
			tutils.Logf("Removing replica meta %s\n", metafile)
			tassert.CheckFatal(t, os.Remove(metafile))
		}
	}

	partsAfterRemove, _ := ecGetAllSlices(t, objName, bucket, o)
	_, ok := partsAfterRemove[mainObjPath]
	if ok || len(partsAfterRemove) != len(foundParts)-deletedFiles*2 {
		t.Errorf("Files are not deleted [%d - %d]: %#v", len(foundParts), len(partsAfterRemove), partsAfterRemove)
		return
	}

	tutils.Logf("Restoring %s\n", objPath)
	_, err = api.GetObject(baseParams, bucket, objPath)
	tassert.CheckFatal(t, err)

	// For cloud buckets, due to performance reason, GFN is not used and
	// EC is not called - object is reread from the cloud bucket instead
	if doEC && o.isAIS {
		deadline := time.Now().Add(sleepRestoreTime)
		var partsAfterRestore map[string]ecSliceMD
		for time.Now().Before(deadline) {
			time.Sleep(time.Millisecond * 250)
			partsAfterRestore, _ = ecGetAllSlices(t, objName, bucket, o)
			if len(partsAfterRestore) == totalCnt {
				break
			}
		}
		ecCheckSlices(t, partsAfterRestore, fullPath+objName, objSize, sliceSize, totalCnt)
	}
}

// Simple stress testing EC for Cloud buckets
func TestECRestoreObjAndSliceCloud(t *testing.T) {
	var (
		bucket   = clibucket
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)

	o := ecOptions{
		objCount: 200,
		concurr:  8,
		pattern:  "obj-rest-cloud-%04d",
		isAIS:    false,
	}.init()

	if !isCloudBucket(t, proxyURL, bucket) {
		t.Skip("test requires a cloud bucket")
	}

	smap := getClusterMap(t, proxyURL)
	err := ecSliceNumInit(t, smap, o)
	tassert.CheckFatal(t, err)

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	fullPath := fmt.Sprintf("cloud/%s/%s", bucket, ecTestDir)
	baseParams := tutils.BaseAPIParams(proxyURL)

	origProps, err := api.HeadBucket(baseParams, bucket)
	tassert.CheckFatal(t, err)
	setBucketECProps(t, bucket, defaultECBckProps(o), baseParams)
	defer setBucketECProps(t, bucket, *origProps, baseParams)

	wg := sync.WaitGroup{}

	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go func(i int) {
			defer wg.Done()
			createDamageRestoreECFile(t, baseParams, bucket, objName, i, fullPath, o)
		}(i)
	}
	wg.Wait()
	defer clearAllECObjects(t, bucket, true, o)

	if t.Failed() {
		t.FailNow()
	}
}

// Quick check that EC can restore a damaged object and a missing slice
//  - PUTs an object to the bucket
//  - filepath.Walk checks that the number of metafiles and slices are correct
//  - Either original object or original object and a random slice are deleted
//  - GET should detect that original object is gone
//  - The target restores the original object from slices and missing slices
func TestECRestoreObjAndSlice(t *testing.T) {
	var (
		bucket   = TestBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)

	o := ecOptions{
		objCount: 50,
		concurr:  8,
		pattern:  "obj-rest-%04d",
		isAIS:    true,
	}.init()

	smap := getClusterMap(t, proxyURL)
	err := ecSliceNumInit(t, smap, o)
	tassert.CheckFatal(t, err)

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	fullPath := fmt.Sprintf("local/%s/%s", bucket, ecTestDir)

	baseParams := tutils.BaseAPIParams(proxyURL)

	newLocalBckWithProps(t, bucket, defaultECBckProps(o), baseParams, o)
	defer tutils.DestroyBucket(t, proxyURL, bucket)

	wg := sync.WaitGroup{}

	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go func(i int) {
			defer wg.Done()
			createDamageRestoreECFile(t, baseParams, bucket, objName, i, fullPath, o)
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	assertBucketSize(t, baseParams, bucket, o.objCount)
	clearAllECObjects(t, bucket, true, o)
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
func createECFile(t *testing.T, bucket, objName, fullPath string, baseParams *api.BaseParams, o *ecOptions) (map[string]ecSliceMD, string) {
	totalCnt := 2 + (o.sliceTotal())*2
	objSize := int64(ecMinBigSize * 2)
	sliceSize := ec.SliceSize(objSize, o.dataCnt)

	err := putECFile(baseParams, bucket, objName)
	tassert.CheckFatal(t, err)

	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, true, objName, bucket, o)
	tassert.Fatalf(t, mainObjPath != "", "Full copy %s was not found", mainObjPath)

	ecCheckSlices(t, foundParts, fullPath+objName, objSize, sliceSize, totalCnt)

	return foundParts, mainObjPath
}

// Creates 2 EC files and then corrupts their slices
// Checks that after corrupting one slice it is still possible to recover an object
// Checks that after corrupting all slices it is not possible to recover an object
func TestECChecksum(t *testing.T) {
	if containers.DockerRunning() {
		t.Skip(fmt.Sprintf("test %q requires Xattributes to be set, doesn't work with docker", t.Name()))
	}

	var (
		bucket   = TestBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		tMock    = cluster.NewTargetMock(cluster.NewBaseBownerMock(TestBucketName))
	)

	o := ecOptions{
		pattern: "obj-cksum-%04d",
		isAIS:   true,
	}.init()

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap, o))

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	fullPath := fmt.Sprintf("local/%s/%s", bucket, ecTestDir)
	baseParams := tutils.BaseAPIParams(proxyURL)

	newLocalBckWithProps(t, bucket, defaultECBckProps(o), baseParams, o)
	defer tutils.DestroyBucket(t, proxyURL, bucket)

	objName1 := fmt.Sprintf(o.pattern, 1)
	objPath1 := ecTestDir + objName1
	foundParts1, mainObjPath1 := createECFile(t, bucket, objName1, fullPath, baseParams, o)

	objName2 := fmt.Sprintf(o.pattern, 2)
	objPath2 := ecTestDir + objName2
	foundParts2, mainObjPath2 := createECFile(t, bucket, objName2, fullPath, baseParams, o)

	tutils.Logf("Removing main object %s\n", mainObjPath1)
	tassert.CheckFatal(t, os.Remove(mainObjPath1))

	// Corrupt just one slice, EC should be able to restore the original object
	for k := range foundParts1 {
		if k != mainObjPath1 && strings.Contains(k, ecSliceDir) {
			err := tutils.SetXattrCksum(k, cmn.NewCksum(cmn.ChecksumXXHash, "01234"), tMock)
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
			err := tutils.SetXattrCksum(k, cmn.NewCksum(cmn.ChecksumXXHash, "01234"), tMock)
			tassert.CheckFatal(t, err)
		}
	}

	_, err = api.GetObject(baseParams, bucket, objPath2)
	tassert.Fatalf(t, err != nil, "Object should not be restored when checksums are wrong")
}

func TestECEnabledDisabledEnabled(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		bucket   = TestBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)

	o := ecOptions{
		objCount: 25,
		concurr:  8,
		pattern:  "obj-rest-%04d",
		isAIS:    true,
	}.init()

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap, o))

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	fullPath := fmt.Sprintf("local/%s/%s", bucket, ecTestDir)

	baseParams := tutils.BaseAPIParams(proxyURL)

	newLocalBckWithProps(t, bucket, defaultECBckProps(o), baseParams, o)
	defer tutils.DestroyBucket(t, proxyURL, bucket)

	// End of preparation, create files with EC enabled, check if are restored properly

	wg := sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go func(i int) {
			defer wg.Done()
			createDamageRestoreECFile(t, baseParams, bucket, objName, i, fullPath, o)
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	assertBucketSize(t, baseParams, bucket, o.objCount)

	// Disable EC, put normal files, check if were created properly
	err := api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "false"})
	tassert.CheckError(t, err)

	wg.Add(o.objCount)
	for i := o.objCount; i < 2*o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go func() {
			defer wg.Done()
			putRandomFile(t, baseParams, bucket, objName, cmn.MiB)
		}()
	}

	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	assertBucketSize(t, baseParams, bucket, o.objCount*2)

	// Enable EC again, check if EC was started properly and creates files with EC correctly
	err = api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "true"})
	tassert.CheckError(t, err)

	wg.Add(o.objCount)
	for i := 2 * o.objCount; i < 3*o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go func(i int) {
			defer wg.Done()
			createDamageRestoreECFile(t, baseParams, bucket, objName, i, fullPath, o)
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	assertBucketSize(t, baseParams, bucket, o.objCount*3)
}

func TestECDisableEnableDuringLoad(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		bucket   = TestBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)

	o := ecOptions{
		objCount: 5,
		concurr:  8,
		pattern:  "obj-disable-enable-load-%04d",
		isAIS:    true,
	}.init()

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap, o))

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	fullPath := fmt.Sprintf("local/%s/%s", bucket, ecTestDir)

	baseParams := tutils.BaseAPIParams(proxyURL)

	newLocalBckWithProps(t, bucket, defaultECBckProps(o), baseParams, o)
	defer tutils.DestroyBucket(t, proxyURL, bucket)

	// End of preparation, create files with EC enabled, check if are restored properly

	wg := &sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go func(i int) {
			defer wg.Done()
			createDamageRestoreECFile(t, baseParams, bucket, objName, i, fullPath, o)
		}(i)
	}
	wg.Wait()

	assertBucketSize(t, baseParams, bucket, o.objCount)

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
				objName := fmt.Sprintf(o.pattern, o.objCount+numCreated)
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

	assertBucketSize(t, baseParams, bucket, o.objCount+numCreated)

	if t.Failed() {
		t.FailNow()
	}

	wg = &sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		j := i + o.objCount + numCreated
		objName := fmt.Sprintf(o.pattern, j)

		go func(i int) {
			defer wg.Done()
			createDamageRestoreECFile(t, baseParams, bucket, objName, i, fullPath, o)
		}(j)
	}

	wg.Wait()

	// Disabling and enabling EC should not result in put's failing
	assertBucketSize(t, baseParams, bucket, 2*o.objCount+numCreated)
}

// Stress test to check that EC works as expected.
//  - Changes bucket props to use EC
//  - Generates `objCount` objects, size between `ecObjMinSize` and `ecObjMinSize`+ecObjMaxSize`
//  - Objects smaller `ecObjLimit` must be copies, while others must be EC'ed
//  - PUTs objects to the bucket
//  - filepath.Walk checks that the number of metafiles and slices are correct
//  - The original object is deleted
//  - GET should detect that original object is gone and that there are EC slices
//  - The target restores the original object from slices/copies and returns it
//  - No errors must occur
func TestECStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Long run only")
	}

	var (
		bucket   = TestBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)

	o := ecOptions{
		objCount: 400,
		concurr:  12,
		pattern:  "obj-stress-%04d",
		isAIS:    true,
	}.init()

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap, o))

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	baseParams := tutils.BaseAPIParams(proxyURL)
	newLocalBckWithProps(t, bucket, defaultECBckProps(o), baseParams, o)

	defer tutils.DestroyBucket(t, proxyURL, bucket)

	doECPutsAndCheck(t, bucket, baseParams, o)

	var msg = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status"}
	reslist, err := api.ListBucket(baseParams, bucket, msg, 0)
	tassert.CheckFatal(t, err)
	reslist.Entries = filterObjListOK(reslist.Entries)

	tassert.Fatalf(t, len(reslist.Entries) == o.objCount, "Invalid number of objects: %d, expected %d", len(reslist.Entries), o.objCount)
}

// Stress 2 buckets at the same time
func TestECStressManyBuckets(t *testing.T) {
	if testing.Short() {
		t.Skip("Long run only")
	}

	const (
		bck1Name = TestBucketName + "1"
		bck2Name = TestBucketName + "2"
	)

	var proxyURL = getPrimaryURL(t, proxyURLReadOnly)

	o1 := ecOptions{
		objCount: 200,
		concurr:  12,
		pattern:  "obj-stress-manybck-%04d",
		isAIS:    true,
	}.init()
	o2 := ecOptions{
		objCount: 200,
		concurr:  12,
		pattern:  "obj-stress-manybck-%04d",
		isAIS:    true,
	}.init()

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap, o1))
	tassert.CheckFatal(t, ecSliceNumInit(t, smap, o2))

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	baseParams := tutils.BaseAPIParams(proxyURL)
	newLocalBckWithProps(t, bck1Name, defaultECBckProps(o1), baseParams, o1)
	newLocalBckWithProps(t, bck2Name, defaultECBckProps(o2), baseParams, o2)
	defer tutils.DestroyBucket(t, proxyURL, bck1Name)
	defer tutils.DestroyBucket(t, proxyURL, bck2Name)

	// Run EC on different buckets concurrently
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		doECPutsAndCheck(t, bck1Name, baseParams, o1)
	}()
	go func() {
		defer wg.Done()
		doECPutsAndCheck(t, bck2Name, baseParams, o2)
	}()
	wg.Wait()

	var msg = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status"}
	reslist, err := api.ListBucket(baseParams, bck1Name, msg, 0)
	tassert.CheckFatal(t, err)
	reslist.Entries = filterObjListOK(reslist.Entries)

	tassert.Fatalf(t, len(reslist.Entries) == o1.objCount, "Bucket %s: Invalid number of objects: %d, expected %d", bck1Name, len(reslist.Entries), o1.objCount)

	msg = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status"}
	reslist, err = api.ListBucket(baseParams, bck2Name, msg, 0)
	tassert.CheckFatal(t, err)
	reslist.Entries = filterObjListOK(reslist.Entries)

	tassert.Fatalf(t, len(reslist.Entries) == o2.objCount, "Bucket %s: Invalid number of objects: %d, expected %d", bck2Name, len(reslist.Entries), o2.objCount)
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
		smallEvery = 7
	)

	var (
		bucket      = TestBucketName
		proxyURL    = getPrimaryURL(t, proxyURLReadOnly)
		waitAllTime = time.Minute * 4 // should be enough for all object to complete EC
		totalSlices atomic.Int64
	)

	o := ecOptions{
		objCount: 400,
		concurr:  12,
		pattern:  objStart + "%04d",
		isAIS:    true,
	}.init()

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap, o))

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	baseParams := tutils.BaseAPIParams(proxyURL)

	newLocalBckWithProps(t, bucket, defaultECBckProps(o), baseParams, o)
	defer tutils.DestroyBucket(t, proxyURL, bucket)

	started := time.Now()

	type sCnt struct {
		obj string
		cnt int
	}
	cntCh := make(chan sCnt, o.objCount)
	wg := &sync.WaitGroup{}
	wg.Add(o.objCount)
	for idx := 0; idx < o.objCount; idx++ {
		o.sema <- struct{}{}

		go func(i int) {
			defer func() {
				<-o.sema
				wg.Done()
			}()

			objName := fmt.Sprintf(o.pattern, i)
			totalCnt, objSize, sliceSize, doEC := randObjectSize(i, smallEvery, o)
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
		}(idx)
	}

	wg.Wait()
	close(cntCh)

	var foundParts map[string]ecSliceMD
	startedWaiting := time.Now()
	deadLine := startedWaiting.Add(waitAllTime)
	for time.Now().Before(deadLine) {
		foundParts, _ = ecGetAllSlices(t, objStart, bucket, o)
		if len(foundParts) == int(totalSlices.Load()) {
			delta := time.Since(startedWaiting)
			t.Logf("waiting %v for EC to complete\n", delta)
			break
		}
		time.Sleep(time.Millisecond * 30)
	}
	if len(foundParts) != int(totalSlices.Load()) {
		slices := make(map[string]int, o.objCount)
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

	tassert.Fatalf(t, len(reslist.Entries) == o.objCount, "Invalid number of objects: %d, expected %d", len(reslist.Entries), o.objCount)
}

// Quick check that EC keeps xattrs:
// - enable EC and versioning for the bucket
// - put/damage/restore one by one a few object two times to increase their versions
// - get the list of the objects at the end and check that they all have the correct versions
func TestECXattrs(t *testing.T) {
	const (
		sliceDelPct      = 50              // %% of objects that have damaged body and a slice
		sleepRestoreTime = time.Second * 5 // wait time after GET restores slices
		finalVersion     = "2"
		smallEvery       = 4
	)

	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		bucket   = TestBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)

	o := ecOptions{
		objCount: 30,
		concurr:  8,
		pattern:  "obj-xattr-%04d",
		isAIS:    true,
	}.init()

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap, o))

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()
	fullPath := fmt.Sprintf("local/%s/%s", bucket, ecTestDir)

	baseParams := tutils.BaseAPIParams(proxyURL)
	bckProps := defaultECBckProps(o)
	bckProps.Versioning.Type = cmn.PropOwn
	bckProps.Versioning.Enabled = true

	newLocalBckWithProps(t, bucket, bckProps, baseParams, o)
	defer tutils.DestroyBucket(t, proxyURL, bucket)

	oneObj := func(idx int, objName string) {
		delSlice := false // delete only main object
		deletedFiles := 1
		if o.sliceTotal() > 2 && o.rnd.Intn(100) < sliceDelPct {
			// delete a random slice, too
			delSlice = true
			deletedFiles = 2
		}

		totalCnt, objSize, sliceSize, doEC := randObjectSize(idx, smallEvery, o)
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

		tutils.Logf("waiting for %s\n", objPath)
		foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, objName, bucket, o)

		ecCheckSlices(t, foundParts, fullPath+objName, objSize, sliceSize, totalCnt)
		if mainObjPath == "" {
			t.Fatalf("Full copy is not found")
		}

		tutils.Logf("Damaging %s [removing %s]\n", objPath, mainObjPath)
		tassert.CheckFatal(t, os.Remove(mainObjPath))
		metafile := strings.ReplaceAll(mainObjPath, ecDataDir, ecMetaDir)
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
				metafile := strings.ReplaceAll(sliceToDel, ecSliceDir, ecMetaDir)
				tutils.Logf("Removing slice meta %s\n", metafile)
				tassert.CheckFatal(t, os.Remove(metafile))
			} else {
				metafile := strings.ReplaceAll(sliceToDel, ecDataDir, ecMetaDir)
				tutils.Logf("Removing replica meta %s\n", metafile)
				tassert.CheckFatal(t, os.Remove(metafile))
			}
		}

		partsAfterRemove, _ := ecGetAllSlices(t, objName, bucket, o)
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
				partsAfterRestore, _ = ecGetAllSlices(t, objName, bucket, o)
				if len(partsAfterRestore) == totalCnt {
					break
				}
			}
			ecCheckSlices(t, partsAfterRestore, fullPath+objName, objSize, sliceSize, totalCnt)
		}
	}

	// PUT objects twice to make their version 2
	for j := 0; j < 2; j++ {
		for i := 0; i < o.objCount; i++ {
			objName := fmt.Sprintf(o.pattern, i)
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
	if len(reslist.Entries) != o.objCount {
		t.Fatalf("Invalid number of objects: %d, expected %d", len(reslist.Entries), 1)
	}

	clearAllECObjects(t, bucket, true, o)
}

// 1. start putting EC files into the cluster
// 2. in the middle of puts destroy bucket
// 3. wait for puts to finish
// 4. create bucket with the same name
// 5. check that EC is working properly for this bucket
func TestECDestroyBucket(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		bucket   = TestBucketName + "-DESTROY"
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)

	o := ecOptions{
		objCount: 100,
		concurr:  10,
		pattern:  "obj-destroy-bck-%04d",
		isAIS:    true,
	}.init()

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap, o))

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	baseParams := tutils.BaseAPIParams(proxyURL)
	bckProps := defaultECBckProps(o)
	newLocalBckWithProps(t, bucket, bckProps, baseParams, o)

	wg := &sync.WaitGroup{}
	errCnt := atomic.NewInt64(0)
	sucCnt := atomic.NewInt64(0)

	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)

		o.sema <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer func() {
				<-o.sema
				wg.Done()
			}()

			if i%10 == 0 {
				tutils.Logf("ec object %s into bucket %s\n", objName, bucket)
			}
			if putECFile(baseParams, bucket, objName) != nil {
				errCnt.Inc()
			} else {
				sucCnt.Inc()
			}
		}(i)

		if i == 4*o.objCount/5 {
			// DestroyBucket when put requests are still executing
			o.sema <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() {
					<-o.sema
					wg.Done()
				}()

				tutils.Logf("Destroying bucket %s\n", bucket)
				tutils.DestroyBucket(t, proxyURL, bucket)
			}()
		}
	}

	wg.Wait()
	tutils.Logf("EC put files resulted in error in %d out of %d files\n", errCnt.Load(), o.objCount)

	// create bucket with the same name and check if puts are successful
	newLocalBckWithProps(t, bucket, bckProps, baseParams, o)
	defer tutils.DestroyBucket(t, proxyURL, bucket)
	doECPutsAndCheck(t, bucket, baseParams, o)

	// check if get requests are successful
	var msg = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status,version"}
	reslist, err := api.ListBucket(baseParams, bucket, msg, 0)
	tassert.CheckError(t, err)

	reslist.Entries = filterObjListOK(reslist.Entries)
	tassert.Errorf(t, len(reslist.Entries) == o.objCount, "Invalid number of objects: %d, expected %d", len(reslist.Entries), o.objCount)
}

// Lost target test:
// - puts some objects
// - kills a random target
// - gets all objects
// - nothing must fail
// - register the target back
func TestECEmergencyTargetForSlices(t *testing.T) {
	const (
		smallEvery = 4
	)

	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		bucket   = TestBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)

	o := ecOptions{
		objCount: 100,
		concurr:  12,
		pattern:  "obj-emt-%04d",
		isAIS:    true,
	}.init()

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap, o))

	// Increase number of EC data slices, now there's just enough targets to handle EC requests
	// Encoding will fail if even one is missing, restoring should still work
	o.dataCnt++

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	fullPath := fmt.Sprintf("local/%s/%s", bucket, ecTestDir)
	baseParams := tutils.BaseAPIParams(proxyURL)
	bckProps := defaultECBckProps(o)
	newLocalBckWithProps(t, bucket, bckProps, baseParams, o)
	defer tutils.DestroyBucket(t, proxyURL, bucket)

	wg := &sync.WaitGroup{}

	// 1. PUT objects
	putOneObj := func(idx int, objName string) {
		defer func() {
			wg.Done()
			<-o.sema
		}()

		start := time.Now()
		totalCnt, objSize, sliceSize, doEC := randObjectSize(idx, smallEvery, o)
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

		foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, objName, bucket, o)

		ecCheckSlices(t, foundParts, fullPath+objName, objSize, sliceSize, totalCnt)
		if mainObjPath == "" {
			t.Errorf("Full copy is not found")
			return
		}
		t.Logf("Object %s EC in %v", objName, time.Since(start))
	}

	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		o.sema <- struct{}{}
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
	objectsExist(t, baseParams, bucket, o.pattern, o.objCount)

	// 4. Check that ListBucket returns correct number of items
	tutils.Logln("DONE\nReading bucket list...")
	var msg = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status,version"}
	reslist, err := api.ListBucket(baseParams, bucket, msg, 0)
	tassert.CheckError(t, err)

	reslist.Entries = filterObjListOK(reslist.Entries)
	tassert.Errorf(t, len(reslist.Entries) == o.objCount, "Invalid number of objects: %d, expected %d", len(reslist.Entries), o.objCount)
}

func TestECEmergencyTargetForReplica(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		bucket   = TestBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)

	o := ecOptions{
		objCount: 50,
		concurr:  8,
		pattern:  "obj-rest-%04d",
		isAIS:    true,
	}.init()

	initialSmap := getClusterMap(t, proxyURL)

	if initialSmap.CountTargets() > 10 {
		// Reason: calculating main obj directory based on DeamonID
		// see getOneObj, 'HACK' annotation
		t.Skip("Test requires at most 10 targets")
	}

	tassert.CheckFatal(t, ecSliceNumInit(t, initialSmap, o))

	for _, target := range initialSmap.Tmap {
		target.Digest()
	}

	// Increase number of EC data slices, now there's just enough targets to handle EC requests
	// Encoding will fail if even one is missing, restoring should still work
	o.dataCnt++

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	fullPath := fmt.Sprintf("local/%s/%s", bucket, ecTestDir)

	baseParams := tutils.BaseAPIParams(proxyURL)
	bckProps := defaultECBckProps(o)
	newLocalBckWithProps(t, bucket, bckProps, baseParams, o)
	defer tutils.DestroyBucket(t, proxyURL, bucket)

	wg := sync.WaitGroup{}

	// PUT object
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go func() {
			defer wg.Done()
			createECReplicas(t, baseParams, bucket, objName, fullPath, o)
		}()
	}

	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	// kill #dataslices of targets, normal EC restore won't be possible
	// 2. Kill a random target
	removedTargets := make([]*cluster.Snode, 0, o.dataCnt)
	smap := getClusterMap(t, proxyURL)

	for i := o.dataCnt - 1; i >= 0; i-- {
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
		targets, err := cluster.HrwTargetList(bucket, ecTestDir+objName, initialSmap, o.parityCnt+1)
		tassert.CheckFatal(t, err)

		mainTarget := targets[0]
		targets = targets[1:]

		replicas, _ := ecGetAllSlices(t, objName, bucket, o)
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
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go getOneObj(objName)
	}
	wg.Wait()

	// it is OK to have some Del failed with "object not found" because
	// some targets are still dead at this point
	clearAllECObjects(t, bucket, false, o)
}

// Lost mountpah test:
// - puts some objects
// - disable a random mountpath
// - gets all objects
// - nothing must fail
// - enable the mountpath back
func TestECEmergencyMpath(t *testing.T) {
	const (
		smallEvery = 4
	)

	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		bucket     = TestBucketName
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	o := ecOptions{
		objCount: 400,
		concurr:  24,
		pattern:  "obj-em-mpath-%04d",
		isAIS:    true,
	}.init()

	smap := getClusterMap(t, proxyURL)
	tassert.CheckFatal(t, ecSliceNumInit(t, smap, o))

	removeTarget := tutils.ExtractTargetNodes(smap)[0]
	mpathList, err := api.GetMountpaths(baseParams, removeTarget)
	tassert.CheckFatal(t, err)
	if len(mpathList.Available) < 2 {
		t.Fatalf("%s requires 2 or more mountpaths", t.Name())
	}

	sgl := tutils.Mem2.NewSGL(0)
	defer sgl.Free()

	fullPath := fmt.Sprintf("local/%s/%s", bucket, ecTestDir)
	bckProps := defaultECBckProps(o)
	newLocalBckWithProps(t, bucket, bckProps, baseParams, o)
	defer tutils.DestroyBucket(t, proxyURL, bucket)

	wg := &sync.WaitGroup{}

	// 1. PUT objects
	putOneObj := func(idx int, objName string) {
		defer func() {
			wg.Done()
			<-o.sema
		}()

		totalCnt, objSize, sliceSize, doEC := randObjectSize(idx, smallEvery, o)
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

		foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, objName, bucket, o)
		ecCheckSlices(t, foundParts, fullPath+objName, objSize, sliceSize, totalCnt)
		if mainObjPath == "" {
			t.Errorf("Full copy is not found")
			return
		}
	}

	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		o.sema <- struct{}{}
		go putOneObj(i, objName)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	// 2. Disable a random mountpath
	mpathID := o.rnd.Intn(len(mpathList.Available))
	removeMpath := mpathList.Available[mpathID]
	tutils.Logf("Disabling a mountpath %s at target: %s\n", removeMpath, removeTarget.DaemonID)
	err = api.DisableMountpath(baseParams, removeTarget.ID(), removeMpath)
	tassert.CheckFatal(t, err)
	defer func() {
		// Enable mountpah
		tutils.Logf("Enabling mountpath %s at target %s...\n", removeMpath, removeTarget.DaemonID)
		err = api.EnableMountpath(baseParams, removeTarget, removeMpath)
		tassert.CheckFatal(t, err)
	}()

	// 3. Read objects
	objectsExist(t, baseParams, bucket, o.pattern, o.objCount)

	// 4. Check that ListBucket returns correct number of items
	tutils.Logf("DONE\nReading bucket list...\n")
	var msg = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status,version"}
	reslist, err := api.ListBucket(baseParams, bucket, msg, 0)
	tassert.CheckFatal(t, err)

	reslist.Entries = filterObjListOK(reslist.Entries)
	if len(reslist.Entries) != o.objCount {
		t.Fatalf("Invalid number of objects: %d, expected %d", len(reslist.Entries), o.objCount)
	}
}

func globalRebCounters(baseParams *api.BaseParams) (int, int) {
	rebStats, err := api.MakeXactGetRequest(baseParams, cmn.ActGlobalReb, cmn.GetWhatStats, "" /* bucket */, false /* all */)
	if err != nil {
		// no xaction - all zeroes
		return 0, 0
	}
	txCount := 0
	rxCount := 0
	for _, daemonStats := range rebStats {
		for _, st := range daemonStats {
			extStats := st.Ext.(map[string]interface{})
			txCount += int(extStats[stats.TxRebCount].(float64))
			rxCount += int(extStats[stats.RxRebCount].(float64))
		}
	}
	return txCount, rxCount
}

// Until EC rebalance is done and gets ability to move and restore files, it is hard
// to check if algorithm works as expected. So, the current test is a rough checking
// that EC rebalance can do something, and it can detect how to fix broken objects.
// The test is simple: two full mountpaths are gone, and the third is renamed. Kind
// of simulation of two cases: mpath dead, and mpath dead + new mpath added. Since
// required parity is 2 or greater, EC rebalance must be able to fix all objects
// without failing any object. And since we delete 2 mpaths only, the number of
// fixed should be fairly big(now it is assumed one 15th of total slice count), and
// not too high(more than third of slices fixed is considered bad).
// Now the number of fixes and failures is returned as stats: TxRebCount & RxRebCount
func TestECRebalance(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}
	if containers.DockerRunning() {
		t.Skip(fmt.Sprintf("test %q requires direct access to filesystem, doesn't work with docker", t.Name()))
	}

	const aisDir = "local" // TODO: hardcode
	const objDir = "obj"   // TODO: hardcode
	const metaDir = "meta" // TODO: hardcode
	var (
		bucket   = TestBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)

	o := ecOptions{
		objCount:  200,
		concurr:   8,
		pattern:   "obj-reb-chk-%04d",
		isAIS:     true,
		dataCnt:   1,
		parityCnt: 2,
		silent:    true,
	}.init()

	smap := getClusterMap(t, proxyURL)
	err := ecSliceNumInit(t, smap, o)
	tassert.CheckFatal(t, err)
	if len(smap.Tmap) < 4 {
		t.Skip(fmt.Sprintf("%q requires at least 4 targets to have parity>=2, found %d", t.Name(), len(smap.Tmap)))
	}

	fullPath := fmt.Sprintf("%s/%s/%s", aisDir, bucket, ecTestDir)
	baseParams := tutils.BaseAPIParams(proxyURL)

	newLocalBckWithProps(t, bucket, defaultECBckProps(o), baseParams, o)
	defer tutils.DestroyBucket(t, proxyURL, bucket)

	var sliceTotal atomic.Int32
	wg := sync.WaitGroup{}

	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go func(i int) {
			defer wg.Done()
			total := createECObject(t, baseParams, bucket, objName, i, fullPath, o)
			// divide returned total by 2 because it includes metadata files
			sliceTotal.Add(int32(total / 2))
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	oldFixed, oldFailed := globalRebCounters(baseParams)
	tutils.Logf("%d objects created, starting global rebalance\n", o.objCount)

	// select a target that loses its mpath(simulate drive death),
	// and that has mpaths changed (simulate mpath added)
	tgtList := tutils.ExtractTargetNodes(smap)
	tgtLost, tgtSwap := tgtList[0], tgtList[1]

	lostFSList, err := api.GetMountpaths(baseParams, tgtLost)
	tassert.CheckFatal(t, err)
	if len(lostFSList.Available) < 2 {
		t.Fatalf("%s has only %d mountpaths, required 2 or more", tgtLost.DaemonID, len(lostFSList.Available))
	}
	swapFSList, err := api.GetMountpaths(baseParams, tgtSwap)
	tassert.CheckFatal(t, err)
	if len(swapFSList.Available) < 2 {
		t.Fatalf("%s has only %d mountpaths, required 2 or more", tgtSwap.DaemonID, len(swapFSList.Available))
	}

	// make troubles in mpaths
	// 1. Remove an mpath
	lostPath := filepath.Join(lostFSList.Available[0], objDir, aisDir, bucket)
	tutils.Logf("Removing mpath %q of target %s\n", lostPath, tgtLost.DaemonID)
	tutils.CheckPathExists(t, lostPath, true /*dir*/)
	tassert.CheckFatal(t, os.RemoveAll(lostPath))

	// 2. Delete one, and rename the second: simulate mpath dead + new mpath attached
	// delete obj1 & delete meta1; rename obj2 -> ob1, and meta2 -> meta1
	swapPathObj1 := filepath.Join(swapFSList.Available[0], objDir, aisDir, bucket)
	tutils.Logf("Removing mpath %q of target %s\n", swapPathObj1, tgtSwap.DaemonID)
	tutils.CheckPathExists(t, swapPathObj1, true /*dir*/)
	tassert.CheckFatal(t, os.RemoveAll(swapPathObj1))

	swapPathMeta1 := filepath.Join(swapFSList.Available[0], metaDir, aisDir, bucket)
	tutils.Logf("Removing mpath %q of target %s\n", swapPathMeta1, tgtSwap.DaemonID)
	tutils.CheckPathExists(t, swapPathMeta1, true /*dir*/)
	tassert.CheckFatal(t, os.RemoveAll(swapPathMeta1))

	swapPathObj2 := filepath.Join(swapFSList.Available[1], objDir, aisDir, bucket)
	tutils.Logf("Renaming mpath %q -> %q of target %s\n", swapPathObj2, swapPathObj1, tgtSwap.DaemonID)
	tutils.CheckPathExists(t, swapPathObj2, true /*dir*/)
	tassert.CheckFatal(t, os.Rename(swapPathObj2, swapPathObj1))

	swapPathMeta2 := filepath.Join(swapFSList.Available[1], metaDir, aisDir, bucket)
	tutils.Logf("Renaming mpath %q -> %q of target %s\n", swapPathMeta2, swapPathMeta1, tgtSwap.DaemonID)
	tutils.CheckPathExists(t, swapPathMeta2, true /*dir*/)
	tassert.CheckFatal(t, os.Rename(swapPathMeta2, swapPathMeta1))

	// Kill a random target
	var removedTarget *cluster.Snode
	smap, removedTarget = tutils.RemoveTarget(t, proxyURL, smap)
	// Initiate rebalance
	tutils.RestoreTarget(t, proxyURL, smap, removedTarget)
	waitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	newFixed, newFailed := globalRebCounters(baseParams)
	objFixed, objFailed := newFixed-oldFixed, newFailed-oldFailed
	tutils.Logf("Total number of replicas/slices is %d\n", sliceTotal.Load())
	tutils.Logf("Rebalance fixed %d objects/slices and failed to resore %d objects\n", objFixed, objFailed)
	if int32(objFixed) < sliceTotal.Load()/15 {
		t.Errorf("The number of fixed objects is too low: %d", objFixed)
	} else if int32(objFixed) > sliceTotal.Load()/3 {
		t.Errorf("The number of fixed objects is too high: %d", objFixed)
	}
	if objFailed > 0 {
		t.Errorf("Failed to fix %d objects", objFailed)
	}
}

// Simple test to check if EC correctly finds all the objects and its slices
// that will be used by rebalance
func TestECBucketEncode(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	const parityCnt = 2
	var (
		bucket   = TestBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)

	m := ioContext{
		t:               t,
		num:             50,
		numGetsEachFile: 1,
		bucket:          bucket,
		proxyURL:        proxyURL,
	}
	m.saveClusterState()
	baseParams := tutils.BaseAPIParams(proxyURL)

	if m.smap.CountTargets() < parityCnt+1 {
		t.Fatalf("Not enough targets to run %s test, must be at least %d", t.Name(), parityCnt+1)
	}

	tutils.CreateFreshBucket(t, proxyURL, bucket)
	defer tutils.DestroyBucket(t, proxyURL, bucket)

	m.puts()

	if t.Failed() {
		t.FailNow()
	}

	reslist, err := api.ListBucketFast(baseParams, bucket, nil)
	if err != nil {
		t.Fatalf("List bucket %s failed, err = %v", bucket, err)
	}
	tutils.Logf("Object count: %d\n", len(reslist.Entries))
	if len(reslist.Entries) != m.num {
		t.Fatalf("List bucket %s invalid number of files %d, expected %d", bucket, len(reslist.Entries), m.num)
	}

	var bucketProps cmn.BucketProps
	tutils.Logf("Enabling EC\n")
	bucketProps.Cksum.Type = "inherit"
	bucketProps.EC = cmn.ECConf{
		Enabled:      true,
		ObjSizeLimit: ecObjLimit,
		DataSlices:   1,
		ParitySlices: parityCnt,
	}
	err = api.SetBucketPropsMsg(baseParams, bucket, bucketProps)
	tassert.CheckFatal(t, err)

	tutils.Logf("Starting making EC\n")
	api.ECEncodeBucket(baseParams, bucket)
	waitForBucketXactionToComplete(t, cmn.ActECEncode, bucket, baseParams, rebalanceTimeout)
	reslist, err = api.ListBucketFast(baseParams, bucket, nil)
	if err != nil {
		t.Fatalf("List bucket %s failed, err = %v", bucket, err)
	}
	tutils.Logf("Object count after EC finishes: %d\n", len(reslist.Entries))
	expect := (parityCnt + 1) * m.num
	if len(reslist.Entries) != expect {
		t.Fatalf("List bucket after EC %s invalid number of files %d, expected %d", bucket, len(reslist.Entries), expect)
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
