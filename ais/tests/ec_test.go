// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/docker"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
)

const (
	ecTestDir = "ec-test/"

	ECPutTimeOut = time.Minute * 4 // maximum wait time after PUT to be sure that the object is EC'ed/replicated

	ecObjLimit     = 256 * cos.KiB
	ecMinSmallSize = 32 * cos.KiB
	ecSmallDelta   = 200 * cos.KiB
	ecMinBigSize   = ecObjLimit * 2
	ecBigDelta     = 10 * cos.MiB
)

type ecSliceMD struct {
	size int64
}

type ecOptions struct {
	seed        int64
	objSize     int64
	concurrency int
	objCount    int
	dataCnt     int
	parityCnt   int
	minTargets  int
	pattern     string
	sema        *cos.DynSemaphore
	silent      bool
	rnd         *rand.Rand
	smap        *cluster.Smap
}

// Initializes the EC options, validates the number of targets.
// If initial dataCnt value is negative, it sets the number of data and
// parity slices to maximum possible for the cluster.
//
//nolint:revive // modifies-value-receiver on purpose
func (o ecOptions) init(t *testing.T, proxyURL string) *ecOptions {
	o.smap = tools.GetClusterMap(t, proxyURL)
	if cnt := o.smap.CountActiveTs(); cnt < o.minTargets {
		t.Skipf("not enough targets in the cluster: expected at least %d, got %d", o.minTargets, cnt)
	}
	if o.concurrency > 0 {
		o.sema = cos.NewDynSemaphore(o.concurrency)
	}
	o.seed = time.Now().UnixNano()
	o.rnd = rand.New(rand.NewSource(o.seed))
	if o.dataCnt < 0 {
		total := o.smap.CountActiveTs() - 2
		o.parityCnt = total / 2
		o.dataCnt = total - o.parityCnt
	}
	return &o
}

func (o *ecOptions) sliceTotal() int {
	return o.dataCnt + o.parityCnt
}

type ecTest struct {
	name   string
	data   int
	parity int
}

var ecTests = []ecTest{
	{"EC 1:1", 1, 1},
	{"EC 1:2", 1, 2},
	{"EC 2:2", 2, 2},
}

func defaultECBckProps(o *ecOptions) *cmn.BucketPropsToUpdate {
	return &cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{
			Enabled:      api.Bool(true),
			ObjSizeLimit: api.Int64(ecObjLimit),
			DataSlices:   api.Int(o.dataCnt),
			ParitySlices: api.Int(o.parityCnt),
		},
	}
}

// Since all replicas are identical, it is difficult to differentiate main one from others.
// The main replica is the replica that is on the target chosen by proxy using HrwTarget
// algorithm on GET request from a client.
// The function uses heuristics to detect the main one: it should be the oldest
func ecGetAllSlices(t *testing.T, bck cmn.Bck, objName string) (map[string]ecSliceMD, string) {
	var (
		main string

		foundParts = make(map[string]ecSliceMD)
		oldest     = time.Now().Add(time.Hour)
	)

	cb := func(fqn string, de fs.DirEntry) error {
		if de.IsDir() {
			return nil
		}
		ct, err := cluster.NewCTFromFQN(fqn, nil)
		tassert.CheckFatal(t, err)
		if !strings.Contains(ct.ObjectName(), objName) {
			return nil
		}
		stat, err := os.Stat(fqn)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		foundParts[fqn] = ecSliceMD{stat.Size()}
		if ct.ContentType() == fs.ObjectType && oldest.After(stat.ModTime()) {
			main = fqn
			oldest = stat.ModTime()
		}
		return nil
	}

	fs.WalkBck(&fs.WalkBckOpts{
		WalkOpts: fs.WalkOpts{
			Bck:      bck,
			CTs:      []string{fs.ECSliceType, fs.ECMetaType, fs.ObjectType},
			Callback: cb,
			Sorted:   true, // false is unsupported and asserts
		},
	})

	return foundParts, main
}

func ecCheckSlices(t *testing.T, sliceList map[string]ecSliceMD,
	bck cmn.Bck, objPath string, objSize, sliceSize int64, totalCnt int) {
	tassert.Errorf(t, len(sliceList) == totalCnt, "Expected number of objects for %s/%s: %d, found: %d\n%+v",
		bck, objPath, totalCnt, len(sliceList), sliceList)

	if !bck.IsAIS() && !bck.IsRemoteAIS() {
		var ok bool
		config := tools.GetClusterConfig(t)
		_, ok = config.Backend.Providers[bck.Provider]
		tassert.Errorf(t, ok, "invalid provider %s, expected to be in: %v",
			bck.Provider, config.Backend.Providers)
	}

	metaCnt := 0
	for k, md := range sliceList {
		ct, err := cluster.NewCTFromFQN(k, nil)
		tassert.CheckFatal(t, err)

		if ct.ContentType() == fs.ECMetaType {
			metaCnt++
			tassert.Errorf(t, md.size <= 4*cos.KiB, "Metafile %q size is too big: %d", k, md.size)
		} else if ct.ContentType() == fs.ECSliceType {
			tassert.Errorf(t, md.size == sliceSize, "Slice %q size mismatch: %d, expected %d", k, md.size, sliceSize)
		} else {
			tassert.Errorf(t, ct.ContentType() == fs.ObjectType, "invalid content type %s, expected: %s", ct.ContentType(), fs.ObjectType)
			tassert.Errorf(t, ct.Bck().Name == bck.Name, "invalid bucket name %s, expected: %s", ct.Bck().Name, bck.Name)
			tassert.Errorf(t, ct.ObjectName() == objPath, "invalid object name %s, expected: %s", ct.ObjectName(), objPath)
			tassert.Errorf(t, md.size == objSize, "%q size mismatch: got %d, expected %d", k, md.size, objSize)
		}
	}

	metaCntMust := totalCnt / 2
	tassert.Errorf(t, metaCnt == metaCntMust, "Number of metafiles for %s mismatch: %d, expected %d", objPath, metaCnt, metaCntMust)
}

func waitForECFinishes(t *testing.T, totalCnt int, objSize, sliceSize int64, doEC bool, bck cmn.Bck, objName string) (
	foundParts map[string]ecSliceMD, mainObjPath string) {
	deadLine := time.Now().Add(ECPutTimeOut)
	for time.Now().Before(deadLine) {
		foundParts, mainObjPath = ecGetAllSlices(t, bck, objName)
		if len(foundParts) == totalCnt {
			same := true
			for nm, md := range foundParts {
				ct, err := cluster.NewCTFromFQN(nm, nil)
				tassert.CheckFatal(t, err)
				if doEC {
					if ct.ContentType() == fs.ECSliceType {
						if md.size != sliceSize {
							same = false
							break
						}
					}
				} else {
					if ct.ContentType() == fs.ObjectType {
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

func doECPutsAndCheck(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, o *ecOptions) {
	const (
		smallEvery = 10 // Every N-th object is small
		objPatt    = "obj-%s-%04d"
	)

	wg := &sync.WaitGroup{}
	sizes := make(chan int64, o.objCount)

	for idx := 0; idx < o.objCount; idx++ {
		wg.Add(1)
		o.sema.Acquire()

		go func(i int) {
			totalCnt, objSize, sliceSize, doEC := randObjectSize(i, smallEvery, o)
			sizes <- objSize
			objName := fmt.Sprintf(objPatt, bck.Name, i)
			objPath := ecTestDir + objName

			if i%10 == 0 {
				if doEC {
					tlog.Logf("Object %s, size %9d[%9d]\n", objName, objSize, sliceSize)
				} else {
					tlog.Logf("Object %s, size %9d[%9s]\n", objName, objSize, "-")
				}
			}

			r, err := readers.NewRandReader(objSize, cos.ChecksumNone)
			defer func() {
				r.Close()
				o.sema.Release()
				wg.Done()
			}()
			tassert.CheckFatal(t, err)
			putArgs := api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r}
			err = api.PutObject(putArgs)
			tassert.CheckFatal(t, err)

			foundParts, _ := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objPath)
			mainObjPath := ""
			if len(foundParts) != totalCnt {
				t.Errorf("Expected number of files %s: %d, found: %d\n%+v",
					objName, totalCnt, len(foundParts), foundParts)
				return
			}
			metaCnt, sliceCnt, replCnt := 0, 0, 0
			for k, md := range foundParts {
				ct, err := cluster.NewCTFromFQN(k, nil)
				tassert.CheckFatal(t, err)
				if ct.ContentType() == fs.ECMetaType {
					metaCnt++
					tassert.Errorf(t, md.size <= 512, "Metafile %q size is too big: %d", k, md.size)
				} else if ct.ContentType() == fs.ECSliceType {
					sliceCnt++
					if md.size != sliceSize && doEC {
						t.Errorf("Slice %q size mismatch: %d, expected %d", k, md.size, sliceSize)
					}
					if md.size != objSize && !doEC {
						t.Errorf("Copy %q size mismatch: %d, expected %d", k, md.size, objSize)
					}
				} else {
					tassert.Errorf(t, ct.ContentType() == fs.ObjectType, "invalid content type %s, expected: %s", ct.ContentType(), fs.ObjectType)
					tassert.Errorf(t, ct.Bck().Provider == bck.Provider, "invalid provider %s, expected: %s", ct.Bck().Provider, apc.AIS)
					tassert.Errorf(t, ct.Bck().Name == bck.Name, "invalid bucket name %s, expected: %s", ct.Bck().Name, bck.Name)
					tassert.Errorf(t, ct.ObjectName() == objPath, "invalid object name %s, expected: %s", ct.ObjectName(), objPath)
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
			partsAfterRemove, _ := ecGetAllSlices(t, bck, objPath)
			_, ok := partsAfterRemove[mainObjPath]
			if ok || len(partsAfterRemove) >= len(foundParts) {
				t.Errorf("Object is not deleted: %#v", partsAfterRemove)
				return
			}

			_, err = api.GetObject(baseParams, bck, objPath, nil)
			tassert.CheckFatal(t, err)

			if doEC {
				partsAfterRestore, _ := ecGetAllSlices(t, bck, objPath)
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
		t.Logf("Average size of the bucket %s: %s\n", bck, cos.ToSizeIEC(szTotal/int64(szLen), 1))
	}
}

func assertBucketSize(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objCount int) {
	bckObjectsCnt := bucketSize(t, baseParams, bck)
	tassert.Fatalf(t, bckObjectsCnt == objCount, "Invalid number of objects: %d, expected %d", bckObjectsCnt, objCount)
}

func bucketSize(t *testing.T, baseParams api.BaseParams, bck cmn.Bck) int {
	msg := &apc.LsoMsg{Props: "size,status"}
	objList, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	return len(objList.Entries)
}

func putRandomFile(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objPath string, size int) {
	r, err := readers.NewRandReader(int64(size), cos.ChecksumNone)
	tassert.CheckFatal(t, err)
	err = api.PutObject(api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    objPath,
		Reader:     r,
	})
	tassert.CheckFatal(t, err)
}

func newLocalBckWithProps(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, bckProps *cmn.BucketPropsToUpdate, o *ecOptions) {
	proxyURL := tools.RandomProxyURL()
	tools.CreateBucketWithCleanup(t, proxyURL, bck, nil)

	tlog.Logf("Changing EC %d:%d [ seed = %d ], concurrent: %d\n",
		o.dataCnt, o.parityCnt, o.seed, o.concurrency)
	_, err := api.SetBucketProps(baseParams, bck, bckProps)
	tassert.CheckFatal(t, err)
}

func setBucketECProps(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, bckProps *cmn.BucketPropsToUpdate) {
	tlog.Logf("Changing EC %d:%d\n", *bckProps.EC.DataSlices, *bckProps.EC.ParitySlices)
	_, err := api.SetBucketProps(baseParams, bck, bckProps)
	tassert.CheckFatal(t, err)
}

func clearAllECObjects(t *testing.T, bck cmn.Bck, failOnDelErr bool, o *ecOptions) {
	var (
		wg       = sync.WaitGroup{}
		proxyURL = tools.RandomProxyURL()
	)

	tlog.Logln("Deleting objects...")
	wg.Add(o.objCount)
	for idx := 0; idx < o.objCount; idx++ {
		go func(i int) {
			defer wg.Done()
			objName := fmt.Sprintf(o.pattern, i)
			objPath := ecTestDir + objName
			err := tools.Del(proxyURL, bck, objPath, nil, nil, true)
			if failOnDelErr {
				tassert.CheckFatal(t, err)
			} else if err != nil {
				t.Log(err.Error())
			}

			deadline := time.Now().Add(time.Second * 10)
			var partsAfterDelete map[string]int64
			for time.Now().Before(deadline) {
				time.Sleep(time.Millisecond * 250)
				partsAfterDelete, _ := ecGetAllSlices(t, bck, objPath)
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
	reqArgs := xact.ArgsMsg{Kind: apc.ActECPut, Bck: bck}
	api.WaitForXactionIdle(tools.BaseAPIParams(proxyURL), reqArgs)
}

func objectsExist(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objPatt string, objCount int) {
	wg := &sync.WaitGroup{}
	getOneObj := func(objName string) {
		defer wg.Done()
		objPath := ecTestDir + objName
		_, err := api.GetObject(baseParams, bck, objPath, nil)
		tassert.CheckFatal(t, err)
	}

	tlog.Logln("Reading all objects...")
	wg.Add(objCount)
	for i := 0; i < objCount; i++ {
		objName := fmt.Sprintf(objPatt, i)
		go getOneObj(objName)
	}
	wg.Wait()
}

// Simulates damaged slice by changing slice's checksum in metadata file
func damageMetadataCksum(t *testing.T, slicePath string) {
	ct, err := cluster.NewCTFromFQN(slicePath, nil)
	tassert.CheckFatal(t, err)
	metaFQN := ct.Make(fs.ECMetaType)
	md, err := ec.LoadMetadata(metaFQN)
	tassert.CheckFatal(t, err)
	md.CksumValue = "01234"
	err = jsp.Save(metaFQN, md, jsp.Plain(), nil)
	tassert.CheckFatal(t, err)
}

// Short test to make sure that EC options cannot be changed after
// EC is enabled
func TestECChange(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{MinTargets: 3})

	var (
		proxyURL = tools.RandomProxyURL()
		bck      = cmn.Bck{
			Name:     testBucketName + "-ec-change",
			Provider: apc.AIS,
		}
	)

	tools.CreateBucketWithCleanup(t, proxyURL, bck, nil)

	bucketProps := &cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{
			Enabled:      api.Bool(true),
			ObjSizeLimit: api.Int64(ecObjLimit),
			DataSlices:   api.Int(1),
			ParitySlices: api.Int(1),
		},
	}
	baseParams := tools.BaseAPIParams(proxyURL)

	tlog.Logln("Resetting bucket properties")
	_, err := api.ResetBucketProps(baseParams, bck)
	tassert.CheckFatal(t, err)

	tlog.Logln("Trying to set too many slices")
	bucketProps.EC.DataSlices = api.Int(25)
	bucketProps.EC.ParitySlices = api.Int(25)
	_, err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.Errorf(t, err != nil, "Enabling EC must fail in case of the number of targets fewer than the number of slices")

	tlog.Logln("Enabling EC")
	bucketProps.EC.DataSlices = api.Int(1)
	bucketProps.EC.ParitySlices = api.Int(1)
	_, err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.CheckFatal(t, err)

	tlog.Logln("Trying to set EC options to the same values")
	_, err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.CheckFatal(t, err)

	tlog.Logln("Trying to disable EC")
	bucketProps.EC.Enabled = api.Bool(false)
	_, err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.Errorf(t, err == nil, "Disabling EC failed: %v", err)

	tlog.Logln("Trying to re-enable EC")
	bucketProps.EC.Enabled = api.Bool(true)
	_, err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.Errorf(t, err == nil, "Enabling EC failed: %v", err)

	tlog.Logln("Trying to modify EC options when EC is enabled")
	bucketProps.EC.Enabled = api.Bool(true)
	bucketProps.EC.ObjSizeLimit = api.Int64(300000)
	_, err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.Errorf(t, err != nil, "Modifiying EC properties must fail")

	tlog.Logln("Resetting bucket properties")
	_, err = api.ResetBucketProps(baseParams, bck)
	tassert.Errorf(t, err == nil, "Resetting properties should work")
}

func createECReplicas(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objName string, o *ecOptions) {
	o.sema.Acquire()
	defer o.sema.Release()

	totalCnt := 2 + o.parityCnt*2
	objSize := int64(ecMinSmallSize + o.rnd.Intn(ecSmallDelta))
	sliceSize := objSize

	objPath := ecTestDir + objName

	tlog.Logf("Creating %s, size %8d\n", objPath, objSize)
	r, err := readers.NewRandReader(objSize, cos.ChecksumNone)
	tassert.CheckFatal(t, err)
	err = api.PutObject(api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r})
	tassert.CheckFatal(t, err)

	tlog.Logf("waiting for %s\n", objPath)
	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, false, bck, objPath)

	ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)
	tassert.Errorf(t, mainObjPath != "", "Full copy is not found")
}

func createECObject(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objName string, idx int, o *ecOptions) {
	const (
		smallEvery = 7 // Every N-th object is small
	)

	o.sema.Acquire()
	defer o.sema.Release()

	totalCnt, objSize, sliceSize, doEC := randObjectSize(idx, smallEvery, o)
	objPath := ecTestDir + objName
	ecStr := "-"
	if doEC {
		ecStr = "EC"
	}

	tlog.LogfCond(!o.silent, "Creating %s, size %8d [%2s]\n", objPath, objSize, ecStr)
	r, err := readers.NewRandReader(objSize, cos.ChecksumNone)
	tassert.CheckFatal(t, err)
	err = api.PutObject(api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r})
	tassert.CheckFatal(t, err)

	tlog.LogfCond(!o.silent, "waiting for %s\n", objPath)
	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objPath)

	ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)
	if mainObjPath == "" {
		t.Errorf("Full copy is not found")
	}
}

func createDamageRestoreECFile(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objName string, idx int, o *ecOptions) {
	const (
		sleepRestoreTime = 5 * time.Second // wait time after GET restores slices
		smallEvery       = 7               // Every N-th object is small
		sliceDelPct      = 50              // %% of objects that have damaged body and a slice
	)

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
	tlog.LogfCond(!o.silent, "Creating %s, size %8d [%2s] [%s]\n", objPath, objSize, ecStr, delStr)
	r, err := readers.NewRandReader(objSize, cos.ChecksumNone)
	tassert.CheckFatal(t, err)
	err = api.PutObject(api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r})
	tassert.CheckFatal(t, err)

	tlog.LogfCond(!o.silent, "waiting for %s\n", objPath)
	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objPath)

	ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)
	if mainObjPath == "" {
		t.Errorf("Full copy is not found")
		return
	}

	tlog.LogfCond(!o.silent, "Damaging %s [removing %s]\n", objPath, mainObjPath)
	tassert.CheckFatal(t, os.Remove(mainObjPath))

	ct, err := cluster.NewCTFromFQN(mainObjPath, nil)
	tassert.CheckFatal(t, err)
	metafile := ct.Make(fs.ECMetaType)
	tlog.LogfCond(!o.silent, "Damaging %s [removing %s]\n", objPath, metafile)
	tassert.CheckFatal(t, cos.RemoveFile(metafile))
	if delSlice {
		sliceToDel := ""
		for k := range foundParts {
			ct, err := cluster.NewCTFromFQN(k, nil)
			tassert.CheckFatal(t, err)
			if k != mainObjPath && ct.ContentType() == fs.ECSliceType && doEC {
				sliceToDel = k
				break
			} else if k != mainObjPath && ct.ContentType() == fs.ObjectType && !doEC {
				sliceToDel = k
				break
			}
		}
		if sliceToDel == "" {
			t.Errorf("Failed to select random slice for %s", objName)
			return
		}
		tlog.LogfCond(!o.silent, "Removing slice/replica: %s\n", sliceToDel)
		tassert.CheckFatal(t, os.Remove(sliceToDel))

		ct, err := cluster.NewCTFromFQN(sliceToDel, nil)
		tassert.CheckFatal(t, err)
		metafile := ct.Make(fs.ECMetaType)
		if doEC {
			tlog.LogfCond(!o.silent, "Removing slice meta %s\n", metafile)
		} else {
			tlog.LogfCond(!o.silent, "Removing replica meta %s\n", metafile)
		}
		tassert.CheckFatal(t, cos.RemoveFile(metafile))
	}

	partsAfterRemove, _ := ecGetAllSlices(t, bck, objPath)
	_, ok := partsAfterRemove[mainObjPath]
	if ok || len(partsAfterRemove) != len(foundParts)-deletedFiles*2 {
		tlog.Logf("Files are not deleted [%d - %d], leftovers:\n", len(foundParts), len(partsAfterRemove))
		for k := range partsAfterRemove {
			tlog.Logf("     %s\n", k)
		}
		// Not an error as a directory can contain leftovers
		tlog.Logln("Some slices were not deleted")
		return
	}

	tlog.LogfCond(!o.silent, "Restoring %s\n", objPath)
	_, err = api.GetObject(baseParams, bck, objPath, nil)
	if err != nil {
		tlog.Logf("... retrying %s\n", objPath)
		time.Sleep(time.Second)
		_, err = api.GetObject(baseParams, bck, objPath, nil)
	}
	tassert.CheckFatal(t, err)

	// For remote buckets, due to performance reason, GFN is not used and
	// EC is not called - object is reread from the remote bucket instead
	if doEC && bck.IsAIS() {
		deadline := time.Now().Add(sleepRestoreTime)
		var partsAfterRestore map[string]ecSliceMD
		for time.Now().Before(deadline) {
			time.Sleep(time.Millisecond * 250)
			partsAfterRestore, _ = ecGetAllSlices(t, bck, objPath)
			if len(partsAfterRestore) == totalCnt {
				break
			}
		}
		ecCheckSlices(t, partsAfterRestore, bck, objPath, objSize, sliceSize, totalCnt)
	}
}

// Simple stress testing EC for remote buckets
func TestECRestoreObjAndSliceRemote(t *testing.T) {
	var (
		bck        = cliBck
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		useDisks   = []bool{false, true}
	)

	o := ecOptions{
		minTargets:  4,
		objCount:    25,
		concurrency: 8,
		pattern:     "obj-rest-remote-%04d",
	}.init(t, proxyURL)

	tools.CheckSkip(t, tools.SkipTestArgs{RemoteBck: true, Bck: bck})

	initMountpaths(t, proxyURL)
	if testing.Short() {
		useDisks = []bool{false}
	}

	for _, useDisk := range useDisks {
		for _, test := range ecTests {
			testName := fmt.Sprintf("%s/disk_only/%t", test.name, useDisk)
			t.Run(testName, func(t *testing.T) {
				if useDisk {
					tools.SetClusterConfig(t, cos.StrKVs{
						"ec.disk_only": fmt.Sprintf("%t", useDisk),
					})
					defer tools.SetClusterConfig(t, cos.StrKVs{
						"ec.disk_only": "false",
					})
				}
				if o.smap.CountActiveTs() <= test.parity+test.data {
					t.Skip(cmn.ErrNotEnoughTargets)
				}
				o.parityCnt = test.parity
				o.dataCnt = test.data
				setBucketECProps(t, baseParams, bck, defaultECBckProps(o))
				defer api.SetBucketProps(baseParams, bck, &cmn.BucketPropsToUpdate{
					EC: &cmn.ECConfToUpdate{Enabled: api.Bool(false)},
				})

				defer func() {
					tlog.Logln("Wait for PUTs to finish...")
					args := xact.ArgsMsg{Kind: apc.ActECPut}
					err := api.WaitForXactionIdle(baseParams, args)
					tassert.CheckError(t, err)

					clearAllECObjects(t, bck, true, o)
					reqArgs := xact.ArgsMsg{Kind: apc.ActECPut, Bck: bck}
					err = api.WaitForXactionIdle(baseParams, reqArgs)
					tassert.CheckError(t, err)
				}()

				wg := sync.WaitGroup{}
				wg.Add(o.objCount)
				for i := 0; i < o.objCount; i++ {
					o.sema.Acquire()
					go func(i int) {
						defer func() {
							o.sema.Release()
							wg.Done()
						}()
						objName := fmt.Sprintf(o.pattern, i)
						createDamageRestoreECFile(t, baseParams, bck, objName, i, o)
					}(i)
				}
				wg.Wait()
			})
		}
	}
}

// Quick check that EC can restore a damaged object and a missing slice
//   - PUTs an object to the bucket
//   - filepath.Walk checks that the number of metafiles and slices are correct
//   - Either original object or original object and a random slice are deleted
//   - GET should detect that original object is gone
//   - The target restores the original object from slices and missing slices
func TestECRestoreObjAndSlice(t *testing.T) {
	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-obj-n-slice",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		useDisks   = []bool{false, true}
	)

	o := ecOptions{
		minTargets:  4,
		objCount:    50,
		concurrency: 8,
		pattern:     "obj-rest-%04d",
		silent:      testing.Short(),
	}.init(t, proxyURL)
	initMountpaths(t, proxyURL)
	if testing.Short() {
		useDisks = []bool{false}
	}

	for _, useDisk := range useDisks {
		for _, test := range ecTests {
			testName := fmt.Sprintf("%s/disk_only/%t", test.name, useDisk)
			t.Run(testName, func(t *testing.T) {
				if useDisk {
					tools.SetClusterConfig(t, cos.StrKVs{
						"ec.disk_only": fmt.Sprintf("%t", useDisk),
					})
					defer tools.SetClusterConfig(t, cos.StrKVs{
						"ec.disk_only": "false",
					})
				}
				if o.smap.CountActiveTs() <= test.parity+test.data {
					t.Skip(cmn.ErrNotEnoughTargets)
				}
				o.parityCnt = test.parity
				o.dataCnt = test.data
				newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

				wg := sync.WaitGroup{}
				wg.Add(o.objCount)
				for i := 0; i < o.objCount; i++ {
					o.sema.Acquire()
					go func(i int) {
						defer func() {
							o.sema.Release()
							wg.Done()
						}()
						objName := fmt.Sprintf(o.pattern, i)
						createDamageRestoreECFile(t, baseParams, bck, objName, i, o)
					}(i)
				}
				wg.Wait()
				assertBucketSize(t, baseParams, bck, o.objCount)
			})
		}
	}
}

func putECFile(baseParams api.BaseParams, bck cmn.Bck, objName string) error {
	objSize := int64(ecMinBigSize * 2)
	objPath := ecTestDir + objName

	r, err := readers.NewRandReader(objSize, cos.ChecksumNone)
	if err != nil {
		return err
	}
	return api.PutObject(api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    objPath,
		Reader:     r,
	})
}

// Returns path to main object and map of all object's slices and ioContext
func createECFile(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objName string, o *ecOptions) (map[string]ecSliceMD, string) {
	totalCnt := 2 + (o.sliceTotal())*2
	objSize := int64(ecMinBigSize * 2)
	sliceSize := ec.SliceSize(objSize, o.dataCnt)

	err := putECFile(baseParams, bck, objName)
	tassert.CheckFatal(t, err)

	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, true, bck, ecTestDir+objName)
	tassert.Fatalf(t, mainObjPath != "", "Full copy %s was not found", mainObjPath)

	objPath := ecTestDir + objName
	ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)

	return foundParts, mainObjPath
}

// Creates 2 EC files and then corrupts their slices
// Checks that after corrupting one slice it is still possible to recover an object
// Checks that after corrupting all slices it is not possible to recover an object
func TestECChecksum(t *testing.T) {
	if docker.IsRunning() {
		t.Skipf("test %q requires xattrs to be set, doesn't work with docker", t.Name())
	}

	var (
		proxyURL = tools.RandomProxyURL()
		bck      = cmn.Bck{
			Name:     testBucketName + "-ec-cksum",
			Provider: apc.AIS,
		}
	)

	o := ecOptions{
		minTargets: 4,
		dataCnt:    1,
		parityCnt:  1,
		pattern:    "obj-cksum-%04d",
	}.init(t, proxyURL)
	baseParams := tools.BaseAPIParams(proxyURL)
	initMountpaths(t, proxyURL)

	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

	objName1 := fmt.Sprintf(o.pattern, 1)
	objPath1 := ecTestDir + objName1
	foundParts1, mainObjPath1 := createECFile(t, baseParams, bck, objName1, o)

	objName2 := fmt.Sprintf(o.pattern, 2)
	objPath2 := ecTestDir + objName2
	foundParts2, mainObjPath2 := createECFile(t, baseParams, bck, objName2, o)

	tlog.Logf("Removing main object %s\n", mainObjPath1)
	tassert.CheckFatal(t, os.Remove(mainObjPath1))

	// Corrupt just one slice, EC should be able to restore the original object
	for k := range foundParts1 {
		ct, err := cluster.NewCTFromFQN(k, nil)
		tassert.CheckFatal(t, err)

		if k != mainObjPath1 && ct.ContentType() == fs.ECSliceType {
			damageMetadataCksum(t, k)
			break
		}
	}

	_, err := api.GetObject(baseParams, bck, objPath1, nil)
	tassert.CheckFatal(t, err)

	tlog.Logf("Removing main object %s\n", mainObjPath2)
	tassert.CheckFatal(t, os.Remove(mainObjPath2))

	// Corrupt all slices, EC should not be able to restore
	for k := range foundParts2 {
		ct, err := cluster.NewCTFromFQN(k, nil)
		tassert.CheckFatal(t, err)

		if k != mainObjPath2 && ct.ContentType() == fs.ECSliceType {
			damageMetadataCksum(t, k)
		}
	}

	_, err = api.GetObject(baseParams, bck, objPath2, nil)
	tassert.Fatalf(t, err != nil, "Object should not be restored when checksums are wrong")
}

func TestECEnabledDisabledEnabled(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-ec-props",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	o := ecOptions{
		minTargets:  4,
		dataCnt:     1,
		parityCnt:   1,
		objCount:    25,
		concurrency: 8,
		pattern:     "obj-rest-%04d",
	}.init(t, proxyURL)

	initMountpaths(t, proxyURL)
	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

	// End of preparation, create files with EC enabled, check if are restored properly

	wg := sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		o.sema.Acquire()
		go func(i int) {
			defer func() {
				o.sema.Release()
				wg.Done()
			}()
			objName := fmt.Sprintf(o.pattern, i)
			createDamageRestoreECFile(t, baseParams, bck, objName, i, o)
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	assertBucketSize(t, baseParams, bck, o.objCount)

	// Disable EC, put normal files, check if were created properly
	_, err := api.SetBucketProps(baseParams, bck, &cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(false)},
	})
	tassert.CheckError(t, err)

	wg.Add(o.objCount)
	for i := o.objCount; i < 2*o.objCount; i++ {
		go func(i int) {
			defer wg.Done()
			objName := fmt.Sprintf(o.pattern, i)
			putRandomFile(t, baseParams, bck, objName, cos.MiB)
		}(i)
	}

	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	assertBucketSize(t, baseParams, bck, o.objCount*2)

	// Enable EC again, check if EC was started properly and creates files with EC correctly
	_, err = api.SetBucketProps(baseParams, bck, &cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(true)},
	})
	tassert.CheckError(t, err)

	wg.Add(o.objCount)
	for i := 2 * o.objCount; i < 3*o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		o.sema.Acquire()
		go func(i int) {
			defer func() {
				o.sema.Release()
				wg.Done()
			}()
			createDamageRestoreECFile(t, baseParams, bck, objName, i, o)
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	assertBucketSize(t, baseParams, bck, o.objCount*3)
}

func TestECDisableEnableDuringLoad(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-ec-load",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	o := ecOptions{
		minTargets:  4,
		dataCnt:     1,
		parityCnt:   1,
		objCount:    5,
		concurrency: 8,
		pattern:     "obj-disable-enable-load-%04d",
	}.init(t, proxyURL)

	initMountpaths(t, proxyURL)
	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)
	// End of preparation, create files with EC enabled, check if are restored properly

	wg := &sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		o.sema.Acquire()
		go func(i int) {
			defer func() {
				o.sema.Release()
				wg.Done()
			}()
			objName := fmt.Sprintf(o.pattern, i)
			createDamageRestoreECFile(t, baseParams, bck, objName, i, o)
		}(i)
	}
	wg.Wait()

	assertBucketSize(t, baseParams, bck, o.objCount)

	var (
		numCreated = 0
		abortCh    = &cos.StopCh{}
		wgPut      = &sync.WaitGroup{}
	)
	abortCh.Init()
	wgPut.Add(1)

	go func() {
		ticker := time.NewTicker(3 * time.Millisecond)
		defer wgPut.Done()
		for {
			select {
			case <-abortCh.Listen():
				ticker.Stop()
				return
			case <-ticker.C:
				objName := fmt.Sprintf(o.pattern, o.objCount+numCreated)
				wgPut.Add(1)
				go func() {
					defer wgPut.Done()
					putRandomFile(t, baseParams, bck, objName, cos.KiB)
				}()
				numCreated++
			}
		}
	}()

	time.Sleep(time.Second)

	tlog.Logf("Disabling EC for the bucket %s\n", bck)
	_, err := api.SetBucketProps(baseParams, bck, &cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(false)},
	})
	tassert.CheckError(t, err)

	time.Sleep(15 * time.Millisecond)
	tlog.Logf("Enabling EC for the bucket %s\n", bck)
	_, err = api.SetBucketProps(baseParams, bck, &cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(true)},
	})
	tassert.CheckError(t, err)
	reqArgs := xact.ArgsMsg{Kind: apc.ActECEncode, Bck: bck}
	_, err = api.WaitForXactionIC(baseParams, reqArgs)
	tassert.CheckError(t, err)

	abortCh.Close()
	wgPut.Wait()

	if t.Failed() {
		t.FailNow()
	}

	// Disabling and enabling EC should not result in put's failing.
	assertBucketSize(t, baseParams, bck, o.objCount+numCreated)
}

// Stress test to check that EC works as expected.
//   - Changes bucket props to use EC
//   - Generates `objCount` objects, size between `ecObjMinSize` and `ecObjMinSize`+ecObjMaxSize`
//   - Objects smaller `ecObjLimit` must be copies, while others must be EC'ed
//   - PUTs objects to the bucket
//   - filepath.Walk checks that the number of metafiles and slices are correct
//   - The original object is deleted
//   - GET should detect that original object is gone and that there are EC slices
//   - The target restores the original object from slices/copies and returns it
//   - No errors must occur
func TestECStress(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-ec-stress",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	o := ecOptions{
		minTargets:  4,
		objCount:    400,
		concurrency: 12,
		pattern:     "obj-stress-%04d",
	}.init(t, proxyURL)
	initMountpaths(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.data+test.parity {
				t.Skip(cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)
			doECPutsAndCheck(t, baseParams, bck, o)

			msg := &apc.LsoMsg{Props: "size,status"}
			objList, err := api.ListObjects(baseParams, bck, msg, 0)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, len(objList.Entries) == o.objCount,
				"Invalid number of objects: %d, expected %d", len(objList.Entries), o.objCount)
		})
	}
}

// Stress 2 buckets at the same time
func TestECStressManyBuckets(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		bck1 = cmn.Bck{
			Name:     testBucketName + "1",
			Provider: apc.AIS,
		}
		bck2 = cmn.Bck{
			Name:     testBucketName + "2",
			Provider: apc.AIS,
		}
		proxyURL = tools.RandomProxyURL()
	)

	o1 := ecOptions{
		minTargets:  4,
		parityCnt:   1,
		dataCnt:     1,
		objCount:    200,
		concurrency: 12,
		pattern:     "obj-stress-manybck-%04d",
	}.init(t, proxyURL)
	o2 := ecOptions{
		minTargets:  4,
		parityCnt:   1,
		dataCnt:     1,
		objCount:    200,
		concurrency: 12,
		pattern:     "obj-stress-manybck-%04d",
	}.init(t, proxyURL)

	initMountpaths(t, proxyURL)
	baseParams := tools.BaseAPIParams(proxyURL)
	newLocalBckWithProps(t, baseParams, bck1, defaultECBckProps(o1), o1)
	newLocalBckWithProps(t, baseParams, bck2, defaultECBckProps(o2), o2)

	// Run EC on different buckets concurrently
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		doECPutsAndCheck(t, baseParams, bck1, o1)
	}()
	go func() {
		defer wg.Done()
		doECPutsAndCheck(t, baseParams, bck2, o2)
	}()
	wg.Wait()

	msg := &apc.LsoMsg{Props: "size,status"}
	objList, err := api.ListObjects(baseParams, bck1, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(objList.Entries) == o1.objCount, "Bucket %s: Invalid number of objects: %d, expected %d", bck1.String(), len(objList.Entries), o1.objCount)

	msg = &apc.LsoMsg{Props: "size,status"}
	objList, err = api.ListObjects(baseParams, bck2, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(objList.Entries) == o2.objCount, "Bucket %s: Invalid number of objects: %d, expected %d", bck2.String(), len(objList.Entries), o2.objCount)
}

// ExtraStress test to check that EC works as expected
//   - Changes bucket props to use EC
//   - Generates `objCount` objects, size between `ecObjMinSize` and `ecObjMinSize`+ecObjMaxSize`
//   - Objects smaller `ecObjLimit` must be copies, while others must be EC'ed
//   - PUTs ALL objects to the bucket stressing both EC and transport
//   - filepath.Walk checks that the number of metafiles at the end is correct
//   - No errors must occur
func TestECExtraStress(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	const (
		objStart = "obj-extra-"
	)

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-extrastress",
			Provider: apc.AIS,
		}
		proxyURL = tools.RandomProxyURL()
	)

	o := ecOptions{
		minTargets:  4,
		objCount:    400,
		concurrency: 12,
		pattern:     objStart + "%04d",
	}.init(t, proxyURL)
	initMountpaths(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.data+test.parity {
				t.Skip(cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			ecStressCore(t, o, proxyURL, bck)
		})
	}
}

func ecStressCore(t *testing.T, o *ecOptions, proxyURL string, bck cmn.Bck) {
	const (
		objStart   = "obj-extra-"
		smallEvery = 7
	)
	var (
		waitAllTime = time.Minute * 4 // should be enough for all object to complete EC
		totalSlices atomic.Int64
		baseParams  = tools.BaseAPIParams(proxyURL)
	)

	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

	started := time.Now()

	type sCnt struct {
		obj string
		cnt int
	}
	cntCh := make(chan sCnt, o.objCount)
	wg := &sync.WaitGroup{}
	wg.Add(o.objCount)
	for idx := 0; idx < o.objCount; idx++ {
		o.sema.Acquire()

		go func(i int) {
			defer func() {
				o.sema.Release()
				wg.Done()
			}()

			objName := fmt.Sprintf(o.pattern, i)
			totalCnt, objSize, sliceSize, doEC := randObjectSize(i, smallEvery, o)
			objPath := ecTestDir + objName
			if doEC {
				tlog.Logf("Object %s, size %9d[%9d]\n", objName, objSize, sliceSize)
			} else {
				tlog.Logf("Object %s, size %9d[%9s]\n", objName, objSize, "-")
			}
			r, err := readers.NewRandReader(objSize, cos.ChecksumNone)
			tassert.Errorf(t, err == nil, "Failed to create reader: %v", err)
			putArgs := api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r}
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
		foundParts, _ = ecGetAllSlices(t, bck, objStart)
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

	msg := &apc.LsoMsg{Props: "size,status"}
	objList, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(objList.Entries) == o.objCount, "Invalid number of objects: %d, expected %d", len(objList.Entries), o.objCount)
}

// Quick check that EC keeps xattrs:
// - enable EC and versioning for the bucket
// - put/damage/restore one by one a few objects two times to increase their versions
// - get the list of the objects at the end and check that they all have the correct versions
func TestECXattrs(t *testing.T) {
	const (
		sleepRestoreTime = time.Second * 5 // wait time after GET restores slices
		finalVersion     = "2"
		smallEvery       = 4
	)

	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-attrs",
			Provider: apc.AIS,
		}
		proxyURL = tools.RandomProxyURL()
	)

	o := ecOptions{
		minTargets:  4,
		dataCnt:     1,
		parityCnt:   1,
		objCount:    30,
		concurrency: 8,
		pattern:     "obj-xattr-%04d",
	}.init(t, proxyURL)
	initMountpaths(t, proxyURL)

	baseParams := tools.BaseAPIParams(proxyURL)
	bckProps := defaultECBckProps(o)
	bckProps.Versioning = &cmn.VersionConfToUpdate{
		Enabled: api.Bool(true),
	}

	newLocalBckWithProps(t, baseParams, bck, bckProps, o)

	oneObj := func(idx int, objName string) {
		totalCnt, objSize, sliceSize, doEC := randObjectSize(idx, smallEvery, o)
		objPath := ecTestDir + objName
		ecStr, delStr := "-", "obj"
		if doEC {
			ecStr = "EC"
		}
		tlog.Logf("Creating %s, size %8d [%2s] [%s]\n", objPath, objSize, ecStr, delStr)
		r, err := readers.NewRandReader(objSize, cos.ChecksumNone)
		tassert.CheckFatal(t, err)
		err = api.PutObject(api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r})
		tassert.CheckFatal(t, err)

		tlog.Logf("waiting for %s\n", objPath)
		foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objPath)

		ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)
		if mainObjPath == "" {
			t.Fatalf("Full copy is not found")
		}

		tlog.Logf("Damaging %s [removing %s]\n", objPath, mainObjPath)
		tassert.CheckFatal(t, os.Remove(mainObjPath))

		ct, err := cluster.NewCTFromFQN(mainObjPath, nil)
		tassert.CheckFatal(t, err)
		metafile := ct.Make(fs.ECMetaType)
		tlog.Logf("Damaging %s [removing %s]\n", objPath, metafile)
		tassert.CheckFatal(t, cos.RemoveFile(metafile))

		partsAfterRemove, _ := ecGetAllSlices(t, bck, objPath)
		_, ok := partsAfterRemove[mainObjPath]
		if ok || len(partsAfterRemove) != len(foundParts)-2 {
			t.Fatalf("Files are not deleted [%d - %d]: %#v", len(foundParts), len(partsAfterRemove), partsAfterRemove)
		}

		tlog.Logf("Restoring %s\n", objPath)
		_, err = api.GetObject(baseParams, bck, objPath, nil)
		if err != nil {
			tlog.Logf("... retrying %s\n", objPath)
			time.Sleep(time.Second)
			_, err = api.GetObject(baseParams, bck, objPath, nil)
		}
		tassert.CheckFatal(t, err)

		if doEC {
			deadline := time.Now().Add(sleepRestoreTime)
			var partsAfterRestore map[string]ecSliceMD
			for time.Now().Before(deadline) {
				time.Sleep(time.Millisecond * 250)
				partsAfterRestore, _ = ecGetAllSlices(t, bck, objPath)
				if len(partsAfterRestore) == totalCnt {
					break
				}
			}
			ecCheckSlices(t, partsAfterRestore, bck, objPath, objSize, sliceSize, totalCnt)
		}
	}

	// PUT objects twice to make their version 2
	for j := 0; j < 2; j++ {
		for i := 0; i < o.objCount; i++ {
			objName := fmt.Sprintf(o.pattern, i)
			oneObj(i, objName)
		}
	}

	msg := &apc.LsoMsg{Props: "size,status,version"}
	objList, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)

	// check that all returned objects and their repicas have the same version
	for _, e := range objList.Entries {
		if e.Version != finalVersion {
			t.Errorf("%s[status=%d] must have version %s but it is %s\n", e.Name, e.Flags, finalVersion, e.Version)
		}
	}

	if len(objList.Entries) != o.objCount {
		t.Fatalf("Invalid number of objects: %d, expected %d", len(objList.Entries), 1)
	}
}

// 1. start putting EC files into the cluster
// 2. in the middle of puts destroy bucket
// 3. wait for puts to finish
// 4. create bucket with the same name
// 5. check that EC is working properly for this bucket
func TestECDestroyBucket(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-DESTROY",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	o := ecOptions{
		minTargets:  4,
		dataCnt:     1,
		parityCnt:   1,
		objCount:    100,
		concurrency: 10,
		pattern:     "obj-destroy-bck-%04d",
	}.init(t, proxyURL)

	initMountpaths(t, proxyURL)
	bckProps := defaultECBckProps(o)
	newLocalBckWithProps(t, baseParams, bck, bckProps, o)

	wg := &sync.WaitGroup{}
	errCnt := atomic.NewInt64(0)
	sucCnt := atomic.NewInt64(0)

	for i := 0; i < o.objCount; i++ {
		o.sema.Acquire()
		wg.Add(1)
		go func(i int) {
			defer func() {
				o.sema.Release()
				wg.Done()
			}()

			objName := fmt.Sprintf(o.pattern, i)
			if i%10 == 0 {
				tlog.Logf("ec object %s into bucket %s\n", objName, bck)
			}
			if putECFile(baseParams, bck, objName) != nil {
				errCnt.Inc()
			} else {
				sucCnt.Inc()
			}
		}(i)

		if i == 4*o.objCount/5 {
			// DestroyBucket when put requests are still executing
			o.sema.Acquire()
			wg.Add(1)
			go func() {
				defer func() {
					o.sema.Release()
					wg.Done()
				}()

				tlog.Logf("Destroying bucket %s\n", bck)
				tools.DestroyBucket(t, proxyURL, bck)
			}()
		}
	}

	wg.Wait()
	tlog.Logf("EC put files resulted in error in %d out of %d files\n", errCnt.Load(), o.objCount)
	args := xact.ArgsMsg{Kind: apc.ActECPut}
	api.WaitForXactionIC(baseParams, args)

	// create bucket with the same name and check if puts are successful
	newLocalBckWithProps(t, baseParams, bck, bckProps, o)
	doECPutsAndCheck(t, baseParams, bck, o)

	// check if get requests are successful
	msg := &apc.LsoMsg{Props: "size,status,version"}
	objList, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(objList.Entries) == o.objCount, "Invalid number of objects: %d, expected %d", len(objList.Entries), o.objCount)
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

	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-slice-emergency",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	o := ecOptions{
		minTargets:  5,
		dataCnt:     -1,
		objCount:    100,
		concurrency: 12,
		pattern:     "obj-emt-%04d",
	}.init(t, proxyURL)
	initMountpaths(t, proxyURL)

	// Increase number of EC data slices, now there's just enough targets to handle EC requests
	// Encoding will fail if even one is missing, restoring should still work
	o.dataCnt++

	sgl := memsys.PageMM().NewSGL(0)
	defer sgl.Free()

	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

	wg := &sync.WaitGroup{}

	// 1. PUT objects
	putOneObj := func(idx int) {
		defer func() {
			wg.Done()
			o.sema.Release()
		}()

		var (
			start   = time.Now()
			objName = fmt.Sprintf(o.pattern, idx)
			objPath = ecTestDir + objName

			totalCnt, objSize, sliceSize, doEC = randObjectSize(idx, smallEvery, o)
		)
		ecStr := "-"
		if doEC {
			ecStr = "EC"
		}
		tlog.Logf("Creating %s, size %8d [%2s]\n", objPath, objSize, ecStr)
		r, err := readers.NewRandReader(objSize, cos.ChecksumNone)
		tassert.CheckFatal(t, err)
		err = api.PutObject(api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r})
		tassert.CheckFatal(t, err)
		t.Logf("Object %s put in %v", objName, time.Since(start))
		start = time.Now()

		foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objPath)

		ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)
		if mainObjPath == "" {
			t.Errorf("Full copy is not found")
			return
		}
		t.Logf("Object %s EC in %v", objName, time.Since(start))
	}

	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		o.sema.Acquire()
		go putOneObj(i)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	_, removedTarget := tools.RmTargetSkipRebWait(t, proxyURL, o.smap)
	defer func() {
		val := &apc.ActValRmNode{DaemonID: removedTarget.ID()}
		rebID, err := api.StopMaintenance(baseParams, val)
		tassert.CheckError(t, err)
		tools.WaitForRebalanceByID(t, -1 /*orig target cnt*/, baseParams, rebID, rebalanceTimeout)
	}()

	// 3. Read objects
	objectsExist(t, baseParams, bck, o.pattern, o.objCount)

	// 4. Check that ListObjects returns correct number of items
	tlog.Logln("Reading bucket list...")
	msg := &apc.LsoMsg{Props: "size,status,version"}
	objList, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(objList.Entries) == o.objCount, "Invalid number of objects: %d, expected %d", len(objList.Entries), o.objCount)
}

func TestECEmergencyTargetForReplica(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-replica-emergency",
			Provider: apc.AIS,
		}
		proxyURL = tools.RandomProxyURL()
	)

	o := ecOptions{
		minTargets:  5,
		dataCnt:     -1,
		objCount:    50,
		concurrency: 8,
		pattern:     "obj-rest-%04d",
	}.init(t, proxyURL)

	if o.smap.CountActiveTs() > 10 {
		// Reason: calculating main obj directory based on DeamonID
		// see getOneObj, 'HACK' annotation
		t.Skip("Test requires at most 10 targets")
	}
	initMountpaths(t, proxyURL)

	for _, target := range o.smap.Tmap {
		target.Digest()
	}

	// Increase number of EC data slices, now there's just enough targets to handle EC requests
	// Encoding will fail if even one is missing, restoring should still work
	o.dataCnt++

	baseParams := tools.BaseAPIParams(proxyURL)
	bckProps := defaultECBckProps(o)
	newLocalBckWithProps(t, baseParams, bck, bckProps, o)

	wg := sync.WaitGroup{}

	// PUT object
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		go func(i int) {
			defer wg.Done()
			objName := fmt.Sprintf(o.pattern, i)
			createECReplicas(t, baseParams, bck, objName, o)
		}(i)
	}

	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	// kill #dataslices of targets, normal EC restore won't be possible
	// 2. Kill a random target
	removedTargets := make(cluster.Nodes, 0, o.dataCnt)
	smap := tools.GetClusterMap(t, proxyURL)

	for i := o.dataCnt - 1; i >= 0; i-- {
		var removedTarget *cluster.Snode
		smap, removedTarget = tools.RmTargetSkipRebWait(t, proxyURL, smap)
		removedTargets = append(removedTargets, removedTarget)
	}

	defer func() {
		var rebID string
		for _, target := range removedTargets {
			rebID, _ = tools.RestoreTarget(t, proxyURL, target)
		}
		if rebID == "" {
			return
		}
		tools.WaitForRebalanceByID(t, -1 /*orig target cnt*/, baseParams, rebID, rebalanceTimeout)
	}()

	hasTarget := func(targets cluster.Nodes, target *cluster.Snode) bool {
		for _, tr := range targets {
			if tr.ID() == target.ID() {
				return true
			}
		}
		return false
	}

	getOneObj := func(i int) {
		defer wg.Done()

		objName := fmt.Sprintf(o.pattern, i)
		// 1) hack: calculate which targets stored a replica
		cbck := cluster.NewBck(bck.Name, bck.Provider, cmn.NsGlobal)
		targets, err := cluster.HrwTargetList(cbck.MakeUname(ecTestDir+objName), o.smap, o.parityCnt+1)
		tassert.CheckFatal(t, err)

		mainTarget := targets[0]
		targets = targets[1:]

		replicas, _ := ecGetAllSlices(t, bck, objName)
		// HACK: this tells directory of target based on last number of it's port
		// This is usually true, but undefined if target has > 9 nodes
		// as the last digit becomes ambiguous
		targetDir := mainTarget.ID()[len(mainTarget.ID())-1]

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
				_, err := api.GetObject(baseParams, bck, objPath, nil)
				tassert.CheckFatal(t, err)
				return
			}
		}
	}

	tlog.Logln("Reading all objects...")
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		go getOneObj(i)
	}
	wg.Wait()

	// it is OK to have some Del failed with "object not found" because
	// some targets are still dead at this point
	clearAllECObjects(t, bck, false, o)
}

// Lost mountpah test:
// - puts some objects
// - disable a random mountpath
// - gets all objects
// - nothing must fail
// - enable the mountpath back
func TestECEmergencyMountpath(t *testing.T) {
	const (
		smallEvery = 4
	)

	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-mpath-emergency",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	o := ecOptions{
		minTargets:  5,
		dataCnt:     1,
		parityCnt:   1,
		objCount:    400,
		concurrency: 24,
		pattern:     "obj-em-mpath-%04d",
	}.init(t, proxyURL)

	removeTarget, _ := o.smap.GetRandTarget()
	mpathList, err := api.GetMountpaths(baseParams, removeTarget)
	tassert.CheckFatal(t, err)
	ensureNoDisabledMountpaths(t, removeTarget, mpathList)
	if len(mpathList.Available) < 2 {
		t.Fatalf("%s requires 2 or more mountpaths", t.Name())
	}
	initMountpaths(t, proxyURL)

	sgl := memsys.PageMM().NewSGL(0)
	defer sgl.Free()

	bckProps := defaultECBckProps(o)
	newLocalBckWithProps(t, baseParams, bck, bckProps, o)

	wg := &sync.WaitGroup{}

	// 1. PUT objects
	putOneObj := func(idx int) {
		defer func() {
			wg.Done()
			o.sema.Release()
		}()
		var (
			objName = fmt.Sprintf(o.pattern, idx)
			objPath = ecTestDir + objName

			totalCnt, objSize, sliceSize, doEC = randObjectSize(idx, smallEvery, o)
		)
		ecStr := "-"
		if doEC {
			ecStr = "EC"
		}
		tlog.Logf("Creating %s, size %8d [%2s]\n", objPath, objSize, ecStr)
		r, err := readers.NewRandReader(objSize, cos.ChecksumNone)
		tassert.CheckFatal(t, err)
		err = api.PutObject(api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r})
		tassert.CheckFatal(t, err)

		foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objPath)
		ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)
		if mainObjPath == "" {
			t.Errorf("Full copy is not found")
			return
		}
	}

	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		o.sema.Acquire()
		go putOneObj(i)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	// 2. Disable a random mountpath
	mpathID := o.rnd.Intn(len(mpathList.Available))
	removeMpath := mpathList.Available[mpathID]
	tlog.Logf("Disabling a mountpath %s at target: %s\n", removeMpath, removeTarget.ID())
	err = api.DisableMountpath(baseParams, removeTarget, removeMpath, false /*dont-resil*/)
	tassert.CheckFatal(t, err)

	tools.WaitForResilvering(t, baseParams, removeTarget)

	defer func() {
		tlog.Logf("Enabling mountpath %s at target %s...\n", removeMpath, removeTarget.ID())
		err = api.EnableMountpath(baseParams, removeTarget, removeMpath)
		tassert.CheckFatal(t, err)

		tools.WaitForResilvering(t, baseParams, removeTarget)
		ensureNumMountpaths(t, removeTarget, mpathList)
	}()

	// 3. Read objects
	objectsExist(t, baseParams, bck, o.pattern, o.objCount)

	// 4. Check that ListObjects returns correct number of items
	tlog.Logf("DONE\nReading bucket list...\n")
	msg := &apc.LsoMsg{Props: "size,status,version"}
	objList, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	if len(objList.Entries) != o.objCount {
		t.Fatalf("Invalid number of objects: %d, expected %d", len(objList.Entries), o.objCount)
	}

	// Wait for ec to finish
	flt := xact.ArgsMsg{Kind: apc.ActECPut, Bck: bck}
	_ = api.WaitForXactionIdle(baseParams, flt)
}

func TestECRebalance(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true, RequiredDeployment: tools.ClusterTypeLocal})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-ec-rebalance",
			Provider: apc.AIS,
		}
		proxyURL = tools.RandomProxyURL()
	)
	o := ecOptions{
		objCount:    30,
		concurrency: 8,
		pattern:     "obj-reb-chk-%04d",
		silent:      true,
	}.init(t, proxyURL)
	initMountpaths(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.parity+test.data+1 {
				t.Skip(cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			ecOnlyRebalance(t, o, proxyURL, bck)
		})
	}
}

func TestECMountpaths(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-ec-mpaths",
			Provider: apc.AIS,
		}
		proxyURL = tools.RandomProxyURL()
	)
	o := ecOptions{
		objCount:    30,
		concurrency: 8,
		pattern:     "obj-reb-mp-%04d",
		silent:      true,
	}.init(t, proxyURL)
	initMountpaths(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.parity+test.data {
				t.Skipf("%s: %v", t.Name(), cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			ecMountpaths(t, o, proxyURL, bck)
		})
	}
}

// The test only checks that the number of object after rebalance equals
// the number of objects before it
func ecOnlyRebalance(t *testing.T, o *ecOptions, proxyURL string, bck cmn.Bck) {
	baseParams := tools.BaseAPIParams(proxyURL)

	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

	wg := sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		go func(i int) {
			defer wg.Done()
			objName := fmt.Sprintf(o.pattern, i)
			createECObject(t, baseParams, bck, objName, i, o)
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	msg := &apc.LsoMsg{Props: apc.GetPropsSize}
	oldObjList, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	tlog.Logf("%d objects created, starting rebalance\n", len(oldObjList.Entries))

	removedTarget, err := o.smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	args := &apc.ActValRmNode{DaemonID: removedTarget.ID()}
	rebID, err := api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	defer func() {
		rebID, _ := tools.RestoreTarget(t, proxyURL, removedTarget)
		tools.WaitForRebalanceByID(t, -1 /*orig target cnt*/, baseParams, rebID, rebalanceTimeout)
	}()
	tools.WaitForRebalanceByID(t, -1, baseParams, rebID, rebalanceTimeout)

	newObjList, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	if len(oldObjList.Entries) != len(newObjList.Entries) {
		for _, o := range oldObjList.Entries {
			found := false
			for _, n := range newObjList.Entries {
				if n.Name == o.Name {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Old %s[%d] not found", o.Name, o.Size)
			}
		}
		t.Fatalf("%d objects before rebalance, %d objects after",
			len(oldObjList.Entries), len(newObjList.Entries))
	}

	for _, en := range newObjList.Entries {
		oah, err := api.GetObject(baseParams, bck, en.Name, nil)
		if err != nil {
			t.Errorf("Failed to read %s: %v", en.Name, err)
			continue // to avoid printing other error in this case
		}
		if oah.Size() != en.Size {
			t.Errorf("%s size mismatch read %d, props %d", en.Name, oah.Size(), en.Size)
		}
	}
}

// Simple test to check if EC correctly finds all the objects and its slices
// that will be used by rebalance
func TestECBucketEncode(t *testing.T) {
	const parityCnt = 2
	var (
		proxyURL = tools.RandomProxyURL()
		m        = ioContext{
			t:        t,
			num:      150,
			proxyURL: proxyURL,
		}
	)

	m.initWithCleanupAndSaveState()
	baseParams := tools.BaseAPIParams(proxyURL)

	if m.smap.CountActiveTs() < parityCnt+1 {
		t.Skipf("Not enough targets to run %s test, must be at least %d", t.Name(), parityCnt+1)
	}

	initMountpaths(t, proxyURL)
	tools.CreateBucketWithCleanup(t, proxyURL, m.bck, nil)

	m.puts()

	objList, err := api.ListObjects(baseParams, m.bck, nil, 0)
	tassert.CheckFatal(t, err)
	tlog.Logf("Object count: %d\n", len(objList.Entries))
	if len(objList.Entries) != m.num {
		t.Fatalf("list_objects %s invalid number of files %d, expected %d", m.bck, len(objList.Entries), m.num)
	}

	tlog.Logf("Enabling EC\n")
	bckPropsToUpate := &cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{
			Enabled:      api.Bool(true),
			ObjSizeLimit: api.Int64(1),
			DataSlices:   api.Int(1),
			ParitySlices: api.Int(parityCnt),
		},
	}
	_, err = api.SetBucketProps(baseParams, m.bck, bckPropsToUpate)
	tassert.CheckFatal(t, err)

	tlog.Logf("EC encode must start automatically for bucket %s\n", m.bck)
	xargs := xact.ArgsMsg{Kind: apc.ActECEncode, Bck: m.bck, Timeout: rebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, xargs)
	tassert.CheckFatal(t, err)

	objList, err = api.ListObjects(baseParams, m.bck, nil, 0)
	tassert.CheckFatal(t, err)

	if len(objList.Entries) != m.num {
		t.Fatalf("bucket %s: expected %d objects, got %d", m.bck, m.num, len(objList.Entries))
	}
	tlog.Logf("Object counts after EC finishes: %d (%d)\n", len(objList.Entries), (parityCnt+1)*m.num)
	//
	// TODO: support querying bucket for total number of entries with respect to mirroring and EC
	//
}

// Creates two buckets (with EC enabled and disabled), fill them with data,
// and then runs two parallel rebalances
func TestECAndRegularRebalance(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true, RequiredDeployment: tools.ClusterTypeLocal})

	var (
		bckReg = cmn.Bck{
			Name:     testBucketName + "-REG",
			Provider: apc.AIS,
		}
		bckEC = cmn.Bck{
			Name:     testBucketName + "-EC",
			Provider: apc.AIS,
		}
		proxyURL = tools.RandomProxyURL()
	)
	o := ecOptions{
		minTargets:  5,
		objCount:    90,
		concurrency: 8,
		pattern:     "obj-reb-chk-%04d",
		silent:      true,
	}.init(t, proxyURL)
	initMountpaths(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.parity+test.data+1 {
				t.Skip(cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			ecAndRegularRebalance(t, o, proxyURL, bckReg, bckEC)
		})
	}
}

func ecAndRegularRebalance(t *testing.T, o *ecOptions, proxyURL string, bckReg, bckEC cmn.Bck) {
	baseParams := tools.BaseAPIParams(proxyURL)

	tools.CreateBucketWithCleanup(t, proxyURL, bckReg, nil)
	newLocalBckWithProps(t, baseParams, bckEC, defaultECBckProps(o), o)

	// select a target that loses its mpath(simulate drive death),
	// and that has mpaths changed (simulate mpath added)
	tgtList := o.smap.Tmap.ActiveNodes()
	tgtLost := tgtList[0]

	tlog.Logf("Put %s in maintenance (no rebalance)\n", tgtLost.StringEx())
	args := &apc.ActValRmNode{DaemonID: tgtLost.ID(), SkipRebalance: true}
	_, err := api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	registered := false
	defer func() {
		if !registered {
			args := &apc.ActValRmNode{DaemonID: tgtLost.ID()}
			rebID, err := api.StopMaintenance(baseParams, args)
			tassert.CheckError(t, err)
			tools.WaitForRebalanceByID(t, -1 /*orig target cnt*/, baseParams, rebID)
		}
	}()

	// fill EC bucket
	wg := sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		go func(i int) {
			defer wg.Done()
			objName := fmt.Sprintf(o.pattern, i)
			createECObject(t, baseParams, bckEC, objName, i, o)
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	_, _, err = tools.PutRandObjs(tools.PutObjectsArgs{
		ProxyURL:  proxyURL,
		Bck:       bckReg,
		ObjPath:   ecTestDir,
		ObjCnt:    o.objCount,
		ObjSize:   fileSize,
		CksumType: bckReg.DefaultProps(initialClusterConfig).Cksum.Type,
	})
	tassert.CheckFatal(t, err)

	msg := &apc.LsoMsg{}
	resECOld, err := api.ListObjects(baseParams, bckEC, msg, 0)
	tassert.CheckFatal(t, err)
	resRegOld, err := api.ListObjects(baseParams, bckReg, msg, 0)
	tassert.CheckFatal(t, err)
	tlog.Logf("Created %d objects in %s, %d objects in %s. Starting rebalance\n",
		len(resECOld.Entries), bckEC, len(resRegOld.Entries), bckReg)

	tlog.Logf("Take %s out of maintenance\n", tgtLost.StringEx())
	args = &apc.ActValRmNode{DaemonID: tgtLost.ID()}
	rebID, err := api.StopMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	registered = true
	tools.WaitForRebalanceByID(t, -1 /*orig target cnt*/, baseParams, rebID, rebalanceTimeout)

	tlog.Logln("Getting the number of objects after rebalance")
	resECNew, err := api.ListObjects(baseParams, bckEC, msg, 0)
	tassert.CheckFatal(t, err)
	tlog.Logf("%d objects in %s after rebalance\n",
		len(resECNew.Entries), bckEC)
	resRegNew, err := api.ListObjects(baseParams, bckReg, msg, 0)
	tassert.CheckFatal(t, err)
	tlog.Logf("%d objects in %s after rebalance\n",
		len(resRegNew.Entries), bckReg)

	tlog.Logln("Test object readability after rebalance")
	for _, obj := range resECOld.Entries {
		_, err := api.GetObject(baseParams, bckEC, obj.Name, nil)
		tassert.CheckError(t, err)
	}
	for _, obj := range resRegOld.Entries {
		_, err := api.GetObject(baseParams, bckReg, obj.Name, nil)
		tassert.CheckError(t, err)
	}
}

// Simple resilver for EC bucket
//  1. Create a bucket
//  2. Remove mpath from one target
//  3. Creates enough objects to have at least one per mpath
//     So, minimal is <target count>*<mpath count>*2.
//     For tests 100 looks good
//  4. Attach removed mpath
//  5. Wait for rebalance to finish
//  6. Check that all objects returns the non-zero number of Data and Parity
//     slices in HEAD response
//  7. Extra check: the number of objects after rebalance equals initial number
func TestECResilver(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-ec-resilver",
			Provider: apc.AIS,
		}
		proxyURL = tools.RandomProxyURL()
	)
	o := ecOptions{
		objCount:    100,
		concurrency: 8,
		pattern:     "obj-reb-loc-%04d",
		silent:      true,
	}.init(t, proxyURL)
	initMountpaths(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.parity+test.data {
				t.Skip(cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			ecResilver(t, o, proxyURL, bck)
		})
	}
}

func ecResilver(t *testing.T, o *ecOptions, proxyURL string, bck cmn.Bck) {
	baseParams := tools.BaseAPIParams(proxyURL)

	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

	tgtList := o.smap.Tmap.ActiveNodes()
	tgtLost := tgtList[0]
	lostFSList, err := api.GetMountpaths(baseParams, tgtLost)
	tassert.CheckFatal(t, err)
	if len(lostFSList.Available) < 2 {
		t.Fatalf("%s has only %d mountpaths, required 2 or more", tgtLost.ID(), len(lostFSList.Available))
	}
	lostPath := lostFSList.Available[0]
	err = api.DetachMountpath(baseParams, tgtLost, lostPath, false /*dont-resil*/)
	tassert.CheckFatal(t, err)
	time.Sleep(time.Second)

	wg := sync.WaitGroup{}

	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		go func(i int) {
			defer wg.Done()
			objName := fmt.Sprintf(o.pattern, i)
			createECObject(t, baseParams, bck, objName, i, o)
		}(i)
	}
	wg.Wait()
	tlog.Logf("Created %d objects\n", o.objCount)

	err = api.AttachMountpath(baseParams, tgtLost, lostPath, false /*force*/)
	tassert.CheckFatal(t, err)
	// loop above may fail (even if AddMountpath works) and mark a test failed
	if t.Failed() {
		t.FailNow()
	}

	tools.WaitForResilvering(t, baseParams, nil)

	msg := &apc.LsoMsg{Props: apc.GetPropsSize}
	resEC, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	tlog.Logf("%d objects in %s after rebalance\n", len(resEC.Entries), bck)
	if len(resEC.Entries) != o.objCount {
		t.Errorf("Expected %d objects after rebalance, found %d", o.objCount, len(resEC.Entries))
	}

	for i := 0; i < o.objCount; i++ {
		objName := ecTestDir + fmt.Sprintf(o.pattern, i)
		props, err := api.HeadObject(baseParams, bck, objName, apc.FltPresent)
		if err != nil {
			t.Errorf("HEAD for %s failed: %v", objName, err)
		} else if props.EC.DataSlices == 0 || props.EC.ParitySlices == 0 {
			t.Errorf("%s has not EC info", objName)
		}
	}
}

// 1. Create bucket
// 2. Choose 2 random nodes, unregister the first one
// 3. Put N objects to EC-enabled bucket
// 4. Register the target back, rebalance kicks in
// 5. Start reading objects in a loop (nothing should fail)
// 6. Unregister the second target while rebalance is running
// 7. Wait until rebalance finishes (if any is running)
// 8. Stop reading loop and read all objects once more (nothing should fail)
// 9. Get the number of objects in the bucket (must be the same as at start)
func TestECAndRegularUnregisterWhileRebalancing(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true, RequiredDeployment: tools.ClusterTypeLocal})

	var (
		bckEC = cmn.Bck{
			Name:     testBucketName + "-EC",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		o          = ecOptions{
			minTargets:  5,
			objCount:    300,
			concurrency: 8,
			pattern:     "obj-reb-chk-%04d",
			silent:      true,
		}.init(t, proxyURL)
	)

	initMountpaths(t, proxyURL)
	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.parity+test.data+1 {
				t.Skip(cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			newLocalBckWithProps(t, baseParams, bckEC, defaultECBckProps(o), o)
			defer tools.WaitForRebalAndResil(t, baseParams)
			ecAndRegularUnregisterWhileRebalancing(t, o, bckEC)

			// Make sure that the next test gets accurate (without any intermediate modifications) smap.
			o.smap = tools.GetClusterMap(t, proxyURL)
		})
	}
}

func ecAndRegularUnregisterWhileRebalancing(t *testing.T, o *ecOptions, bckEC cmn.Bck) {
	const startTimeout = 10 * time.Second
	var (
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		smap       = o.smap
	)
	// select a target that loses its mpath(simulate drive death),
	// and that has mpaths changed (simulate mpath added)
	tgtList := smap.Tmap.ActiveNodes()
	tgtLost := tgtList[0]
	tgtGone := tgtList[1]

	tlog.Logf("Put %s in maintenance (no rebalance)\n", tgtLost.StringEx())
	args := &apc.ActValRmNode{DaemonID: tgtLost.ID(), SkipRebalance: true}
	_, err := api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	_, err = tools.WaitForClusterState(proxyURL, "target removed",
		smap.Version, smap.CountActivePs(), smap.CountActiveTs()-1)
	tassert.CheckFatal(t, err)
	registered := false

	// FIXME: There are multiple defers calling JoinCluster, and it's very unclear what will happen when.
	// This is the first defer, so it will be called last. Hence, we wait for rebalance to complete here.
	// See: https://blog.golang.org/defer-panic-and-recover
	defer func() {
		if !registered {
			args := &apc.ActValRmNode{DaemonID: tgtLost.ID()}
			rebID, err := api.StopMaintenance(baseParams, args)
			tassert.CheckError(t, err)
			tools.WaitForRebalanceByID(t, -1 /*orig target cnt*/, baseParams, rebID, rebalanceTimeout)
		}
	}()

	// fill EC bucket
	wg := sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		go func(i int) {
			defer wg.Done()
			objName := fmt.Sprintf(o.pattern, i)
			createECObject(t, baseParams, bckEC, objName, i, o)
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	msg := &apc.LsoMsg{}
	resECOld, err := api.ListObjects(baseParams, bckEC, msg, 0)
	tassert.CheckFatal(t, err)
	tlog.Logf("Created %d objects in %s - starting global rebalance...\n", len(resECOld.Entries), bckEC)

	tlog.Logf("Take %s out of maintenance\n", tgtLost.StringEx())
	args = &apc.ActValRmNode{DaemonID: tgtLost.ID()}
	_, err = api.StopMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	registered = true

	stopCh := cos.NewStopCh()
	wg.Add(1)
	defer func() {
		stopCh.Close()
		wg.Wait()
	}()
	go func() {
		defer wg.Done()
		for {
			for _, obj := range resECOld.Entries {
				_, err := api.GetObject(baseParams, bckEC, obj.Name, nil)
				tassert.CheckError(t, err)
				select {
				case <-stopCh.Listen():
					return
				default:
				}
				time.Sleep(time.Millisecond) // do not flood targets...
			}
		}
	}()
	xargs := xact.ArgsMsg{Kind: apc.ActRebalance, Timeout: startTimeout}
	err = api.WaitForXactionNode(baseParams, xargs, xactSnapRunning)
	tassert.CheckError(t, err)

	err = api.AbortXaction(baseParams, xargs)
	tassert.CheckError(t, err)
	tools.WaitForRebalAndResil(t, baseParams, rebalanceTimeout)
	tassert.CheckError(t, err)

	tlog.Logf("Put %s in maintenance\n", tgtGone.StringEx())
	args = &apc.ActValRmNode{DaemonID: tgtGone.ID()}
	rebID, err := api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	defer func() {
		args = &apc.ActValRmNode{DaemonID: tgtGone.ID()}
		rebID, _ := api.StopMaintenance(baseParams, args)
		tools.WaitForRebalanceByID(t, -1 /*orig target cnt*/, baseParams, rebID)
	}()

	stopCh.Close()

	tassert.CheckFatal(t, err)
	tools.WaitForRebalanceByID(t, -1 /*orig target cnt*/, baseParams, rebID, rebalanceTimeout)
	tlog.Logln("Reading objects")
	for _, obj := range resECOld.Entries {
		_, err := api.GetObject(baseParams, bckEC, obj.Name, nil)
		tassert.CheckError(t, err)
	}
	tlog.Logln("Getting the number of objects after rebalance")
	resECNew, err := api.ListObjects(baseParams, bckEC, msg, 0)
	tassert.CheckFatal(t, err)
	tlog.Logf("%d objects in %s after rebalance\n",
		len(resECNew.Entries), bckEC)
	if len(resECNew.Entries) != len(resECOld.Entries) {
		t.Errorf("The number of objects before and after rebalance mismatches")
	}

	tlog.Logln("Test object readability after rebalance")
	for _, obj := range resECOld.Entries {
		_, err := api.GetObject(baseParams, bckEC, obj.Name, nil)
		tassert.CheckError(t, err)
	}

	tlog.Logln("Getting the number of objects after reading")
	resECNew, err = api.ListObjects(baseParams, bckEC, msg, 0)
	tassert.CheckFatal(t, err)
	tlog.Logf("%d objects in %s after reading\n",
		len(resECNew.Entries), bckEC)
	if len(resECNew.Entries) != len(resECOld.Entries) {
		t.Errorf("Incorrect number of objects: %d (expected %d)",
			len(resECNew.Entries), len(resECOld.Entries))
	}
}

// The test only checks that the number of object after rebalance equals
// the number of objects before it
func ecMountpaths(t *testing.T, o *ecOptions, proxyURL string, bck cmn.Bck) {
	type removedMpath struct {
		si    *cluster.Snode
		mpath string
	}
	baseParams := tools.BaseAPIParams(proxyURL)
	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

	wg := sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		go func(i int) {
			defer wg.Done()
			objName := fmt.Sprintf(o.pattern, i)
			createECObject(t, baseParams, bck, objName, i, o)
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	msg := &apc.LsoMsg{Props: apc.GetPropsSize}
	objList, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	tlog.Logf("%d objects created, removing %d mountpaths\n", len(objList.Entries), o.parityCnt)

	allMpaths := tools.GetTargetsMountpaths(t, o.smap, baseParams)
	removed := make(map[string]*removedMpath, o.parityCnt)
	defer func() {
		for _, rmMpath := range removed {
			err := api.AttachMountpath(baseParams, rmMpath.si, rmMpath.mpath, true /*force*/)
			tassert.CheckError(t, err)
		}
		tools.WaitForResilvering(t, baseParams, nil)
	}()
	// Choose `parity` random mpaths and disable them
	i := 0
	for tsi, paths := range allMpaths {
		mpath := paths[rand.Intn(len(paths))]
		uid := tsi.ID() + "/" + mpath
		if _, ok := removed[uid]; ok {
			continue
		}
		err := api.DetachMountpath(baseParams, tsi, mpath, true /*dont-resil*/)
		tassert.CheckFatal(t, err)
		rmMpath := &removedMpath{si: tsi, mpath: mpath}
		removed[uid] = rmMpath
		i++
		tlog.Logf("%d. Disabled %s : %s\n", i, tsi.StringEx(), mpath)
		if i >= o.parityCnt {
			break
		}
	}

	for _, en := range objList.Entries {
		_, err := api.GetObject(baseParams, bck, en.Name, nil)
		tassert.CheckError(t, err)
	}
}

// Test EC metadata versioning.
func TestECGenerations(t *testing.T) {
	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-obj-gens",
			Provider: apc.AIS,
		}
		proxyURL    = tools.RandomProxyURL()
		baseParams  = tools.BaseAPIParams(proxyURL)
		generations = 3
	)

	o := ecOptions{
		minTargets:  4,
		objCount:    10,
		concurrency: 4,
		pattern:     "obj-gen-%04d",
		silent:      testing.Short(),
	}.init(t, proxyURL)
	initMountpaths(t, proxyURL)
	lastWrite := make([]int64, o.objCount)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.parity+test.data {
				t.Skip(cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

			wg := sync.WaitGroup{}
			for gen := 0; gen < generations; gen++ {
				wg.Add(o.objCount)
				for i := 0; i < o.objCount; i++ {
					o.sema.Acquire()
					go func(i, gen int) {
						defer func() {
							o.sema.Release()
							wg.Done()
						}()
						objName := fmt.Sprintf(o.pattern, i)
						createDamageRestoreECFile(t, baseParams, bck, objName, i, o)
						if gen == generations-2 {
							lastWrite[i] = mono.NanoTime()
						}
					}(i, gen)
				}
				wg.Wait()
			}

			currentTime := mono.NanoTime()
			for i := 0; i < o.objCount; i++ {
				objName := ecTestDir + fmt.Sprintf(o.pattern, i)
				props, err := api.HeadObject(baseParams, bck, objName, apc.FltPresent)
				tassert.CheckError(t, err)
				if err == nil && props.EC.Generation > lastWrite[i] && props.EC.Generation < currentTime {
					t.Errorf("Object %s, generation %d expected between %d and %d",
						objName, props.EC.Generation, lastWrite[i], currentTime)
				}
			}
		})
	}
}
