// Package integration_test.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/sys"
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
	seed         int64
	objSize      int64
	objSizeLimit int64
	concurrency  int
	objCount     int
	dataCnt      int
	parityCnt    int
	minTargets   int
	pattern      string
	sema         *cos.DynSemaphore
	silent       bool
	rnd          *rand.Rand
	smap         *meta.Smap
}

// Initializes the EC options, validates the number of targets.
// If initial dataCnt value is negative, it sets the number of data and
// parity slices to maximum possible for the cluster.
func (o *ecOptions) init(t *testing.T, proxyURL string) {
	o.smap = tools.GetClusterMap(t, proxyURL)
	if cnt := o.smap.CountActiveTs(); cnt < o.minTargets {
		t.Skipf("not enough targets in the cluster: expected at least %d, got %d", o.minTargets, cnt)
	}
	if o.concurrency > 0 {
		o.sema = cos.NewDynSemaphore(o.concurrency)
	}
	o.seed = time.Now().UnixNano()
	o.rnd = rand.New(cos.NewRandSource(uint64(o.seed)))
	if o.dataCnt < 0 {
		total := o.smap.CountActiveTs() - 2
		o.parityCnt = total / 2
		o.dataCnt = total - o.parityCnt
	}
}

func (o *ecOptions) sliceTotal() int {
	if o.objSizeLimit == cmn.ObjSizeToAlwaysReplicate {
		return 0
	}
	return o.dataCnt + o.parityCnt
}

type ecTest struct {
	name         string
	objSizeLimit int64
	data         int
	parity       int
}

var ecTests = []ecTest{
	{"EC 1:1", cmn.ObjSizeToAlwaysReplicate, 1, 1},
	{"EC 1:1", ecObjLimit, 1, 1},
	{"EC 1:2", ecObjLimit, 1, 2},
	{"EC 2:2", ecObjLimit, 2, 2},
}

func defaultECBckProps(o *ecOptions) *cmn.BpropsToSet {
	return &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{
			Enabled:      apc.Ptr(true),
			ObjSizeLimit: apc.Ptr[int64](ecObjLimit),
			DataSlices:   apc.Ptr(o.dataCnt),
			ParitySlices: apc.Ptr(o.parityCnt),
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
		ct, err := core.NewCTFromFQN(fqn, nil)
		tassert.CheckFatal(t, err)
		if !strings.Contains(ct.ObjectName(), objName) {
			return nil
		}
		stat, err := os.Stat(fqn)
		if err != nil {
			if cos.IsNotExist(err) {
				return nil
			}
			return err
		}
		foundParts[fqn] = ecSliceMD{stat.Size()}
		if ct.ContentType() == fs.ObjCT && oldest.After(stat.ModTime()) {
			main = fqn
			oldest = stat.ModTime()
		}
		return nil
	}

	fs.WalkBck(&fs.WalkBckOpts{
		WalkOpts: fs.WalkOpts{
			Bck:      bck,
			CTs:      []string{fs.ECSliceCT, fs.ECMetaCT, fs.ObjCT},
			Callback: cb,
			Sorted:   true, // false is unsupported and asserts
		},
	})

	return foundParts, main
}

func ecCheckSlices(t *testing.T, sliceList map[string]ecSliceMD,
	bck cmn.Bck, objPath string, objSize, sliceSize int64, totalCnt int) {
	tassert.Errorf(t, len(sliceList) == totalCnt, "Expected number of objects for %s/%s: %d, found: %d\n%+v",
		bck.String(), objPath, totalCnt, len(sliceList), sliceList)

	var metaCnt int
	for k, md := range sliceList {
		ct, err := core.NewCTFromFQN(k, nil)
		tassert.CheckFatal(t, err)

		switch {
		case ct.ContentType() == fs.ECMetaCT:
			metaCnt++
			tassert.Errorf(t, md.size <= 4*cos.KiB, "Metafile %q size is too big: %d", k, md.size)
		case ct.ContentType() == fs.ECSliceCT:
			tassert.Errorf(t, md.size == sliceSize, "Slice %q size mismatch: %d, expected %d", k, md.size, sliceSize)
		default:
			tassert.Errorf(t, ct.ContentType() == fs.ObjCT, "invalid content type %s, expected: %s", ct.ContentType(), fs.ObjCT)
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
				ct, err := core.NewCTFromFQN(nm, nil)
				tassert.CheckFatal(t, err)
				if doEC {
					if ct.ContentType() == fs.ECSliceCT {
						if md.size != sliceSize {
							same = false
							break
						}
					}
				} else {
					if ct.ContentType() == fs.ObjCT {
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
		doEC = o.objSizeLimit != cmn.ObjSizeToAlwaysReplicate && o.objSize >= o.objSizeLimit
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
	objSize = int64(ecMinBigSize + o.rnd.IntN(ecBigDelta))
	sliceSize = ec.SliceSize(objSize, o.dataCnt)
	if (n+1)%every == 0 || o.objSizeLimit == cmn.ObjSizeToAlwaysReplicate {
		// Small object case
		// full object copy+meta: 1+1
		// number of metafiles: parity
		// number of slices: parity
		totalCnt = 2 + o.parityCnt*2
		objSize = int64(ecMinSmallSize + o.rnd.IntN(ecSmallDelta))
		sliceSize = objSize
	}
	doEC = objSize >= o.objSizeLimit
	if o.objSizeLimit == cmn.ObjSizeToAlwaysReplicate {
		doEC = false
		totalCnt = 2 + o.parityCnt*2
	}
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
	sizesCh := make(chan int64, o.objCount)

	for idx := range o.objCount {
		wg.Add(1)
		o.sema.Acquire()

		go func(i int) {
			totalCnt, objSize, sliceSize, doEC := randObjectSize(i, smallEvery, o)
			sizesCh <- objSize
			objName := fmt.Sprintf(objPatt, bck.Name, i)
			objPath := ecTestDir + objName

			if i%10 == 0 {
				if doEC {
					tlog.Logfln("Object %s, size %9d[%9d]", objName, objSize, sliceSize)
				} else {
					tlog.Logfln("Object %s, size %9d[%9s]", objName, objSize, "-")
				}
			}

			r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
			defer func() {
				r.Close()
				o.sema.Release()
				wg.Done()
			}()
			tassert.CheckFatal(t, err)
			putArgs := api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r}
			_, err = api.PutObject(&putArgs)
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
				ct, err := core.NewCTFromFQN(k, nil)
				tassert.CheckFatal(t, err)

				switch {
				case ct.ContentType() == fs.ECMetaCT:
					metaCnt++
					tassert.Errorf(t, md.size <= 512, "Metafile %q size is too big: %d", k, md.size)
				case ct.ContentType() == fs.ECSliceCT:
					sliceCnt++
					if md.size != sliceSize && doEC {
						t.Errorf("Slice %q size mismatch: %d, expected %d", k, md.size, sliceSize)
					}
					if md.size != objSize && !doEC {
						t.Errorf("Copy %q size mismatch: %d, expected %d", k, md.size, objSize)
					}
				default:
					tassert.Errorf(t, ct.ContentType() == fs.ObjCT, "invalid content type %s, expected: %s", ct.ContentType(), fs.ObjCT)
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
				t.Error("Full copy is not found")
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
	close(sizesCh)

	szTotal := int64(0)
	szLen := len(sizesCh)
	for sz := range sizesCh {
		szTotal += sz
	}
	if szLen != 0 {
		t.Logf("Average size of the bucket %s: %s\n", bck.String(), cos.IEC(szTotal/int64(szLen), 1))
	}
}

func assertBucketSize(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objCount int) {
	bckObjectsCnt := bucketSize(t, baseParams, bck)
	tassert.Fatalf(t, bckObjectsCnt == objCount, "Invalid number of objects: %d, expected %d", bckObjectsCnt, objCount)
}

func bucketSize(t *testing.T, baseParams api.BaseParams, bck cmn.Bck) int {
	msg := &apc.LsoMsg{Props: "size,status"}
	lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	return len(lst.Entries)
}

func putRandomFile(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objPath string, size int) {
	r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: int64(size), CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)
	_, err = api.PutObject(&api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    objPath,
		Reader:     r,
	})
	tassert.CheckFatal(t, err)
}

func newLocalBckWithProps(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, bckProps *cmn.BpropsToSet, o *ecOptions) {
	proxyURL := tools.RandomProxyURL()
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	tlog.Logfln("Changing EC %d:%d, objLimit [%d] [ seed = %d ], concurrent: %d",
		o.dataCnt, o.parityCnt, o.objSizeLimit, o.seed, o.concurrency)
	_, err := api.SetBucketProps(baseParams, bck, bckProps)
	tassert.CheckFatal(t, err)
}

func setBucketECProps(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, bckProps *cmn.BpropsToSet) {
	tlog.Logfln("Changing EC %d:%d", *bckProps.EC.DataSlices, *bckProps.EC.ParitySlices)
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
	for idx := range o.objCount {
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
	api.WaitForSnapsIdle(tools.BaseAPIParams(proxyURL), &reqArgs)
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
	for i := range objCount {
		objName := fmt.Sprintf(objPatt, i)
		go getOneObj(objName)
	}
	wg.Wait()
}

// Simulates damaged slice by changing slice's checksum in metadata file
func damageMetadataCksum(t *testing.T, slicePath string) {
	ct, err := core.NewCTFromFQN(slicePath, nil)
	tassert.CheckFatal(t, err)
	metaFQN := ct.GenFQN(fs.ECMetaCT)
	md, err := ec.LoadMetadata(metaFQN)
	tassert.CheckFatal(t, err)
	md.CksumValue = "01234"
	err = jsp.Save(metaFQN, md, jsp.Plain(), nil)
	tassert.CheckFatal(t, err)
}

// Short test to make sure that EC options cannot be changed after
// EC is enabled
func TestECChange(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 3})

	var (
		proxyURL = tools.RandomProxyURL()
		bck      = cmn.Bck{
			Name:     testBucketName + "-ec-change",
			Provider: apc.AIS,
		}
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	bucketProps := &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{
			Enabled:      apc.Ptr(true),
			ObjSizeLimit: apc.Ptr[int64](ecObjLimit),
			DataSlices:   apc.Ptr(1),
			ParitySlices: apc.Ptr(1),
		},
	}
	baseParams := tools.BaseAPIParams(proxyURL)

	tlog.Logln("Resetting bucket properties")
	_, err := api.ResetBucketProps(baseParams, bck)
	tassert.CheckFatal(t, err)

	tlog.Logln("Trying to set too many slices")
	bucketProps.EC.DataSlices = apc.Ptr(25)
	bucketProps.EC.ParitySlices = apc.Ptr(25)
	_, err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.Errorf(t, err != nil, "Enabling EC must fail in case of the number of targets fewer than the number of slices")

	tlog.Logln("Enabling EC")
	bucketProps.EC.DataSlices = apc.Ptr(1)
	bucketProps.EC.ParitySlices = apc.Ptr(1)
	_, err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.CheckFatal(t, err)

	tlog.Logln("Trying to set EC options to the same values")
	_, err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.CheckFatal(t, err)

	tlog.Logln("Trying to disable EC")
	bucketProps.EC.Enabled = apc.Ptr(false)
	_, err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.Errorf(t, err == nil, "Disabling EC failed: %v", err)

	tlog.Logln("Trying to re-enable EC")
	bucketProps.EC.Enabled = apc.Ptr(true)
	_, err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.Errorf(t, err == nil, "Enabling EC failed: %v", err)

	tlog.Logln("Trying to modify EC options when EC is enabled")
	bucketProps.EC.Enabled = apc.Ptr(true)
	bucketProps.EC.ObjSizeLimit = apc.Ptr[int64](300000)
	_, err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.Errorf(t, err != nil, "Modifying EC properties must fail")

	tlog.Logln("Resetting bucket properties")
	_, err = api.ResetBucketProps(baseParams, bck)
	tassert.Errorf(t, err == nil, "Resetting properties should work")
}

func createECReplicas(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objName string, o *ecOptions) {
	o.sema.Acquire()
	defer o.sema.Release()

	totalCnt := 2 + o.parityCnt*2
	objSize := int64(ecMinSmallSize + o.rnd.IntN(ecSmallDelta))
	sliceSize := objSize

	objPath := ecTestDir + objName

	tlog.Logfln("Creating %s, size %8d", objPath, objSize)
	r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)
	_, err = api.PutObject(&api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r})
	tassert.CheckFatal(t, err)

	tlog.Logfln("waiting for %s", objPath)
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
	r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)
	_, err = api.PutObject(&api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r})
	tassert.CheckFatal(t, err)

	tlog.LogfCond(!o.silent, "waiting for %s\n", objPath)
	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objPath)

	ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)
	if mainObjPath == "" {
		t.Error("Full copy is not found")
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
	if o.dataCnt+o.parityCnt > 2 && o.rnd.IntN(100) < sliceDelPct {
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
	r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)
	_, err = api.PutObject(&api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r})
	tassert.CheckFatal(t, err)

	tlog.LogfCond(!o.silent, "waiting for %s\n", objPath)
	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objPath)

	ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)
	if mainObjPath == "" {
		t.Error("Full copy is not found")
		return
	}

	tlog.LogfCond(!o.silent, "Damaging %s [removing %s]\n", objPath, mainObjPath)
	tassert.CheckFatal(t, os.Remove(mainObjPath))

	ct, err := core.NewCTFromFQN(mainObjPath, nil)
	tassert.CheckFatal(t, err)
	metafile := ct.GenFQN(fs.ECMetaCT)
	tlog.LogfCond(!o.silent, "Damaging %s [removing %s]\n", objPath, metafile)
	tassert.CheckFatal(t, cos.RemoveFile(metafile))
	if delSlice {
		sliceToDel := ""
		for k := range foundParts {
			ct, err := core.NewCTFromFQN(k, nil)
			tassert.CheckFatal(t, err)
			if k != mainObjPath && ct.ContentType() == fs.ECSliceCT && doEC {
				sliceToDel = k
				break
			} else if k != mainObjPath && ct.ContentType() == fs.ObjCT && !doEC {
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

		ct, err := core.NewCTFromFQN(sliceToDel, nil)
		tassert.CheckFatal(t, err)
		metafile := ct.GenFQN(fs.ECMetaCT)
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
		tlog.Logfln("Files are not deleted [%d - %d], leftovers:", len(foundParts), len(partsAfterRemove))
		for k := range partsAfterRemove {
			tlog.Logfln("     %s", k)
		}
		// Not an error as a directory can contain leftovers
		tlog.Logln("Some slices were not deleted")
		return
	}

	tlog.LogfCond(!o.silent, "Restoring %s\n", objPath)
	_, err = api.GetObject(baseParams, bck, objPath, nil)
	if err != nil {
		tlog.Logfln("... retrying %s", objPath)
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

	o := &ecOptions{
		minTargets:   4,
		objCount:     25,
		concurrency:  8,
		pattern:      "obj-rest-remote-%04d",
		objSizeLimit: ecObjLimit,
	}
	o.init(t, proxyURL)

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: bck})

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
						"ec.disk_only": strconv.FormatBool(useDisk),
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
				o.objSizeLimit = test.objSizeLimit
				setBucketECProps(t, baseParams, bck, defaultECBckProps(o))
				defer api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{
					EC: &cmn.ECConfToSet{Enabled: apc.Ptr(false)},
				})

				defer func() {
					tlog.Logln("Wait for PUTs to finish...")
					args := xact.ArgsMsg{Kind: apc.ActECPut}
					err := api.WaitForSnapsIdle(baseParams, &args)
					tassert.CheckError(t, err)

					clearAllECObjects(t, bck, true, o)
					reqArgs := xact.ArgsMsg{Kind: apc.ActECPut, Bck: bck}
					err = api.WaitForSnapsIdle(baseParams, &reqArgs)
					tassert.CheckError(t, err)
				}()

				wg := sync.WaitGroup{}
				wg.Add(o.objCount)
				for i := range o.objCount {
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

	o := &ecOptions{
		minTargets:  4,
		objCount:    50,
		concurrency: 8,
		pattern:     "obj-rest-%04d",
		silent:      testing.Short(),
	}
	o.init(t, proxyURL)
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
						"ec.disk_only": strconv.FormatBool(useDisk),
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
				o.objSizeLimit = test.objSizeLimit
				newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

				wg := sync.WaitGroup{}
				wg.Add(o.objCount)
				for i := range o.objCount {
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

	r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
	if err != nil {
		return err
	}
	_, err = api.PutObject(&api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    objPath,
		Reader:     r,
	})
	return err
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

	o := &ecOptions{
		minTargets:   4,
		dataCnt:      1,
		parityCnt:    1,
		pattern:      "obj-cksum-%04d",
		objSizeLimit: ecObjLimit,
	}
	o.init(t, proxyURL)
	baseParams := tools.BaseAPIParams(proxyURL)
	initMountpaths(t, proxyURL)

	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

	objName1 := fmt.Sprintf(o.pattern, 1)
	objPath1 := ecTestDir + objName1
	foundParts1, mainObjPath1 := createECFile(t, baseParams, bck, objName1, o)

	objName2 := fmt.Sprintf(o.pattern, 2)
	objPath2 := ecTestDir + objName2
	foundParts2, mainObjPath2 := createECFile(t, baseParams, bck, objName2, o)

	tlog.Logfln("Removing main object %s", mainObjPath1)
	tassert.CheckFatal(t, os.Remove(mainObjPath1))

	// Corrupt just one slice, EC should be able to restore the original object
	for k := range foundParts1 {
		ct, err := core.NewCTFromFQN(k, nil)
		tassert.CheckFatal(t, err)

		if k != mainObjPath1 && ct.ContentType() == fs.ECSliceCT {
			damageMetadataCksum(t, k)
			break
		}
	}

	_, err := api.GetObject(baseParams, bck, objPath1, nil)
	tassert.CheckFatal(t, err)

	tlog.Logfln("Removing main object %s", mainObjPath2)
	tassert.CheckFatal(t, os.Remove(mainObjPath2))

	// Corrupt all slices, EC should not be able to restore
	for k := range foundParts2 {
		ct, err := core.NewCTFromFQN(k, nil)
		tassert.CheckFatal(t, err)

		if k != mainObjPath2 && ct.ContentType() == fs.ECSliceCT {
			damageMetadataCksum(t, k)
		}
	}

	_, err = api.GetObject(baseParams, bck, objPath2, nil)
	tassert.Fatalf(t, err != nil, "Object should not be restored when checksums are wrong")
}

func TestECEnabledDisabledEnabled(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-ec-props",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	o := &ecOptions{
		minTargets:   4,
		dataCnt:      1,
		parityCnt:    1,
		objCount:     25,
		concurrency:  8,
		pattern:      "obj-rest-%04d",
		objSizeLimit: ecObjLimit,
	}
	o.init(t, proxyURL)

	initMountpaths(t, proxyURL)
	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

	// End of preparation, create files with EC enabled, check if are restored properly

	wg := sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := range o.objCount {
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
	_, err := api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{Enabled: apc.Ptr(false)},
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
	_, err = api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{Enabled: apc.Ptr(true)},
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
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-ec-load",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	o := &ecOptions{
		minTargets:   4,
		dataCnt:      1,
		parityCnt:    1,
		objCount:     5,
		concurrency:  8,
		pattern:      "obj-disable-enable-load-%04d",
		objSizeLimit: ecObjLimit,
	}
	o.init(t, proxyURL)

	initMountpaths(t, proxyURL)
	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)
	// End of preparation, create files with EC enabled, check if are restored properly

	wg := &sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := range o.objCount {
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

	tlog.Logfln("Disabling EC for the bucket %s", bck.String())
	_, err := api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{Enabled: apc.Ptr(false)},
	})
	tassert.CheckError(t, err)

	time.Sleep(15 * time.Millisecond)
	tlog.Logfln("Enabling EC for the bucket %s", bck.String())
	_, err = api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{Enabled: apc.Ptr(true)},
	})
	tassert.CheckError(t, err)
	reqArgs := xact.ArgsMsg{Kind: apc.ActECEncode, Bck: bck}
	_, err = api.WaitForXactionIC(baseParams, &reqArgs)
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
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-ec-stress",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	o := &ecOptions{
		minTargets:  4,
		objCount:    400,
		concurrency: 12,
		pattern:     "obj-stress-%04d",
	}
	o.init(t, proxyURL)
	initMountpaths(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.data+test.parity {
				t.Skip(cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			o.objSizeLimit = test.objSizeLimit
			newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)
			doECPutsAndCheck(t, baseParams, bck, o)

			msg := &apc.LsoMsg{Props: "size,status"}
			lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, len(lst.Entries) == o.objCount,
				"Invalid number of objects: %d, expected %d", len(lst.Entries), o.objCount)
		})
	}
}

// Stress 2 buckets at the same time
func TestECStressManyBuckets(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

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

	o1 := &ecOptions{
		minTargets:   4,
		parityCnt:    1,
		dataCnt:      1,
		objCount:     200,
		concurrency:  12,
		pattern:      "obj-stress-manybck-%04d",
		objSizeLimit: ecObjLimit,
	}
	o1.init(t, proxyURL)
	o2 := &ecOptions{
		minTargets:   4,
		parityCnt:    1,
		dataCnt:      1,
		objCount:     200,
		concurrency:  12,
		pattern:      "obj-stress-manybck-%04d",
		objSizeLimit: ecObjLimit,
	}
	o2.init(t, proxyURL)

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
	lst, err := api.ListObjects(baseParams, bck1, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == o1.objCount, "Bucket %s: Invalid number of objects: %d, expected %d", bck1.String(), len(lst.Entries), o1.objCount)

	msg = &apc.LsoMsg{Props: "size,status"}
	lst, err = api.ListObjects(baseParams, bck2, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == o2.objCount, "Bucket %s: Invalid number of objects: %d, expected %d", bck2.String(), len(lst.Entries), o2.objCount)
}

// ExtraStress test to check that EC works as expected
//   - Changes bucket props to use EC
//   - Generates `objCount` objects, size between `ecObjMinSize` and `ecObjMinSize`+ecObjMaxSize`
//   - Objects smaller `ecObjLimit` must be copies, while others must be EC'ed
//   - PUTs ALL objects to the bucket stressing both EC and transport
//   - filepath.Walk checks that the number of metafiles at the end is correct
//   - No errors must occur
func TestECExtraStress(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

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

	o := &ecOptions{
		minTargets:  4,
		objCount:    400,
		concurrency: 12,
		pattern:     objStart + "%04d",
	}
	o.init(t, proxyURL)
	initMountpaths(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.data+test.parity {
				t.Skip(cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			o.objSizeLimit = test.objSizeLimit
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
	for idx := range o.objCount {
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
				tlog.Logfln("Object %s, size %9d[%9d]", objName, objSize, sliceSize)
			} else {
				tlog.Logfln("Object %s, size %9d[%9s]", objName, objSize, "-")
			}
			r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
			tassert.Errorf(t, err == nil, "Failed to create reader: %v", err)
			putArgs := api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r}
			_, err = api.PutObject(&putArgs)
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
	lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == o.objCount, "Invalid number of objects: %d, expected %d", len(lst.Entries), o.objCount)
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

	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-attrs",
			Provider: apc.AIS,
		}
		proxyURL = tools.RandomProxyURL()
	)

	o := &ecOptions{
		minTargets:   4,
		dataCnt:      1,
		parityCnt:    1,
		objCount:     30,
		concurrency:  8,
		pattern:      "obj-xattr-%04d",
		objSizeLimit: ecObjLimit,
	}
	o.init(t, proxyURL)
	initMountpaths(t, proxyURL)

	baseParams := tools.BaseAPIParams(proxyURL)
	bckProps := defaultECBckProps(o)
	bckProps.Versioning = &cmn.VersionConfToSet{
		Enabled: apc.Ptr(true),
	}

	newLocalBckWithProps(t, baseParams, bck, bckProps, o)

	oneObj := func(idx int, objName string) {
		totalCnt, objSize, sliceSize, doEC := randObjectSize(idx, smallEvery, o)
		objPath := ecTestDir + objName
		ecStr, delStr := "-", "obj"
		if doEC {
			ecStr = "EC"
		}
		tlog.Logfln("Creating %s, size %8d [%2s] [%s]", objPath, objSize, ecStr, delStr)
		r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
		tassert.CheckFatal(t, err)
		_, err = api.PutObject(&api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r})
		tassert.CheckFatal(t, err)

		tlog.Logfln("waiting for %s", objPath)
		foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objPath)

		ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)
		if mainObjPath == "" {
			t.Fatal("Full copy is not found")
		}

		tlog.Logfln("Damaging %s [removing %s]", objPath, mainObjPath)
		tassert.CheckFatal(t, os.Remove(mainObjPath))

		ct, err := core.NewCTFromFQN(mainObjPath, nil)
		tassert.CheckFatal(t, err)
		metafile := ct.GenFQN(fs.ECMetaCT)
		tlog.Logfln("Damaging %s [removing %s]", objPath, metafile)
		tassert.CheckFatal(t, cos.RemoveFile(metafile))

		partsAfterRemove, _ := ecGetAllSlices(t, bck, objPath)
		_, ok := partsAfterRemove[mainObjPath]
		if ok || len(partsAfterRemove) != len(foundParts)-2 {
			t.Fatalf("Files are not deleted [%d - %d]: %#v", len(foundParts), len(partsAfterRemove), partsAfterRemove)
		}

		tlog.Logfln("Restoring %s", objPath)
		_, err = api.GetObject(baseParams, bck, objPath, nil)
		if err != nil {
			tlog.Logfln("... retrying %s", objPath)
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
	for range 2 {
		for i := range o.objCount {
			objName := fmt.Sprintf(o.pattern, i)
			oneObj(i, objName)
		}
	}

	msg := &apc.LsoMsg{Props: "size,status,version"}
	lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)

	// check that all returned objects and their repicas have the same version
	for _, e := range lst.Entries {
		if e.Version != finalVersion {
			t.Errorf("%s[status=%d] must have version %s but it is %s\n", e.Name, e.Flags, finalVersion, e.Version)
		}
	}

	if len(lst.Entries) != o.objCount {
		t.Fatalf("Invalid number of objects: %d, expected %d", len(lst.Entries), 1)
	}
}

// 1. start putting EC files into the cluster
// 2. in the middle of puts destroy bucket
// 3. wait for puts to finish
// 4. create bucket with the same name
// 5. check that EC is working properly for this bucket
func TestECDestroyBucket(t *testing.T) {
	t.Skipf("skipping %s", t.Name()) // TODO -- FIXME: revisit
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-DESTROY",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	o := &ecOptions{
		minTargets:   4,
		dataCnt:      1,
		parityCnt:    1,
		objCount:     100,
		concurrency:  10,
		pattern:      "obj-destroy-bck-%04d",
		objSizeLimit: ecObjLimit,
	}
	o.init(t, proxyURL)

	initMountpaths(t, proxyURL)
	bckProps := defaultECBckProps(o)
	newLocalBckWithProps(t, baseParams, bck, bckProps, o)

	wg := &sync.WaitGroup{}
	errCnt := atomic.NewInt64(0)
	sucCnt := atomic.NewInt64(0)

	for i := range o.objCount {
		o.sema.Acquire()
		wg.Add(1)
		go func(i int) {
			defer func() {
				o.sema.Release()
				wg.Done()
			}()

			objName := fmt.Sprintf(o.pattern, i)
			if i%10 == 0 {
				tlog.Logfln("PUT %s", bck.Cname(objName))
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

				tlog.Logfln("Destroying bucket %s", bck.String())
				tools.DestroyBucket(t, proxyURL, bck)
			}()
		}
	}

	wg.Wait()
	tlog.Logfln("EC put files resulted in error in %d out of %d files", errCnt.Load(), o.objCount)
	args := xact.ArgsMsg{Kind: apc.ActECPut}
	api.WaitForXactionIC(baseParams, &args)

	// create bucket with the same name and check if puts are successful
	newLocalBckWithProps(t, baseParams, bck, bckProps, o)
	doECPutsAndCheck(t, baseParams, bck, o)

	// check if get requests are successful
	msg := &apc.LsoMsg{Props: "size,status,version"}
	lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(lst.Entries) == o.objCount, "Invalid number of objects: %d, expected %d", len(lst.Entries), o.objCount)
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

	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-slice-emergency",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	o := &ecOptions{
		minTargets:   5,
		dataCnt:      -1,
		objCount:     100,
		concurrency:  12,
		pattern:      "obj-emt-%04d",
		objSizeLimit: ecObjLimit,
	}
	o.init(t, proxyURL)
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
		tlog.Logfln("Creating %s, size %8d [%2s]", objPath, objSize, ecStr)
		r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
		tassert.CheckFatal(t, err)
		_, err = api.PutObject(&api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r})
		tassert.CheckFatal(t, err)
		t.Logf("Object %s put in %v", objName, time.Since(start))
		start = time.Now()

		foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objPath)

		ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)
		if mainObjPath == "" {
			t.Error("Full copy is not found")
			return
		}
		t.Logf("Object %s EC in %v", objName, time.Since(start))
	}

	wg.Add(o.objCount)
	for i := range o.objCount {
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
		tools.WaitForRebalanceByID(t, baseParams, rebID)
	}()

	// 3. Read objects
	objectsExist(t, baseParams, bck, o.pattern, o.objCount)

	// 4. Check that ListObjects returns correct number of items
	tlog.Logln("Reading bucket list...")
	msg := &apc.LsoMsg{Props: "size,status,version"}
	lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(lst.Entries) == o.objCount, "Invalid number of objects: %d, expected %d", len(lst.Entries), o.objCount)
}

func TestECEmergencyTargetForReplica(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-replica-emergency",
			Provider: apc.AIS,
		}
		proxyURL = tools.RandomProxyURL()
	)

	o := &ecOptions{
		minTargets:   5,
		dataCnt:      -1,
		objCount:     50,
		concurrency:  8,
		pattern:      "obj-rest-%04d",
		objSizeLimit: ecObjLimit,
	}
	o.init(t, proxyURL)

	if o.smap.CountActiveTs() > 10 {
		// Reason: calculating main obj directory based on DeamonID
		// see getOneObj, 'HACK' annotation
		t.Skip("Test requires at most 10 targets")
	}
	initMountpaths(t, proxyURL)

	// Increase number of EC data slices, now there's just enough targets to handle EC requests
	// Encoding will fail if even one is missing, restoring should still work
	o.parityCnt++

	baseParams := tools.BaseAPIParams(proxyURL)
	bckProps := defaultECBckProps(o)
	newLocalBckWithProps(t, baseParams, bck, bckProps, o)

	wg := sync.WaitGroup{}

	// PUT object
	wg.Add(o.objCount)
	for i := range o.objCount {
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

	// kill #parity-slices of targets, normal EC restore won't be possible
	// 2. Kill a random target
	removedTargets := make(meta.Nodes, 0, o.parityCnt)
	smap := tools.GetClusterMap(t, proxyURL)

	for i := o.dataCnt - 1; i >= 0; i-- {
		var removedTarget *meta.Snode
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
		tools.WaitForRebalanceByID(t, baseParams, rebID)
	}()

	hasTarget := func(targets meta.Nodes, target *meta.Snode) bool {
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
		cbck := meta.NewBck(bck.Name, bck.Provider, cmn.NsGlobal)
		uname := cbck.MakeUname(ecTestDir + objName)
		targets, err := o.smap.HrwTargetList(cos.UnsafeSptr(uname), o.parityCnt+1)
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
	for i := range o.objCount {
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

// TODO -- FIXME: use runProviderTests (currently, only ais://)
func TestECEmergencyMountpath(t *testing.T) {
	const (
		smallEvery = 4
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-mpath-emergency",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	o := &ecOptions{
		minTargets:   5,
		dataCnt:      1,
		parityCnt:    1,
		objCount:     400,
		concurrency:  24,
		pattern:      "obj-em-mpath-%04d",
		objSizeLimit: ecObjLimit,
	}
	o.init(t, proxyURL)

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
		tlog.Logfln("Creating %s, size %8d [%2s]", objPath, objSize, ecStr)
		r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
		tassert.CheckFatal(t, err)
		_, err = api.PutObject(&api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r})
		tassert.CheckFatal(t, err)

		foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objPath)
		ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)
		if mainObjPath == "" {
			t.Error("Full copy is not found")
			return
		}
	}

	wg.Add(o.objCount)
	for i := range o.objCount {
		o.sema.Acquire()
		go putOneObj(i)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	// 2. Disable a random mountpath
	mpathID := o.rnd.IntN(len(mpathList.Available))
	removeMpath := mpathList.Available[mpathID]
	tlog.Logfln("Disabling a mountpath %s at target: %s", removeMpath, removeTarget.ID())
	err = api.DisableMountpath(baseParams, removeTarget, removeMpath, false /*dont-resil*/)
	tassert.CheckFatal(t, err)

	tools.WaitForResilvering(t, baseParams, removeTarget)

	defer func() {
		tlog.Logfln("Enabling mountpath %s at target %s...", removeMpath, removeTarget.ID())
		err = api.EnableMountpath(baseParams, removeTarget, removeMpath)
		tassert.CheckFatal(t, err)

		tools.WaitForResilvering(t, baseParams, removeTarget)
		ensureNumMountpaths(t, removeTarget, mpathList)
	}()

	// 3. Read objects
	objectsExist(t, baseParams, bck, o.pattern, o.objCount)

	// 4. Check that ListObjects returns correct number of items
	tlog.Logfln("DONE\nReading bucket list...")
	msg := &apc.LsoMsg{Props: "size,status,version"}
	lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	if len(lst.Entries) != o.objCount {
		t.Fatalf("Invalid number of objects: %d, expected %d", len(lst.Entries), o.objCount)
	}

	// Wait for ec to finish
	flt := xact.ArgsMsg{Kind: apc.ActECPut, Bck: bck}
	_ = api.WaitForSnapsIdle(baseParams, &flt)
}

func TestECRebalance(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, RequiredDeployment: tools.ClusterTypeLocal})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-ec-rebalance",
			Provider: apc.AIS,
		}
		proxyURL = tools.RandomProxyURL()
	)
	o := &ecOptions{
		objCount:     30,
		concurrency:  8,
		pattern:      "obj-reb-chk-%04d",
		silent:       true,
		objSizeLimit: ecObjLimit,
	}
	o.init(t, proxyURL)
	initMountpaths(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.parity+test.data+1 {
				t.Skip(cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			o.objSizeLimit = test.objSizeLimit
			ecOnlyRebalance(t, o, proxyURL, bck)
		})
	}
}

func TestECMountpaths(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-ec-mpaths",
			Provider: apc.AIS,
		}
		proxyURL = tools.RandomProxyURL()
	)
	o := &ecOptions{
		objCount:     30,
		concurrency:  8,
		pattern:      "obj-reb-mp-%04d",
		silent:       true,
		objSizeLimit: ecObjLimit,
	}
	o.init(t, proxyURL)
	initMountpaths(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.parity+test.data {
				t.Skipf("%s: %v", t.Name(), cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			o.objSizeLimit = test.objSizeLimit
			ecMountpaths(t, o, proxyURL, bck)
		})
	}

	reqArgs := xact.ArgsMsg{Kind: apc.ActECPut, Bck: bck}
	api.WaitForSnapsIdle(tools.BaseAPIParams(proxyURL), &reqArgs)
}

// The test only checks that the number of object after rebalance equals
// the number of objects before it
func ecOnlyRebalance(t *testing.T, o *ecOptions, proxyURL string, bck cmn.Bck) {
	baseParams := tools.BaseAPIParams(proxyURL)

	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

	wg := sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := range o.objCount {
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
	oldObjList, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tlog.Logfln("%d objects created, starting rebalance", len(oldObjList.Entries))

	removedTarget, err := o.smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	args := &apc.ActValRmNode{DaemonID: removedTarget.ID()}
	rebID, err := api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	defer func() {
		rebID, _ := tools.RestoreTarget(t, proxyURL, removedTarget)
		tools.WaitForRebalanceByID(t, baseParams, rebID)
	}()
	tools.WaitForRebalanceByID(t, baseParams, rebID)

	newObjList, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
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
	const (
		parityCnt = 2
		dataCnt   = 1
	)
	var (
		proxyURL = tools.RandomProxyURL()
		m        = ioContext{
			t:        t,
			num:      150,
			proxyURL: proxyURL,
			fileSize: cos.KiB,
			chunksConf: &ioCtxChunksConf{
				multipart: true,
				numChunks: 4, // will create 4 chunks
			},
		}
	)

	m.initAndSaveState(true /*cleanup*/)
	baseParams := tools.BaseAPIParams(proxyURL)

	if nt := m.smap.CountActiveTs(); nt < parityCnt+dataCnt+1 {
		t.Skipf("%s: not enough targets (%d): (d=%d, p=%d) requires at least %d",
			t.Name(), nt, dataCnt, parityCnt, parityCnt+dataCnt+1)
	}

	initMountpaths(t, proxyURL)
	tools.CreateBucket(t, proxyURL, m.bck, nil, true /*cleanup*/)

	m.puts()

	lst, err := api.ListObjects(baseParams, m.bck, nil, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tlog.Logfln("Object count: %d", len(lst.Entries))
	if len(lst.Entries) != m.num {
		t.Fatalf("list_objects %s invalid number of files %d, expected %d", m.bck.String(), len(lst.Entries), m.num)
	}

	tlog.Logfln("Enabling EC")
	bckPropsToUpate := &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{
			Enabled:      apc.Ptr(true),
			ObjSizeLimit: apc.Ptr[int64](1),
			DataSlices:   apc.Ptr(1),
			ParitySlices: apc.Ptr(parityCnt),
		},
	}
	xid, err := api.SetBucketProps(baseParams, m.bck, bckPropsToUpate)
	tassert.CheckFatal(t, err)

	tlog.Logfln("Wait for ec-bucket[%s] %s", xid, m.bck.String())
	xargs := xact.ArgsMsg{Kind: apc.ActECEncode, Bck: m.bck, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &xargs)
	tassert.CheckFatal(t, err)

	lst, err = api.ListObjects(baseParams, m.bck, nil, api.ListArgs{})
	tassert.CheckFatal(t, err)

	if len(lst.Entries) != m.num {
		t.Fatalf("bucket %s: expected %d objects, got %d", m.bck.String(), m.num, len(lst.Entries))
	}
	tlog.Logfln("Object counts after EC finishes: %d (%d)", len(lst.Entries), (parityCnt+1)*m.num)

	// Validate object content after EC encoding
	tlog.Logfln("Validating object content after EC encoding")
	m.gets(nil /*api.GetArgs*/, true /*withValidation*/)
	if m.numGetErrs.Load() > 0 {
		t.Fatalf("Content validation failed: %d GET errors after EC encoding", m.numGetErrs.Load())
	}
	tlog.Logfln("Content validation successful: all %d objects verified", m.num)

	//
	// TODO: support querying bucket for total number of entries with respect to mirroring and EC
	//
}

// Creates two buckets (with EC enabled and disabled), fill them with data,
// and then runs two parallel rebalances
func TestECAndRegularRebalance(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, RequiredDeployment: tools.ClusterTypeLocal})

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
	o := &ecOptions{
		minTargets:   5,
		objCount:     90,
		concurrency:  8,
		pattern:      "obj-reb-chk-%04d",
		silent:       true,
		objSizeLimit: ecObjLimit,
	}
	o.init(t, proxyURL)
	initMountpaths(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.parity+test.data+1 {
				t.Skip(cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			o.objSizeLimit = test.objSizeLimit
			ecAndRegularRebalance(t, o, proxyURL, bckReg, bckEC)
		})
	}
}

func ecAndRegularRebalance(t *testing.T, o *ecOptions, proxyURL string, bckReg, bckEC cmn.Bck) {
	baseParams := tools.BaseAPIParams(proxyURL)

	tools.CreateBucket(t, proxyURL, bckReg, nil, true /*cleanup*/)
	newLocalBckWithProps(t, baseParams, bckEC, defaultECBckProps(o), o)

	// select a target that loses its mpath(simulate drive death),
	// and that has mpaths changed (simulate mpath added)
	tgtList := o.smap.Tmap.ActiveNodes()
	tgtLost := tgtList[0]

	tlog.Logfln("Put %s in maintenance (no rebalance)", tgtLost.StringEx())
	args := &apc.ActValRmNode{DaemonID: tgtLost.ID(), SkipRebalance: true}
	_, err := api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	registered := false
	defer func() {
		if !registered {
			args := &apc.ActValRmNode{DaemonID: tgtLost.ID()}
			rebID, err := api.StopMaintenance(baseParams, args)
			tassert.CheckError(t, err)
			tools.WaitForRebalanceByID(t, baseParams, rebID)
		}
	}()

	// fill EC bucket
	wg := sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := range o.objCount {
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
	resECOld, err := api.ListObjects(baseParams, bckEC, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	resRegOld, err := api.ListObjects(baseParams, bckReg, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tlog.Logfln("Created %d objects in %s, %d objects in %s. Starting rebalance",
		len(resECOld.Entries), bckEC.String(), len(resRegOld.Entries), bckReg.String())

	tlog.Logfln("Take %s out of maintenance mode ...", tgtLost.StringEx())
	args = &apc.ActValRmNode{DaemonID: tgtLost.ID()}
	rebID, err := api.StopMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	registered = true
	tools.WaitForRebalanceByID(t, baseParams, rebID)

	tlog.Logln("list objects after rebalance")
	resECNew, err := api.ListObjects(baseParams, bckEC, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tlog.Logfln("%d objects in %s after rebalance",
		len(resECNew.Entries), bckEC.String())
	resRegNew, err := api.ListObjects(baseParams, bckReg, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tlog.Logfln("%d objects in %s after rebalance",
		len(resRegNew.Entries), bckReg.String())

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
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, RequiredDeployment: tools.ClusterTypeLocal})

	var (
		bckEC = cmn.Bck{
			Name:     testBucketName + "-EC",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		o          = &ecOptions{
			minTargets:   5,
			objCount:     300,
			concurrency:  8,
			pattern:      "obj-reb-chk-%04d",
			silent:       true,
			objSizeLimit: ecObjLimit,
		}
	)
	o.init(t, proxyURL)

	initMountpaths(t, proxyURL)
	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.parity+test.data+1 {
				t.Skip(cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			o.objSizeLimit = test.objSizeLimit
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

	tlog.Logfln("Put %s in maintenance (no rebalance)", tgtLost.StringEx())
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
			tools.WaitForRebalanceByID(t, baseParams, rebID)
		}
	}()

	// fill EC bucket
	wg := sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := range o.objCount {
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
	resECOld, err := api.ListObjects(baseParams, bckEC, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tlog.Logfln("Created %d objects in %s - starting global rebalance...", len(resECOld.Entries), bckEC.String())

	tlog.Logfln("Take %s out of maintenance mode ...", tgtLost.StringEx())
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
	_, err = api.WaitForSnaps(baseParams, &xargs, xargs.Started())
	tassert.CheckError(t, err)

	err = api.AbortXaction(baseParams, &xargs)
	tassert.CheckError(t, err)
	tools.WaitForRebalAndResil(t, baseParams)
	tassert.CheckError(t, err)

	tlog.Logfln("Put %s in maintenance", tgtGone.StringEx())
	args = &apc.ActValRmNode{DaemonID: tgtGone.ID()}
	rebID, err := api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	defer func() {
		args = &apc.ActValRmNode{DaemonID: tgtGone.ID()}
		rebID, _ := api.StopMaintenance(baseParams, args)
		tools.WaitForRebalanceByID(t, baseParams, rebID)
	}()

	stopCh.Close()

	tassert.CheckFatal(t, err)
	tools.WaitForRebalanceByID(t, baseParams, rebID)
	tlog.Logln("Reading objects")
	for _, obj := range resECOld.Entries {
		_, err := api.GetObject(baseParams, bckEC, obj.Name, nil)
		tassert.CheckError(t, err)
	}
	tlog.Logln("list objects after rebalance")
	resECNew, err := api.ListObjects(baseParams, bckEC, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tlog.Logfln("%d objects in %s after rebalance",
		len(resECNew.Entries), bckEC.String())
	if len(resECNew.Entries) != len(resECOld.Entries) {
		t.Error("The number of objects before and after rebalance mismatches")
	}

	tlog.Logln("Test object readability after rebalance")
	for _, obj := range resECOld.Entries {
		_, err := api.GetObject(baseParams, bckEC, obj.Name, nil)
		tassert.CheckError(t, err)
	}

	tlog.Logln("list objects after reading")
	resECNew, err = api.ListObjects(baseParams, bckEC, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tlog.Logfln("%d objects in %s after reading",
		len(resECNew.Entries), bckEC.String())
	if len(resECNew.Entries) != len(resECOld.Entries) {
		t.Errorf("Incorrect number of objects: %d (expected %d)",
			len(resECNew.Entries), len(resECOld.Entries))
	}
}

// The test only checks that the number of object after rebalance equals
// the number of objects before it
func ecMountpaths(t *testing.T, o *ecOptions, proxyURL string, bck cmn.Bck) {
	type removedMpath struct {
		si    *meta.Snode
		mpath string
	}
	baseParams := tools.BaseAPIParams(proxyURL)
	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

	wg := sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := range o.objCount {
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
	lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tlog.Logfln("%d objects created, removing %d mountpaths", len(lst.Entries), o.parityCnt)

	allMpaths := tools.GetTargetsMountpaths(t, o.smap, baseParams)
	removed := make(map[string]*removedMpath, o.parityCnt)
	defer func() {
		for _, rmMpath := range removed {
			err := api.AttachMountpath(baseParams, rmMpath.si, rmMpath.mpath)
			tassert.CheckError(t, err)
		}
		tools.WaitForResilvering(t, baseParams, nil)
	}()
	// Choose `parity` random mpaths and disable them
	i := 0
	for tsi, paths := range allMpaths {
		mpath := paths[rand.IntN(len(paths))]
		uid := tsi.ID() + "/" + mpath
		if _, ok := removed[uid]; ok {
			continue
		}
		err := api.DetachMountpath(baseParams, tsi, mpath, true /*dont-resil*/)
		tassert.CheckFatal(t, err)
		rmMpath := &removedMpath{si: tsi, mpath: mpath}
		removed[uid] = rmMpath
		i++
		tlog.Logfln("%d. Disabled %s : %s", i, tsi.StringEx(), mpath)
		if i >= o.parityCnt {
			break
		}
	}

	for _, en := range lst.Entries {
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

	o := &ecOptions{
		minTargets:   4,
		objCount:     10,
		concurrency:  4,
		pattern:      "obj-gen-%04d",
		silent:       testing.Short(),
		objSizeLimit: ecObjLimit,
	}
	o.init(t, proxyURL)
	initMountpaths(t, proxyURL)
	lastWrite := make([]int64, o.objCount)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.parity+test.data {
				t.Skip(cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			o.objSizeLimit = test.objSizeLimit
			newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

			wg := sync.WaitGroup{}
			for gen := range generations {
				wg.Add(o.objCount)
				for i := range o.objCount {
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
			for i := range o.objCount {
				objName := ecTestDir + fmt.Sprintf(o.pattern, i)
				hargs := api.HeadArgs{FltPresence: apc.FltPresent}
				props, err := api.HeadObject(baseParams, bck, objName, hargs)
				tassert.CheckError(t, err)
				if err == nil && props.EC.Generation > lastWrite[i] && props.EC.Generation < currentTime {
					t.Errorf("Object %s, generation %d expected between %d and %d",
						objName, props.EC.Generation, lastWrite[i], currentTime)
				}
			}
		})
	}
}

func newObjSlices(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objName string, idx int, o *ecOptions) map[string]ecSliceMD {
	// Call it so only big and erasure encoded objects are created
	totalCnt, objSize, sliceSize, doEC := randObjectSize(idx, idx+2, o)
	objPath := ecTestDir + objName
	ecStr, delStr := "-", "obj"
	if doEC {
		ecStr = "EC"
	}
	tlog.LogfCond(!o.silent, "Creating %s, size %8d [%2s] [%s]\n", objPath, objSize, ecStr, delStr)
	r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)
	_, err = api.PutObject(&api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: objPath, Reader: r})
	tassert.CheckFatal(t, err)

	tlog.LogfCond(!o.silent, "waiting for %s\n", objPath)
	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objPath)

	ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)
	tassert.Errorf(t, mainObjPath != "", "Full copy is not found")
	return foundParts
}

// Check that BckEncode recovers objects and slices
func TestECBckEncodeRecover(t *testing.T) {
	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-benc",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	o := &ecOptions{
		minTargets:  4,
		objCount:    512,
		concurrency: 4,
		pattern:     "obj-%04d",
		silent:      testing.Short(),
	}
	o.init(t, proxyURL)

	if testing.Short() {
		o.objCount = max(o.objCount/4, 128)
	}

	// Damage this number of objects for each test case
	objToDamage := o.objCount / 8
	initMountpaths(t, proxyURL)

	tassert.Fatalf(t, o.objCount > 2*objToDamage,
		"The total number of objects must be twice as greater as the number of damaged ones")
	var (
		mtx           sync.Mutex
		objSlicesOrig = make(map[string]map[string]ecSliceMD, o.objCount)
	)

	for _, test := range ecTests {
		types := []string{"%ob", "%ec"}
		// In case of only 1 data and parity, there is no slice, so %ec directory is empty
		if test.data == 1 && test.parity == 1 {
			types = []string{"%ob"}
		}
		testName := fmt.Sprintf("%s - %v", test.name, types)
		t.Run(testName, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.parity+test.data {
				t.Skip(cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			o.objSizeLimit = test.objSizeLimit
			newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

			wg := cos.NewLimitedWaitGroup(o.concurrency, sys.NumCPU())
			for i := range o.objCount {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					objName := fmt.Sprintf(o.pattern, i)
					parts := newObjSlices(t, baseParams, bck, objName, i, o)
					mtx.Lock()
					objSlicesOrig[objName] = parts
					mtx.Unlock()
				}(i)
			}
			wg.Wait()
			assertBucketSize(t, baseParams, bck, o.objCount)

			// Delete a few slices and objects their metafiles
			touched := make(map[string]bool)
			for _, tp := range types {
				damaged := make(map[string]bool, objToDamage)
				toDel := objToDamage
				for objName, parts := range objSlicesOrig {
					// Skip objects damaged during the previous loop
					if yes, ok := touched[objName]; ok && yes {
						continue
					}
					for k := range parts {
						if !strings.Contains(k, tp) {
							continue
						}
						tlog.Logfln("1. %s - deleting slice/object %s", objName, k)
						os.Remove(k)
						damaged[objName] = true
						mdFile := strings.Replace(k, tp, "%mt", 1)
						tlog.Logfln("2. %s - deleting its MD %s", objName, mdFile)
						tassert.CheckFatal(t, os.Remove(mdFile))
						break
					}
					toDel--
					if toDel == 0 {
						break
					}
				}
				touched = damaged
			}

			bp := tools.BaseAPIParams(tools.RandomProxyURL())
			xid, err := api.ECEncodeBucket(bp, bck, test.data, test.parity, true /*recover missing ...*/)
			tassert.CheckFatal(t, err)

			// First, wait for EC-encode xaction to complete
			reqArgs := xact.ArgsMsg{ID: xid, Kind: apc.ActECEncode, Bck: bck}
			api.WaitForSnapsIdle(tools.BaseAPIParams(proxyURL), &reqArgs)
			// Second, wait for EC-recover xaction to complete
			reqECArgs := xact.ArgsMsg{ID: xid, Kind: apc.ActECRespond, Bck: bck}
			api.WaitForSnapsIdle(tools.BaseAPIParams(proxyURL), &reqECArgs)

			// [RETRY]
			var errStr string
			for range 4 {
				time.Sleep(8 * time.Second)
				errStr = ""
				// Check that all slices and metafiles are recovered
				for objName, parts := range objSlicesOrig {
					for k := range parts {
						if _, err := os.Stat(k); err == nil {
							continue
						}
						tlog.Logfln("Slice/MD of object %s: %s is not found", objName, k)
						errStr = "Some objects were not recovered"
					}
				}
				if errStr == "" {
					break
				}
			}
			tassert.Error(t, errStr == "", errStr)
		})
	}
}
