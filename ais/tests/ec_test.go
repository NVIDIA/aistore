// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

const (
	ecTestDir = "ec-test/"

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
	seed        int64
	objSize     int64
	concurrency int
	objCount    int
	dataCnt     int
	parityCnt   int
	minTgt      int
	pattern     string
	sema        *cmn.DynSemaphore
	silent      bool
	rnd         *rand.Rand
	smap        *cluster.Smap
}

// Intializes the EC options, validatas the number of targets.
// If initial dataCnt value is negative, it sets the number of data and
// parity slices to maximum possible for the cluster.
func (o ecOptions) init(t *testing.T, proxyURL string) *ecOptions {
	o.smap = tutils.GetClusterMap(t, proxyURL)
	if o.smap.CountTargets() <= o.minTgt {
		t.Fatalf("insufficient number of targets. Required at least %d targets", o.minTgt)
	}
	if o.concurrency > 0 {
		o.sema = cmn.NewDynSemaphore(o.concurrency)
	}
	o.seed = time.Now().UnixNano()
	o.rnd = rand.New(rand.NewSource(o.seed))
	if o.dataCnt < 0 {
		total := o.smap.CountTargets() - 2
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

func defaultECBckProps(o *ecOptions) cmn.BucketPropsToUpdate {
	return cmn.BucketPropsToUpdate{
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
		noObjCnt int64
		main     string

		foundParts = make(map[string]ecSliceMD)
		oldest     = time.Now().Add(time.Hour)

		bckProvider = cmn.ProviderAIS
		bmd         = cluster.NewBaseBownerMock(
			cluster.NewBck(
				bck.Name, bck.Provider, bck.Ns,
				&cmn.BucketProps{Cksum: cmn.CksumConf{Type: cmn.ChecksumXXHash}},
			),
		)
	)

	if !bck.IsAIS() {
		config := tutils.GetClusterConfig(t)
		bckProvider = config.Cloud.Provider
		bmd.Add(cluster.NewBck(bck.Name, bck.Provider, bck.Ns, cmn.DefaultBucketProps()))
	}

	tMock := cluster.NewTargetMock(bmd)
	bckPattern := bck.Name + "/"

	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return nil
		}
		if tutils.IsTrashDir(path) {
			return filepath.SkipDir
		}
		// Check if the path is of test's bucket
		if strings.Contains(path, "%") && !strings.Contains(path, bckPattern) {
			return filepath.SkipDir
		}
		if info.IsDir() {
			return nil
		}

		if !strings.Contains(path, objName) {
			return nil
		}

		sliceLom := &cluster.LOM{T: tMock, FQN: path}
		if err = sliceLom.Init(cmn.Bck{Provider: bckProvider}); err != nil {
			return nil
		}

		_ = sliceLom.Load(false)
		if !cmn.StringInSlice(sliceLom.ParsedFQN.ContentType, []string{ec.SliceType, ec.MetaType, fs.ObjectType}) {
			return nil
		}

		var cksumVal string
		if sliceLom.ParsedFQN.ContentType == ec.SliceType {
			ct, err := cluster.NewCTFromFQN(path, nil)
			if err != nil {
				return err
			}

			fqn := ct.Make(ec.MetaType)
			// Extract checksum from metadata for slices
			md, err := ec.LoadMetadata(fqn)
			if err == nil {
				cksumVal = md.CksumValue
			}
		} else if sliceLom.Cksum() != nil {
			_, cksumVal = sliceLom.Cksum().Get()
		}

		foundParts[path] = ecSliceMD{info.Size(), cksumVal}
		if sliceLom.ParsedFQN.ContentType == fs.ObjectType && oldest.After(info.ModTime()) {
			main = path
			oldest = info.ModTime()
		}

		return nil
	}

	err := filepath.Walk(rootDir, fsWalkFunc)
	tassert.CheckFatal(t, err)

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
	bck cmn.Bck, objPath string, objSize, sliceSize int64, totalCnt int) {
	tassert.Errorf(t, len(sliceList) == totalCnt, "Expected number of objects for %s/%s: %d, found: %d\n%+v",
		bck, objPath, totalCnt, len(sliceList), sliceList)

	var bckProvider string
	if bck.IsAIS() {
		bckProvider = cmn.ProviderAIS
	} else {
		config := tutils.GetClusterConfig(t)
		bckProvider = config.Cloud.Provider
	}

	sliced := sliceSize < objSize
	hashes := make(map[string]bool)
	metaCnt := 0
	for k, md := range sliceList {
		ct, err := cluster.NewCTFromFQN(k, nil)
		tassert.CheckFatal(t, err)

		if ct.ContentType() == ec.MetaType {
			metaCnt++
			tassert.Errorf(t, md.size <= 512, "Metafile %q size is too big: %d", k, md.size)
		} else if ct.ContentType() == ec.SliceType {
			tassert.Errorf(t, md.size == sliceSize, "Slice %q size mismatch: %d, expected %d", k, md.size, sliceSize)
			if sliced {
				if _, ok := hashes[md.hash]; ok {
					t.Logf("Duplicated slice hash(slice) %s/%s: %s\n", bck, objPath, md.hash)
				}
				hashes[md.hash] = true
			}
		} else {
			tassert.Errorf(t, ct.ContentType() == fs.ObjectType, "invalid content type %s, expected: %s", ct.ContentType(), fs.ObjectType)
			tassert.Errorf(t, ct.Bck().Provider == bckProvider, "invalid provider %s, expected: %s", ct.Bck().Provider, bckProvider)
			tassert.Errorf(t, ct.Bck().Name == bck.Name, "invalid bucket name %s, expected: %s", ct.Bck().Name, bck.Name)
			tassert.Errorf(t, ct.ObjName() == objPath, "invalid object name %s, expected: %s", ct.ObjName(), objPath)
			tassert.Errorf(t, md.size == objSize, "%q size mismatch: got %d, expected %d", k, md.size, objSize)
			if sliced {
				if _, ok := hashes[md.hash]; ok {
					t.Logf("Duplicated slice hash(main) %s/%s: %s\n", bck, objPath, md.hash)
				}
				hashes[md.hash] = true
			}
		}
	}

	metaCntMust := totalCnt / 2
	tassert.Errorf(t, metaCnt == metaCntMust, "Number of metafiles mismatch: %d, expected %d", metaCnt, metaCntMust)
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
					if ct.ContentType() == ec.SliceType {
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
					tutils.Logf("Object %s, size %9d[%9d]\n", objName, objSize, sliceSize)
				} else {
					tutils.Logf("Object %s, size %9d[%9s]\n", objName, objSize, "-")
				}
			}

			r, err := readers.NewRandReader(objSize, cmn.ChecksumNone)
			defer func() {
				r.Close()
				o.sema.Release()
				wg.Done()
			}()
			tassert.CheckFatal(t, err)
			putArgs := api.PutObjectArgs{BaseParams: baseParams, Bck: bck, Object: objPath, Reader: r}
			err = api.PutObject(putArgs)
			tassert.CheckFatal(t, err)

			foundParts, _ := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objName)
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
				if ct.ContentType() == ec.MetaType {
					metaCnt++
					tassert.Errorf(t, md.size <= 512, "Metafile %q size is too big: %d", k, md.size)
				} else if ct.ContentType() == ec.SliceType {
					sliceCnt++
					if md.size != sliceSize && doEC {
						t.Errorf("Slice %q size mismatch: %d, expected %d", k, md.size, sliceSize)
					}
					if md.size != objSize && !doEC {
						t.Errorf("Copy %q size mismatch: %d, expected %d", k, md.size, objSize)
					}
				} else {
					tassert.Errorf(t, ct.ContentType() == fs.ObjectType, "invalid content type %s, expected: %s", ct.ContentType(), fs.ObjectType)
					tassert.Errorf(t, ct.Bck().Provider == bck.Provider, "invalid provider %s, expected: %s", ct.Bck().Provider, cmn.ProviderAIS)
					tassert.Errorf(t, ct.Bck().Name == bck.Name, "invalid bucket name %s, expected: %s", ct.Bck().Name, bck.Name)
					tassert.Errorf(t, ct.ObjName() == objPath, "invalid object name %s, expected: %s", ct.ObjName(), objPath)
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
			partsAfterRemove, _ := ecGetAllSlices(t, bck, objName)
			_, ok := partsAfterRemove[mainObjPath]
			if ok || len(partsAfterRemove) >= len(foundParts) {
				t.Errorf("Object is not deleted: %#v", partsAfterRemove)
				return
			}

			_, err = api.GetObject(baseParams, bck, objPath)
			tassert.CheckFatal(t, err)

			if doEC {
				partsAfterRestore, _ := ecGetAllSlices(t, bck, objName)
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
		t.Logf("Average size of the bucket %s: %s\n", bck, cmn.B2S(szTotal/int64(szLen), 1))
	}
}

func assertBucketSize(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objCount int) {
	bckObjectsCnt := bucketSize(t, baseParams, bck)
	tassert.Fatalf(t, bckObjectsCnt == objCount, "Invalid number of objects: %d, expected %d", bckObjectsCnt, objCount)
}

func bucketSize(t *testing.T, baseParams api.BaseParams, bck cmn.Bck) int {
	var msg = &cmn.SelectMsg{PageSize: pagesize, Props: "size,status"}
	reslist, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	reslist.Entries = filterObjListOK(reslist.Entries)

	return len(reslist.Entries)
}

func putRandomFile(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objPath string, size int) {
	r, err := readers.NewRandReader(int64(size), cmn.ChecksumNone)
	tassert.CheckFatal(t, err)
	defer r.Close()

	putArgs := api.PutObjectArgs{BaseParams: baseParams, Bck: bck, Object: objPath, Reader: r}
	err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)
}

func newLocalBckWithProps(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, bckProps cmn.BucketPropsToUpdate, o *ecOptions) {
	proxyURL := tutils.RandomProxyURL()
	tutils.CreateFreshBucket(t, proxyURL, bck)

	tutils.Logf("Changing EC %d:%d [ seed = %d ], concurrent: %d\n",
		o.dataCnt, o.parityCnt, o.seed, o.concurrency)
	err := api.SetBucketProps(baseParams, bck, bckProps)

	if err != nil {
		tutils.DestroyBucket(t, proxyURL, bck)
	}
	tassert.CheckFatal(t, err)
}

func setBucketECProps(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, bckProps cmn.BucketPropsToUpdate) {
	tutils.Logf("Changing EC %d:%d\n", bckProps.EC.DataSlices, bckProps.EC.ParitySlices)
	err := api.SetBucketProps(baseParams, bck, bckProps)
	tassert.CheckFatal(t, err)
}

func clearAllECObjects(t *testing.T, bck cmn.Bck, failOnDelErr bool, o *ecOptions) {
	var (
		wg       = sync.WaitGroup{}
		proxyURL = tutils.RandomProxyURL()
	)

	tutils.Logln("Deleting objects...")
	wg.Add(o.objCount)
	for idx := 0; idx < o.objCount; idx++ {
		go func(i int) {
			defer wg.Done()
			objName := fmt.Sprintf(o.pattern, i)
			objPath := ecTestDir + objName
			err := tutils.Del(proxyURL, bck, objPath, nil, nil, true)
			if failOnDelErr {
				tassert.CheckFatal(t, err)
			} else if err != nil {
				t.Log(err.Error())
			}

			deadline := time.Now().Add(time.Second * 10)
			var partsAfterDelete map[string]int64
			for time.Now().Before(deadline) {
				time.Sleep(time.Millisecond * 250)
				partsAfterDelete, _ := ecGetAllSlices(t, bck, objName)
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

func objectsExist(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objPatt string, objCount int) {
	wg := &sync.WaitGroup{}
	getOneObj := func(objName string) {
		defer wg.Done()
		objPath := ecTestDir + objName
		_, err := api.GetObject(baseParams, bck, objPath)
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

// Simulates damaged slice by changing slice's checksum in metadata file
func damageMetadataCksum(t *testing.T, slicePath string) {
	ct, err := cluster.NewCTFromFQN(slicePath, nil)
	tassert.CheckFatal(t, err)
	metaFQN := ct.Make(ec.MetaType)
	md, err := ec.LoadMetadata(metaFQN)
	tassert.CheckFatal(t, err)
	md.CksumValue = "01234"
	err = jsp.Save(metaFQN, md, jsp.Plain())
	tassert.CheckFatal(t, err)
}

// Short test to make sure that EC options cannot be changed after
// EC is enabled
func TestECChange(t *testing.T) {
	var (
		proxyURL = tutils.RandomProxyURL()
		bck      = cmn.Bck{
			Name:     TestBucketName + "-ec-change",
			Provider: cmn.ProviderAIS,
		}
	)

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	bucketProps := cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{
			Enabled:      api.Bool(true),
			ObjSizeLimit: api.Int64(ecObjLimit),
			DataSlices:   api.Int(1),
			ParitySlices: api.Int(1),
		},
	}
	baseParams := tutils.BaseAPIParams(proxyURL)

	tutils.Logln("Resetting bucket properties")
	err := api.ResetBucketProps(baseParams, bck)
	tassert.CheckFatal(t, err)

	tutils.Logln("Trying to set too many slices")
	bucketProps.EC.DataSlices = api.Int(25)
	bucketProps.EC.ParitySlices = api.Int(25)
	err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.Errorf(t, err != nil, "Enabling EC must fail in case of the number of targets fewer than the number of slices")

	tutils.Logln("Enabling EC")
	bucketProps.EC.DataSlices = api.Int(1)
	bucketProps.EC.ParitySlices = api.Int(1)
	err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.CheckFatal(t, err)

	tutils.Logln("Trying to set EC options to the same values")
	err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.CheckFatal(t, err)

	tutils.Logln("Trying to disable EC")
	bucketProps.EC.Enabled = api.Bool(false)
	err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.Errorf(t, err == nil, "Disabling EC failed: %v", err)

	tutils.Logln("Trying to re-enable EC")
	bucketProps.EC.Enabled = api.Bool(true)
	err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.Errorf(t, err == nil, "Enabling EC failed: %v", err)

	tutils.Logln("Trying to modify EC options when EC is enabled")
	bucketProps.EC.Enabled = api.Bool(true)
	bucketProps.EC.ObjSizeLimit = api.Int64(300000)
	err = api.SetBucketProps(baseParams, bck, bucketProps)
	tassert.Errorf(t, err != nil, "Modifiying EC properties must fail")

	tutils.Logln("Resetting bucket properties")
	err = api.ResetBucketProps(baseParams, bck)
	tassert.Errorf(t, err == nil, "Resetting properties should work")
}

func createECReplicas(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objName string, o *ecOptions) {
	o.sema.Acquire()
	defer o.sema.Release()

	totalCnt := 2 + o.parityCnt*2
	objSize := int64(ecMinSmallSize + o.rnd.Intn(ecSmallDelta))
	sliceSize := objSize

	objPath := ecTestDir + objName

	tutils.Logf("Creating %s, size %8d\n", objPath, objSize)
	r, err := readers.NewRandReader(objSize, cmn.ChecksumNone)
	tassert.CheckFatal(t, err)
	defer r.Close()

	putArgs := api.PutObjectArgs{BaseParams: baseParams, Bck: bck, Object: objPath, Reader: r}
	err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)

	tutils.Logf("waiting for %s\n", objPath)
	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, false, bck, objName)

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
	if !o.silent {
		tutils.Logf("Creating %s, size %8d [%2s]\n", objPath, objSize, ecStr)
	}
	r, err := readers.NewRandReader(objSize, cmn.ChecksumNone)
	tassert.CheckFatal(t, err)
	defer r.Close()

	putArgs := api.PutObjectArgs{BaseParams: baseParams, Bck: bck, Object: objPath, Reader: r}
	err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)

	if !o.silent {
		tutils.Logf("waiting for %s\n", objPath)
	}
	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objName)

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

	o.sema.Acquire()
	defer o.sema.Release()

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
	r, err := readers.NewRandReader(objSize, cmn.ChecksumNone)
	tassert.CheckFatal(t, err)
	defer r.Close()

	putArgs := api.PutObjectArgs{BaseParams: baseParams, Bck: bck, Object: objPath, Reader: r}
	err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)

	tutils.Logf("waiting for %s\n", objPath)
	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objName)

	ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)
	if mainObjPath == "" {
		t.Errorf("Full copy is not found")
		return
	}

	tutils.Logf("Damaging %s [removing %s]\n", objPath, mainObjPath)
	tassert.CheckFatal(t, os.Remove(mainObjPath))

	ct, err := cluster.NewCTFromFQN(mainObjPath, nil)
	tassert.CheckFatal(t, err)
	metafile := ct.Make(ec.MetaType)
	tutils.Logf("Damaging %s [removing %s]\n", objPath, metafile)
	tassert.CheckFatal(t, os.Remove(metafile))
	if delSlice {
		sliceToDel := ""
		for k := range foundParts {
			ct, err := cluster.NewCTFromFQN(k, nil)
			tassert.CheckFatal(t, err)
			if k != mainObjPath && ct.ContentType() == ec.SliceType && doEC {
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
		tutils.Logf("Removing slice/replica: %s\n", sliceToDel)
		tassert.CheckFatal(t, os.Remove(sliceToDel))

		ct, err := cluster.NewCTFromFQN(sliceToDel, nil)
		tassert.CheckFatal(t, err)
		metafile := ct.Make(ec.MetaType)
		if doEC {
			tutils.Logf("Removing slice meta %s\n", metafile)
		} else {
			tutils.Logf("Removing replica meta %s\n", metafile)
		}
		tassert.CheckFatal(t, os.Remove(metafile))
	}

	partsAfterRemove, _ := ecGetAllSlices(t, bck, objName)
	_, ok := partsAfterRemove[mainObjPath]
	if ok || len(partsAfterRemove) != len(foundParts)-deletedFiles*2 {
		t.Errorf("Files are not deleted [%d - %d]: %#v", len(foundParts), len(partsAfterRemove), partsAfterRemove)
		return
	}

	tutils.Logf("Restoring %s\n", objPath)
	_, err = api.GetObject(baseParams, bck, objPath)
	tassert.CheckFatal(t, err)

	// For cloud buckets, due to performance reason, GFN is not used and
	// EC is not called - object is reread from the cloud bucket instead
	if doEC && bck.IsAIS() {
		deadline := time.Now().Add(sleepRestoreTime)
		var partsAfterRestore map[string]ecSliceMD
		for time.Now().Before(deadline) {
			time.Sleep(time.Millisecond * 250)
			partsAfterRestore, _ = ecGetAllSlices(t, bck, objName)
			if len(partsAfterRestore) == totalCnt {
				break
			}
		}
		ecCheckSlices(t, partsAfterRestore, bck, objPath, objSize, sliceSize, totalCnt)
	}
}

// Simple stress testing EC for Cloud buckets
func TestECRestoreObjAndSliceCloud(t *testing.T) {
	var (
		bck = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.AnyCloud,
		}
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	o := ecOptions{
		minTgt:      3,
		objCount:    100,
		concurrency: 8,
		pattern:     "obj-rest-cloud-%04d",
	}.init(t, proxyURL)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: bck})

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountTargets() <= test.parity+test.data {
				t.Skip("insufficient number of targets")
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			setBucketECProps(t, baseParams, bck, defaultECBckProps(o))
			defer api.SetBucketProps(baseParams, bck, cmn.BucketPropsToUpdate{
				EC: &cmn.ECConfToUpdate{Enabled: api.Bool(false)},
			})

			wg := sync.WaitGroup{}

			wg.Add(o.objCount)
			for i := 0; i < o.objCount; i++ {
				objName := fmt.Sprintf(o.pattern, i)
				go func(i int) {
					defer wg.Done()
					createDamageRestoreECFile(t, baseParams, bck, objName, i, o)
				}(i)
			}
			wg.Wait()
			defer clearAllECObjects(t, bck, true, o)
		})
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
		bck = cmn.Bck{
			Name:     TestBucketName + "-obj-n-slice",
			Provider: cmn.ProviderAIS,
		}
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	o := ecOptions{
		minTgt:      3,
		objCount:    50,
		concurrency: 8,
		pattern:     "obj-rest-%04d",
	}.init(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountTargets() <= test.parity+test.data {
				t.Skip("insufficient number of targets")
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)
			defer tutils.DestroyBucket(t, proxyURL, bck)

			wg := sync.WaitGroup{}
			wg.Add(o.objCount)
			for i := 0; i < o.objCount; i++ {
				go func(objName string, i int) {
					defer wg.Done()
					createDamageRestoreECFile(t, baseParams, bck, objName, i, o)
				}(fmt.Sprintf(o.pattern, i), i)
			}
			wg.Wait()
			assertBucketSize(t, baseParams, bck, o.objCount)
		})
	}
}

func putECFile(baseParams api.BaseParams, bck cmn.Bck, objName string) error {
	objSize := int64(ecMinBigSize * 2)
	objPath := ecTestDir + objName

	r, err := readers.NewRandReader(objSize, cmn.ChecksumNone)
	if err != nil {
		return err
	}
	defer r.Close()

	putArgs := api.PutObjectArgs{BaseParams: baseParams, Bck: bck, Object: objPath, Reader: r}
	return api.PutObject(putArgs)
}

// Returns path to main object and map of all object's slices and ioContext
func createECFile(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objName string, o *ecOptions) (map[string]ecSliceMD, string) {
	totalCnt := 2 + (o.sliceTotal())*2
	objSize := int64(ecMinBigSize * 2)
	sliceSize := ec.SliceSize(objSize, o.dataCnt)

	err := putECFile(baseParams, bck, objName)
	tassert.CheckFatal(t, err)

	foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, true, bck, objName)
	tassert.Fatalf(t, mainObjPath != "", "Full copy %s was not found", mainObjPath)

	objPath := ecTestDir + objName
	ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)

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
		proxyURL = tutils.RandomProxyURL()
		bck      = cmn.Bck{
			Name:     TestBucketName + "-ec-cksum",
			Provider: cmn.ProviderAIS,
		}
	)

	o := ecOptions{
		minTgt:    3,
		dataCnt:   1,
		parityCnt: 1,
		pattern:   "obj-cksum-%04d",
	}.init(t, proxyURL)
	baseParams := tutils.BaseAPIParams(proxyURL)

	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	objName1 := fmt.Sprintf(o.pattern, 1)
	objPath1 := ecTestDir + objName1
	foundParts1, mainObjPath1 := createECFile(t, baseParams, bck, objName1, o)

	objName2 := fmt.Sprintf(o.pattern, 2)
	objPath2 := ecTestDir + objName2
	foundParts2, mainObjPath2 := createECFile(t, baseParams, bck, objName2, o)

	tutils.Logf("Removing main object %s\n", mainObjPath1)
	tassert.CheckFatal(t, os.Remove(mainObjPath1))

	// Corrupt just one slice, EC should be able to restore the original object
	for k := range foundParts1 {
		ct, err := cluster.NewCTFromFQN(k, nil)
		tassert.CheckFatal(t, err)

		if k != mainObjPath1 && ct.ContentType() == ec.SliceType {
			damageMetadataCksum(t, k)
			break
		}
	}

	_, err := api.GetObject(baseParams, bck, objPath1)
	tassert.CheckFatal(t, err)

	tutils.Logf("Removing main object %s\n", mainObjPath2)
	tassert.CheckFatal(t, os.Remove(mainObjPath2))

	// Corrupt all slices, EC should not be able to restore
	for k := range foundParts2 {
		ct, err := cluster.NewCTFromFQN(k, nil)
		tassert.CheckFatal(t, err)

		if k != mainObjPath2 && ct.ContentType() == ec.SliceType {
			damageMetadataCksum(t, k)
		}
	}

	_, err = api.GetObject(baseParams, bck, objPath2)
	tassert.Fatalf(t, err != nil, "Object should not be restored when checksums are wrong")
}

func TestECEnabledDisabledEnabled(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     TestBucketName + "-ec-props",
			Provider: cmn.ProviderAIS,
		}
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	o := ecOptions{
		minTgt:      3,
		dataCnt:     1,
		parityCnt:   1,
		objCount:    25,
		concurrency: 8,
		pattern:     "obj-rest-%04d",
	}.init(t, proxyURL)

	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	// End of preparation, create files with EC enabled, check if are restored properly

	wg := sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go func(i int) {
			defer wg.Done()
			createDamageRestoreECFile(t, baseParams, bck, objName, i, o)
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	assertBucketSize(t, baseParams, bck, o.objCount)

	// Disable EC, put normal files, check if were created properly
	err := api.SetBucketProps(baseParams, bck, cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(false)},
	})
	tassert.CheckError(t, err)

	wg.Add(o.objCount)
	for i := o.objCount; i < 2*o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go func() {
			defer wg.Done()
			putRandomFile(t, baseParams, bck, objName, cmn.MiB)
		}()
	}

	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	assertBucketSize(t, baseParams, bck, o.objCount*2)

	// Enable EC again, check if EC was started properly and creates files with EC correctly
	err = api.SetBucketProps(baseParams, bck, cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(true)},
	})
	tassert.CheckError(t, err)

	wg.Add(o.objCount)
	for i := 2 * o.objCount; i < 3*o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go func(i int) {
			defer wg.Done()
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
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     TestBucketName + "-ec-load",
			Provider: cmn.ProviderAIS,
		}
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	o := ecOptions{
		minTgt:      3,
		dataCnt:     1,
		parityCnt:   1,
		objCount:    5,
		concurrency: 8,
		pattern:     "obj-disable-enable-load-%04d",
	}.init(t, proxyURL)

	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	// End of preparation, create files with EC enabled, check if are restored properly

	wg := &sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go func(i int) {
			defer wg.Done()
			createDamageRestoreECFile(t, baseParams, bck, objName, i, o)
		}(i)
	}
	wg.Wait()

	assertBucketSize(t, baseParams, bck, o.objCount)

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
				go putRandomFile(t, baseParams, bck, objName, cmn.KiB)
				numCreated++
			}
		}
	}()

	time.Sleep(time.Second)

	tutils.Logf("Disabling EC for the bucket %s\n", bck)
	err := api.SetBucketProps(baseParams, bck, cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(false)},
	})
	tassert.CheckError(t, err)

	time.Sleep(15 * time.Millisecond)
	tutils.Logf("Enabling EC for the bucket %s\n", bck)
	err = api.SetBucketProps(baseParams, bck, cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(true)},
	})
	tassert.CheckError(t, err)
	reqArgs := api.XactReqArgs{Kind: cmn.ActECEncode, Bck: bck, Latest: true}
	api.WaitForXaction(baseParams, reqArgs)

	close(abortCh)

	if t.Failed() {
		t.FailNow()
	}
	// Disabling and enabling EC should not result in put's failing
	assertBucketSize(t, baseParams, bck, o.objCount+numCreated)
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
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     TestBucketName + "-ec-stress",
			Provider: cmn.ProviderAIS,
		}
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	o := ecOptions{
		minTgt:      3,
		objCount:    400,
		concurrency: 12,
		pattern:     "obj-stress-%04d",
	}.init(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountTargets() <= test.data+test.parity {
				t.Skip("insufficient taregts")
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

			defer tutils.DestroyBucket(t, proxyURL, bck)

			doECPutsAndCheck(t, baseParams, bck, o)

			var msg = &cmn.SelectMsg{PageSize: pagesize, Props: "size,status"}
			reslist, err := api.ListObjects(baseParams, bck, msg, 0)
			tassert.CheckFatal(t, err)
			reslist.Entries = filterObjListOK(reslist.Entries)

			tassert.Fatalf(t, len(reslist.Entries) == o.objCount,
				"Invalid number of objects: %d, expected %d", len(reslist.Entries), o.objCount)
		})
	}
}

// Stress 2 buckets at the same time
func TestECStressManyBuckets(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		bck1 = cmn.Bck{
			Name:     TestBucketName + "1",
			Provider: cmn.ProviderAIS,
		}
		bck2 = cmn.Bck{
			Name:     TestBucketName + "2",
			Provider: cmn.ProviderAIS,
		}
		proxyURL = tutils.RandomProxyURL()
	)

	o1 := ecOptions{
		minTgt:      3,
		parityCnt:   1,
		dataCnt:     1,
		objCount:    200,
		concurrency: 12,
		pattern:     "obj-stress-manybck-%04d",
	}.init(t, proxyURL)
	o2 := ecOptions{
		minTgt:      3,
		parityCnt:   1,
		dataCnt:     1,
		objCount:    200,
		concurrency: 12,
		pattern:     "obj-stress-manybck-%04d",
	}.init(t, proxyURL)

	baseParams := tutils.BaseAPIParams(proxyURL)
	newLocalBckWithProps(t, baseParams, bck1, defaultECBckProps(o1), o1)
	newLocalBckWithProps(t, baseParams, bck2, defaultECBckProps(o2), o2)
	defer tutils.DestroyBucket(t, proxyURL, bck1)
	defer tutils.DestroyBucket(t, proxyURL, bck2)

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

	var msg = &cmn.SelectMsg{PageSize: pagesize, Props: "size,status"}
	reslist, err := api.ListObjects(baseParams, bck1, msg, 0)
	tassert.CheckFatal(t, err)
	reslist.Entries = filterObjListOK(reslist.Entries)

	tassert.Fatalf(t, len(reslist.Entries) == o1.objCount, "Bucket %s: Invalid number of objects: %d, expected %d", bck1, len(reslist.Entries), o1.objCount)

	msg = &cmn.SelectMsg{PageSize: pagesize, Props: "size,status"}
	reslist, err = api.ListObjects(baseParams, bck2, msg, 0)
	tassert.CheckFatal(t, err)
	reslist.Entries = filterObjListOK(reslist.Entries)

	tassert.Fatalf(t, len(reslist.Entries) == o2.objCount, "Bucket %s: Invalid number of objects: %d, expected %d", bck2, len(reslist.Entries), o2.objCount)
}

// ExtraStress test to check that EC works as expected
//  - Changes bucket props to use EC
//  - Generates `objCount` objects, size between `ecObjMinSize` and `ecObjMinSize`+ecObjMaxSize`
//  - Objects smaller `ecObjLimit` must be copies, while others must be EC'ed
//  - PUTs ALL objects to the bucket stressing both EC and transport
//  - filepath.Walk checks that the number of metafiles at the end is correct
//  - No errors must occur
func TestECExtraStress(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	const (
		objStart = "obj-extra-"
	)

	var (
		bck = cmn.Bck{
			Name:     TestBucketName + "-extrastress",
			Provider: cmn.ProviderAIS,
		}
		proxyURL = tutils.RandomProxyURL()
	)

	o := ecOptions{
		minTgt:      3,
		objCount:    400,
		concurrency: 12,
		pattern:     objStart + "%04d",
	}.init(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountTargets() <= test.data+test.parity {
				t.Skip("insufficient taregts")
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
		baseParams  = tutils.BaseAPIParams(proxyURL)
	)

	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)
	defer tutils.DestroyBucket(t, proxyURL, bck)

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
				tutils.Logf("Object %s, size %9d[%9d]\n", objName, objSize, sliceSize)
			} else {
				tutils.Logf("Object %s, size %9d[%9s]\n", objName, objSize, "-")
			}
			r, err := readers.NewRandReader(objSize, cmn.ChecksumNone)
			tassert.Errorf(t, err == nil, "Failed to create reader: %v", err)
			putArgs := api.PutObjectArgs{BaseParams: baseParams, Bck: bck, Object: objPath, Reader: r}
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

	var msg = &cmn.SelectMsg{PageSize: pagesize, Props: "size,status"}
	reslist, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	reslist.Entries = filterObjListOK(reslist.Entries)

	tassert.Fatalf(t, len(reslist.Entries) == o.objCount, "Invalid number of objects: %d, expected %d", len(reslist.Entries), o.objCount)
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

	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     TestBucketName + "-attrs",
			Provider: cmn.ProviderAIS,
		}
		proxyURL = tutils.RandomProxyURL()
	)

	o := ecOptions{
		minTgt:      3,
		dataCnt:     1,
		parityCnt:   1,
		objCount:    30,
		concurrency: 8,
		pattern:     "obj-xattr-%04d",
	}.init(t, proxyURL)

	baseParams := tutils.BaseAPIParams(proxyURL)
	bckProps := defaultECBckProps(o)
	bckProps.Versioning = &cmn.VersionConfToUpdate{
		Enabled: api.Bool(true),
	}

	newLocalBckWithProps(t, baseParams, bck, bckProps, o)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	oneObj := func(idx int, objName string) {
		totalCnt, objSize, sliceSize, doEC := randObjectSize(idx, smallEvery, o)
		objPath := ecTestDir + objName
		ecStr, delStr := "-", "obj"
		if doEC {
			ecStr = "EC"
		}
		tutils.Logf("Creating %s, size %8d [%2s] [%s]\n", objPath, objSize, ecStr, delStr)
		r, err := readers.NewRandReader(objSize, cmn.ChecksumNone)
		tassert.CheckFatal(t, err)
		defer r.Close()
		putArgs := api.PutObjectArgs{BaseParams: baseParams, Bck: bck, Object: objPath, Reader: r}
		err = api.PutObject(putArgs)
		tassert.CheckFatal(t, err)

		tutils.Logf("waiting for %s\n", objPath)
		foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objName)

		ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)
		if mainObjPath == "" {
			t.Fatalf("Full copy is not found")
		}

		tutils.Logf("Damaging %s [removing %s]\n", objPath, mainObjPath)
		tassert.CheckFatal(t, os.Remove(mainObjPath))

		ct, err := cluster.NewCTFromFQN(mainObjPath, nil)
		tassert.CheckFatal(t, err)
		metafile := ct.Make(ec.MetaType)
		tutils.Logf("Damaging %s [removing %s]\n", objPath, metafile)
		tassert.CheckFatal(t, os.Remove(metafile))

		partsAfterRemove, _ := ecGetAllSlices(t, bck, objName)
		_, ok := partsAfterRemove[mainObjPath]
		if ok || len(partsAfterRemove) != len(foundParts)-2 {
			t.Fatalf("Files are not deleted [%d - %d]: %#v", len(foundParts), len(partsAfterRemove), partsAfterRemove)
		}

		tutils.Logf("Restoring %s\n", objPath)
		_, err = api.GetObject(baseParams, bck, objPath)
		tassert.CheckFatal(t, err)

		if doEC {
			deadline := time.Now().Add(sleepRestoreTime)
			var partsAfterRestore map[string]ecSliceMD
			for time.Now().Before(deadline) {
				time.Sleep(time.Millisecond * 250)
				partsAfterRestore, _ = ecGetAllSlices(t, bck, objName)
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

	var msg = &cmn.SelectMsg{PageSize: pagesize, Props: "size,status,version"}
	reslist, err := api.ListObjects(baseParams, bck, msg, 0)
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
}

// 1. start putting EC files into the cluster
// 2. in the middle of puts destroy bucket
// 3. wait for puts to finish
// 4. create bucket with the same name
// 5. check that EC is working properly for this bucket
func TestECDestroyBucket(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     TestBucketName + "-DESTROY",
			Provider: cmn.ProviderAIS,
		}
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	o := ecOptions{
		minTgt:      3,
		dataCnt:     1,
		parityCnt:   1,
		objCount:    100,
		concurrency: 10,
		pattern:     "obj-destroy-bck-%04d",
	}.init(t, proxyURL)

	bckProps := defaultECBckProps(o)
	newLocalBckWithProps(t, baseParams, bck, bckProps, o)

	wg := &sync.WaitGroup{}
	errCnt := atomic.NewInt64(0)
	sucCnt := atomic.NewInt64(0)

	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)

		o.sema.Acquire()
		wg.Add(1)
		go func(i int) {
			defer func() {
				o.sema.Release()
				wg.Done()
			}()

			if i%10 == 0 {
				tutils.Logf("ec object %s into bucket %s\n", objName, bck)
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

				tutils.Logf("Destroying bucket %s\n", bck)
				tutils.DestroyBucket(t, proxyURL, bck)
			}()
		}
	}

	wg.Wait()
	tutils.Logf("EC put files resulted in error in %d out of %d files\n", errCnt.Load(), o.objCount)

	// create bucket with the same name and check if puts are successful
	newLocalBckWithProps(t, baseParams, bck, bckProps, o)
	defer tutils.DestroyBucket(t, proxyURL, bck)
	doECPutsAndCheck(t, baseParams, bck, o)

	// check if get requests are successful
	var msg = &cmn.SelectMsg{PageSize: pagesize, Props: "size,status,version"}
	reslist, err := api.ListObjects(baseParams, bck, msg, 0)
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

	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     TestBucketName + "-slice-emergency",
			Provider: cmn.ProviderAIS,
		}
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	o := ecOptions{
		minTgt:      4,
		dataCnt:     -1,
		objCount:    100,
		concurrency: 12,
		pattern:     "obj-emt-%04d",
	}.init(t, proxyURL)

	// Increase number of EC data slices, now there's just enough targets to handle EC requests
	// Encoding will fail if even one is missing, restoring should still work
	o.dataCnt++

	sgl := tutils.MMSA.NewSGL(0)
	defer sgl.Free()

	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	wg := &sync.WaitGroup{}

	// 1. PUT objects
	putOneObj := func(idx int, objName string) {
		defer func() {
			wg.Done()
			o.sema.Release()
		}()

		start := time.Now()
		totalCnt, objSize, sliceSize, doEC := randObjectSize(idx, smallEvery, o)
		objPath := ecTestDir + objName
		ecStr := "-"
		if doEC {
			ecStr = "EC"
		}
		tutils.Logf("Creating %s, size %8d [%2s]\n", objPath, objSize, ecStr)
		r, err := readers.NewRandReader(objSize, cmn.ChecksumNone)
		tassert.CheckFatal(t, err)
		defer r.Close()
		putArgs := api.PutObjectArgs{BaseParams: baseParams, Bck: bck, Object: objPath, Reader: r}
		err = api.PutObject(putArgs)
		tassert.CheckFatal(t, err)
		t.Logf("Object %s put in %v", objName, time.Since(start))
		start = time.Now()

		foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objName)

		ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)
		if mainObjPath == "" {
			t.Errorf("Full copy is not found")
			return
		}
		t.Logf("Object %s EC in %v", objName, time.Since(start))
	}

	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		o.sema.Acquire()
		go putOneObj(i, objName)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	// 2. Kill a random target
	var (
		removedTarget *cluster.Snode
		smap          = o.smap
	)
	smap, removedTarget = tutils.RemoveTarget(t, proxyURL, smap)

	defer func() {
		smap = tutils.RestoreTarget(t, proxyURL, smap, removedTarget)
		tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)
	}()

	// 3. Read objects
	objectsExist(t, baseParams, bck, o.pattern, o.objCount)

	// 4. Check that ListObjects returns correct number of items
	tutils.Logln("DONE\nReading bucket list...")
	var msg = &cmn.SelectMsg{PageSize: pagesize, Props: "size,status,version"}
	reslist, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckError(t, err)

	reslist.Entries = filterObjListOK(reslist.Entries)
	tassert.Errorf(t, len(reslist.Entries) == o.objCount, "Invalid number of objects: %d, expected %d", len(reslist.Entries), o.objCount)
}

func TestECEmergencyTargetForReplica(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     TestBucketName + "-replica-emergency",
			Provider: cmn.ProviderAIS,
		}
		proxyURL = tutils.RandomProxyURL()
	)

	o := ecOptions{
		minTgt:      4,
		dataCnt:     -1,
		objCount:    50,
		concurrency: 8,
		pattern:     "obj-rest-%04d",
	}.init(t, proxyURL)

	if o.smap.CountTargets() > 10 {
		// Reason: calculating main obj directory based on DeamonID
		// see getOneObj, 'HACK' annotation
		t.Skip("Test requires at most 10 targets")
	}

	for _, target := range o.smap.Tmap {
		target.Digest()
	}

	// Increase number of EC data slices, now there's just enough targets to handle EC requests
	// Encoding will fail if even one is missing, restoring should still work
	o.dataCnt++

	baseParams := tutils.BaseAPIParams(proxyURL)
	bckProps := defaultECBckProps(o)
	newLocalBckWithProps(t, baseParams, bck, bckProps, o)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	wg := sync.WaitGroup{}

	// PUT object
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		go func(objName string) {
			defer wg.Done()
			createECReplicas(t, baseParams, bck, objName, o)
		}(fmt.Sprintf(o.pattern, i))
	}

	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	// kill #dataslices of targets, normal EC restore won't be possible
	// 2. Kill a random target
	removedTargets := make(cluster.Nodes, 0, o.dataCnt)
	smap := tutils.GetClusterMap(t, proxyURL)

	for i := o.dataCnt - 1; i >= 0; i-- {
		var removedTarget *cluster.Snode
		smap, removedTarget = tutils.RemoveTarget(t, proxyURL, smap)
		removedTargets = append(removedTargets, removedTarget)
	}

	defer func() {
		// Restore targets
		smap = tutils.GetClusterMap(t, proxyURL)
		for _, target := range removedTargets {
			smap = tutils.RestoreTarget(t, proxyURL, smap, target)
		}
		tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)
	}()

	hasTarget := func(targets cluster.Nodes, target *cluster.Snode) bool {
		for _, tr := range targets {
			if tr.ID() == target.ID() {
				return true
			}
		}
		return false
	}

	getOneObj := func(objName string) {
		defer wg.Done()

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
				_, err := api.GetObject(baseParams, bck, objPath)
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
	clearAllECObjects(t, bck, false, o)
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

	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     TestBucketName + "-mpath-emergency",
			Provider: cmn.ProviderAIS,
		}
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	o := ecOptions{
		minTgt:      3,
		dataCnt:     1,
		parityCnt:   1,
		objCount:    400,
		concurrency: 24,
		pattern:     "obj-em-mpath-%04d",
	}.init(t, proxyURL)

	removeTarget := tutils.ExtractTargetNodes(o.smap)[0]
	mpathList, err := api.GetMountpaths(baseParams, removeTarget)
	tassert.CheckFatal(t, err)
	if len(mpathList.Available) < 2 {
		t.Fatalf("%s requires 2 or more mountpaths", t.Name())
	}

	sgl := tutils.MMSA.NewSGL(0)
	defer sgl.Free()

	bckProps := defaultECBckProps(o)
	newLocalBckWithProps(t, baseParams, bck, bckProps, o)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	wg := &sync.WaitGroup{}

	// 1. PUT objects
	putOneObj := func(idx int, objName string) {
		defer func() {
			wg.Done()
			o.sema.Release()
		}()

		totalCnt, objSize, sliceSize, doEC := randObjectSize(idx, smallEvery, o)
		objPath := ecTestDir + objName
		ecStr := "-"
		if doEC {
			ecStr = "EC"
		}
		tutils.Logf("Creating %s, size %8d [%2s]\n", objPath, objSize, ecStr)
		r, err := readers.NewRandReader(objSize, cmn.ChecksumNone)
		tassert.CheckFatal(t, err)
		defer r.Close()
		putArgs := api.PutObjectArgs{BaseParams: baseParams, Bck: bck, Object: objPath, Reader: r}
		err = api.PutObject(putArgs)
		tassert.CheckFatal(t, err)

		foundParts, mainObjPath := waitForECFinishes(t, totalCnt, objSize, sliceSize, doEC, bck, objName)
		ecCheckSlices(t, foundParts, bck, objPath, objSize, sliceSize, totalCnt)
		if mainObjPath == "" {
			t.Errorf("Full copy is not found")
			return
		}
	}

	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		o.sema.Acquire()
		go putOneObj(i, objName)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	// 2. Disable a random mountpath
	mpathID := o.rnd.Intn(len(mpathList.Available))
	removeMpath := mpathList.Available[mpathID]
	tutils.Logf("Disabling a mountpath %s at target: %s\n", removeMpath, removeTarget.ID())
	err = api.DisableMountpath(baseParams, removeTarget.ID(), removeMpath)
	tassert.CheckFatal(t, err)
	defer func() {
		// Enable mountpah
		tutils.Logf("Enabling mountpath %s at target %s...\n", removeMpath, removeTarget.ID())
		err = api.EnableMountpath(baseParams, removeTarget, removeMpath)
		tassert.CheckFatal(t, err)
	}()

	// 3. Read objects
	objectsExist(t, baseParams, bck, o.pattern, o.objCount)

	// 4. Check that ListObjects returns correct number of items
	tutils.Logf("DONE\nReading bucket list...\n")
	var msg = &cmn.SelectMsg{PageSize: pagesize, Props: "size,status,version"}
	reslist, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)

	reslist.Entries = filterObjListOK(reslist.Entries)
	if len(reslist.Entries) != o.objCount {
		t.Fatalf("Invalid number of objects: %d, expected %d", len(reslist.Entries), o.objCount)
	}
}

// NOTE: wipes out all content including ais system files and user data
func deleteAllFiles(t *testing.T, path string) {
	if len(path) < 5 {
		t.Fatalf("Invalid path %q", path)
		return
	}
	walkDel := func(fqn string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if tutils.IsTrashDir(path) {
			return filepath.SkipDir
		}
		if info.IsDir() {
			return nil
		}
		cmn.Assert(len(fqn) > 5)
		if err := os.Remove(fqn); err != nil {
			t.Logf("Failed to delete %q: %v", fqn, err)
		}
		return nil
	}
	filepath.Walk(path, walkDel)
}

func moveAllFiles(t *testing.T, pathFrom, pathTo string) {
	if len(pathFrom) < 5 || len(pathTo) < 5 {
		t.Fatalf("Invalid path %q or %q", pathFrom, pathTo)
		return
	}
	walkMove := func(fqn string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if tutils.IsTrashDir(fqn) {
			return filepath.SkipDir
		}
		if info.IsDir() {
			return nil
		}
		cmn.Assert(len(fqn) > 5)
		newPath := filepath.Join(pathTo, strings.TrimPrefix(fqn, pathFrom))
		newDir := filepath.Dir(newPath)
		if err := cmn.CreateDir(newDir); err != nil {
			t.Logf("Failed to create directory %q: %v", newDir, err)
			return nil
		}
		if err := os.Rename(fqn, newPath); err != nil {
			t.Logf("Failed to move %q to %q: %v", fqn, newPath, err)
		}
		return nil
	}
	filepath.Walk(pathFrom, walkMove)
}

func TestECRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	if containers.DockerRunning() {
		t.Skip(fmt.Sprintf("test %q requires direct access to filesystem, doesn't work with docker", t.Name()))
	}

	var (
		bck = cmn.Bck{
			Name:     TestBucketName + "-ec-rebalance",
			Provider: cmn.ProviderAIS,
		}
		proxyURL = tutils.RandomProxyURL()
	)
	o := ecOptions{
		objCount:    30,
		concurrency: 8,
		pattern:     "obj-reb-chk-%04d",
		silent:      true,
	}.init(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountTargets() <= test.parity+test.data {
				t.Skip("insufficient number of targets")
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			ecOnlyRebalance(t, o, proxyURL, bck)
		})
	}
}

// The test only checks that the number of object after rebalance equals
// the number of objects before it
func ecOnlyRebalance(t *testing.T, o *ecOptions, proxyURL string, bck cmn.Bck) {
	var (
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	wg := sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go func(i int) {
			defer wg.Done()
			createECObject(t, baseParams, bck, objName, i, o)
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	msg := &cmn.SelectMsg{Props: cmn.GetPropsSize}
	res, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckError(t, err)
	oldBucketList := filterObjListOK(res.Entries)
	tutils.Logf("%d objects created, starting rebalance\n", len(oldBucketList))

	// select a target that loses its mpath(simulate drive death),
	// and that has mpaths changed (simulate mpath added)
	tgtList := tutils.ExtractTargetNodes(o.smap)
	tgtLost, tgtSwap := tgtList[0], tgtList[1]

	lostFSList, err := api.GetMountpaths(baseParams, tgtLost)
	tassert.CheckFatal(t, err)
	if len(lostFSList.Available) < 2 {
		t.Fatalf("%s has only %d mountpaths, need at least 2", tgtLost.ID(), len(lostFSList.Available))
	}
	swapFSList, err := api.GetMountpaths(baseParams, tgtSwap)
	tassert.CheckFatal(t, err)
	if len(swapFSList.Available) < 2 {
		t.Fatalf("%s has only %d mountpaths, need at least 2", tgtSwap.ID(), len(swapFSList.Available))
	}

	// make troubles in mpaths
	// 1. Remove an mpath (only if parity is greater than 1, otherwise this
	//    step and the next one running together can delete all object data in
	//    case of the object is replicated)
	if o.parityCnt > 1 {
		lostPath := lostFSList.Available[0]
		tutils.Logf("Removing mountpath %q on target %s\n", lostPath, tgtLost.ID())
		tutils.CheckPathExists(t, lostPath, true /*dir*/)
		deleteAllFiles(t, lostPath)
	}

	// 2. Delete one, and rename the second: simulate mpath dead + new mpath attached
	// delete obj1 & delete meta1; rename obj2 -> ob1, and meta2 -> meta1
	swapPathObj1 := swapFSList.Available[0]
	tutils.Logf("Removing mountpath %q on target %s\n", swapPathObj1, tgtSwap.ID())
	tutils.CheckPathExists(t, swapPathObj1, true /*dir*/)
	deleteAllFiles(t, swapPathObj1)

	swapPathObj2 := swapFSList.Available[1]
	tutils.Logf("Renaming mountpath %q -> %q on target %s\n", swapPathObj2, swapPathObj1, tgtSwap.ID())
	tutils.CheckPathExists(t, swapPathObj2, true /*dir*/)
	moveAllFiles(t, swapPathObj2, swapPathObj1)

	// Kill a random target
	var (
		removedTarget *cluster.Snode
		smap          = o.smap
	)
	smap, removedTarget = tutils.RemoveTarget(t, proxyURL, smap)
	// Initiate rebalance
	tutils.RestoreTarget(t, proxyURL, smap, removedTarget)
	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	res, err = api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckError(t, err)
	newBucketList := filterObjListOK(res.Entries)
	if len(oldBucketList) != len(newBucketList) {
		for _, o := range oldBucketList {
			found := false
			for _, n := range newBucketList {
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
			len(oldBucketList), len(newBucketList))
	}

	for _, entry := range newBucketList {
		n, err := api.GetObject(baseParams, bck, entry.Name)
		if err != nil {
			t.Errorf("Failed to read %s: %v", entry.Name, err)
			continue // to avoid printing other error in this case
		}
		if n != entry.Size {
			t.Errorf("%s size mismatch read %d, props %d", entry.Name, n, entry.Size)
		}
	}
}

// Simple test to check if EC correctly finds all the objects and its slices
// that will be used by rebalance
func TestECBucketEncode(t *testing.T) {
	const parityCnt = 2
	var (
		proxyURL = tutils.RandomProxyURL()
		m        = ioContext{
			t:        t,
			num:      150,
			proxyURL: proxyURL,
		}
	)

	m.saveClusterState()
	baseParams := tutils.BaseAPIParams(proxyURL)

	if m.smap.CountTargets() < parityCnt+1 {
		t.Fatalf("Not enough targets to run %s test, must be at least %d", t.Name(), parityCnt+1)
	}

	tutils.CreateFreshBucket(t, proxyURL, m.bck)
	defer tutils.DestroyBucket(t, proxyURL, m.bck)

	m.puts()
	if t.Failed() {
		t.FailNow()
	}

	reslist, err := api.ListObjectsFast(baseParams, m.bck, nil)
	if err != nil {
		t.Fatalf("list_objects %s failed, err = %v", m.bck, err)
	}
	tutils.Logf("Object count: %d\n", len(reslist.Entries))
	if len(reslist.Entries) != m.num {
		t.Fatalf("list_objects %s invalid number of files %d, expected %d", m.bck, len(reslist.Entries), m.num)
	}

	tutils.Logf("Enabling EC\n")
	bckPropsToUpate := cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{
			Enabled:      api.Bool(true),
			ObjSizeLimit: api.Int64(1),
			DataSlices:   api.Int(1),
			ParitySlices: api.Int(parityCnt),
		},
	}
	err = api.SetBucketProps(baseParams, m.bck, bckPropsToUpate)
	tassert.CheckFatal(t, err)

	tutils.Logf("EC encode must start automatically for bucket %s\n", m.bck)
	xactArgs := api.XactReqArgs{Kind: cmn.ActECEncode, Bck: m.bck, Timeout: rebalanceTimeout}
	err = api.WaitForXaction(baseParams, xactArgs)
	tassert.CheckFatal(t, err)

	reslist, err = api.ListObjectsFast(baseParams, m.bck, nil)
	tassert.CheckFatal(t, err)

	if len(reslist.Entries) != m.num {
		t.Fatalf("bucket %s: expected %d objects, got %d", m.bck, m.num, len(reslist.Entries))
	}
	tutils.Logf("Object counts after EC finishes: %d (%d)\n", len(reslist.Entries), (parityCnt+1)*m.num)
	//
	// TODO: support querying bucket for total number of entries with respect to mirroring and EC
	//
}

func init() {
	proxyURL := tutils.GetPrimaryURL()
	primary, err := tutils.GetPrimaryProxy(proxyURL)
	if err != nil {
		tutils.Logf("ERROR: %v", err)
	}
	baseParams := tutils.BaseAPIParams(proxyURL)
	cfg, err := api.GetDaemonConfig(baseParams, primary.ID())
	if err != nil {
		tutils.Logf("ERROR: %v", err)
	}

	smap, err := api.GetClusterMap(baseParams)
	if err != nil {
		tutils.Logf("ERROR: %v", err)
	}
	mpaths := make([]string, 0, len(smap.Tmap)*5)
	for _, target := range smap.Tmap {
		mpathList, err := api.GetMountpaths(baseParams, target)
		if err != nil {
			tutils.Logf("ERROR: %v", err)
		}
		mpaths = append(mpaths, mpathList.Available...)
	}

	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	config.Cloud = cfg.Cloud
	cmn.GCO.CommitUpdate(config)

	fs.Init()
	fs.DisableFsIDCheck()
	for _, mpath := range mpaths {
		_ = fs.Add(mpath)
	}

	_ = fs.CSM.RegisterContentType(fs.ObjectType, &fs.ObjectContentResolver{})
	_ = fs.CSM.RegisterContentType(fs.WorkfileType, &fs.WorkfileContentResolver{})
	_ = fs.CSM.RegisterContentType(ec.SliceType, &ec.SliceSpec{})
	_ = fs.CSM.RegisterContentType(ec.MetaType, &ec.MetaSpec{})
}

// Creates two buckets (with EC enabled and disabled), fill them with data,
// and then runs two parallel rebalances
func TestECAndRegularRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	if containers.DockerRunning() {
		t.Skip(fmt.Sprintf("test %q requires direct access to filesystem, doesn't work with docker", t.Name()))
	}

	var (
		bckReg = cmn.Bck{
			Name:     TestBucketName + "-REG",
			Provider: cmn.ProviderAIS,
		}
		bckEC = cmn.Bck{
			Name:     TestBucketName + "-EC",
			Provider: cmn.ProviderAIS,
		}
		proxyURL = tutils.RandomProxyURL()
	)
	o := ecOptions{
		minTgt:      4,
		objCount:    90,
		concurrency: 8,
		pattern:     "obj-reb-chk-%04d",
		silent:      true,
	}.init(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountTargets() <= test.parity+test.data+1 {
				t.Skip("insufficient number of targets")
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			ecAndRegularRebalance(t, o, proxyURL, bckReg, bckEC)
		})
	}
}

func ecAndRegularRebalance(t *testing.T, o *ecOptions, proxyURL string, bckReg, bckEC cmn.Bck) {
	const (
		fileSize = 32 * cmn.KiB
	)
	var (
		baseParams = tutils.BaseAPIParams(proxyURL)
		cksumType  = cmn.DefaultBucketProps().Cksum.Type
	)

	tutils.CreateFreshBucket(t, proxyURL, bckReg)
	defer tutils.DestroyBucket(t, proxyURL, bckReg)
	newLocalBckWithProps(t, baseParams, bckEC, defaultECBckProps(o), o)
	defer tutils.DestroyBucket(t, proxyURL, bckEC)

	// select a target that loses its mpath(simulate drive death),
	// and that has mpaths changed (simulate mpath added)
	tgtList := tutils.ExtractTargetNodes(o.smap)
	tgtLost := tgtList[0]

	tutils.Logf("Unregistering %s...\n", tgtLost.ID())
	err := tutils.UnregisterNode(proxyURL, tgtLost.ID())
	tassert.CheckFatal(t, err)
	registered := false
	smap := tutils.GetClusterMap(t, proxyURL)
	defer func() {
		if !registered {
			err = tutils.RegisterNode(proxyURL, tgtLost, smap)
			tassert.CheckError(t, err)
		}
	}()

	// fill EC bucket
	wg := sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go func(i int) {
			defer wg.Done()
			createECObject(t, baseParams, bckEC, objName, i, o)
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	// fill regular bucket
	rpattern := "obj-reg-chk-%04d"
	fileList := make([]string, 0, o.objCount)
	for i := 0; i < o.objCount; i++ {
		fileList = append(fileList, fmt.Sprintf(rpattern, i))
	}
	errCh := make(chan error, len(fileList))
	objsPutCh := make(chan string, len(fileList))
	tutils.PutObjsFromList(proxyURL, bckReg, ecTestDir, fileSize, fileList, errCh, objsPutCh, cksumType)

	msg := &cmn.SelectMsg{}
	resECOld, err := api.ListObjects(baseParams, bckEC, msg, 0)
	tassert.CheckError(t, err)
	oldECList := filterObjListOK(resECOld.Entries)
	resRegOld, err := api.ListObjects(baseParams, bckReg, msg, 0)
	tassert.CheckError(t, err)
	oldRegList := filterObjListOK(resRegOld.Entries)
	tutils.Logf("Created %d objects in %s, %d objects in %s. Starting rebalance\n",
		len(oldECList), bckEC, len(oldRegList), bckReg)

	tutils.Logf("Registering node %s\n", tgtLost)
	err = tutils.RegisterNode(proxyURL, tgtLost, smap)
	tassert.CheckFatal(t, err)
	registered = true
	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	tutils.Logln("Getting the number of objects after rebalance")
	resECNew, err := api.ListObjects(baseParams, bckEC, msg, 0)
	tassert.CheckError(t, err)
	newECList := filterObjListOK(resECNew.Entries)
	tutils.Logf("%d objects in %s after rebalance\n",
		len(newECList), bckEC)
	resRegNew, err := api.ListObjects(baseParams, bckReg, msg, 0)
	tassert.CheckError(t, err)
	newRegList := filterObjListOK(resRegNew.Entries)
	tutils.Logf("%d objects in %s after rebalance\n",
		len(newRegList), bckReg)

	tutils.Logln("Test object readability after rebalance")
	for _, obj := range oldECList {
		_, err := api.GetObject(baseParams, bckEC, obj.Name)
		tassert.CheckError(t, err)
	}
	for _, obj := range oldRegList {
		_, err := api.GetObject(baseParams, bckReg, obj.Name)
		tassert.CheckError(t, err)
	}
}

// Simple resilver for EC bucket
// 1. Create a bucket
// 2. Remove mpath from one target
// 3. Creates enough objects to have at least one per mpath
//    So, minimal is <target count>*<mpath count>*2.
//    For tests 100 looks good
// 4. Attach removed mpath
// 5. Wait for rebalance to finish
// 6. Check that all objects returns the non-zero number of Data and Parity
//    slices in HEAD response
// 7. Extra check: the number of objects after rebalance equals initial number
func TestECResilver(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     TestBucketName + "-ec-resilver",
			Provider: cmn.ProviderAIS,
		}
		proxyURL = tutils.RandomProxyURL()
	)
	o := ecOptions{
		objCount:    100,
		concurrency: 8,
		pattern:     "obj-reb-loc-%04d",
		silent:      true,
	}.init(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountTargets() <= test.parity+test.data {
				t.Skip("insufficient number of targets")
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			ecResilver(t, o, proxyURL, bck)
		})
	}
}

func ecResilver(t *testing.T, o *ecOptions, proxyURL string, bck cmn.Bck) {
	var (
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	tgtList := tutils.ExtractTargetNodes(o.smap)
	tgtLost := tgtList[0]
	lostFSList, err := api.GetMountpaths(baseParams, tgtLost)
	tassert.CheckFatal(t, err)
	if len(lostFSList.Available) < 2 {
		t.Fatalf("%s has only %d mountpaths, required 2 or more", tgtLost.ID(), len(lostFSList.Available))
	}
	lostPath := lostFSList.Available[0]
	err = api.RemoveMountpath(baseParams, tgtLost.ID(), lostPath)
	tassert.CheckFatal(t, err)

	wg := sync.WaitGroup{}

	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go func(i int) {
			defer wg.Done()
			createECObject(t, baseParams, bck, objName, i, o)
		}(i)
	}
	wg.Wait()
	tutils.Logf("Created %d objects\n", o.objCount)

	err = api.AddMountpath(baseParams, tgtLost, lostPath)
	tassert.CheckFatal(t, err)
	// loop above may fail (even if AddMountpath works) and mark a test failed
	if t.Failed() {
		t.FailNow()
	}

	tutils.Logf("Wait for resilver to complete...\n")
	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	var msg = &cmn.SelectMsg{PageSize: pagesize, Props: "size"}
	resEC, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckError(t, err)
	newECList := filterObjListOK(resEC.Entries)
	tutils.Logf("%d objects in %s after rebalance\n", len(newECList), bck)
	if len(newECList) != o.objCount {
		t.Errorf("Expected %d objects after rebalance, found %d", o.objCount, len(newECList))
	}

	for i := 0; i < o.objCount; i++ {
		objName := ecTestDir + fmt.Sprintf(o.pattern, i)
		props, err := api.HeadObject(baseParams, bck, objName)
		if err != nil {
			t.Errorf("HEAD for %s failed: %v", objName, err)
		} else if props.DataSlices == 0 || props.ParitySlices == 0 {
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
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	if containers.DockerRunning() {
		t.Skip(fmt.Sprintf("test %q requires direct access to filesystem, doesn't work with docker", t.Name()))
	}
	var (
		bckEC = cmn.Bck{
			Name:     TestBucketName + "-EC",
			Provider: cmn.ProviderAIS,
		}
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		smap       = tutils.GetClusterMap(t, proxyURL)
		o          = ecOptions{
			minTgt:      4,
			objCount:    300,
			concurrency: 8,
			pattern:     "obj-reb-chk-%04d",
			silent:      true,
		}.init(t, proxyURL)
	)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountTargets() <= test.parity+test.data+1 {
				t.Skip("insufficient number of targets")
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			newLocalBckWithProps(t, baseParams, bckEC, defaultECBckProps(o), o)
			defer tutils.DestroyBucket(t, proxyURL, bckEC)
			ecAndRegularUnregisterWhileRebalancing(t, o, smap, bckEC)
		})
	}
}

func ecAndRegularUnregisterWhileRebalancing(t *testing.T, o *ecOptions, smap *cluster.Smap, bckEC cmn.Bck) {
	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)
	// select a target that loses its mpath(simulate drive death),
	// and that has mpaths changed (simulate mpath added)
	tgtList := tutils.ExtractTargetNodes(smap)
	tgtLost := tgtList[0]
	tgtGone := tgtList[1]

	tutils.Logf("Unregistering %s...\n", tgtLost.ID())
	err := tutils.UnregisterNode(proxyURL, tgtLost.ID())
	tassert.CheckFatal(t, err)
	registered := false
	smap = tutils.GetClusterMap(t, proxyURL)
	defer func() {
		if !registered {
			err = tutils.RegisterNode(proxyURL, tgtLost, smap)
			tassert.CheckError(t, err)
		}
	}()

	// fill EC bucket
	wg := sync.WaitGroup{}
	wg.Add(o.objCount)
	for i := 0; i < o.objCount; i++ {
		objName := fmt.Sprintf(o.pattern, i)
		go func(i int) {
			defer wg.Done()
			createECObject(t, baseParams, bckEC, objName, i, o)
		}(i)
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	msg := &cmn.SelectMsg{}
	resECOld, err := api.ListObjects(baseParams, bckEC, msg, 0)
	tassert.CheckError(t, err)
	oldECList := filterObjListOK(resECOld.Entries)
	tutils.Logf("Created %d objects in %s. Starting rebalance\n", len(oldECList), bckEC)

	tutils.Logf("Registering node %s\n", tgtLost.ID())
	err = tutils.RegisterNode(proxyURL, tgtLost, smap)
	tassert.CheckFatal(t, err)
	registered = true

	stopCh := cmn.NewStopCh()
	wg.Add(1)
	defer func() {
		stopCh.Close()
		wg.Wait()
	}()
	go func() {
		defer wg.Done()
		for {
			for _, obj := range oldECList {
				_, err := api.GetObject(baseParams, bckEC, obj.Name)
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

	tutils.Logf("Unregistering %s...\n", tgtGone.ID())
	err = tutils.UnregisterNode(proxyURL, tgtGone.ID())
	tassert.CheckFatal(t, err)
	smap = tutils.GetClusterMap(t, proxyURL)
	defer tutils.RegisterNode(proxyURL, tgtGone, smap)

	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)
	stopCh.Close()

	tutils.Logln("Getting the number of objects after rebalance")
	resECNew, err := api.ListObjects(baseParams, bckEC, msg, 0)
	tassert.CheckError(t, err)
	newECList := filterObjListOK(resECNew.Entries)
	tutils.Logf("%d objects in %s after rebalance\n",
		len(newECList), bckEC)
	if len(newECList) != len(oldECList) {
		t.Errorf("The number of objects before and after rebalance mismatches")
	}

	tutils.Logln("Test object readability after rebalance")
	for _, obj := range oldECList {
		_, err := api.GetObject(baseParams, bckEC, obj.Name)
		tassert.CheckError(t, err)
	}

	tutils.Logln("Getting the number of objects after reading")
	resECNew, err = api.ListObjects(baseParams, bckEC, msg, 0)
	tassert.CheckError(t, err)
	newECList = filterObjListOK(resECNew.Entries)
	tutils.Logf("%d objects in %s after reading\n",
		len(newECList), bckEC)
	if len(newECList) != len(oldECList) {
		t.Errorf("Incorrect number of objects: %d (expected %d)",
			len(newECList), len(oldECList))
	}
}
