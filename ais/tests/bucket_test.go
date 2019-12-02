// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func testBucketProps(t *testing.T) *cmn.BucketProps {
	proxyURL := getPrimaryURL(t, proxyURLReadOnly)
	globalConfig := getClusterConfig(t, proxyURL)

	return &cmn.BucketProps{
		Cksum: cmn.CksumConf{Type: cmn.PropInherit},
		LRU:   globalConfig.LRU,
	}
}

func TestDefaultBucketProps(t *testing.T) {
	const dataSlices = 7
	var (
		proxyURL     = getPrimaryURL(t, proxyURLReadOnly)
		globalConfig = getClusterConfig(t, proxyURL)
	)

	setClusterConfig(t, proxyURL, cmn.SimpleKVs{
		"ec_enabled":     "true",
		"ec_data_slices": strconv.FormatUint(dataSlices, 10),
	})
	defer setClusterConfig(t, proxyURL, cmn.SimpleKVs{
		"ec_enabled":       "false",
		"ec_data_slices":   fmt.Sprintf("%d", globalConfig.EC.DataSlices),
		"ec_parity_slices": fmt.Sprintf("%d", globalConfig.EC.ParitySlices),
	})

	tutils.CreateFreshBucket(t, proxyURL, TestBucketName)
	defer tutils.DestroyBucket(t, proxyURL, TestBucketName)
	p, err := api.HeadBucket(tutils.DefaultBaseAPIParams(t), TestBucketName)
	tassert.CheckFatal(t, err)
	if !p.EC.Enabled {
		t.Error("EC should be enabled for ais buckets")
	}
	if p.EC.DataSlices != dataSlices {
		t.Errorf("Invalid number of EC data slices: expected %d, got %d", dataSlices, p.EC.DataSlices)
	}
}

func TestResetBucketProps(t *testing.T) {
	var (
		proxyURL     = getPrimaryURL(t, proxyURLReadOnly)
		globalProps  cmn.BucketProps
		globalConfig = getClusterConfig(t, proxyURL)
	)

	setClusterConfig(t, proxyURL, cmn.SimpleKVs{"ec_enabled": "true"})
	defer setClusterConfig(t, proxyURL, cmn.SimpleKVs{
		"ec_enabled":       "false",
		"ec_data_slices":   fmt.Sprintf("%d", globalConfig.EC.DataSlices),
		"ec_parity_slices": fmt.Sprintf("%d", globalConfig.EC.ParitySlices),
	})

	tutils.CreateFreshBucket(t, proxyURL, TestBucketName)
	defer tutils.DestroyBucket(t, proxyURL, TestBucketName)

	bucketProps := defaultBucketProps()
	bucketProps.Cksum.Type = cmn.ChecksumNone
	bucketProps.Cksum.ValidateWarmGet = true
	bucketProps.Cksum.EnableReadRange = true
	bucketProps.EC.DataSlices = 1
	bucketProps.EC.ParitySlices = 2
	bucketProps.EC.Enabled = false

	globalProps.CloudProvider = cmn.ProviderAIS
	globalProps.Cksum = globalConfig.Cksum
	globalProps.LRU = testBucketProps(t).LRU
	globalProps.EC.ParitySlices = 1

	bParams := tutils.DefaultBaseAPIParams(t)
	err := api.SetBucketPropsMsg(bParams, TestBucketName, bucketProps)
	tassert.CheckFatal(t, err)

	p, err := api.HeadBucket(bParams, TestBucketName)
	tassert.CheckFatal(t, err)

	// check that bucket props do get set
	validateBucketProps(t, bucketProps, *p)
	err = api.ResetBucketProps(bParams, TestBucketName)
	tassert.CheckFatal(t, err)

	p, err = api.HeadBucket(bParams, TestBucketName)
	tassert.CheckFatal(t, err)

	// check that bucket props are reset
	validateBucketProps(t, globalProps, *p)
}

func TestSetInvalidBucketProps(t *testing.T) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	tests := []struct {
		name string
		nvs  cmn.SimpleKVs
	}{
		{
			name: "humongous number of copies",
			nvs: cmn.SimpleKVs{
				"mirror.enabled": "true",
				"mirror.copies":  "120",
			},
		},
		{
			name: "too many copies",
			nvs: cmn.SimpleKVs{
				"mirror.enabled": "true",
				"mirror.copies":  "12",
			},
		},
		{
			name: "humongous number of slices",
			nvs: cmn.SimpleKVs{
				"ec.enabled":       "true",
				"ec.parity_slices": "120",
			},
		},
		{
			name: "too many slices",
			nvs: cmn.SimpleKVs{
				"ec.enabled":       "true",
				"ec.parity_slices": "12",
			},
		},
		{
			name: "enable both ec and mirroring",
			nvs: cmn.SimpleKVs{
				"mirror.enabled": "true",
				"ec.enabled":     "true",
			},
		},
	}

	tutils.CreateFreshBucket(t, proxyURL, TestBucketName)
	defer tutils.DestroyBucket(t, proxyURL, TestBucketName)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := api.SetBucketProps(baseParams, TestBucketName, test.nvs)
			if err == nil {
				t.Error("expected error when setting bad input")
			}
		})
	}
}

func TestCloudListObjectVersions(t *testing.T) {
	var (
		workerCount = 10
		objectDir   = "cloud-version-test"
		objectSize  = 256
		objectCount = 1340 // must be greater than 1000(AWS page size)

		bucket   = clibucket
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		wg       = &sync.WaitGroup{}
	)

	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}
	if !isCloudBucket(t, proxyURL, bucket) {
		t.Skip("test requires a cloud bucket")
	}

	// Enable local versioning management
	baseParams := tutils.BaseAPIParams(proxyURL)
	err := api.SetBucketProps(baseParams, bucket,
		cmn.SimpleKVs{cmn.HeaderBucketVerEnabled: "true"})
	if err != nil {
		t.Fatal(err)
	}
	defer api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketVerEnabled: "false"})

	// Enabling local versioning may not work if the cloud bucket has
	// versioning disabled. So, request props and do double check
	bprops, err := api.HeadBucket(baseParams, bucket)
	if err != nil {
		t.Fatal(err)
	}

	if !bprops.Versioning.Enabled {
		t.Skip("test requires a cloud bucket with enabled versioning")
	}

	tutils.Logf("Filling the bucket %q\n", bucket)
	for wid := 0; wid < workerCount; wid++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			reader, err := tutils.NewRandReader(int64(objectSize), true)
			tassert.CheckFatal(t, err)
			objectsToPut := objectCount / workerCount
			if wid == workerCount-1 { // last worker puts leftovers
				objectsToPut += objectCount % workerCount
			}
			putRR(t, reader, bucket, objectDir, objectsToPut)
		}(wid)
	}
	wg.Wait()

	tutils.Logf("Reading bucket %q objects\n", bucket)
	msg := &cmn.SelectMsg{Prefix: objectDir, Props: cmn.GetPropsVersion}
	query := url.Values{}
	query.Add(cmn.URLParamProvider, cmn.Cloud)
	bckObjs, err := api.ListBucket(baseParams, bucket, msg, 0, query)
	tassert.CheckError(t, err)

	tutils.Logf("Checking bucket %q object versions[total: %d]\n", bucket, len(bckObjs.Entries))
	for _, entry := range bckObjs.Entries {
		if entry.Version == "" {
			t.Errorf("Object %s does not have version", entry.Name)
		}
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			err := api.DeleteObject(baseParams, bucket, name, cmn.Cloud)
			tassert.CheckError(t, err)
		}(entry.Name)
	}
	wg.Wait()
}

func TestListObjects(t *testing.T) {
	type objEntry struct {
		name string
		size int64
	}

	var (
		iterations  = 10
		workerCount = 10
		dirLen      = 10

		bucket = t.Name() + "Bucket"
		wg     = &sync.WaitGroup{}

		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	if testing.Short() {
		iterations = 3
	}

	tests := []struct {
		fast     bool
		withSize bool
		pageSize int
	}{
		{fast: false, withSize: true, pageSize: 0},

		{fast: true, withSize: false, pageSize: 0},
		{fast: true, withSize: true, pageSize: 0},
		{fast: true, withSize: false, pageSize: 2000},
	}

	for _, test := range tests {
		name := "slow"
		if test.fast {
			name = "fast"
		}
		if test.withSize {
			name += "/with_size"
		} else {
			name += "/without_size"
		}
		if test.fast && test.pageSize == 0 {
			name += "/without_paging"
		} else {
			name += "/with_paging"
		}
		t.Run(name, func(t *testing.T) {
			var (
				objs     sync.Map
				prefixes sync.Map
			)

			tutils.CreateFreshBucket(t, proxyURL, bucket)
			defer tutils.DestroyBucket(t, proxyURL, bucket)

			totalObjects := 0
			for iter := 1; iter <= iterations; iter++ {
				tutils.Logf("listing iteration: %d/%d (total_objs: %d)\n", iter, iterations, totalObjects)
				objectCount := rand.Intn(800) + 1010
				totalObjects += objectCount
				for wid := 0; wid < workerCount; wid++ {
					wg.Add(1)
					go func(wid int) {
						defer wg.Done()

						objectSize := int64(rand.Intn(256) + 20)
						reader, err := tutils.NewRandReader(objectSize, true)
						tassert.CheckFatal(t, err)
						objDir := tutils.RandomObjDir(dirLen, 5)
						objectsToPut := objectCount / workerCount
						if wid == workerCount-1 { // last worker puts leftovers
							objectsToPut += objectCount % workerCount
						}
						objNames := putRR(t, reader, bucket, objDir, objectsToPut)
						for _, objName := range objNames {
							objs.Store(objName, objEntry{
								name: objName,
								size: objectSize,
							})
						}

						if objDir != "" {
							prefixes.Store(objDir, objectsToPut)
						}
					}(wid)
				}
				wg.Wait()

				// Confirm PUTs by listing objects.
				msg := &cmn.SelectMsg{Fast: test.fast}
				msg.AddProps(cmn.GetPropsChecksum, cmn.GetPropsAtime, cmn.GetPropsVersion, cmn.GetPropsCopies)
				if test.withSize {
					msg.AddProps(cmn.GetPropsSize)
				}
				bckList, err := api.ListBucket(baseParams, bucket, msg, 0)
				tassert.CheckFatal(t, err)

				if len(bckList.Entries) != totalObjects {
					t.Errorf("actual objects %d, expected: %d", len(bckList.Entries), totalObjects)
				}
				if bckList.PageMarker != "" {
					t.Errorf("page marker was unexpectedly set to: %s", bckList.PageMarker)
				}

				empty := &cmn.BucketEntry{}
				for _, entry := range bckList.Entries {
					e, exists := objs.Load(entry.Name)
					if !exists {
						t.Errorf("object with name %s was listed but was not ever put", entry.Name)
						continue
					}

					obj := e.(objEntry)
					if test.withSize && obj.size != entry.Size {
						t.Errorf(
							"sizes do not match for object %s, expected: %d, got: %d",
							obj.name, obj.size, entry.Size,
						)
					} else if !test.withSize && entry.Size != 0 {
						t.Errorf("expected the size to be set to 0 for obj: %s", obj.name)
					}

					if test.fast {
						if entry.Checksum != empty.Checksum ||
							entry.Atime != empty.Atime ||
							entry.Version != empty.Version ||
							entry.TargetURL != empty.TargetURL ||
							entry.Flags != empty.Flags ||
							entry.Copies != empty.Copies {
							t.Errorf("some fields on object %q, do not have default values: %#v", entry.Name, entry)
						}
					} else {
						if entry.Checksum == empty.Checksum ||
							entry.Atime == empty.Atime ||
							entry.Version == empty.Version ||
							entry.Flags == empty.Flags ||
							entry.Copies == empty.Copies {
							t.Errorf("some fields on object %q, have default values: %#v", entry.Name, entry)
						}
					}
				}

				// Check if names in the entries are unique.
				objs.Range(func(key, _ interface{}) bool {
					objName := key.(string)
					i := sort.Search(len(bckList.Entries), func(i int) bool {
						return bckList.Entries[i].Name >= objName
					})
					if i == len(bckList.Entries) || bckList.Entries[i].Name != objName {
						t.Errorf("object %s was not found in the result of bucket listing", objName)
					}
					return true
				})

				// Check listing bucket with predefined prefix.
				prefixes.Range(func(key, value interface{}) bool {
					prefix := key.(string)
					expectedObjCount := value.(int)

					msg := &cmn.SelectMsg{
						Prefix: prefix,
					}
					if !test.fast {
						bckList, err = api.ListBucket(baseParams, bucket, msg, 0)
					} else {
						bckList, err = api.ListBucketFast(baseParams, bucket, msg)
					}
					tassert.CheckFatal(t, err)

					if expectedObjCount != len(bckList.Entries) {
						t.Errorf(
							"(prefix: %s), actual objects %d, expected: %d",
							prefix, len(bckList.Entries), expectedObjCount,
						)
					}

					for _, entry := range bckList.Entries {
						if !strings.HasPrefix(entry.Name, prefix) {
							t.Errorf("object %q does not have expected prefix: %q", entry.Name, prefix)
						}
					}
					return true
				})
			}
		})
	}
}

func TestListObjectsPrefix(t *testing.T) {
	const (
		fileSize = 1024
		numFiles = 20
		prefix   = "some_prefix"
	)

	var (
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	for _, provider := range []string{cmn.AIS, cmn.Cloud} {
		t.Run(provider, func(t *testing.T) {
			var (
				bucket     string
				errCh      = make(chan error, numFiles*5)
				filesPutCh = make(chan string, numfiles)
			)

			if cmn.IsProviderCloud(provider) {
				bucket = clibucket

				if !isCloudBucket(t, proxyURL, bucket) {
					t.Skipf("test requires a cloud bucket")
				}

				tutils.Logf("Cleaning up the cloud bucket %s\n", bucket)
				msg := &cmn.SelectMsg{Prefix: prefix}
				bckList, err := listObjects(t, proxyURL, msg, bucket, 0)
				tassert.CheckFatal(t, err)
				for _, entry := range bckList.Entries {
					err := tutils.Del(proxyURL, bucket, entry.Name, provider, nil, nil, false /*silent*/)
					tassert.CheckFatal(t, err)
				}
			} else {
				bucket = TestBucketName
				tutils.CreateFreshBucket(t, proxyURL, bucket)
				defer tutils.DestroyBucket(t, proxyURL, bucket)
			}

			sgl := tutils.Mem2.NewSGL(fileSize)
			defer sgl.Free()
			tutils.Logf("Create a list of %d objects\n", numFiles)

			fileList := make([]string, 0, numFiles)
			for i := 0; i < numFiles; i++ {
				fname := fmt.Sprintf("obj%d", i+1)
				fileList = append(fileList, fname)
			}

			tutils.PutObjsFromList(proxyURL, bucket, "", readerType, prefix, fileSize, fileList, errCh, filesPutCh, sgl)
			defer func() {
				// Cleanup objects created by the test
				for _, fname := range fileList {
					err := tutils.Del(proxyURL, bucket, prefix+"/"+fname, provider, nil, nil, true /*silent*/)
					tassert.CheckError(t, err)
				}
			}()

			close(filesPutCh)
			selectErr(errCh, "put", t, true /*fatal*/)
			close(errCh)

			tests := []struct {
				name     string
				prefix   string
				pageSize int
				limit    int
				expected int
			}{
				{
					"full_list_default_pageSize_no_limit",
					prefix, 0, 0,
					numFiles,
				},
				{
					"full_list_small_pageSize_no_limit",
					prefix, numFiles / 7, 0,
					numFiles,
				},
				{
					"full_list_limited",
					prefix, 0, 8,
					8,
				},
				{
					"full_list_prefixed",
					prefix + "/obj1", 0, 0,
					11, // obj1 and obj10..obj19
				},
				{
					"full_list_limited_prefixed",
					prefix + "/obj1", 0, 2,
					2, // obj1 and obj10
				},
				{
					"empty_list_prefixed",
					prefix + "/nothing", 0, 0,
					0,
				},
			}

			for _, test := range tests {
				for _, fast := range []bool{false /*slow*/, true /*fast*/} {
					name := test.name
					if !fast {
						name += "/slow"
					} else {
						name += "/fast"
					}
					t.Run(name, func(t *testing.T) {
						tutils.Logf("Prefix: %q, Expected objects: %d\n", test.prefix, test.expected)
						msg := &cmn.SelectMsg{Fast: fast, PageSize: test.pageSize, Prefix: test.prefix}
						tutils.Logf(
							"LIST bucket %s [fast: %v, prefix: %q, page_size: %d]\n",
							bucket, msg.Fast, msg.Prefix, msg.PageSize,
						)

						bckList, err := api.ListBucket(baseParams, bucket, msg, test.limit)
						tassert.CheckFatal(t, err)

						tutils.Logf("LIST output: %d objects\n", len(bckList.Entries))

						if len(bckList.Entries) != test.expected {
							t.Errorf("returned %d objects instead of %d", len(bckList.Entries), test.expected)
						}
					})
				}
			}
		})
	}
}

func TestListAndSummaryCloudObjects(t *testing.T) {
	tests := []struct {
		cached  bool
		fast    bool
		summary bool
	}{
		{summary: false, cached: false, fast: false},
		{summary: false, cached: false, fast: true},
		{summary: false, cached: true, fast: false},
		{summary: false, cached: true, fast: true},

		{summary: true, cached: false, fast: false},
		{summary: true, cached: false, fast: true},
		{summary: true, cached: true, fast: false},
		{summary: true, cached: true, fast: true},
	}

	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	for _, test := range tests {
		p := make([]string, 3)
		p[0] = "list"
		if test.summary {
			p[0] = "summary"
		}
		p[1] = "all"
		if test.cached {
			p[1] = "cached"
		}
		p[2] = "slow"
		if test.fast {
			p[2] = "fast"
		}

		t.Run(strings.Join(p, "/"), func(t *testing.T) {
			var (
				m = &ioContext{
					t:      t,
					bucket: clibucket,

					num: 2234,
				}
				cacheSize  = 1234 // determines number of objects which should be cached
				baseParams = tutils.DefaultBaseAPIParams(t)
			)

			m.saveClusterState()
			if m.originalTargetCount < 2 {
				t.Fatalf("must have at least 2 target in the cluster")
			}

			if !isCloudBucket(t, m.proxyURL, m.bucket) {
				t.Skipf("%s requires a cloud bucket", t.Name())
			}

			m.cloudPuts()
			defer m.cloudDelete()

			tutils.Logln("evicting cloud bucket...")
			err := api.EvictCloudBucket(baseParams, m.bucket)
			tassert.CheckFatal(t, err)

			expectedFiles := m.num
			if test.cached {
				m.cloudPrefetch(cacheSize)
				expectedFiles = cacheSize
			}

			tutils.Logln("checking objects...")

			msg := &cmn.SelectMsg{
				Cached: test.cached,
				Fast:   test.fast,
			}

			if test.summary {
				summaries, err := api.GetBucketsSummaries(baseParams, m.bucket, cmn.Cloud, msg)
				tassert.CheckFatal(t, err)

				if len(summaries) != 1 {
					t.Fatalf("number of summaries (%d) is larger than 1", len(summaries))
				}

				summary := summaries[m.bucket]
				if summary.ObjCount != uint64(expectedFiles) {
					t.Errorf("number of objects in summary (%d) after eviction is different than expected (%d)", summary.ObjCount, expectedFiles)
				}
			} else {
				objList, err := api.ListBucket(baseParams, m.bucket, msg, 0)
				tassert.CheckFatal(t, err)

				if len(objList.Entries) != expectedFiles {
					t.Errorf("number of listed objects (%d) after eviction is different than expected (%d)", len(objList.Entries), expectedFiles)
				}
			}
		})
	}
}

func TestBucketSingleProp(t *testing.T) {
	const (
		dataSlices      = 1
		paritySlices    = 1
		objLimit        = 300 * cmn.KiB
		mirrorThreshold = 15
	)
	var (
		m = ioContext{
			t: t,
		}
		baseParams = tutils.DefaultBaseAPIParams(t)
	)

	m.saveClusterState()
	if m.originalTargetCount < 3 {
		t.Fatalf("must have at least 3 target in the cluster")
	}

	tutils.CreateFreshBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bucket)

	tutils.Logf("Changing bucket %q properties...\n", m.bucket)

	// Enabling EC should set default value for number of slices if it is 0
	err := api.SetBucketProps(baseParams, m.bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "true"})
	tassert.CheckError(t, err)
	p, err := api.HeadBucket(baseParams, m.bucket)
	tassert.CheckFatal(t, err)
	if !p.EC.Enabled {
		t.Error("EC was not enabled")
	}
	if p.EC.DataSlices != 1 {
		t.Errorf("Number of data slices is incorrect: %d (expected 1)", p.EC.DataSlices)
	}
	if p.EC.ParitySlices != 1 {
		t.Errorf("Number of parity slices is incorrect: %d (expected 1)", p.EC.ParitySlices)
	}

	// Need to disable EC first
	err = api.SetBucketProps(baseParams, m.bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "false"})
	tassert.CheckError(t, err)

	// Enabling mirroring should set default value for number of copies if it is 0
	err = api.SetBucketProps(baseParams, m.bucket, cmn.SimpleKVs{cmn.HeaderBucketMirrorEnabled: "true"})
	tassert.CheckError(t, err)
	p, err = api.HeadBucket(baseParams, m.bucket)
	tassert.CheckFatal(t, err)
	if !p.Mirror.Enabled {
		t.Error("Mirroring was not enabled")
	}
	if p.Mirror.Copies != 2 {
		t.Errorf("Number of copies is incorrect: %d (expected 2)", p.Mirror.Copies)
	}

	// Need to disable mirroring first
	err = api.SetBucketProps(baseParams, m.bucket, cmn.SimpleKVs{cmn.HeaderBucketMirrorEnabled: "false"})
	tassert.CheckError(t, err)

	// Change a few more bucket properties
	err = api.SetBucketProps(
		baseParams, m.bucket,
		cmn.SimpleKVs{
			cmn.HeaderBucketECData:         strconv.Itoa(dataSlices),
			cmn.HeaderBucketECParity:       strconv.Itoa(paritySlices),
			cmn.HeaderBucketECObjSizeLimit: strconv.Itoa(objLimit),
		},
	)
	tassert.CheckError(t, err)

	// Enable EC again
	err = api.SetBucketProps(baseParams, m.bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "true"})
	tassert.CheckError(t, err)
	p, err = api.HeadBucket(tutils.DefaultBaseAPIParams(t), m.bucket)
	tassert.CheckFatal(t, err)
	if p.EC.DataSlices != dataSlices {
		t.Errorf("Number of data slices was not changed to %d. Current value %d", dataSlices, p.EC.DataSlices)
	}
	if p.EC.ParitySlices != paritySlices {
		t.Errorf("Number of parity slices was not changed to %d. Current value %d", paritySlices, p.EC.ParitySlices)
	}
	if p.EC.ObjSizeLimit != objLimit {
		t.Errorf("Minimal EC object size was not changed to %d. Current value %d", objLimit, p.EC.ObjSizeLimit)
	}

	// Need to disable EC first
	err = api.SetBucketProps(baseParams, m.bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "false"})
	tassert.CheckError(t, err)

	// Change mirroring threshold
	err = api.SetBucketProps(baseParams, m.bucket, cmn.SimpleKVs{cmn.HeaderBucketMirrorThresh: strconv.Itoa(mirrorThreshold)})
	tassert.CheckError(t, err)
	p, err = api.HeadBucket(tutils.DefaultBaseAPIParams(t), m.bucket)
	tassert.CheckFatal(t, err)
	if p.Mirror.UtilThresh != mirrorThreshold {
		t.Errorf("Mirror utilization threshold was not changed to %d. Current value %d", mirrorThreshold, p.Mirror.UtilThresh)
	}

	// Disable mirroring
	err = api.SetBucketProps(baseParams, m.bucket, cmn.SimpleKVs{cmn.HeaderBucketMirrorEnabled: "false"})
	tassert.CheckError(t, err)
}

func TestSetBucketPropsOfNonexistentBucket(t *testing.T) {
	var (
		bucket string

		baseParams = tutils.DefaultBaseAPIParams(t)
		query      = url.Values{}
	)
	query.Set(cmn.URLParamProvider, cmn.Cloud)

	bucket, err := tutils.GenerateNonexistentBucketName(t.Name()+"Bucket", baseParams)
	tassert.CheckFatal(t, err)

	err = api.SetBucketProps(baseParams, bucket, cmn.SimpleKVs{cmn.HeaderBucketECEnabled: "true"}, query)
	if err == nil {
		t.Fatalf("Expected SetBucketProps error, but got none.")
	}

	errAsHTTPError, ok := err.(*cmn.HTTPError)
	if !ok {
		t.Fatalf("Expected error of *cmn.HTTPError type.")
	}
	if errAsHTTPError.Status != http.StatusNotFound {
		t.Errorf("Expected status: %d, but got: %d.", http.StatusNotFound, errAsHTTPError.Status)
	}
}

func TestSetAllBucketPropsOfNonexistentBucket(t *testing.T) {
	var (
		bucket string

		baseParams  = tutils.DefaultBaseAPIParams(t)
		bucketProps = defaultBucketProps()
		query       = url.Values{}
	)
	query.Set(cmn.URLParamProvider, cmn.Cloud)

	bucket, err := tutils.GenerateNonexistentBucketName(t.Name()+"Bucket", baseParams)
	tassert.CheckFatal(t, err)

	err = api.SetBucketPropsMsg(baseParams, bucket, bucketProps, query)
	if err == nil {
		t.Fatalf("Expected SetBucketPropsMsg error, but got none.")
	}

	errAsHTTPError, ok := err.(*cmn.HTTPError)
	if !ok {
		t.Fatalf("Expected error of *cmn.HTTPError type.")
	}
	if errAsHTTPError.Status != http.StatusNotFound {
		t.Errorf("Expected status: %d, but got: %d.", http.StatusNotFound, errAsHTTPError.Status)
	}
}

func TestBucketInvalidName(t *testing.T) {
	var (
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.DefaultBaseAPIParams(t)
	)

	invalidNames := []string{"*", ".", "", " ", "bucket and name", "bucket/name"}
	for _, name := range invalidNames {
		if err := api.CreateBucket(baseParams, name); err == nil {
			tutils.DestroyBucket(t, proxyURL, name)
			t.Errorf("accepted bucket with name %s", name)
		}
	}
}

//===============================================================
//
// n-way mirror
//
//===============================================================
func TestLocalMirror(t *testing.T) {
	tests := []struct {
		numCopies []int // each of the number in the list represents the number of copies enforced on the bucket
	}{
		{[]int{1}}, // set number copies = 1 -- no copies should be created
		{[]int{2}},
		{[]int{2, 3}}, // first set number of copies to 2, then to 3
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.numCopies), func(t *testing.T) {
			testLocalMirror(t, test.numCopies)
		})
	}
}
func testLocalMirror(t *testing.T, numCopies []int) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		m = ioContext{
			t:               t,
			num:             10000,
			numGetsEachFile: 5,
			bucket:          cmn.RandString(10),
		}
	)

	m.saveClusterState()

	{
		targets := tutils.ExtractTargetNodes(m.smap)
		baseParams := tutils.DefaultBaseAPIParams(t)
		mpList, err := api.GetMountpaths(baseParams, targets[0])
		tassert.CheckFatal(t, err)

		l := len(mpList.Available)
		max := cmn.MaxInArray(numCopies...) + 1
		if l < max {
			t.Skipf("test %q requires at least %d mountpaths (target %s has %d)", t.Name(), max, targets[0], l)
		}
	}

	tutils.CreateFreshBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bucket)

	{
		var (
			bucketProps = defaultBucketProps()
		)
		primary, err := tutils.GetPrimaryProxy(proxyURLReadOnly)
		tassert.CheckFatal(t, err)

		baseParams := tutils.DefaultBaseAPIParams(t)
		config, err := api.GetDaemonConfig(baseParams, primary.ID())
		tassert.CheckFatal(t, err)

		// copy default config and change one field
		bucketProps.Mirror = config.Mirror
		bucketProps.Mirror.Enabled = true
		err = api.SetBucketPropsMsg(baseParams, m.bucket, bucketProps)
		tassert.CheckFatal(t, err)

		p, err := api.HeadBucket(baseParams, m.bucket)
		tassert.CheckFatal(t, err)
		if p.Mirror.Copies != 2 {
			t.Fatalf("%d copies != 2", p.Mirror.Copies)
		}
	}

	m.puts()
	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()

	baseParams := tutils.BaseAPIParams(m.proxyURL)

	for _, copies := range numCopies {
		makeNCopies(t, copies, m.bucket, baseParams)
		if copies > 1 {
			time.Sleep(10 * time.Second)
		}
	}

	m.wg.Wait()

	m.ensureNumCopies(numCopies[len(numCopies)-1])
}

func makeNCopies(t *testing.T, ncopies int, bucket string, baseParams api.BaseParams) {
	tutils.Logf("Set copies = %d\n", ncopies)
	if err := api.MakeNCopies(baseParams, bucket, ncopies); err != nil {
		t.Fatalf("Failed to start copies=%d xaction, err: %v", ncopies, err)
	}
	timedout := 60 // seconds
	ok := false
	for i := 0; i < timedout+1; i++ {
		time.Sleep(time.Second)

		allDetails, err := api.MakeXactGetRequest(baseParams, cmn.ActMakeNCopies, cmn.ActXactStats, bucket, true)
		tassert.CheckFatal(t, err)
		ok = true
		for tid := range allDetails {
			detail := allDetails[tid][0] // TODO
			if detail.Running() {
				ok = false
				break
			}
		}
		if ok {
			break
		}
	}
	if !ok {
		t.Fatalf("timed-out waiting for %s to finish", cmn.ActMakeNCopies)
	}
}

func TestCloudMirror(t *testing.T) {
	var (
		m = &ioContext{
			t:      t,
			num:    64,
			bucket: clibucket,
		}
		baseParams = tutils.DefaultBaseAPIParams(t)
	)

	m.saveClusterState()
	if !isCloudBucket(t, m.proxyURL, m.bucket) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}

	// evict
	err := api.EvictCloudBucket(baseParams, m.bucket)
	tassert.CheckFatal(t, err)

	// enable mirror
	err = api.SetBucketProps(baseParams, m.bucket, cmn.SimpleKVs{cmn.HeaderBucketMirrorEnabled: "true"})
	tassert.CheckFatal(t, err)
	defer api.SetBucketProps(baseParams, m.bucket, cmn.SimpleKVs{cmn.HeaderBucketMirrorEnabled: "false"})

	// list
	objectList, err := api.ListBucket(baseParams, m.bucket, nil, 0)
	tassert.CheckFatal(t, err)

	l := len(objectList.Entries)
	if l < m.num {
		t.Skipf("%s: insufficient number of objects in the Cloud bucket %s, required %d", t.Name(), m.bucket, m.num)
	}
	smap := getClusterMap(t, baseParams.URL)
	{
		target := tutils.ExtractTargetNodes(smap)[0]
		mpList, err := api.GetMountpaths(baseParams, target)
		tassert.CheckFatal(t, err)

		numps := len(mpList.Available)
		if numps < 4 {
			t.Skipf("test %q requires at least 4 mountpaths (target %s has %d)", t.Name(), target.ID(), numps)
		}
	}

	// cold GET - causes local mirroring
	tutils.Logf("cold GET %d object into a 2-way mirror...\n", m.num)
	j := int(time.Now().UnixNano() % int64(l))
	for i := 0; i < m.num; i++ {
		e := objectList.Entries[(j+i)%l]
		_, err := api.GetObject(baseParams, m.bucket, e.Name)
		tassert.CheckFatal(t, err)
	}
	m.ensureNumCopies(2)
	time.Sleep(4 * time.Second)

	// Increase number of copies
	makeNCopies(t, 3, m.bucket, baseParams)
	m.ensureNumCopies(3)
}

func TestBucketReadOnly(t *testing.T) {
	var (
		m = ioContext{
			t:               t,
			num:             10,
			numGetsEachFile: 2,
		}
	)
	m.init()
	tutils.CreateFreshBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bucket)
	baseParams := tutils.DefaultBaseAPIParams(t)

	m.puts()
	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()
	m.wg.Wait()

	p, err := api.HeadBucket(baseParams, m.bucket)
	tassert.CheckFatal(t, err)

	// make bucket read-only
	aattrs := cmn.MakeAccess(p.AccessAttrs, cmn.DenyAccess, cmn.AccessPUT|cmn.AccessDELETE)
	s := strconv.FormatUint(aattrs, 10)
	err = api.SetBucketProps(baseParams, m.bucket, cmn.SimpleKVs{cmn.HeaderBucketAccessAttrs: s})
	tassert.CheckFatal(t, err)

	m.init()
	nerr := m.puts(true /* don't fail */)
	if nerr != m.num {
		t.Fatalf("num failed PUTs %d, expecting %d", nerr, m.num)
	}

	// restore write access
	s = strconv.FormatUint(p.AccessAttrs, 10)
	err = api.SetBucketProps(baseParams, m.bucket, cmn.SimpleKVs{cmn.HeaderBucketAccessAttrs: s})
	tassert.CheckFatal(t, err)

	// write some more and destroy
	m.init()
	nerr = m.puts(true /* don't fail */)
	if nerr != 0 {
		t.Fatalf("num failed PUTs %d, expecting 0 (zero)", nerr)
	}
}

func TestRenameEmptyBucket(t *testing.T) {
	const (
		dstBckName = TestBucketName + "_new"
	)
	var (
		m = ioContext{
			t:  t,
			wg: &sync.WaitGroup{},
		}
		baseParams = tutils.DefaultBaseAPIParams(t)
	)

	// Initialize ioContext
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create ais bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bucket)
	tutils.DestroyBucket(t, m.proxyURL, dstBckName)

	m.setRandBucketProps()
	srcProps, err := api.HeadBucket(baseParams, m.bucket)
	tassert.CheckFatal(t, err)

	// Rename it
	tutils.Logf("rename %s => %s\n", m.bucket, dstBckName)
	err = api.RenameBucket(baseParams, m.bucket, dstBckName)
	tassert.CheckFatal(t, err)

	// Check if the new bucket appears in the list
	names, err := api.GetBucketNames(baseParams, cmn.AIS)
	tassert.CheckFatal(t, err)

	exists := cmn.StringInSlice(dstBckName, names.AIS)
	if !exists {
		t.Error("new bucket not found in buckets list")
	}

	tutils.Logln("checking bucket props...")
	dstProps, err := api.HeadBucket(baseParams, dstBckName)
	tassert.CheckFatal(t, err)
	if !reflect.DeepEqual(srcProps, dstProps) {
		t.Fatalf("source and destination bucket props do not match: %v - %v", srcProps, dstProps)
	}

	// Destroy renamed ais bucket
	tutils.DestroyBucket(t, m.proxyURL, dstBckName)
}

func TestRenameNonEmptyBucket(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}
	const (
		dstBckName = TestBucketName + "_new"
	)
	var (
		m = ioContext{
			t:               t,
			num:             1000,
			numGetsEachFile: 2,
		}
		baseParams = tutils.DefaultBaseAPIParams(t)
	)

	// Initialize ioContext
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create ais bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bucket)
	tutils.DestroyBucket(t, m.proxyURL, dstBckName)

	m.setRandBucketProps()
	srcProps, err := api.HeadBucket(baseParams, m.bucket)
	tassert.CheckFatal(t, err)

	// Put some files
	m.puts()

	// Rename it
	tutils.Logf("rename %s => %s\n", m.bucket, dstBckName)
	srcBckName := m.bucket
	m.bucket = dstBckName
	err = api.RenameBucket(baseParams, srcBckName, m.bucket)
	tassert.CheckFatal(t, err)

	waitForBucketXactionToComplete(t, cmn.ActRenameLB /*kind*/, srcBckName, baseParams, rebalanceTimeout)

	// Gets on renamed ais bucket
	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()
	m.wg.Wait()
	m.ensureNoErrors()

	tutils.Logln("checking bucket props...")
	dstProps, err := api.HeadBucket(baseParams, dstBckName)
	tassert.CheckFatal(t, err)
	if !reflect.DeepEqual(srcProps, dstProps) {
		t.Fatalf("source and destination bucket props do not match: %v - %v", srcProps, dstProps)
	}
}

func TestRenameAlreadyExistingBucket(t *testing.T) {
	const (
		tmpBckName = "tmp_bck_name"
	)
	var (
		m = ioContext{
			t:  t,
			wg: &sync.WaitGroup{},
		}
		baseParams = tutils.DefaultBaseAPIParams(t)
	)

	// Initialize ioContext
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bucket)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bucket)

	m.setRandBucketProps()

	tutils.CreateFreshBucket(t, m.proxyURL, tmpBckName)
	defer tutils.DestroyBucket(t, m.proxyURL, tmpBckName)

	// Rename it
	tutils.Logf("try rename %s => %s\n", m.bucket, tmpBckName)
	err := api.RenameBucket(baseParams, m.bucket, tmpBckName)
	if err == nil {
		t.Fatal("expected error on renaming already existing bucket")
	}

	// Check if the old bucket still appears in the list
	names, err := api.GetBucketNames(baseParams, cmn.AIS)
	tassert.CheckFatal(t, err)

	if !cmn.StringInSlice(m.bucket, names.AIS) || !cmn.StringInSlice(tmpBckName, names.AIS) {
		t.Error("one of the buckets was not found in buckets list")
	}

	srcProps, err := api.HeadBucket(baseParams, m.bucket)
	tassert.CheckFatal(t, err)

	dstProps, err := api.HeadBucket(baseParams, tmpBckName)
	tassert.CheckFatal(t, err)

	if reflect.DeepEqual(srcProps, dstProps) {
		t.Fatalf("source and destination bucket props match, even though they should not: %v - %v", srcProps, dstProps)
	}
}

func TestCopyBucket(t *testing.T) {
	numput := 100
	tests := []struct {
		provider         string
		dstBckExist      bool // determines if destination bucket exists before copy or not
		dstBckHasObjects bool // determines if destination bucket contains any objects before copy or not
		multipleDests    bool // determines if there are multiple destinations to which objects are copied
	}{
		// ais
		{provider: cmn.ProviderAIS, dstBckExist: false, dstBckHasObjects: false, multipleDests: false},
		{provider: cmn.ProviderAIS, dstBckExist: true, dstBckHasObjects: false, multipleDests: false},
		{provider: cmn.ProviderAIS, dstBckExist: true, dstBckHasObjects: true, multipleDests: false},
		{provider: cmn.ProviderAIS, dstBckExist: false, dstBckHasObjects: false, multipleDests: true},
		{provider: cmn.ProviderAIS, dstBckExist: true, dstBckHasObjects: true, multipleDests: true},

		// cloud
		{provider: cmn.Cloud, dstBckExist: false, dstBckHasObjects: false},
		{provider: cmn.Cloud, dstBckExist: true, dstBckHasObjects: false},
		{provider: cmn.Cloud, dstBckExist: true, dstBckHasObjects: true},
		{provider: cmn.Cloud, dstBckExist: false, dstBckHasObjects: false, multipleDests: true},
		{provider: cmn.Cloud, dstBckExist: true, dstBckHasObjects: true, multipleDests: true},
	}

	for _, test := range tests {
		// Bucket must exist when we require it to have objects.
		cmn.Assert(test.dstBckExist || !test.dstBckHasObjects)

		testName := test.provider + "/"
		if test.dstBckExist {
			testName += "present/"
			if test.dstBckHasObjects {
				testName += "with_objs"
			} else {
				testName += "without_objs"
			}
		} else {
			testName += "absent"
		}
		if test.multipleDests {
			testName += "/multiple_dests"
		}

		t.Run(testName, func(t *testing.T) {
			var (
				srcBckList *cmn.BucketList

				srcm = &ioContext{
					t:               t,
					num:             numput,
					bucket:          "src_copy_bck",
					numGetsEachFile: 1,
				}
				dstms = []*ioContext{
					{
						t:               t,
						num:             numput,
						bucket:          "dst_copy_bck_1",
						numGetsEachFile: 1,
					},
				}
				baseParams = tutils.DefaultBaseAPIParams(t)
			)

			if test.multipleDests {
				dstms = append(dstms, &ioContext{
					t:               t,
					num:             numput,
					bucket:          "dst_copy_bck_2",
					numGetsEachFile: 1,
				})
			}

			if test.provider == cmn.Cloud {
				srcm.bucket = clibucket

				if !isCloudBucket(t, proxyURL, srcm.bucket) {
					t.Skip("test requires a cloud bucket")
				}
			}

			// Initialize ioContext
			srcm.saveClusterState()
			for _, dstm := range dstms {
				dstm.init()
			}
			if srcm.originalTargetCount < 1 {
				t.Fatalf("Must have 1 or more targets in the cluster, have only %d", srcm.originalTargetCount)
			}

			if test.provider == cmn.ProviderAIS {
				tutils.CreateFreshBucket(t, srcm.proxyURL, srcm.bucket)
				defer tutils.DestroyBucket(t, srcm.proxyURL, srcm.bucket)
				srcm.setRandBucketProps()
			}

			if test.dstBckExist {
				for _, dstm := range dstms {
					tutils.CreateFreshBucket(t, dstm.proxyURL, dstm.bucket)
					dstm.setRandBucketProps()
				}
			} else { // cleanup
				for _, dstm := range dstms {
					tutils.DestroyBucket(t, dstm.proxyURL, dstm.bucket)
				}
			}
			for _, dstm := range dstms {
				defer tutils.DestroyBucket(t, dstm.proxyURL, dstm.bucket)
			}

			srcProps, err := api.HeadBucket(baseParams, srcm.bucket)
			tassert.CheckFatal(t, err)

			if test.dstBckHasObjects {
				for _, dstm := range dstms {
					dstm.puts()
				}
			}

			if test.provider == cmn.ProviderAIS {
				srcm.puts()

				srcBckList, err = api.ListBucket(baseParams, srcm.bucket, nil, 0)
				tassert.CheckFatal(t, err)
			} else if test.provider == cmn.Cloud {
				srcm.cloudPuts()
				defer srcm.cloudDelete()

				srcBckList, err = api.ListBucket(baseParams, srcm.bucket, nil, 0)
				tassert.CheckFatal(t, err)
			}

			for _, dstm := range dstms {
				tutils.Logf("copying %s => %s\n", srcm.bucket, dstm.bucket)
				err = api.CopyBucket(baseParams, srcm.bucket, dstm.bucket)
				tassert.CheckFatal(t, err)
			}

			for _, dstm := range dstms {
				waitForBucketXactionToComplete(t, cmn.ActCopyBucket /*kind*/, dstm.bucket, baseParams, rebalanceTimeout)
			}

			tutils.Logln("checking and comparing bucket props")
			for _, dstm := range dstms {
				dstProps, err := api.HeadBucket(baseParams, dstm.bucket)
				tassert.CheckFatal(t, err)

				if dstProps.CloudProvider != cmn.ProviderAIS {
					t.Fatalf("destination bucket does not seem to be 'ais': %s", dstProps.CloudProvider)
				}
				// Clear providers to make sure that they will fail on different providers.
				srcProps.CloudProvider = ""
				dstProps.CloudProvider = ""

				// If bucket existed before, we need to ensure that the bucket
				// props were **not** copied over.
				if test.dstBckExist && reflect.DeepEqual(srcProps, dstProps) {
					t.Fatalf("source and destination bucket props match, even though they should not:\n%#v\n%#v",
						srcProps, dstProps)
				}

				// If bucket did not exist before, we need to ensure that
				// the bucket props match the source bucket props (except provider).
				if !test.dstBckExist && !reflect.DeepEqual(srcProps, dstProps) {
					t.Fatalf("source and destination bucket props do not match:\n%#v\n%#v", srcProps, dstProps)
				}
			}

			tutils.Logln("checking and comparing objects")

			for _, dstm := range dstms {
				expectedObjCount := srcm.num
				if test.dstBckHasObjects {
					expectedObjCount += dstm.num
				}

				dstBckList, err := api.ListBucketFast(baseParams, dstm.bucket, nil)
				tassert.CheckFatal(t, err)
				if len(dstBckList.Entries) != expectedObjCount {
					t.Fatalf("list-bucket: dst %d != %d src", len(dstBckList.Entries), expectedObjCount)
				}
				for _, a := range srcBckList.Entries {
					var found bool
					for _, b := range dstBckList.Entries {
						if a.Name == b.Name {
							found = true
							break
						}
					}
					if !found {
						t.Fatalf("%s/%s is missing in the copied objects", srcm.bucket, a.Name)
					}
				}
			}
		})
	}
}

func TestDirectoryExistenceWhenModifyingBucket(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}
	const (
		newTestBucketName = TestBucketName + "_new"
	)
	var (
		m = ioContext{
			t:  t,
			wg: &sync.WaitGroup{},
		}
		baseParams = tutils.DefaultBaseAPIParams(t)
	)

	// Initialize ioContext
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	localBucketDir := ""
	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if localBucketDir != "" {
			return filepath.SkipDir
		}
		if strings.HasSuffix(path, "/local") && strings.Contains(path, fs.ObjectType) {
			localBucketDir = path
			return filepath.SkipDir
		}
		return nil
	}
	filepath.Walk(rootDir, fsWalkFunc)
	tutils.Logf("ais bucket's dir: %s\n", localBucketDir)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bucket)
	tutils.DestroyBucket(t, m.proxyURL, newTestBucketName)

	bucketFQN := filepath.Join(localBucketDir, m.bucket)
	tutils.CheckPathExists(t, bucketFQN, true /*dir*/)

	err := api.RenameBucket(baseParams, m.bucket, newTestBucketName)
	tassert.CheckFatal(t, err)

	waitForBucketXactionToComplete(t, cmn.ActRenameLB /*kind*/, m.bucket, baseParams, rebalanceTimeout)

	tutils.CheckPathNotExists(t, bucketFQN)

	newBucketFQN := filepath.Join(localBucketDir, newTestBucketName)
	tutils.CheckPathExists(t, newBucketFQN, true /*dir*/)

	tutils.DestroyBucket(t, m.proxyURL, newTestBucketName)
}
