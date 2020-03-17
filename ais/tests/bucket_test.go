// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestDefaultBucketProps(t *testing.T) {
	const dataSlices = 7
	var (
		proxyURL     = tutils.GetPrimaryURL()
		globalConfig = tutils.GetClusterConfig(t)
		bck          = cmn.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}
	)

	tutils.SetClusterConfig(t, cmn.SimpleKVs{
		"ec.enabled":     "true",
		"ec.data_slices": strconv.FormatUint(dataSlices, 10),
	})
	defer tutils.SetClusterConfig(t, cmn.SimpleKVs{
		"ec.enabled":       "false",
		"ec.data_slices":   fmt.Sprintf("%d", globalConfig.EC.DataSlices),
		"ec.parity_slices": fmt.Sprintf("%d", globalConfig.EC.ParitySlices),
	})

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)
	p, err := api.HeadBucket(tutils.DefaultBaseAPIParams(t), bck)
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
		proxyURL     = tutils.GetPrimaryURL()
		globalConfig = tutils.GetClusterConfig(t)
		baseParams   = tutils.DefaultBaseAPIParams(t)
		bck          = cmn.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}
	)

	tutils.SetClusterConfig(t, cmn.SimpleKVs{"ec.enabled": "true"})
	defer tutils.SetClusterConfig(t, cmn.SimpleKVs{
		"ec.enabled":       "false",
		"ec.data_slices":   fmt.Sprintf("%d", globalConfig.EC.DataSlices),
		"ec.parity_slices": fmt.Sprintf("%d", globalConfig.EC.ParitySlices),
	})

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	defaultProps, err := api.HeadBucket(baseParams, bck)
	tassert.CheckFatal(t, err)

	propsToUpdate := cmn.BucketPropsToUpdate{
		Cksum: &cmn.CksumConfToUpdate{
			Type:            api.String(cmn.ChecksumNone),
			ValidateWarmGet: api.Bool(true),
			EnableReadRange: api.Bool(true),
		},
		EC: &cmn.ECConfToUpdate{
			Enabled:      api.Bool(false),
			DataSlices:   api.Int(1),
			ParitySlices: api.Int(2),
		},
	}

	err = api.SetBucketProps(baseParams, bck, propsToUpdate)
	tassert.CheckFatal(t, err)

	p, err := api.HeadBucket(baseParams, bck)
	tassert.CheckFatal(t, err)

	// check that bucket props do get set
	validateBucketProps(t, propsToUpdate, p)
	err = api.ResetBucketProps(baseParams, bck)
	tassert.CheckFatal(t, err)

	p, err = api.HeadBucket(baseParams, bck)
	tassert.CheckFatal(t, err)

	if !p.Equal(&defaultProps) {
		t.Errorf("props have not been reset properly: expected: %+v, got: %+v", defaultProps, p)
	}
}

func TestSetInvalidBucketProps(t *testing.T) {
	var (
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}

		tests = []struct {
			name  string
			props cmn.BucketPropsToUpdate
		}{
			{
				name: "humongous number of copies",
				props: cmn.BucketPropsToUpdate{
					Mirror: &cmn.MirrorConfToUpdate{
						Enabled: api.Bool(true),
						Copies:  api.Int64(120),
					},
				},
			},
			{
				name: "too many copies",
				props: cmn.BucketPropsToUpdate{
					Mirror: &cmn.MirrorConfToUpdate{
						Enabled: api.Bool(true),
						Copies:  api.Int64(12),
					},
				},
			},
			{
				name: "humongous number of slices",
				props: cmn.BucketPropsToUpdate{
					EC: &cmn.ECConfToUpdate{
						Enabled:      api.Bool(true),
						ParitySlices: api.Int(120),
					},
				},
			},
			{
				name: "too many slices",
				props: cmn.BucketPropsToUpdate{
					EC: &cmn.ECConfToUpdate{
						Enabled:      api.Bool(true),
						ParitySlices: api.Int(12),
					},
				},
			},
			{
				name: "enable both ec and mirroring",
				props: cmn.BucketPropsToUpdate{
					EC:     &cmn.ECConfToUpdate{Enabled: api.Bool(true)},
					Mirror: &cmn.MirrorConfToUpdate{Enabled: api.Bool(true)},
				},
			},
		}
	)

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := api.SetBucketProps(baseParams, bck, test.props)
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
		bck         = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.Cloud,
		}
		proxyURL = tutils.GetPrimaryURL()
		wg       = &sync.WaitGroup{}
		sema     = make(chan struct{}, 40) // throttle DELETE
	)

	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}
	if !isCloudBucket(t, proxyURL, bck) {
		t.Skip("test requires a cloud bucket")
	}

	// Enable local versioning management
	baseParams := tutils.BaseAPIParams(proxyURL)
	err := api.SetBucketProps(baseParams, bck, cmn.BucketPropsToUpdate{
		Versioning: &cmn.VersionConfToUpdate{Enabled: api.Bool(true)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer api.SetBucketProps(baseParams, bck, cmn.BucketPropsToUpdate{
		Versioning: &cmn.VersionConfToUpdate{Enabled: api.Bool(false)},
	})

	// Enabling local versioning may not work if the cloud bucket has
	// versioning disabled. So, request props and do double check
	bprops, err := api.HeadBucket(baseParams, bck)
	if err != nil {
		t.Fatal(err)
	}

	if !bprops.Versioning.Enabled {
		t.Skip("test requires a cloud bucket with enabled versioning")
	}

	tutils.Logf("Filling the bucket %s\n", bck)
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
			putRR(t, reader, bck, objectDir, objectsToPut)
		}(wid)
	}
	wg.Wait()

	tutils.Logf("Reading bucket %q objects\n", bck)
	msg := &cmn.SelectMsg{Prefix: objectDir, Props: cmn.GetPropsVersion}
	bckObjs, err := api.ListBucket(baseParams, bck, msg, 0)
	tassert.CheckError(t, err)

	tutils.Logf("Checking bucket %q object versions[total: %d]\n", bck, len(bckObjs.Entries))
	for _, entry := range bckObjs.Entries {
		if entry.Version == "" {
			t.Errorf("Object %s does not have version", entry.Name)
		}
		wg.Add(1)
		go func(name string) {
			sema <- struct{}{}
			err := api.DeleteObject(baseParams, bck, name)
			<-sema
			tassert.CheckError(t, err)
			wg.Done()
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

		bck = cmn.Bck{
			Name:     t.Name() + "Bucket",
			Provider: cmn.ProviderAIS,
		}
		wg = &sync.WaitGroup{}

		proxyURL   = tutils.GetPrimaryURL()
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

			tutils.CreateFreshBucket(t, proxyURL, bck)
			defer tutils.DestroyBucket(t, proxyURL, bck)

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
						objNames := putRR(t, reader, bck, objDir, objectsToPut)
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
				msg := &cmn.SelectMsg{Fast: test.fast, PageSize: test.pageSize}
				msg.AddProps(cmn.GetPropsChecksum, cmn.GetPropsAtime, cmn.GetPropsVersion, cmn.GetPropsCopies)
				if test.withSize {
					msg.AddProps(cmn.GetPropsSize)
				}
				bckList, err := api.ListBucket(baseParams, bck, msg, 0)
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
						t.Errorf("failed to locate object %s in bucket %s", entry.Name, bck)
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
						bckList, err = api.ListBucket(baseParams, bck, msg, 0)
					} else {
						bckList, err = api.ListBucketFast(baseParams, bck, msg)
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
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	for _, provider := range []string{cmn.ProviderAIS, cmn.Cloud} {
		t.Run(provider, func(t *testing.T) {
			var (
				bck        cmn.Bck
				errCh      = make(chan error, numFiles*5)
				filesPutCh = make(chan string, numfiles)
			)

			if cmn.IsProviderCloud(cmn.Bck{Provider: provider, Ns: cmn.NsGlobal}, true /*acceptAnon*/) {
				bck = cmn.Bck{
					Name:     clibucket,
					Provider: provider,
				}

				if !isCloudBucket(t, proxyURL, bck) {
					t.Skipf("test requires a cloud bucket")
				}

				tutils.Logf("Cleaning up the cloud bucket %s\n", bck)
				msg := &cmn.SelectMsg{Prefix: prefix}
				bckList, err := listObjects(t, proxyURL, bck, msg, 0)
				tassert.CheckFatal(t, err)
				for _, entry := range bckList.Entries {
					err := tutils.Del(proxyURL, bck, entry.Name, nil, nil, false /*silent*/)
					tassert.CheckFatal(t, err)
				}
			} else {
				bck = cmn.Bck{
					Name:     TestBucketName,
					Provider: provider,
				}
				tutils.CreateFreshBucket(t, proxyURL, bck)
				defer tutils.DestroyBucket(t, proxyURL, bck)
			}

			tutils.Logf("Create a list of %d objects\n", numFiles)

			fileList := make([]string, 0, numFiles)
			for i := 0; i < numFiles; i++ {
				fname := fmt.Sprintf("obj%d", i+1)
				fileList = append(fileList, fname)
			}

			tutils.PutObjsFromList(proxyURL, bck, prefix, fileSize, fileList, errCh, filesPutCh)
			defer func() {
				// Cleanup objects created by the test
				for _, fname := range fileList {
					err := tutils.Del(proxyURL, bck, prefix+"/"+fname, nil, nil, true /*silent*/)
					tassert.CheckError(t, err)
				}
			}()

			close(filesPutCh)
			tassert.SelectErr(t, errCh, "put", true /*fatal*/)
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
							bck, msg.Fast, msg.Prefix, msg.PageSize,
						)

						bckList, err := api.ListBucket(baseParams, bck, msg, test.limit)
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

func TestBucketListAndSummary(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	type test struct {
		provider string
		summary  bool
		cached   bool
		fast     bool
	}

	var tests []test
	for _, provider := range []string{cmn.ProviderAIS, cmn.Cloud} {
		for _, summary := range []bool{false, true} {
			for _, cached := range []bool{false, true} {
				for _, fast := range []bool{false, true} {
					tests = append(tests, test{
						provider: provider,
						summary:  summary,
						cached:   cached,
						fast:     fast,
					})
				}
			}
		}
	}

	for _, test := range tests {
		p := make([]string, 4)
		p[0] = test.provider
		p[1] = "list"
		if test.summary {
			p[1] = "summary"
		}
		p[2] = "all"
		if test.cached {
			p[2] = "cached"
		}
		p[3] = "slow"
		if test.fast {
			p[3] = "fast"
		}
		t.Run(strings.Join(p, "/"), func(t *testing.T) {
			var (
				m = &ioContext{
					t: t,
					bck: cmn.Bck{
						Name:     cmn.RandString(10),
						Provider: test.provider,
					},

					num: 2234,
				}
				cacheSize  = 1234 // determines number of objects which should be cached
				proxyURL   = tutils.GetPrimaryURL()
				baseParams = tutils.DefaultBaseAPIParams(t)
			)

			m.saveClusterState()
			if m.originalTargetCount < 2 {
				t.Fatalf("must have at least 2 target in the cluster")
			}

			expectedFiles := m.num
			if cmn.IsProviderAIS(cmn.Bck{Provider: test.provider, Ns: cmn.NsGlobal}) {
				tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
				defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

				m.puts()
			} else if cmn.IsProviderCloud(cmn.Bck{Provider: test.provider, Ns: cmn.NsGlobal}, true /*acceptAnon*/) {
				m.bck.Name = clibucket

				if !isCloudBucket(t, proxyURL, m.bck) {
					t.Skip("test requires a cloud bucket")
				}

				m.cloudPuts()
				defer m.cloudDelete()

				tutils.Logln("evicting cloud bucket...")
				err := api.EvictCloudBucket(baseParams, m.bck)
				tassert.CheckFatal(t, err)

				if test.cached {
					m.cloudPrefetch(cacheSize)
					expectedFiles = cacheSize
				}
			} else {
				panic(test.provider)
			}

			tutils.Logln("checking objects...")

			msg := &cmn.SelectMsg{
				Cached: test.cached,
				Fast:   test.fast,
			}

			if test.summary {
				summaries, err := api.GetBucketsSummaries(baseParams, m.bck, msg)
				tassert.CheckFatal(t, err)

				if len(summaries) != 1 {
					t.Fatalf("number of summaries (%d) is larger than 1", len(summaries))
				}

				summary := summaries[m.bck.Name]
				if summary.ObjCount != uint64(expectedFiles) {
					t.Errorf("number of objects in summary (%d) is different than expected (%d)", summary.ObjCount, expectedFiles)
				}
			} else {
				objList, err := api.ListBucket(baseParams, m.bck, msg, 0)
				tassert.CheckFatal(t, err)

				if len(objList.Entries) != expectedFiles {
					t.Errorf("number of listed objects (%d) is different than expected (%d)", len(objList.Entries), expectedFiles)
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

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	tutils.Logf("Changing bucket %q properties...\n", m.bck)

	// Enabling EC should set default value for number of slices if it is 0
	err := api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(true)},
	})
	tassert.CheckError(t, err)
	p, err := api.HeadBucket(baseParams, m.bck)
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
	err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(false)},
	})
	tassert.CheckError(t, err)

	// Enabling mirroring should set default value for number of copies if it is 0
	err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{Enabled: api.Bool(true)},
	})
	tassert.CheckError(t, err)
	p, err = api.HeadBucket(baseParams, m.bck)
	tassert.CheckFatal(t, err)
	if !p.Mirror.Enabled {
		t.Error("Mirroring was not enabled")
	}
	if p.Mirror.Copies != 2 {
		t.Errorf("Number of copies is incorrect: %d (expected 2)", p.Mirror.Copies)
	}

	// Need to disable mirroring first
	err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{Enabled: api.Bool(false)},
	})
	tassert.CheckError(t, err)

	// Change a few more bucket properties
	err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{
			DataSlices:   api.Int(dataSlices),
			ParitySlices: api.Int(paritySlices),
			ObjSizeLimit: api.Int64(objLimit),
		},
	})
	tassert.CheckError(t, err)

	// Enable EC again
	err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(true)},
	})
	tassert.CheckError(t, err)
	p, err = api.HeadBucket(tutils.DefaultBaseAPIParams(t), m.bck)
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
	err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(false)},
	})
	tassert.CheckError(t, err)

	// Change mirroring threshold
	err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{UtilThresh: api.Int64(mirrorThreshold)}},
	)
	tassert.CheckError(t, err)
	p, err = api.HeadBucket(tutils.DefaultBaseAPIParams(t), m.bck)
	tassert.CheckFatal(t, err)
	if p.Mirror.UtilThresh != mirrorThreshold {
		t.Errorf("Mirror utilization threshold was not changed to %d. Current value %d", mirrorThreshold, p.Mirror.UtilThresh)
	}

	// Disable mirroring
	err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{Enabled: api.Bool(false)},
	})
	tassert.CheckError(t, err)
}

func TestSetBucketPropsOfNonexistentBucket(t *testing.T) {
	var (
		baseParams = tutils.DefaultBaseAPIParams(t)
	)
	bucket, err := tutils.GenerateNonexistentBucketName(t.Name()+"Bucket", baseParams)
	tassert.CheckFatal(t, err)

	bck := cmn.Bck{
		Name:     bucket,
		Provider: cmn.Cloud,
	}

	err = api.SetBucketProps(baseParams, bck, cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(true)},
	})
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
		baseParams  = tutils.DefaultBaseAPIParams(t)
		bucketProps = cmn.BucketPropsToUpdate{}
	)

	bucket, err := tutils.GenerateNonexistentBucketName(t.Name()+"Bucket", baseParams)
	tassert.CheckFatal(t, err)

	bck := cmn.Bck{
		Name:     bucket,
		Provider: cmn.Cloud,
	}

	err = api.SetBucketProps(baseParams, bck, bucketProps)
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

func TestBucketInvalidName(t *testing.T) {
	var (
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.DefaultBaseAPIParams(t)
	)

	invalidNames := []string{"*", ".", "", " ", "bucket and name", "bucket/name", "#name", "$name", "~name"}
	for _, name := range invalidNames {
		bck := cmn.Bck{
			Name:     name,
			Provider: cmn.ProviderAIS,
		}
		if err := api.CreateBucket(baseParams, bck); err == nil {
			tutils.DestroyBucket(t, proxyURL, bck)
			t.Errorf("created bucket with invalid name %q", name)
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
			bck: cmn.Bck{
				Name:     cmn.RandString(10),
				Provider: cmn.ProviderAIS,
			},
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

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	{
		baseParams := tutils.DefaultBaseAPIParams(t)
		err := api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
			Mirror: &cmn.MirrorConfToUpdate{
				Enabled: api.Bool(true),
			},
		})
		tassert.CheckFatal(t, err)

		p, err := api.HeadBucket(baseParams, m.bck)
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
		makeNCopies(t, baseParams, m.bck, copies)
		if copies > 1 {
			time.Sleep(10 * time.Second)
		}
	}

	m.wg.Wait()

	m.ensureNumCopies(numCopies[len(numCopies)-1])
}

func makeNCopies(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, ncopies int) {
	tutils.Logf("Set copies = %d\n", ncopies)
	if err := api.MakeNCopies(baseParams, bck, ncopies); err != nil {
		t.Fatalf("Failed to start copies=%d xaction, err: %v", ncopies, err)
	}
	xactArgs := api.XactReqArgs{Kind: cmn.ActMakeNCopies, Bck: bck, Timeout: 60 * time.Second}
	err := api.WaitForXaction(baseParams, xactArgs)
	tassert.CheckFatal(t, err)
}

func TestCloudMirror(t *testing.T) {
	var (
		m = &ioContext{
			t:   t,
			num: 64,
			bck: cmn.Bck{
				Name:     clibucket,
				Provider: cmn.Cloud,
			},
		}
		baseParams = tutils.DefaultBaseAPIParams(t)
	)

	m.saveClusterState()
	if !isCloudBucket(t, m.proxyURL, m.bck) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}

	m.cloudPuts()
	defer m.cloudDelete()

	tutils.Logln("evicting cloud bucket...")
	err := api.EvictCloudBucket(baseParams, m.bck)
	tassert.CheckFatal(t, err)

	// enable mirror
	err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{Enabled: api.Bool(true)},
	})
	tassert.CheckFatal(t, err)
	defer api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{Enabled: api.Bool(false)},
	})

	// list
	objectList, err := api.ListBucket(baseParams, m.bck, nil, 0)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(objectList.Entries) == m.num, "insufficient number of objects in the Cloud bucket %s, required %d", m.bck, m.num)

	smap := tutils.GetClusterMap(t, baseParams.URL)
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
	m.cloudPrefetch(m.num)
	m.ensureNumCopies(2)
	time.Sleep(3 * time.Second)

	// Increase number of copies
	makeNCopies(t, baseParams, m.bck, 3)
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
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)
	baseParams := tutils.DefaultBaseAPIParams(t)

	m.puts()
	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()
	m.wg.Wait()

	p, err := api.HeadBucket(baseParams, m.bck)
	tassert.CheckFatal(t, err)

	// make bucket read-only
	aattrs := cmn.MakeAccess(p.AccessAttrs, cmn.DenyAccess, cmn.AccessPUT|cmn.AccessDELETE)
	err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{AccessAttrs: api.Uint64(aattrs)})
	tassert.CheckFatal(t, err)

	m.init()
	nerr := m.puts(true /* don't fail */)
	if nerr != m.num {
		t.Fatalf("num failed PUTs %d, expecting %d", nerr, m.num)
	}

	// restore write access
	err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{AccessAttrs: api.Uint64(p.AccessAttrs)})
	tassert.CheckFatal(t, err)

	// write some more and destroy
	m.init()
	nerr = m.puts(true /* don't fail */)
	if nerr != 0 {
		t.Fatalf("num failed PUTs %d, expecting 0 (zero)", nerr)
	}
}

func TestRenameEmptyBucket(t *testing.T) {
	var (
		m = ioContext{
			t:  t,
			wg: &sync.WaitGroup{},
		}
		baseParams = tutils.DefaultBaseAPIParams(t)
		dstBck     = cmn.Bck{
			Name:     TestBucketName + "_new",
			Provider: cmn.ProviderAIS,
		}
	)

	// Initialize ioContext
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	srcBck := m.bck
	tutils.CreateFreshBucket(t, m.proxyURL, srcBck)
	defer func() {
		tutils.DestroyBucket(t, m.proxyURL, srcBck)
		tutils.DestroyBucket(t, m.proxyURL, dstBck)
	}()
	tutils.DestroyBucket(t, m.proxyURL, dstBck)

	m.setRandBucketProps()
	srcProps, err := api.HeadBucket(baseParams, srcBck)
	tassert.CheckFatal(t, err)

	// Rename it
	tutils.Logf("rename %s => %s\n", srcBck, dstBck)
	err = api.RenameBucket(baseParams, srcBck, dstBck)
	tassert.CheckFatal(t, err)

	// Check if the new bucket appears in the list
	names, err := api.GetBucketNames(baseParams, srcBck)
	tassert.CheckFatal(t, err)

	exists := cmn.StringInSlice(dstBck.Name, names.AIS)
	if !exists {
		t.Error("new bucket not found in buckets list")
	}

	tutils.Logln("checking bucket props...")
	dstProps, err := api.HeadBucket(baseParams, dstBck)
	tassert.CheckFatal(t, err)
	if !srcProps.Equal(&dstProps) {
		t.Fatalf("source and destination bucket props do not match: %v - %v", srcProps, dstProps)
	}
}

func TestRenameNonEmptyBucket(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}
	var (
		m = ioContext{
			t:               t,
			num:             1000,
			numGetsEachFile: 2,
		}
		baseParams = tutils.DefaultBaseAPIParams(t)
		dstBck     = cmn.Bck{
			Name:     TestBucketName + "_new",
			Provider: cmn.ProviderAIS,
		}
	)

	// Initialize ioContext
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	srcBck := m.bck
	tutils.CreateFreshBucket(t, m.proxyURL, srcBck)
	defer func() {
		tutils.DestroyBucket(t, m.proxyURL, srcBck)
		tutils.DestroyBucket(t, m.proxyURL, dstBck)
	}()
	tutils.DestroyBucket(t, m.proxyURL, dstBck)

	m.setRandBucketProps()
	srcProps, err := api.HeadBucket(baseParams, srcBck)
	tassert.CheckFatal(t, err)

	// Put some files
	m.puts()

	// Rename it
	tutils.Logf("rename %s => %s\n", srcBck, dstBck)
	m.bck = dstBck
	err = api.RenameBucket(baseParams, srcBck, dstBck)
	tassert.CheckFatal(t, err)

	xactArgs := api.XactReqArgs{Kind: cmn.ActRenameLB, Bck: dstBck, Timeout: rebalanceTimeout}
	err = api.WaitForXaction(baseParams, xactArgs)
	tassert.CheckFatal(t, err)

	// Gets on renamed ais bucket
	m.wg.Add(m.num * m.numGetsEachFile)
	m.gets()
	m.wg.Wait()
	m.ensureNoErrors()

	tutils.Logln("checking bucket props...")
	dstProps, err := api.HeadBucket(baseParams, dstBck)
	tassert.CheckFatal(t, err)
	if !srcProps.Equal(&dstProps) {
		t.Fatalf("source and destination bucket props do not match: %v - %v", srcProps, dstProps)
	}
}

func TestRenameAlreadyExistingBucket(t *testing.T) {
	var (
		m = ioContext{
			t:  t,
			wg: &sync.WaitGroup{},
		}
		baseParams = tutils.DefaultBaseAPIParams(t)
		tmpBck     = cmn.Bck{
			Name:     "tmp_bck_name",
			Provider: cmn.ProviderAIS,
		}
	)

	// Initialize ioContext
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	m.setRandBucketProps()

	tutils.CreateFreshBucket(t, m.proxyURL, tmpBck)
	defer tutils.DestroyBucket(t, m.proxyURL, tmpBck)

	// Rename it
	tutils.Logf("try rename %s => %s\n", m.bck, tmpBck)
	err := api.RenameBucket(baseParams, m.bck, tmpBck)
	if err == nil {
		t.Fatal("expected error on renaming already existing bucket")
	}

	// Check if the old bucket still appears in the list
	names, err := api.GetBucketNames(baseParams, m.bck)
	tassert.CheckFatal(t, err)

	if !cmn.StringInSlice(m.bck.Name, names.AIS) || !cmn.StringInSlice(tmpBck.Name, names.AIS) {
		t.Error("one of the buckets was not found in buckets list")
	}

	srcProps, err := api.HeadBucket(baseParams, m.bck)
	tassert.CheckFatal(t, err)

	dstProps, err := api.HeadBucket(baseParams, tmpBck)
	tassert.CheckFatal(t, err)

	if srcProps.Equal(&dstProps) {
		t.Fatalf("source and destination bucket props match, even though they should not: %v - %v", srcProps, dstProps)
	}
}

// Tries to rename same source bucket to two destination buckets - the second should fail.
func TestRenameBucketTwice(t *testing.T) {
	var (
		m = ioContext{
			t:   t,
			wg:  &sync.WaitGroup{},
			num: 500,
		}
		baseParams = tutils.DefaultBaseAPIParams(t)
		dstBck1    = cmn.Bck{
			Name:     TestBucketName + "_new1",
			Provider: cmn.ProviderAIS,
		}
		dstBck2 = cmn.Bck{
			Name:     TestBucketName + "_new2",
			Provider: cmn.ProviderAIS,
		}
	)

	// Initialize ioContext
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	srcBck := m.bck
	tutils.CreateFreshBucket(t, m.proxyURL, srcBck)
	defer func() {
		tutils.DestroyBucket(t, m.proxyURL, srcBck)
		tutils.DestroyBucket(t, m.proxyURL, dstBck1)
		tutils.DestroyBucket(t, m.proxyURL, dstBck2)
	}()

	m.puts()

	// Rename to first destination
	tutils.Logf("rename %s => %s\n", srcBck, dstBck1)
	err := api.RenameBucket(baseParams, srcBck, dstBck1)
	tassert.CheckFatal(t, err)

	// Try to rename to first destination again - already in progress
	tutils.Logf("try rename %s => %s\n", srcBck, dstBck1)
	err = api.RenameBucket(baseParams, srcBck, dstBck1)
	if err == nil {
		t.Error("renaming bucket that is under renaming did not fail")
	}

	// Try to rename to second destination - this should fail
	tutils.Logf("try rename %s => %s\n", srcBck, dstBck2)
	err = api.RenameBucket(baseParams, srcBck, dstBck2)
	if err == nil {
		t.Error("renaming bucket that is under renaming did not fail")
	}

	// Wait for rename to complete
	xactArgs := api.XactReqArgs{Kind: cmn.ActRenameLB, Bck: dstBck1, Timeout: rebalanceTimeout}
	err = api.WaitForXaction(baseParams, xactArgs)
	tassert.CheckFatal(t, err)

	// Check if the new bucket appears in the list
	names, err := api.GetBucketNames(baseParams, srcBck)
	tassert.CheckFatal(t, err)

	if cmn.StringInSlice(srcBck.Name, names.AIS) {
		t.Error("source bucket found in buckets list")
	}
	if !cmn.StringInSlice(dstBck1.Name, names.AIS) {
		t.Error("destination bucket not found in buckets list")
	}
	if cmn.StringInSlice(dstBck2.Name, names.AIS) {
		t.Error("second (failed) destination bucket not found in buckets list")
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
					t:   t,
					num: numput,
					bck: cmn.Bck{
						Name:     "src_copy_bck",
						Provider: cmn.ProviderAIS,
					},
					numGetsEachFile: 1,
				}
				dstms = []*ioContext{
					{
						t:   t,
						num: numput,
						bck: cmn.Bck{
							Name:     "dst_copy_bck_1",
							Provider: cmn.ProviderAIS,
						},
						numGetsEachFile: 1,
					},
				}
				baseParams = tutils.DefaultBaseAPIParams(t)
				proxyURL   = tutils.GetPrimaryURL()
			)

			if test.multipleDests {
				dstms = append(dstms, &ioContext{
					t:   t,
					num: numput,
					bck: cmn.Bck{
						Name:     "dst_copy_bck_2",
						Provider: cmn.ProviderAIS,
					},
					numGetsEachFile: 1,
				})
			}

			if cmn.IsProviderCloud(cmn.Bck{Provider: test.provider, Ns: cmn.NsGlobal}, true /*acceptAnon*/) {
				srcm.bck = cmn.Bck{
					Name:     clibucket,
					Provider: cmn.Cloud,
				}

				if !isCloudBucket(t, proxyURL, srcm.bck) {
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

			if cmn.IsProviderAIS(cmn.Bck{Provider: test.provider, Ns: cmn.NsGlobal}) {
				tutils.CreateFreshBucket(t, srcm.proxyURL, srcm.bck)
				defer tutils.DestroyBucket(t, srcm.proxyURL, srcm.bck)
				srcm.setRandBucketProps()
			}

			if test.dstBckExist {
				for _, dstm := range dstms {
					tutils.CreateFreshBucket(t, dstm.proxyURL, dstm.bck)
					dstm.setRandBucketProps()
				}
			} else { // cleanup
				for _, dstm := range dstms {
					tutils.DestroyBucket(t, dstm.proxyURL, dstm.bck)
				}
			}
			for _, dstm := range dstms {
				defer tutils.DestroyBucket(t, dstm.proxyURL, dstm.bck)
			}

			srcProps, err := api.HeadBucket(baseParams, srcm.bck)
			tassert.CheckFatal(t, err)

			if test.dstBckHasObjects {
				for _, dstm := range dstms {
					dstm.puts()
				}
			}

			if cmn.IsProviderAIS(cmn.Bck{Provider: test.provider, Ns: cmn.NsGlobal}) {
				srcm.puts()

				srcBckList, err = api.ListBucket(baseParams, srcm.bck, nil, 0)
				tassert.CheckFatal(t, err)
			} else if cmn.IsProviderCloud(cmn.Bck{Provider: test.provider, Ns: cmn.NsGlobal}, true /*acceptAnon*/) {
				srcm.cloudPuts()
				defer srcm.cloudDelete()

				srcBckList, err = api.ListBucket(baseParams, srcm.bck, nil, 0)
				tassert.CheckFatal(t, err)
			} else {
				panic(test.provider)
			}

			for _, dstm := range dstms {
				tutils.Logf("copying %s => %s\n", srcm.bck, dstm.bck)
				err = api.CopyBucket(baseParams, srcm.bck, dstm.bck)
				tassert.CheckFatal(t, err)
			}

			for _, dstm := range dstms {
				xactArgs := api.XactReqArgs{
					Kind:    cmn.ActCopyBucket,
					Bck:     dstm.bck,
					Timeout: rebalanceTimeout,
				}
				err = api.WaitForXaction(baseParams, xactArgs)
				tassert.CheckFatal(t, err)
			}

			tutils.Logln("checking and comparing bucket props")
			for _, dstm := range dstms {
				dstProps, err := api.HeadBucket(baseParams, dstm.bck)
				tassert.CheckFatal(t, err)

				if dstProps.CloudProvider != cmn.ProviderAIS {
					t.Fatalf("destination bucket does not seem to be 'ais': %s", dstProps.CloudProvider)
				}
				// Clear providers to make sure that they will fail on different providers.
				srcProps.CloudProvider = ""
				dstProps.CloudProvider = ""

				// If bucket existed before, we need to ensure that the bucket
				// props were **not** copied over.
				if test.dstBckExist && srcProps.Equal(&dstProps) {
					t.Fatalf("source and destination bucket props match, even though they should not:\n%#v\n%#v",
						srcProps, dstProps)
				}

				// If bucket did not exist before, we need to ensure that
				// the bucket props match the source bucket props (except provider).
				if !test.dstBckExist && !srcProps.Equal(&dstProps) {
					t.Fatalf("source and destination bucket props do not match:\n%#v\n%#v", srcProps, dstProps)
				}
			}

			tutils.Logln("checking and comparing objects")

			for _, dstm := range dstms {
				expectedObjCount := srcm.num
				if test.dstBckHasObjects {
					expectedObjCount += dstm.num
				}

				dstBckList, err := api.ListBucketFast(baseParams, dstm.bck, nil)
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
						t.Fatalf("%s/%s is missing in the copied objects", srcm.bck, a.Name)
					}
				}
			}
		})
	}
}

// Tries to rename and then copy bucket at the same time.
// TODO: This test should be enabled (not skipped)
func TestRenameAndCopyBucket(t *testing.T) {
	t.Skip("fails - necessary checks are not yet implemented")

	var (
		m = ioContext{
			t:   t,
			wg:  &sync.WaitGroup{},
			num: 500,
		}
		baseParams = tutils.DefaultBaseAPIParams(t)
		dstBck1    = cmn.Bck{
			Name:     TestBucketName + "_new1",
			Provider: cmn.ProviderAIS,
		}
		dstBck2 = cmn.Bck{
			Name:     TestBucketName + "_new2",
			Provider: cmn.ProviderAIS,
		}
	)

	// Initialize ioContext
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	srcBck := m.bck
	tutils.CreateFreshBucket(t, m.proxyURL, srcBck)
	defer func() {
		tutils.DestroyBucket(t, m.proxyURL, srcBck)
		tutils.DestroyBucket(t, m.proxyURL, dstBck1)
		tutils.DestroyBucket(t, m.proxyURL, dstBck2)
	}()

	m.puts()

	// Rename to first destination
	tutils.Logf("rename %s => %s\n", srcBck, dstBck1)
	err := api.RenameBucket(baseParams, srcBck, dstBck1)
	tassert.CheckFatal(t, err)

	// Try to copy to first destination - rename in progress, both for srcBck and dstBck1
	tutils.Logf("try copy %s => %s\n", srcBck, dstBck1)
	err = api.CopyBucket(baseParams, srcBck, dstBck1)
	if err == nil {
		t.Error("coping bucket that is under renaming did not fail")
	}

	// Try to copy to second destination - rename in progress for srcBck
	tutils.Logf("try copy %s => %s\n", srcBck, dstBck2)
	err = api.CopyBucket(baseParams, srcBck, dstBck2)
	if err == nil {
		t.Error("coping bucket that is under renaming did not fail")
	}

	// Try to copy from dstBck1 to dstBck1 - rename in progress for dstBck1
	tutils.Logf("try copy %s => %s\n", dstBck1, dstBck2)
	err = api.CopyBucket(baseParams, srcBck, dstBck1)
	if err == nil {
		t.Error("coping bucket that is under renaming did not fail")
	}

	// Wait for rename to complete
	xactArgs := api.XactReqArgs{Kind: cmn.ActRenameLB, Bck: dstBck1, Timeout: rebalanceTimeout}
	err = api.WaitForXaction(baseParams, xactArgs)
	tassert.CheckFatal(t, err)

	// Check if the new bucket appears in the list
	names, err := api.GetBucketNames(baseParams, srcBck)
	tassert.CheckFatal(t, err)

	if cmn.StringInSlice(srcBck.Name, names.AIS) {
		t.Error("source bucket found in buckets list")
	}
	if !cmn.StringInSlice(dstBck1.Name, names.AIS) {
		t.Error("destination bucket not found in buckets list")
	}
	if cmn.StringInSlice(dstBck2.Name, names.AIS) {
		t.Error("second (failed) destination bucket found in buckets list")
	}
}

// Tries to copy and then rename bucket at the same time - similar to
// `TestRenameAndCopyBucket` but in different order of operations.
// TODO: This test should be enabled (not skipped)
func TestCopyAndRenameBucket(t *testing.T) {
	t.Skip("fails - necessary checks are not yet implemented")

	var (
		m = ioContext{
			t:   t,
			wg:  &sync.WaitGroup{},
			num: 500,
		}
		baseParams = tutils.DefaultBaseAPIParams(t)
		dstBck1    = cmn.Bck{
			Name:     TestBucketName + "_new1",
			Provider: cmn.ProviderAIS,
		}
		dstBck2 = cmn.Bck{
			Name:     TestBucketName + "_new2",
			Provider: cmn.ProviderAIS,
		}
	)

	// Initialize ioContext
	m.saveClusterState()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	srcBck := m.bck
	tutils.CreateFreshBucket(t, m.proxyURL, srcBck)
	defer func() {
		tutils.DestroyBucket(t, m.proxyURL, srcBck)
		tutils.DestroyBucket(t, m.proxyURL, dstBck1)
		tutils.DestroyBucket(t, m.proxyURL, dstBck2)
	}()

	m.puts()

	// Rename to first destination
	tutils.Logf("copy %s => %s\n", srcBck, dstBck1)
	err := api.CopyBucket(baseParams, srcBck, dstBck1)
	tassert.CheckFatal(t, err)

	// Try to rename to first destination - copy in progress, both for srcBck and dstBck1
	tutils.Logf("try rename %s => %s\n", srcBck, dstBck1)
	err = api.RenameBucket(baseParams, srcBck, dstBck1)
	if err == nil {
		t.Error("renaming bucket that is under coping did not fail")
	}

	// Try to rename to second destination - copy in progress for srcBck
	tutils.Logf("try rename %s => %s\n", srcBck, dstBck2)
	err = api.RenameBucket(baseParams, srcBck, dstBck2)
	if err == nil {
		t.Error("renaming bucket that is under coping did not fail")
	}

	// Try to rename from dstBck1 to dstBck1 - rename in progress for dstBck1
	tutils.Logf("try rename %s => %s\n", dstBck1, dstBck2)
	err = api.RenameBucket(baseParams, srcBck, dstBck1)
	if err == nil {
		t.Error("renaming bucket that is under coping did not fail")
	}

	// Wait for copy to complete
	xactArgs := api.XactReqArgs{Kind: cmn.ActCopyBucket, Bck: dstBck1, Timeout: rebalanceTimeout}
	err = api.WaitForXaction(baseParams, xactArgs)
	tassert.CheckFatal(t, err)

	// Check if the new bucket appears in the list
	names, err := api.GetBucketNames(baseParams, srcBck)
	tassert.CheckFatal(t, err)

	if !cmn.StringInSlice(srcBck.Name, names.AIS) {
		t.Error("source bucket not found in buckets list")
	}
	if !cmn.StringInSlice(dstBck1.Name, names.AIS) {
		t.Error("destination bucket not found in buckets list")
	}
	if cmn.StringInSlice(dstBck2.Name, names.AIS) {
		t.Error("second (failed) destination bucket found in buckets list")
	}
}
