// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
	"golang.org/x/sync/errgroup"
)

func Test_BucketNames(t *testing.T) {
	var (
		bck = cmn.Bck{
			Name:     t.Name() + "Bucket",
			Provider: cmn.ProviderAIS,
		}
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)
	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	buckets, err := api.ListBuckets(baseParams, cmn.QueryBcks{})
	tassert.CheckFatal(t, err)

	printBucketNames(buckets)

	for _, provider := range []string{cmn.ProviderAmazon, cmn.ProviderGoogle, cmn.ProviderAzure} {
		query := cmn.QueryBcks{Provider: provider}
		cloudBuckets, err := api.ListBuckets(baseParams, query)
		tassert.CheckError(t, err)
		if len(cloudBuckets) != len(buckets.Select(query)) {
			t.Fatalf("%s: cloud buckets: %d != %d\n", provider, len(cloudBuckets), len(buckets.Select(query)))
		}
	}

	// NsGlobal
	query := cmn.QueryBcks{Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
	aisBuckets, err := api.ListBuckets(baseParams, query)
	tassert.CheckError(t, err)
	if len(aisBuckets) != len(buckets.Select(query)) {
		t.Fatalf("ais buckets: %d != %d\n", len(aisBuckets), len(buckets.Select(query)))
	}

	// NsAnyRemote
	query = cmn.QueryBcks{Ns: cmn.NsAnyRemote}
	buckets, err = api.ListBuckets(baseParams, query)
	tassert.CheckError(t, err)
	query = cmn.QueryBcks{Provider: cmn.ProviderAIS, Ns: cmn.NsAnyRemote}
	aisBuckets, err = api.ListBuckets(baseParams, query)
	tassert.CheckError(t, err)
	if len(aisBuckets) != len(buckets.Select(query)) {
		t.Fatalf("ais buckets: %d != %d\n", len(aisBuckets), len(buckets.Select(query)))
	}
}

func printBucketNames(bcks cmn.BucketNames) {
	for _, bck := range bcks {
		fmt.Fprintf(os.Stdout, "  provider: %s, name: %s\n", bck.Provider, bck.Name)
	}
}

func TestDefaultBucketProps(t *testing.T) {
	const dataSlices = 7
	var (
		proxyURL     = tutils.RandomProxyURL()
		baseParams   = tutils.BaseAPIParams(proxyURL)
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
	p, err := api.HeadBucket(baseParams, bck)
	tassert.CheckFatal(t, err)
	if !p.EC.Enabled {
		t.Error("EC should be enabled for ais buckets")
	}
	if p.EC.DataSlices != dataSlices {
		t.Errorf("Invalid number of EC data slices: expected %d, got %d", dataSlices, p.EC.DataSlices)
	}
}

func TestCreateWithBucketProps(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}
	)
	propsToSet := cmn.BucketPropsToUpdate{
		Cksum: &cmn.CksumConfToUpdate{
			Type:            api.String(cmn.ChecksumMD5),
			ValidateWarmGet: api.Bool(true),
			EnableReadRange: api.Bool(true),
			ValidateColdGet: api.Bool(false),
			ValidateObjMove: api.Bool(true),
		},
	}
	tutils.CreateFreshBucket(t, proxyURL, bck, propsToSet)
	defer tutils.DestroyBucket(t, proxyURL, bck)
	p, err := api.HeadBucket(baseParams, bck)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, propsToSet, p)
}

func TestStressCreateDestroyBucket(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	const (
		bckCount  = 10
		iterCount = 20
	)

	var (
		baseParams = tutils.BaseAPIParams()
		group, _   = errgroup.WithContext(context.Background())
	)

	for i := 0; i < bckCount; i++ {
		group.Go(func() error {
			var (
				m = &ioContext{
					t:      t,
					num:    100,
					silent: true,
				}
			)

			m.init()

			for i := 0; i < iterCount; i++ {
				if err := api.CreateBucket(baseParams, m.bck); err != nil {
					return err
				}
				if rand.Intn(iterCount) == 0 { // just test couple times, no need to flood
					if err := api.CreateBucket(baseParams, m.bck); err == nil {
						return fmt.Errorf("expected error to occur on bucket %q - create second time", m.bck)
					}
				}
				m.puts()
				if _, err := api.ListObjects(baseParams, m.bck, nil, 0); err != nil {
					return err
				}
				m.gets()
				if err := api.DestroyBucket(baseParams, m.bck); err != nil {
					return err
				}
				if rand.Intn(iterCount) == 0 { // just test couple times, no need to flood
					if err := api.DestroyBucket(baseParams, m.bck); err == nil {
						return fmt.Errorf("expected error to occur on bucket %q - destroy second time", m.bck)
					}
				}
			}
			return nil
		})
	}
	err := group.Wait()
	tassert.CheckFatal(t, err)
}

func TestResetBucketProps(t *testing.T) {
	var (
		proxyURL     = tutils.RandomProxyURL()
		globalConfig = tutils.GetClusterConfig(t)
		baseParams   = tutils.BaseAPIParams(proxyURL)
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

	_, err = api.SetBucketProps(baseParams, bck, propsToUpdate)
	tassert.CheckFatal(t, err)

	p, err := api.HeadBucket(baseParams, bck)
	tassert.CheckFatal(t, err)

	// check that bucket props do get set
	validateBucketProps(t, propsToUpdate, p)
	_, err = api.ResetBucketProps(baseParams, bck)
	tassert.CheckFatal(t, err)

	p, err = api.HeadBucket(baseParams, bck)
	tassert.CheckFatal(t, err)

	if !p.Equal(defaultProps) {
		t.Errorf("props have not been reset properly: expected: %+v, got: %+v", defaultProps, p)
	}
}

func TestSetInvalidBucketProps(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL()
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
			_, err := api.SetBucketProps(baseParams, bck, test.props)
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
			Provider: cmn.AnyCloud,
		}
		proxyURL = tutils.RandomProxyURL()
		wg       = cmn.NewLimitedWaitGroup(40)
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true, Cloud: true, Bck: bck})

	baseParams := tutils.BaseAPIParams(proxyURL)
	p, err := api.HeadBucket(baseParams, bck)
	tassert.CheckFatal(t, err)

	// Enable local versioning management
	_, err = api.SetBucketProps(baseParams, bck, cmn.BucketPropsToUpdate{
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

	tutils.Logf("Filling bucket %s\n", bck)
	for wid := 0; wid < workerCount; wid++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			objectsToPut := objectCount / workerCount
			if wid == workerCount-1 { // last worker puts leftovers
				objectsToPut += objectCount % workerCount
			}
			tutils.PutRR(t, baseParams, int64(objectSize), p.Cksum.Type, bck, objectDir, objectsToPut, fnlen)
		}(wid)

		wg.Wait()

		tutils.Logf("Reading %q objects\n", bck)
		msg := &cmn.SelectMsg{Prefix: objectDir, Props: cmn.GetPropsVersion}
		bckObjs, err := api.ListObjects(baseParams, bck, msg, 0)
		tassert.CheckFatal(t, err)

		tutils.Logf("Checking %q object versions[total: %d]\n", bck, len(bckObjs.Entries))
		for _, entry := range bckObjs.Entries {
			if entry.Version == "" {
				t.Errorf("Object %s does not have version", entry.Name)
			}
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				err := api.DeleteObject(baseParams, bck, name)
				tassert.CheckError(t, err)
			}(entry.Name)
		}
		wg.Wait()
	}
}

// Minimalistic list objects test to check that everything works correctly.
func TestListObjectsSmoke(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		var (
			baseParams = tutils.BaseAPIParams()
			m          = ioContext{
				t:        t,
				num:      100,
				bck:      bck.Bck,
				fileSize: 5 * cmn.KiB,
			}

			iters = 5
			msg   = &cmn.SelectMsg{PageSize: 10}
		)

		m.init()

		m.puts()
		defer m.del()

		// Run couple iterations to see that we get deterministic results.
		tutils.Logf("run %d list objects iterations\n", iters)
		for iter := 0; iter < iters; iter++ {
			objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
			tassert.CheckFatal(t, err)
			tassert.Errorf(
				t, len(objList.Entries) == m.num,
				"unexpected number of entries (got: %d, expected: %d) on iter: %d",
				len(objList.Entries), m.num, iter,
			)
		}
	})
}

func TestListObjectsProps(t *testing.T) {
	var (
		baseParams = tutils.BaseAPIParams()
		m          = ioContext{
			t:        t,
			num:      rand.Intn(5000) + 1000,
			fileSize: 5 * cmn.KiB,
		}
	)

	m.init()
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	m.puts()

	tutils.Logln("asking for default props...")

	msg := &cmn.SelectMsg{
		PageSize: 100,
		Props:    strings.Join(cmn.GetPropsDefault, ","),
	}
	objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Errorf(
		t, len(objList.Entries) == m.num,
		"unexpected number of entries (got: %d, expected: %d)", len(objList.Entries), m.num,
	)
	for _, entry := range objList.Entries {
		tassert.Errorf(t, entry.Name != "", "name is not set")
		tassert.Errorf(t, entry.Size != 0, "size is not set")
		tassert.Errorf(t, entry.Checksum != "", "checksum is not set")
		tassert.Errorf(t, entry.Atime != "", "atime is not set")
		tassert.Errorf(t, entry.Version != "", "version is not set")

		tassert.Errorf(t, entry.TargetURL == "", "targetURL is set")
		tassert.Errorf(t, entry.Copies == 0, "copies is set")
	}

	tutils.Logln("trying again with different subset of props...")

	msg = &cmn.SelectMsg{
		PageSize: 100,
		Props:    strings.Join([]string{cmn.GetPropsChecksum, cmn.GetPropsVersion, cmn.GetPropsCopies}, ","),
	}
	objList, err = api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Errorf(
		t, len(objList.Entries) == m.num,
		"unexpected number of entries (got: %d, expected: %d)", len(objList.Entries), m.num,
	)
	for _, entry := range objList.Entries {
		tassert.Errorf(t, entry.Name != "", "name is not set")
		tassert.Errorf(t, entry.Checksum != "", "checksum is not set")
		tassert.Errorf(t, entry.Version != "", "version is not set")
		tassert.Errorf(t, entry.Copies == 1, "copies is not set")

		tassert.Errorf(t, entry.Size == 0, "size is set")
		tassert.Errorf(t, entry.Atime == "", "atime is set")
		tassert.Errorf(t, entry.TargetURL == "", "targetURL is set")
	}
}

// Runs cloud list objects with `cached == true`.
func TestListObjectsCloudCached(t *testing.T) {
	var (
		baseParams = tutils.BaseAPIParams()
		m          = ioContext{
			t:        t,
			bck:      cmn.Bck{Name: clibucket, Provider: cmn.AnyCloud},
			num:      rand.Intn(100) + 10,
			fileSize: 5 * cmn.KiB,
		}
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: m.bck})
	t.Skip("not working....")

	m.init()

	m.cloudPuts(false /*evict*/)
	defer m.del()

	msg := &cmn.SelectMsg{
		PageSize: 10,
		Cached:   true,
	}
	objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Errorf(
		t, len(objList.Entries) == m.num,
		"unexpected number of entries (got: %d, expected: %d)", len(objList.Entries), m.num,
	)
	for _, entry := range objList.Entries {
		tassert.Errorf(t, entry.Name != "", "name is not set")
		tassert.Errorf(t, entry.Size != 0, "size is not set")
		tassert.Errorf(t, entry.Checksum != "", "checksum is not set")
		tassert.Errorf(t, entry.Atime != "", "atime is not set")
		tassert.Errorf(t, entry.Version != "", "version is not set")

		tassert.Errorf(t, entry.TargetURL == "", "targetURL is set")
		tassert.Errorf(t, entry.Copies == 0, "copies is set")
	}
}

// Runs cloud list objects with subset of all props.
func TestListObjectsCloudProps(t *testing.T) {
	var (
		baseParams = tutils.BaseAPIParams()
		m          = ioContext{
			t:        t,
			bck:      cmn.Bck{Name: clibucket, Provider: cmn.AnyCloud},
			num:      rand.Intn(100) + 10,
			fileSize: 5 * cmn.KiB,
		}
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: m.bck})
	t.Skip("not working....")

	m.init()

	m.cloudPuts(false /*evict*/)
	defer m.del()

	msg := &cmn.SelectMsg{
		PageSize: 10,
		Props:    strings.Join([]string{cmn.GetPropsChecksum, cmn.GetPropsVersion, cmn.GetPropsCopies}, ","),
	}
	objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Errorf(
		t, len(objList.Entries) == m.num,
		"unexpected number of entries (got: %d, expected: %d)", len(objList.Entries), m.num,
	)
	for _, entry := range objList.Entries {
		tassert.Errorf(t, entry.Name != "", "name is not set")
		tassert.Errorf(t, entry.Checksum != "", "checksum is not set")
		tassert.Errorf(t, entry.Version != "", "version is not set")
		tassert.Errorf(t, entry.Copies == 1, "copies is not set")

		tassert.Errorf(t, entry.Size == 0, "size is set")
		tassert.Errorf(t, entry.Atime == "", "atime is set")
		tassert.Errorf(t, entry.TargetURL == "", "targetURL is set")
	}
}

// Runs standard list objects but selects new random proxy every page.
func TestListObjectsRandProxy(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		var (
			m = ioContext{
				t:        t,
				bck:      bck.Bck,
				num:      rand.Intn(5000) + 1000,
				fileSize: 5 * cmn.KiB,
			}

			totalCnt = 0
			msg      = &cmn.SelectMsg{PageSize: 100}
		)

		if !bck.IsAIS() {
			t.Skip("not working...")
			m.num = rand.Intn(300) + 100
		}

		m.init()

		m.puts()
		defer m.del()

		for {
			baseParams := tutils.BaseAPIParams()
			objList, err := api.ListObjectsPage(baseParams, m.bck, msg)
			tassert.CheckFatal(t, err)
			totalCnt += len(objList.Entries)
			if objList.PageMarker == "" {
				break
			}
		}
		tassert.Fatalf(
			t, totalCnt == m.num,
			"unexpected total number of objects (got: %d, expected: %d)", totalCnt, m.num,
		)
	})
}

// Runs standard list objects but changes the page size every request.
func TestListObjectsRandPageSize(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		var (
			baseParams = tutils.BaseAPIParams()
			m          = ioContext{
				t:        t,
				bck:      bck.Bck,
				num:      rand.Intn(5000) + 1000,
				fileSize: 5 * cmn.KiB,
			}

			totalCnt = 0
			msg      = &cmn.SelectMsg{}
		)

		if !bck.IsAIS() {
			t.Skip("not working...")
			m.num = rand.Intn(300) + 100
		}

		m.init()

		m.puts()
		defer m.del()

		for {
			msg.PageSize = uint(rand.Intn(100) + 50)

			objList, err := api.ListObjectsPage(baseParams, m.bck, msg)
			tassert.CheckFatal(t, err)
			totalCnt += len(objList.Entries)
			if objList.PageMarker == "" {
				break
			}
			tassert.Errorf(
				t, uint(len(objList.Entries)) == msg.PageSize,
				"unexpected size of the page returned (got: %d, expected: %d)",
				len(objList.Entries), msg.PageSize,
			)
		}
		tassert.Fatalf(
			t, totalCnt == m.num,
			"unexpected total number of objects (got: %d, expected: %d)", totalCnt, m.num,
		)
	})
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

		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	if testing.Short() {
		iterations = 3
	}

	tests := []struct {
		pageSize uint
	}{
		{pageSize: 0},
		{pageSize: 2000},
	}

	for _, test := range tests {
		var name string
		if test.pageSize == 0 {
			name = "pagesize:default"
		} else {
			name += "pagesize:" + strconv.FormatUint(uint64(test.pageSize), 10)
		}
		t.Run(name, func(t *testing.T) {
			var (
				objs     sync.Map
				prefixes sync.Map
			)

			tutils.CreateFreshBucket(t, proxyURL, bck)
			defer tutils.DestroyBucket(t, proxyURL, bck)

			p := cmn.DefaultBucketProps()

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
						objDir := tutils.RandomObjDir(dirLen, 5)
						objectsToPut := objectCount / workerCount
						if wid == workerCount-1 { // last worker puts leftovers
							objectsToPut += objectCount % workerCount
						}
						objNames := tutils.PutRR(t, baseParams, objectSize, p.Cksum.Type, bck, objDir, objectsToPut, fnlen)
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
				msg := &cmn.SelectMsg{PageSize: test.pageSize}
				msg.AddProps(cmn.GetPropsChecksum, cmn.GetPropsAtime, cmn.GetPropsVersion, cmn.GetPropsCopies, cmn.GetPropsSize)
				tassert.CheckError(t, api.ListObjectsInvalidateCache(baseParams, bck, msg))
				bckList, err := api.ListObjects(baseParams, bck, msg, 0)
				tassert.CheckFatal(t, err)

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
					if obj.size != entry.Size {
						t.Errorf(
							"sizes do not match for object %s, expected: %d, got: %d",
							obj.name, obj.size, entry.Size,
						)
					}

					if entry.Checksum == empty.Checksum ||
						entry.Atime == empty.Atime ||
						entry.Version == empty.Version ||
						entry.Flags == empty.Flags ||
						entry.Copies == empty.Copies {
						t.Errorf("some fields of object %q, have default values: %#v", entry.Name, entry)
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

				if len(bckList.Entries) != totalObjects {
					t.Fatalf("actual objects %d, expected: %d", len(bckList.Entries), totalObjects)
				}

				// Check listing bucket with predefined prefix.
				prefixes.Range(func(key, value interface{}) bool {
					prefix := key.(string)
					expectedObjCount := value.(int)

					msg := &cmn.SelectMsg{
						Prefix: prefix,
					}
					bckList, err = api.ListObjects(baseParams, bck, msg, 0)
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

			prefixes.Range(func(key, value interface{}) bool {
				msg := &cmn.SelectMsg{Prefix: key.(string)}
				tassert.CheckError(t, api.ListObjectsInvalidateCache(baseParams, bck, msg))
				return true
			})
			tassert.CheckError(t, api.ListObjectsInvalidateCache(baseParams, bck, &cmn.SelectMsg{}))
		})
	}
}

func TestListObjectsPrefix(t *testing.T) {
	const (
		fileSize = 1024
		numFiles = 30
		prefix   = "some_prefix"
	)

	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	for _, provider := range []string{cmn.ProviderAIS, cmn.AnyCloud} {
		t.Run(provider, func(t *testing.T) {
			var (
				bck        cmn.Bck
				errCh      = make(chan error, numFiles*5)
				filesPutCh = make(chan string, numfiles)
				cksumType  string
				customPage = true
			)
			bckTest := cmn.Bck{Provider: provider, Ns: cmn.NsGlobal}
			if bckTest.IsCloud(cmn.AnyCloud) {
				bck = cmn.Bck{
					Name:     clibucket,
					Provider: provider,
				}

				tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: bck})

				bckProp, err := api.HeadBucket(baseParams, bck)
				tassert.CheckFatal(t, err)
				cksumType = bckProp.Cksum.Type
				customPage = bckProp.Provider != cmn.ProviderAzure

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

				p := cmn.DefaultBucketProps()
				cksumType = p.Cksum.Type
			}

			tutils.Logf("Create a list of %d objects\n", numFiles)

			fileList := make([]string, 0, numFiles)
			for i := 0; i < numFiles; i++ {
				fname := fmt.Sprintf("obj%d", i+1)
				fileList = append(fileList, fname)
			}

			tutils.PutObjsFromList(proxyURL, bck, prefix, fileSize, fileList, errCh, filesPutCh, cksumType)
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
				pageSize uint
				limit    uint
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
					"full_list_overlimited_prefixed",
					prefix + "/obj1", 0, 20,
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
				if test.pageSize != 0 && !customPage {
					tutils.Logf("Bucket %s does not support custom paging. Skipping\n", bck.Name)
					continue
				}
				t.Run(test.name, func(t *testing.T) {
					tutils.Logf("Prefix: %q, Expected objects: %d\n", test.prefix, test.expected)
					msg := &cmn.SelectMsg{PageSize: test.pageSize, Prefix: test.prefix}
					tutils.Logf(
						"list_objects %s [prefix: %q, page_size: %d]\n",
						bck, msg.Prefix, msg.PageSize,
					)

					bckList, err := api.ListObjects(baseParams, bck, msg, test.limit)
					tassert.CheckFatal(t, err)

					tutils.Logf("list_objects output: %d objects\n", len(bckList.Entries))

					if len(bckList.Entries) != test.expected {
						t.Errorf("returned %d objects instead of %d", len(bckList.Entries), test.expected)
					}
				})
			}
		})
	}
}

func TestListObjectsPassthrough(t *testing.T) {
	var (
		baseParams = tutils.BaseAPIParams()
		m          = ioContext{
			t:        t,
			num:      1001,
			fileSize: cmn.KiB,
		}
	)

	m.init()

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	m.puts()

	for _, passthrough := range []bool{true, false} {
		t.Run(fmt.Sprintf("passthrough=%t", passthrough), func(t *testing.T) {
			if !passthrough {
				tutils.SetClusterConfig(t, cmn.SimpleKVs{"client.passthrough": "false"})
				defer tutils.SetClusterConfig(t, cmn.SimpleKVs{"client.passthrough": "true"})
			}

			var (
				msg = &cmn.SelectMsg{PageSize: 13, Passthrough: passthrough}
			)

			// Do it 2 times - first: fill the cache; second: use it.
			for iter := 0; iter < 2; iter++ {
				started := time.Now()
				objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
				tassert.CheckFatal(t, err)
				finished := time.Now()

				tutils.Logf(
					"[iter: %d] passthrough: %5t, page_size: %d, time: %s\n",
					iter, passthrough, msg.PageSize, finished.Sub(started),
				)

				tassert.Errorf(
					t, len(objList.Entries) == m.num,
					"unexpected number of entries (got: %d, expected: %d)", len(objList.Entries), m.num,
				)
			}

			if !passthrough {
				err := api.ListObjectsInvalidateCache(baseParams, m.bck, msg)
				tassert.CheckError(t, err)
			}
		})
	}
}

func TestBucketListAndSummary(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	type test struct {
		provider string
		summary  bool
		cached   bool
		fast     bool // TODO: it makes sense only for summary
	}

	var tests []test
	for _, provider := range []string{cmn.ProviderAIS, cmn.AnyCloud} {
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
				baseParams = tutils.BaseAPIParams()
			)

			m.saveClusterState()
			if m.originalTargetCount < 2 {
				t.Fatalf("must have at least 2 target in the cluster")
			}

			expectedFiles := m.num
			bckTest := cmn.Bck{Provider: test.provider, Ns: cmn.NsGlobal}
			if bckTest.IsAIS() {
				tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
				defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

				m.puts()
			} else if bckTest.IsCloud(cmn.AnyCloud) {
				m.bck.Name = clibucket

				tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: m.bck})

				m.cloudPuts(true /*evict*/)
				defer m.del()

				if test.cached {
					m.cloudPrefetch(cacheSize)
					expectedFiles = cacheSize
				}
			} else {
				t.Fatal(test.provider)
			}

			tutils.Logln("checking objects...")

			msg := &cmn.SelectMsg{Cached: test.cached, Fast: test.fast}

			if test.summary {
				summaries, err := api.GetBucketsSummaries(baseParams, cmn.QueryBcks(m.bck), msg)
				tassert.CheckFatal(t, err)

				if len(summaries) == 0 {
					t.Fatalf("summary for bucket %q should exist", m.bck)
				}
				if len(summaries) != 1 {
					t.Fatalf("number of summaries (%d) is larger than 1", len(summaries))
				}

				summary := summaries[0]
				if summary.ObjCount != uint64(expectedFiles) {
					t.Errorf("number of objects in summary (%d) is different than expected (%d)", summary.ObjCount, expectedFiles)
				}
			} else {
				objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
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
		baseParams = tutils.BaseAPIParams()
	)

	m.saveClusterState()
	if m.originalTargetCount < 3 {
		t.Fatalf("must have at least 3 target in the cluster")
	}

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	tutils.Logf("Changing bucket %q properties...\n", m.bck)

	// Enabling EC should set default value for number of slices if it is 0
	_, err := api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
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
	_, err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(false)},
	})
	tassert.CheckError(t, err)

	// Enabling mirroring should set default value for number of copies if it is 0
	_, err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
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
	_, err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{Enabled: api.Bool(false)},
	})
	tassert.CheckError(t, err)

	// Change a few more bucket properties
	_, err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{
			DataSlices:   api.Int(dataSlices),
			ParitySlices: api.Int(paritySlices),
			ObjSizeLimit: api.Int64(objLimit),
		},
	})
	tassert.CheckError(t, err)

	// Enable EC again
	_, err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(true)},
	})
	tassert.CheckError(t, err)
	p, err = api.HeadBucket(baseParams, m.bck)
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
	_, err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(false)},
	})
	tassert.CheckError(t, err)

	// Change mirroring threshold
	_, err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{UtilThresh: api.Int64(mirrorThreshold)}},
	)
	tassert.CheckError(t, err)
	p, err = api.HeadBucket(baseParams, m.bck)
	tassert.CheckFatal(t, err)
	if p.Mirror.UtilThresh != mirrorThreshold {
		t.Errorf("Mirror utilization threshold was not changed to %d. Current value %d", mirrorThreshold, p.Mirror.UtilThresh)
	}

	// Disable mirroring
	_, err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{Enabled: api.Bool(false)},
	})
	tassert.CheckError(t, err)
}

func TestSetBucketPropsOfNonexistentBucket(t *testing.T) {
	var (
		baseParams = tutils.BaseAPIParams()
	)
	bucket, err := tutils.GenerateNonexistentBucketName(t.Name()+"Bucket", baseParams)
	tassert.CheckFatal(t, err)

	bck := cmn.Bck{
		Name:     bucket,
		Provider: cmn.AnyCloud,
	}

	_, err = api.SetBucketProps(baseParams, bck, cmn.BucketPropsToUpdate{
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
		baseParams  = tutils.BaseAPIParams()
		bucketProps = cmn.BucketPropsToUpdate{}
	)

	bucket, err := tutils.GenerateNonexistentBucketName(t.Name()+"Bucket", baseParams)
	tassert.CheckFatal(t, err)

	bck := cmn.Bck{
		Name:     bucket,
		Provider: cmn.AnyCloud,
	}

	_, err = api.SetBucketProps(baseParams, bck, bucketProps)
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
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
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

	if testing.Short() {
		m.num = 250
		m.numGetsEachFile = 3
	}

	m.saveClusterState()

	{
		targets := tutils.ExtractTargetNodes(m.smap)
		baseParams := tutils.BaseAPIParams()
		mpList, err := api.GetMountpaths(baseParams, targets[0])
		tassert.CheckFatal(t, err)

		l := len(mpList.Available)
		max := cmn.Max(numCopies...) + 1
		if l < max {
			t.Skipf("test %q requires at least %d mountpaths (target %s has %d)", t.Name(), max, targets[0], l)
		}
	}

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	{
		baseParams := tutils.BaseAPIParams()
		_, err := api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
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

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.gets()
	}()

	baseParams := tutils.BaseAPIParams(m.proxyURL)

	for _, copies := range numCopies {
		makeNCopies(t, baseParams, m.bck, copies)
	}

	// wait for all GETs to complete
	wg.Wait()

	m.ensureNumCopies(numCopies[len(numCopies)-1])
}

func makeNCopies(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, ncopies int) {
	tutils.Logf("Set copies = %d\n", ncopies)

	xactID, err := api.MakeNCopies(baseParams, bck, ncopies)
	tassert.CheckFatal(t, err)

	err = api.WaitForXactionJtx(baseParams, xactID)
	tassert.CheckFatal(t, err)
}

func TestCloudMirror(t *testing.T) {
	var (
		m = &ioContext{
			t:   t,
			num: 64,
			bck: cmn.Bck{
				Name:     clibucket,
				Provider: cmn.AnyCloud,
			},
		}
		baseParams = tutils.BaseAPIParams()
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: m.bck})

	m.saveClusterState()
	m.cloudPuts(true /*evict*/)
	defer m.del()

	// enable mirror
	_, err := api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{Enabled: api.Bool(true)},
	})
	tassert.CheckFatal(t, err)
	defer api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{Enabled: api.Bool(false)},
	})

	// list
	objectList, err := api.ListObjects(baseParams, m.bck, nil, 0)
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
	baseParams := tutils.BaseAPIParams()

	m.puts()
	m.gets()

	p, err := api.HeadBucket(baseParams, m.bck)
	tassert.CheckFatal(t, err)

	// make bucket read-only
	// NOTE: must allow PATCH - otherwise api.SetBucketProps a few lines down below won't work
	aattrs := cmn.ReadOnlyAccess() | cmn.AccessPATCH
	_, err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{Access: api.AccessAttrs(aattrs)})
	tassert.CheckFatal(t, err)

	m.init()
	nerr := m.puts(true /* don't fail */)
	if nerr != m.num {
		t.Fatalf("num failed PUTs %d, expecting %d", nerr, m.num)
	}

	// restore write access
	_, err = api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{Access: api.AccessAttrs(p.Access)})
	tassert.CheckFatal(t, err)

	// write some more and destroy
	m.init()
	nerr = m.puts(true /* don't fail */)
	if nerr != 0 {
		t.Fatalf("num failed PUTs %d, expecting 0 (zero)", nerr)
	}
}

func TestRenameEmptyBucket(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	var (
		m = ioContext{
			t: t,
		}
		baseParams = tutils.BaseAPIParams()
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
	uuid, err := api.RenameBucket(baseParams, srcBck, dstBck)
	tassert.CheckFatal(t, err)

	err = api.WaitForXactionJtx(baseParams, uuid, rebalanceTimeout)
	tassert.CheckFatal(t, err)

	// Check if the new bucket appears in the list
	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks(srcBck))
	tassert.CheckFatal(t, err)

	if !bcks.Contains(cmn.QueryBcks(dstBck)) {
		t.Error("new bucket not found in buckets list")
	}

	tutils.Logln("checking bucket props...")
	dstProps, err := api.HeadBucket(baseParams, dstBck)
	tassert.CheckFatal(t, err)
	if !srcProps.Equal(dstProps) {
		t.Fatalf("source and destination bucket props do not match: %v - %v", srcProps, dstProps)
	}
}

func TestRenameNonEmptyBucket(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	var (
		m = ioContext{
			t:               t,
			num:             1000,
			numGetsEachFile: 2,
		}
		baseParams = tutils.BaseAPIParams()
		dstBck     = cmn.Bck{
			Name:     TestBucketName + "_new",
			Provider: cmn.ProviderAIS,
		}
	)

	// Initialize ioContext
	m.saveClusterState()
	m.proxyURL = tutils.RandomProxyURL()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	srcBck := m.bck
	tutils.CreateFreshBucket(t, m.proxyURL, srcBck)
	defer func() {
		// This bucket should not be present (thus ignoring error) but
		// try to delete in case something failed.
		api.DestroyBucket(baseParams, srcBck)
		// This bucket should be present.
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
	xactID, err := api.RenameBucket(baseParams, srcBck, dstBck)
	tassert.CheckFatal(t, err)

	err = api.WaitForXactionJtx(baseParams, xactID, rebalanceTimeout)
	tassert.CheckFatal(t, err)

	// Gets on renamed ais bucket
	m.gets()
	m.ensureNoErrors()

	tutils.Logln("checking bucket props...")
	dstProps, err := api.HeadBucket(baseParams, dstBck)
	tassert.CheckFatal(t, err)
	if !srcProps.Equal(dstProps) {
		t.Fatalf("source and destination bucket props do not match: %v - %v", srcProps, dstProps)
	}
}

func TestRenameAlreadyExistingBucket(t *testing.T) {
	var (
		m = ioContext{
			t: t,
		}
		baseParams = tutils.BaseAPIParams()
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
	_, err := api.RenameBucket(baseParams, m.bck, tmpBck)
	if err == nil {
		t.Fatal("expected error on renaming already existing bucket")
	}

	// Check if the old bucket still appears in the list
	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks(m.bck))
	tassert.CheckFatal(t, err)

	if !bcks.Contains(cmn.QueryBcks(m.bck)) || !bcks.Contains(cmn.QueryBcks(tmpBck)) {
		t.Error("one of the buckets was not found in buckets list")
	}

	srcProps, err := api.HeadBucket(baseParams, m.bck)
	tassert.CheckFatal(t, err)

	dstProps, err := api.HeadBucket(baseParams, tmpBck)
	tassert.CheckFatal(t, err)

	if srcProps.Equal(dstProps) {
		t.Fatalf("source and destination bucket props match, even though they should not: %v - %v", srcProps, dstProps)
	}
}

// Tries to rename same source bucket to two destination buckets - the second should fail.
func TestRenameBucketTwice(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	var (
		m = ioContext{
			t:   t,
			num: 500,
		}
		baseParams = tutils.BaseAPIParams()
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
	m.proxyURL = tutils.RandomProxyURL()
	if m.originalTargetCount < 1 {
		t.Fatalf("Must have 1 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	srcBck := m.bck
	tutils.CreateFreshBucket(t, m.proxyURL, srcBck)
	defer func() {
		// These buckets should not be present (thus ignoring error) but
		// try to delete in case something failed.
		api.DestroyBucket(baseParams, srcBck)
		api.DestroyBucket(baseParams, dstBck2)
		// This one should be present.
		tutils.DestroyBucket(t, m.proxyURL, dstBck1)
	}()

	m.puts()

	// Rename to first destination
	tutils.Logf("rename %s => %s\n", srcBck, dstBck1)
	xactID, err := api.RenameBucket(baseParams, srcBck, dstBck1)
	tassert.CheckFatal(t, err)

	// Try to rename to first destination again - already in progress
	tutils.Logf("try rename %s => %s\n", srcBck, dstBck1)
	_, err = api.RenameBucket(baseParams, srcBck, dstBck1)
	if err == nil {
		t.Error("multiple rename operations on same bucket should fail")
	}

	// Try to rename to second destination - this should fail
	tutils.Logf("try rename %s => %s\n", srcBck, dstBck2)
	_, err = api.RenameBucket(baseParams, srcBck, dstBck2)
	if err == nil {
		t.Error("multiple rename operations on same bucket should fail")
	}

	// Wait for rename to complete
	err = api.WaitForXactionJtx(baseParams, xactID, rebalanceTimeout)
	tassert.CheckFatal(t, err)

	// Check if the new bucket appears in the list
	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks(srcBck))
	tassert.CheckFatal(t, err)

	if bcks.Contains(cmn.QueryBcks(srcBck)) {
		t.Error("source bucket found in buckets list")
	}
	if !bcks.Contains(cmn.QueryBcks(dstBck1)) {
		t.Error("destination bucket not found in buckets list")
	}
	if bcks.Contains(cmn.QueryBcks(dstBck2)) {
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
		{provider: cmn.AnyCloud, dstBckExist: false, dstBckHasObjects: false},
		{provider: cmn.AnyCloud, dstBckExist: true, dstBckHasObjects: false},
		{provider: cmn.AnyCloud, dstBckExist: true, dstBckHasObjects: true},
		{provider: cmn.AnyCloud, dstBckExist: false, dstBckHasObjects: false, multipleDests: true},
		{provider: cmn.AnyCloud, dstBckExist: true, dstBckHasObjects: true, multipleDests: true},
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
				}
				dstms = []*ioContext{
					{
						t:   t,
						num: numput,
						bck: cmn.Bck{
							Name:     "dst_copy_bck_1",
							Provider: cmn.ProviderAIS,
						},
					},
				}
				baseParams = tutils.BaseAPIParams()
			)

			if test.multipleDests {
				dstms = append(dstms, &ioContext{
					t:   t,
					num: numput,
					bck: cmn.Bck{
						Name:     "dst_copy_bck_2",
						Provider: cmn.ProviderAIS,
					},
				})
			}
			bckTest := cmn.Bck{Provider: test.provider, Ns: cmn.NsGlobal}
			if bckTest.IsCloud(cmn.AnyCloud) {
				srcm.bck = cmn.Bck{
					Name:     clibucket,
					Provider: cmn.AnyCloud,
				}

				tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: srcm.bck})
			}

			// Initialize ioContext
			srcm.saveClusterState()
			for _, dstm := range dstms {
				dstm.init()
			}
			if srcm.originalTargetCount < 1 {
				t.Fatalf("Must have 1 or more targets in the cluster, have only %d", srcm.originalTargetCount)
			}
			if bckTest.IsAIS() {
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

			if bckTest.IsAIS() {
				srcm.puts()

				srcBckList, err = api.ListObjects(baseParams, srcm.bck, nil, 0)
				tassert.CheckFatal(t, err)
			} else if bckTest.IsCloud(cmn.AnyCloud) {
				srcm.cloudPuts(false /*evict*/)
				defer srcm.del()

				srcBckList, err = api.ListObjects(baseParams, srcm.bck, nil, 0)
				tassert.CheckFatal(t, err)
			} else {
				panic(test.provider)
			}

			xactIDs := make([]string, len(dstms))
			for idx, dstm := range dstms {
				tutils.Logf("copying %s => %s\n", srcm.bck, dstm.bck)
				uuid, err := api.CopyBucket(baseParams, srcm.bck, dstm.bck)
				xactIDs[idx] = uuid
				tassert.CheckFatal(t, err)
			}

			for _, uuid := range xactIDs {
				err = api.WaitForXactionJtx(baseParams, uuid, copyBucketTimeout)
				tassert.CheckFatal(t, err)
			}

			tutils.Logln("checking and comparing bucket props")
			for _, dstm := range dstms {
				dstProps, err := api.HeadBucket(baseParams, dstm.bck)
				tassert.CheckFatal(t, err)

				if dstProps.Provider != cmn.ProviderAIS {
					t.Fatalf("destination bucket does not seem to be 'ais': %s", dstProps.Provider)
				}
				// Clear providers to make sure that they will fail on different providers.
				srcProps.Provider = ""
				dstProps.Provider = ""

				// If bucket existed before, we need to ensure that the bucket
				// props were **not** copied over.
				if test.dstBckExist && srcProps.Equal(dstProps) {
					t.Fatalf("source and destination bucket props match, even though they should not:\n%#v\n%#v",
						srcProps, dstProps)
				}

				// If bucket did not exist before, we need to ensure that
				// the bucket props match the source bucket props (except provider).
				if !test.dstBckExist && !srcProps.Equal(dstProps) {
					t.Fatalf("source and destination bucket props do not match:\n%#v\n%#v", srcProps, dstProps)
				}
			}

			tutils.Logln("checking and comparing objects")

			for _, dstm := range dstms {
				expectedObjCount := srcm.num
				if test.dstBckHasObjects {
					expectedObjCount += dstm.num
				}

				dstBckList, err := api.ListObjects(baseParams, dstm.bck, nil, 0)
				tassert.CheckFatal(t, err)
				if len(dstBckList.Entries) != expectedObjCount {
					t.Fatalf("list_objects: dst %d != %d src", len(dstBckList.Entries), expectedObjCount)
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
			num: 500,
		}
		baseParams = tutils.BaseAPIParams()
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
	xactID, err := api.RenameBucket(baseParams, srcBck, dstBck1)
	tassert.CheckFatal(t, err)

	// Try to copy to first destination - rename in progress, both for srcBck and dstBck1
	tutils.Logf("try copy %s => %s\n", srcBck, dstBck1)
	_, err = api.CopyBucket(baseParams, srcBck, dstBck1)
	if err == nil {
		t.Error("coping bucket that is under renaming did not fail")
	}

	// Try to copy to second destination - rename in progress for srcBck
	tutils.Logf("try copy %s => %s\n", srcBck, dstBck2)
	_, err = api.CopyBucket(baseParams, srcBck, dstBck2)
	if err == nil {
		t.Error("coping bucket that is under renaming did not fail")
	}

	// Try to copy from dstBck1 to dstBck1 - rename in progress for dstBck1
	tutils.Logf("try copy %s => %s\n", dstBck1, dstBck2)
	_, err = api.CopyBucket(baseParams, srcBck, dstBck1)
	if err == nil {
		t.Error("coping bucket that is under renaming did not fail")
	}

	// Wait for rename to complete
	err = api.WaitForXactionJtx(baseParams, xactID, rebalanceTimeout)
	tassert.CheckFatal(t, err)

	// Check if the new bucket appears in the list
	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks(srcBck))
	tassert.CheckFatal(t, err)

	if bcks.Contains(cmn.QueryBcks(srcBck)) {
		t.Error("source bucket found in buckets list")
	}
	if !bcks.Contains(cmn.QueryBcks(dstBck1)) {
		t.Error("destination bucket not found in buckets list")
	}
	if bcks.Contains(cmn.QueryBcks(dstBck2)) {
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
			num: 500,
		}
		baseParams = tutils.BaseAPIParams()
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
	xactID, err := api.CopyBucket(baseParams, srcBck, dstBck1)
	tassert.CheckFatal(t, err)

	// Try to rename to first destination - copy in progress, both for srcBck and dstBck1
	tutils.Logf("try rename %s => %s\n", srcBck, dstBck1)
	_, err = api.RenameBucket(baseParams, srcBck, dstBck1)
	if err == nil {
		t.Error("renaming bucket that is under coping did not fail")
	}

	// Try to rename to second destination - copy in progress for srcBck
	tutils.Logf("try rename %s => %s\n", srcBck, dstBck2)
	_, err = api.RenameBucket(baseParams, srcBck, dstBck2)
	if err == nil {
		t.Error("renaming bucket that is under coping did not fail")
	}

	// Try to rename from dstBck1 to dstBck1 - rename in progress for dstBck1
	tutils.Logf("try rename %s => %s\n", dstBck1, dstBck2)
	_, err = api.RenameBucket(baseParams, srcBck, dstBck1)
	if err == nil {
		t.Error("renaming bucket that is under coping did not fail")
	}

	// Wait for copy to complete
	err = api.WaitForXactionJtx(baseParams, xactID, rebalanceTimeout)
	tassert.CheckFatal(t, err)

	// Check if the new bucket appears in the list
	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks(srcBck))
	tassert.CheckFatal(t, err)

	if bcks.Contains(cmn.QueryBcks(srcBck)) {
		t.Error("source bucket found in buckets list")
	}
	if !bcks.Contains(cmn.QueryBcks(dstBck1)) {
		t.Error("destination bucket not found in buckets list")
	}
	if bcks.Contains(cmn.QueryBcks(dstBck2)) {
		t.Error("second (failed) destination bucket found in buckets list")
	}
}

func TestBackendBucket(t *testing.T) {
	var (
		cloudBck = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.AnyCloud,
		}
		aisBck = cmn.Bck{
			Name:     cmn.RandString(10),
			Provider: cmn.ProviderAIS,
		}
		m = ioContext{
			t:   t,
			num: 10,
			bck: cloudBck,
		}

		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: cloudBck})

	m.init()

	tutils.CreateFreshBucket(t, proxyURL, aisBck)
	defer tutils.DestroyBucket(t, proxyURL, aisBck)

	p, err := api.HeadBucket(baseParams, cloudBck)
	tassert.CheckFatal(t, err)
	cloudBck.Provider = p.Provider

	m.cloudPuts(false /*evict*/)
	defer m.del()

	cloudObjList, err := api.ListObjects(baseParams, cloudBck, nil, 0)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(cloudObjList.Entries) > 0, "empty object list")

	// Connect backend bucket to a aisBck
	_, err = api.SetBucketProps(baseParams, aisBck, cmn.BucketPropsToUpdate{
		BackendBck: &cmn.BckToUpdate{
			Name:     api.String(cloudBck.Name),
			Provider: api.String(cloudBck.Provider),
		},
	})
	tassert.CheckFatal(t, err)
	// Try putting one of the original cloud objects, it should work.
	err = tutils.PutObjRR(baseParams, aisBck, cloudObjList.Entries[0].Name, 128, cmn.ChecksumNone)
	tassert.Errorf(t, err == nil, "expected err==nil (put to a BackendBck should be allowed via aisBck )")

	p, err = api.HeadBucket(baseParams, aisBck)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(
		t, p.BackendBck.Equal(cloudBck),
		"backend bucket wasn't set correctly (got: %s, expected: %s)",
		p.BackendBck, cloudBck,
	)

	// Try to cache object.
	cachedObjName := cloudObjList.Entries[0].Name
	_, err = api.GetObject(baseParams, aisBck, cachedObjName)
	tassert.CheckFatal(t, err)

	// Check if listing objects will result in listing backend bucket objects.
	msg := &cmn.SelectMsg{}
	msg.AddProps(cmn.GetPropsAll...)
	aisObjList, err := api.ListObjects(baseParams, aisBck, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(
		t, len(cloudObjList.Entries) == len(aisObjList.Entries),
		"object lists cloud vs ais does not match (got: %+v, expected: %+v)",
		aisObjList.Entries, cloudObjList.Entries,
	)

	// Check if cached listing works correctly.
	aisObjList, err = api.ListObjects(baseParams, aisBck, &cmn.SelectMsg{Cached: true}, 0)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(
		t, len(aisObjList.Entries) == 1,
		"bucket contains incorrect number of cached objects (got: %+v, expected: [%s])",
		aisObjList.Entries, cachedObjName,
	)

	// Disconnect backend bucket.
	_, err = api.SetBucketProps(baseParams, aisBck, cmn.BucketPropsToUpdate{
		BackendBck: &cmn.BckToUpdate{
			Name:     api.String(""),
			Provider: api.String(""),
		},
	})
	tassert.CheckFatal(t, err)
	p, err = api.HeadBucket(baseParams, aisBck)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, p.BackendBck.IsEmpty(), "backend bucket isn't empty")

	// Check if we can still get object and list objects.
	_, err = api.GetObject(baseParams, aisBck, cachedObjName)
	tassert.CheckFatal(t, err)

	aisObjList, err = api.ListObjects(baseParams, aisBck, nil, 0)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(
		t, len(aisObjList.Entries) == 1,
		"bucket contains incorrect number of objects (got: %+v, expected: [%s])",
		aisObjList.Entries, cachedObjName,
	)

	// Check that we cannot do cold gets anymore.
	_, err = api.GetObject(baseParams, aisBck, cloudObjList.Entries[1].Name)
	tassert.Fatalf(t, err != nil, "expected error (object should not exist)")

	// Check that we cannot do put anymore.
	err = tutils.PutObjRR(baseParams, aisBck, cachedObjName, 256, cmn.ChecksumNone)
	tassert.Errorf(t, err != nil, "expected err!=nil (put should not be allowed with objSrc!=BackendBck  )")
}

//
// even more checksum tests
//

func TestAllChecksums(t *testing.T) {
	checksums := cmn.SupportedChecksums()
	for _, mirrored := range []bool{false, true} {
		for _, cksumType := range checksums {
			if testing.Short() && cksumType != cmn.ChecksumNone && cksumType != cmn.ChecksumXXHash {
				continue
			}
			tag := cksumType
			if mirrored {
				tag = cksumType + "/mirrored"
			}
			t.Run(tag, func(t *testing.T) {
				started := time.Now()
				testWarmValidation(t, cksumType, mirrored, false)
				tutils.Logf("Time: %v\n", time.Since(started))
			})
		}
	}
	for _, cksumType := range checksums {
		if testing.Short() && cksumType != cmn.ChecksumNone && cksumType != cmn.ChecksumXXHash {
			continue
		}
		tag := cksumType + "/EC"
		t.Run(tag, func(t *testing.T) {
			started := time.Now()
			testWarmValidation(t, cksumType, false, true)
			tutils.Logf("Time: %v\n", time.Since(started))
		})
	}
}
func testWarmValidation(t *testing.T, cksumType string, mirrored, eced bool) {
	const (
		copyCnt   = 2
		parityCnt = 2
	)
	var (
		m = ioContext{
			t:               t,
			num:             1000,
			numGetsEachFile: 1,
			fileSize:        uint64(cmn.KiB + rand.Int63n(cmn.KiB*10)),
		}
		numCorrupted = rand.Intn(m.num/100) + 2
	)
	if testing.Short() {
		m.num = 40
		m.fileSize = cmn.KiB
		numCorrupted = 13
	}
	m.saveClusterState()
	baseParams := tutils.BaseAPIParams(m.proxyURL)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	{
		if mirrored {
			_, err := api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
				Cksum: &cmn.CksumConfToUpdate{
					Type:            api.String(cksumType),
					ValidateWarmGet: api.Bool(true),
				},
				Mirror: &cmn.MirrorConfToUpdate{
					Enabled: api.Bool(true),
					Copies:  api.Int64(copyCnt),
				},
			})
			tassert.CheckFatal(t, err)
		} else if eced {
			if m.smap.CountTargets() < parityCnt+1 {
				t.Fatalf("Not enough targets to run %s test, must be at least %d", t.Name(), parityCnt+1)
			}
			_, err := api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
				Cksum: &cmn.CksumConfToUpdate{
					Type:            api.String(cksumType),
					ValidateWarmGet: api.Bool(true),
				},
				EC: &cmn.ECConfToUpdate{
					Enabled:      api.Bool(true),
					ObjSizeLimit: api.Int64(cmn.GiB), // only slices
					DataSlices:   api.Int(1),
					ParitySlices: api.Int(parityCnt),
				},
			})
			tassert.CheckFatal(t, err)
		} else {
			_, err := api.SetBucketProps(baseParams, m.bck, cmn.BucketPropsToUpdate{
				Cksum: &cmn.CksumConfToUpdate{
					Type:            api.String(cksumType),
					ValidateWarmGet: api.Bool(true),
				},
			})
			tassert.CheckFatal(t, err)
		}

		p, err := api.HeadBucket(baseParams, m.bck)
		tassert.CheckFatal(t, err)
		if p.Cksum.Type != cksumType {
			t.Fatalf("failed to set checksum: %q != %q", p.Cksum.Type, cksumType)
		}
		if !p.Cksum.ValidateWarmGet {
			t.Fatal("failed to set checksum: validate_warm_get not enabled")
		}
		if mirrored && !p.Mirror.Enabled {
			t.Fatal("failed to mirroring")
		}
		if eced && !p.EC.Enabled {
			t.Fatal("failed to enable erasure coding")
		}
	}

	m.puts()

	// wait for mirroring
	if mirrored {
		// TODO: there must be a better way for waiting for all copies.
		if testing.Short() {
			time.Sleep(5 * time.Second)
		} else {
			time.Sleep(10 * time.Second)
		}
		m.ensureNumCopies(copyCnt)
	}
	// wait for erasure-coding
	if eced {
		// TODO: must be able to wait for Kind = cmn.ActECPut
		if testing.Short() {
			time.Sleep(3 * time.Second)
		} else {
			time.Sleep(8 * time.Second)
		}
	}

	// read all
	if cksumType != cmn.ChecksumNone {
		tutils.Logf("Reading %q objects with checksum validation by AIS targets\n", m.bck)
	} else {
		tutils.Logf("Reading %q objects\n", m.bck)
	}
	m.gets()

	msg := &cmn.SelectMsg{}
	bckObjs, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(t, err)
	if len(bckObjs.Entries) == 0 {
		t.Errorf("%s is empty\n", m.bck)
		return
	}

	if cksumType != cmn.ChecksumNone {
		tutils.Logf("Reading %d objects from %s with end-to-end %s validation\n", len(bckObjs.Entries), m.bck, cksumType)
		wg := cmn.NewLimitedWaitGroup(40)

		for _, entry := range bckObjs.Entries {
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				_, err = api.GetObjectWithValidation(baseParams, m.bck, name)
				tassert.CheckError(t, err)
			}(entry.Name)
		}

		wg.Wait()
	}

	if containers.DockerRunning() {
		tutils.Logln("Skipping object corruption test in docker")
		return
	}

	// corrupt random and read again
	{
		i := rand.Intn(len(bckObjs.Entries))
		if i+numCorrupted > len(bckObjs.Entries) {
			i -= numCorrupted
		}
		objCh := make(chan string, numCorrupted)
		tutils.Logf("Corrupting %d objects\n", numCorrupted)
		go func() {
			for j := i; j < i+numCorrupted; j++ {
				objName := bckObjs.Entries[j].Name
				corruptSingleBitInFile(t, m.bck, objName)
				objCh <- objName
			}
		}()
		for j := 0; j < numCorrupted; j++ {
			objName := <-objCh
			_, err = api.GetObject(baseParams, m.bck, objName)
			if mirrored || eced {
				if err != nil && cksumType != cmn.ChecksumNone {
					t.Errorf("%s/%s corruption detected but not resolved, mirror=%t, ec=%t\n",
						m.bck, objName, mirrored, eced)
				}
			} else {
				if err == nil && cksumType != cmn.ChecksumNone {
					t.Errorf("%s/%s corruption undetected\n", m.bck, objName)
				}
			}
		}
	}
}
