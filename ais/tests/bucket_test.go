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
	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/NVIDIA/aistore/tutils/tassert"
	"golang.org/x/sync/errgroup"
)

func TestHTTPProviderBucket(t *testing.T) {
	var (
		bck = cmn.Bck{
			Name:     t.Name() + "Bucket",
			Provider: cmn.ProviderHTTP,
		}
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	err := api.CreateBucket(baseParams, bck)
	tassert.Fatalf(t, err != nil, "expected error")

	_, err = api.GetObject(baseParams, bck, "nonexisting")
	tassert.Fatalf(t, err != nil, "expected error")

	_, err = api.ListObjects(baseParams, bck, nil, 0)
	tassert.Fatalf(t, err != nil, "expected error")

	reader, _ := readers.NewRandReader(cmn.KiB, cmn.ChecksumNone)
	err = api.PutObject(api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bck,
		Object:     "something",
		Reader:     reader,
	})
	tassert.Fatalf(t, err != nil, "expected error")
}

func Test_BucketNames(t *testing.T) {
	var (
		bck = cmn.Bck{
			Name:     t.Name() + "Bucket",
			Provider: cmn.ProviderAIS,
		}
		proxyURL   = tutils.RandomProxyURL(t)
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
		proxyURL     = tutils.RandomProxyURL(t)
		baseParams   = tutils.BaseAPIParams(proxyURL)
		globalConfig = tutils.GetClusterConfig(t)
		bck          = cmn.Bck{
			Name:     testBucketName,
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
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     testBucketName,
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
			m := &ioContext{
				t:      t,
				num:    100,
				silent: true,
			}

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
		proxyURL     = tutils.RandomProxyURL(t)
		globalConfig = tutils.GetClusterConfig(t)
		baseParams   = tutils.BaseAPIParams(proxyURL)
		bck          = cmn.Bck{
			Name:     testBucketName,
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
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     testBucketName,
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

func TestListObjectCloudVersions(t *testing.T) {
	var (
		m = ioContext{
			t:        t,
			bck:      cliBck,
			num:      50,
			fileSize: 128,
			prefix:   cmn.RandString(6) + "-",
		}
		baseParams = tutils.BaseAPIParams()
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true, Cloud: true, Bck: m.bck})

	m.init()

	p, err := api.HeadBucket(baseParams, m.bck)
	tassert.CheckFatal(t, err)

	if !p.Versioning.Enabled {
		t.Skip("test requires a cloud bucket with enabled versioning")
	}

	m.puts()
	defer m.del()

	tutils.Logf("Reading %q objects\n", m.bck)
	msg := &cmn.SelectMsg{Prefix: m.prefix, Props: cmn.GetPropsVersion}
	bckObjs, err := api.ListObjects(baseParams, m.bck, msg, 0)
	tassert.CheckFatal(t, err)

	tutils.Logf("Checking %q object versions [total: %d]\n", m.bck, len(bckObjs.Entries))
	for _, entry := range bckObjs.Entries {
		tassert.Errorf(t, entry.Version != "", "Object %s does not have version", entry.Name)
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

func TestListObjectsGoBack(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		var (
			baseParams = tutils.BaseAPIParams()
			m          = ioContext{
				t:        t,
				num:      2000,
				bck:      bck.Bck,
				fileSize: 128,
			}

			msg = &cmn.SelectMsg{PageSize: 50}
		)

		if !bck.IsAIS() {
			m.num = 300
		}

		m.init()
		m.puts()
		defer m.del()

		var (
			tokens          []string
			entries         []*cmn.BucketEntry
			expectedEntries []*cmn.BucketEntry
		)
		tutils.Logln("listing couple pages to move iterator on targets")
		for page := 0; page < m.num/int(msg.PageSize); page++ {
			tokens = append(tokens, msg.ContinuationToken)
			objPage, err := api.ListObjectsPage(baseParams, m.bck, msg)
			tassert.CheckFatal(t, err)
			expectedEntries = append(expectedEntries, objPage.Entries...)
		}

		tutils.Logln("list bucket in reverse order")

		for i := len(tokens) - 1; i >= 0; i-- {
			msg.ContinuationToken = tokens[i]
			objPage, err := api.ListObjectsPage(baseParams, m.bck, msg)
			tassert.CheckFatal(t, err)
			entries = append(entries, objPage.Entries...)
		}

		cmn.SortBckEntries(entries)
		cmn.SortBckEntries(expectedEntries)

		tassert.Fatalf(
			t, len(expectedEntries) == m.num,
			"unexpected number of expected entries (got: %d, expected: %d)",
			len(expectedEntries), m.num,
		)

		tassert.Fatalf(
			t, len(entries) == len(expectedEntries),
			"unexpected number of entries (got: %d, expected: %d)",
			len(entries), len(expectedEntries),
		)

		for idx := range expectedEntries {
			tassert.Errorf(
				t, entries[idx].Name == expectedEntries[idx].Name,
				"unexpected entry (got: %q, expected: %q)",
				entries[idx], expectedEntries[idx],
			)
		}
	})
}

func TestListObjectsRerequestPage(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		var (
			baseParams = tutils.BaseAPIParams()
			m          = ioContext{
				t:        t,
				bck:      bck.Bck,
				num:      500,
				fileSize: 128,
			}
			rerequests = 5
		)

		if !bck.IsAIS() {
			m.num = 50
		}

		m.init()
		m.puts()
		defer m.del()

		var (
			err     error
			objList *cmn.BucketList

			totalCnt = 0
			msg      = &cmn.SelectMsg{PageSize: 10}
		)
		tutils.Logln("starting rerequesting routine...")
		for {
			prevToken := msg.ContinuationToken
			for i := 0; i < rerequests; i++ {
				msg.ContinuationToken = prevToken
				objList, err = api.ListObjectsPage(baseParams, m.bck, msg)
				tassert.CheckFatal(t, err)
			}
			totalCnt += len(objList.Entries)
			if objList.ContinuationToken == "" {
				break
			}
		}
		tassert.Fatalf(
			t, totalCnt == m.num,
			"unexpected total number of objects (got: %d, expected: %d)", totalCnt, m.num,
		)
	})
}

func TestListObjectsStartAfter(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		var (
			baseParams = tutils.BaseAPIParams()
			m          = ioContext{
				t:        t,
				num:      200,
				bck:      bck.Bck,
				fileSize: 128,
			}
		)

		if !bck.IsAIS() {
			m.num = 20
		}

		m.init()
		m.puts()
		defer m.del()

		objList, err := api.ListObjects(baseParams, m.bck, nil, 0)
		tassert.CheckFatal(t, err)

		middleObjName := objList.Entries[m.num/2-1].Name
		tutils.Logf("start listing bucket after: %q...\n", middleObjName)

		msg := &cmn.SelectMsg{PageSize: 10, StartAfter: middleObjName}
		objList, err = api.ListObjects(baseParams, m.bck, msg, 0)
		if bck.IsAIS() {
			tassert.CheckFatal(t, err)
			tassert.Errorf(
				t, len(objList.Entries) == m.num/2,
				"unexpected number of entries (got: %d, expected: %d)",
				len(objList.Entries), m.num/2,
			)
		} else {
			tassert.Errorf(t, err != nil, "expected error to occur")
		}
	})
}

func TestListObjectsProps(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		var (
			baseParams = tutils.BaseAPIParams()
			m          = ioContext{
				t:        t,
				num:      rand.Intn(5000) + 1000,
				bck:      bck.Bck,
				fileSize: 128,
			}
		)

		if !bck.IsAIS() {
			m.num = rand.Intn(250) + 100
		}

		m.init()
		m.puts()
		defer m.del()

		checkProps := func(useCache bool, props []string, f func(entry *cmn.BucketEntry)) {
			msg := &cmn.SelectMsg{PageSize: 100, UseCache: useCache}
			msg.AddProps(props...)
			objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
			tassert.CheckFatal(t, err)
			tassert.Errorf(
				t, len(objList.Entries) == m.num,
				"unexpected number of entries (got: %d, expected: %d)", len(objList.Entries), m.num,
			)
			for _, entry := range objList.Entries {
				tassert.Errorf(t, entry.Name != "", "name is not set")
				f(entry)
			}
		}

		for _, useCache := range []bool{false, true} {
			tutils.Logf("[cache=%t] trying empty (default) subset of props...\n", useCache)
			checkProps(useCache, []string{}, func(entry *cmn.BucketEntry) {
				tassert.Errorf(t, entry.Size != 0, "size is not set")
				tassert.Errorf(t, entry.Version == "", "version is set")
				tassert.Errorf(t, entry.Checksum != "", "checksum is not set")
				tassert.Errorf(t, entry.Atime != "", "atime is not set")

				tassert.Errorf(t, entry.TargetURL == "", "targetURL is set")
				tassert.Errorf(t, entry.Copies == 0, "copies is set")
			})

			tutils.Logf("[cache=%t] trying default subset of props...\n", useCache)
			checkProps(useCache, cmn.GetPropsDefault, func(entry *cmn.BucketEntry) {
				tassert.Errorf(t, entry.Size != 0, "size is not set")
				tassert.Errorf(t, entry.Version == "", "version is set")
				tassert.Errorf(t, entry.Checksum != "", "checksum is not set")
				tassert.Errorf(t, entry.Atime != "", "atime is not set")

				tassert.Errorf(t, entry.TargetURL == "", "targetURL is set")
				tassert.Errorf(t, entry.Copies == 0, "copies is set")
			})

			tutils.Logf("[cache=%t] trying specific subset of props...\n", useCache)
			checkProps(useCache, []string{cmn.GetPropsChecksum, cmn.GetPropsVersion, cmn.GetPropsCopies}, func(entry *cmn.BucketEntry) {
				tassert.Errorf(t, entry.Checksum != "", "checksum is not set")
				if bck.IsAIS() {
					tassert.Errorf(t, entry.Version != "", "version is not set")
				}
				tassert.Errorf(t, entry.Copies == 1, "copies is not set")

				tassert.Errorf(t, entry.Size == 0, "size is set")
				tassert.Errorf(t, entry.Atime == "", "atime is set")
				tassert.Errorf(t, entry.TargetURL == "", "targetURL is set")
			})

			tutils.Logf("[cache=%t] trying small subset of props...\n", useCache)
			checkProps(useCache, []string{cmn.GetPropsSize}, func(entry *cmn.BucketEntry) {
				tassert.Errorf(t, entry.Size != 0, "size is not set")

				tassert.Errorf(t, entry.Version == "", "version is set")
				tassert.Errorf(t, entry.Checksum == "", "checksum is set")
				tassert.Errorf(t, entry.Atime == "", "atime is set")
				tassert.Errorf(t, entry.TargetURL == "", "targetURL is set")
				tassert.Errorf(t, entry.Copies == 0, "copies is set")
			})

			tutils.Logf("[cache=%t] trying all props...\n", useCache)
			checkProps(useCache, cmn.GetPropsAll, func(entry *cmn.BucketEntry) {
				tassert.Errorf(t, entry.Size != 0, "size is not set")
				if bck.IsAIS() {
					tassert.Errorf(t, entry.Version != "", "version is not set")
				}
				tassert.Errorf(t, entry.Checksum != "", "checksum is not set")
				tassert.Errorf(t, entry.Atime != "", "atime is not set")
				tassert.Errorf(t, entry.TargetURL != "", "targetURL is not set")
				tassert.Errorf(t, entry.Copies != 0, "copies is not set")
			})
		}
	})
}

// Runs cloud list objects with `cached == true` (for both evicted and not evicted objects).
func TestListObjectsCloudCached(t *testing.T) {
	var (
		baseParams = tutils.BaseAPIParams()

		m = ioContext{
			t:        t,
			bck:      cliBck,
			num:      rand.Intn(100) + 10,
			fileSize: 128,
		}
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: m.bck})

	m.init()
	defer m.del()

	for _, evict := range []bool{false, true} {
		tutils.Logf("list cloud objects with evict=%t\n", evict)
		m.cloudPuts(evict)

		msg := &cmn.SelectMsg{PageSize: 10, Flags: cmn.SelectCached}
		objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
		tassert.CheckFatal(t, err)
		if evict {
			tassert.Errorf(
				t, len(objList.Entries) == 0,
				"unexpected number of entries (got: %d, expected: 0)", len(objList.Entries),
			)
		} else {
			tassert.Errorf(
				t, len(objList.Entries) == m.num,
				"unexpected number of entries (got: %d, expected: %d)", len(objList.Entries), m.num,
			)
			for _, entry := range objList.Entries {
				tassert.Errorf(t, entry.Name != "", "name is not set")
				tassert.Errorf(t, entry.Size != 0, "size is not set")
				tassert.Errorf(t, entry.Checksum != "", "checksum is not set")
				tassert.Errorf(t, entry.Atime != "", "atime is not set")
				// NOTE: `entry.Version` value depends on cloud configuration.

				tassert.Errorf(t, entry.TargetURL == "", "targetURL is set")
				tassert.Errorf(t, entry.Copies == 0, "copies is set")
			}
		}
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
			if objList.ContinuationToken == "" {
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
				fileSize: 128,
			}

			totalCnt = 0
			msg      = &cmn.SelectMsg{}
		)

		if !bck.IsAIS() {
			m.num = rand.Intn(200) + 100
		}

		m.init()
		m.puts()
		defer m.del()

		for {
			msg.PageSize = uint(rand.Intn(50) + 50)

			objList, err := api.ListObjectsPage(baseParams, m.bck, msg)
			tassert.CheckFatal(t, err)
			totalCnt += len(objList.Entries)
			if objList.ContinuationToken == "" {
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

		proxyURL   = tutils.RandomProxyURL(t)
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
		{pageSize: uint(rand.Intn(15000))},
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

			p := cmn.DefaultAISBckProps()

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
						objNames := tutils.PutRR(t, baseParams, objectSize, p.Cksum.Type, bck, objDir, objectsToPut)
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
				tassert.CheckError(t, api.ListObjectsInvalidateCache(baseParams, bck))
				bckList, err := api.ListObjects(baseParams, bck, msg, 0)
				tassert.CheckFatal(t, err)

				if bckList.ContinuationToken != "" {
					t.Errorf("continuation token was unexpectedly set to: %s", bckList.ContinuationToken)
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

			tassert.CheckError(t, api.ListObjectsInvalidateCache(baseParams, bck))
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
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	providers := []string{cmn.ProviderAIS}
	if cliBck.IsCloud() {
		providers = append(providers, cliBck.Provider)
	}

	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			var (
				bck        cmn.Bck
				errCh      = make(chan error, numFiles*5)
				filesPutCh = make(chan string, numFiles)
				cksumType  string
				customPage = true
			)
			bckTest := cmn.Bck{Provider: provider, Ns: cmn.NsGlobal}
			if bckTest.IsCloud() {
				bck = cliBck

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
					Name:     testBucketName,
					Provider: provider,
				}
				tutils.CreateFreshBucket(t, proxyURL, bck)
				defer tutils.DestroyBucket(t, proxyURL, bck)

				p := cmn.DefaultAISBckProps()
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

func TestListObjectsCache(t *testing.T) {
	var (
		baseParams = tutils.BaseAPIParams()
		m          = ioContext{
			t:        t,
			num:      rand.Intn(3000) + 1481,
			fileSize: cmn.KiB,
		}
	)

	m.init()

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	m.puts()

	for _, useCache := range []bool{true, false} {
		t.Run(fmt.Sprintf("cache=%t", useCache), func(t *testing.T) {
			// Do it N times - first: fill the cache; next calls: use it.
			for iter := 0; iter < 10; iter++ {
				var (
					started = time.Now()
					msg     = &cmn.SelectMsg{
						PageSize: uint(rand.Intn(20)) + 4,
						UseCache: useCache,
					}
				)

				objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
				tassert.CheckFatal(t, err)

				tutils.Logf(
					"[iter: %d] cache: %5t, page_size: %d, time: %s\n",
					iter, useCache, msg.PageSize, time.Since(started),
				)

				tassert.Errorf(
					t, len(objList.Entries) == m.num,
					"unexpected number of entries (got: %d, expected: %d)", len(objList.Entries), m.num,
				)
			}

			if useCache {
				err := api.ListObjectsInvalidateCache(baseParams, m.bck)
				tassert.CheckError(t, err)
			}
		})
	}
}

func TestListObjectsWithRebalance(t *testing.T) {
	// TODO: This test doesn't work as list objects doesn't correctly handle
	//  objects which are yet to be migrated.
	t.Skip("list objects does not work correctly with rebalance")

	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		baseParams = tutils.BaseAPIParams()
		wg         = &sync.WaitGroup{}
		m          = ioContext{
			t:        t,
			num:      10000,
			fileSize: 128,
		}
	)

	m.saveClusterState()
	m.expectTargets(2)

	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	target := m.unregisterTarget()

	m.puts()

	wg.Add(1)
	go func() {
		defer wg.Done()
		m.reregisterTarget(target)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 15; i++ {
			tutils.Logf("listing all objects, iter: %d\n", i)
			bckList, err := api.ListObjects(baseParams, m.bck, nil, 0)
			tassert.CheckFatal(t, err)
			tassert.Errorf(t, len(bckList.Entries) == m.num, "entries mismatch (%d vs %d)", len(bckList.Entries), m.num)

			time.Sleep(time.Second)
		}
	}()

	wg.Wait()
	m.assertClusterState()
	tutils.WaitForRebalanceToComplete(t, baseParams)
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
	m.expectTargets(3)

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
		Mirror: &cmn.MirrorConfToUpdate{UtilThresh: api.Int64(mirrorThreshold)},
	},
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
	baseParams := tutils.BaseAPIParams()
	bucket, err := tutils.GenerateNonexistentBucketName(t.Name()+"Bucket", baseParams)
	tassert.CheckFatal(t, err)

	bck := cmn.Bck{
		Name:     bucket,
		Provider: cliBck.Provider,
	}

	_, err = api.SetBucketProps(baseParams, bck, cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{Enabled: api.Bool(true)},
	})
	if err == nil {
		t.Fatalf("Expected SetBucketProps error, but got none.")
	}

	status := api.HTTPStatus(err)
	if status < http.StatusBadRequest {
		t.Errorf("Expected status: %d, got %d", http.StatusNotFound, status)
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
		Provider: cliBck.Provider,
	}

	_, err = api.SetBucketProps(baseParams, bck, bucketProps)
	if err == nil {
		t.Fatalf("Expected SetBucketProps error, but got none.")
	}

	status := api.HTTPStatus(err)
	if status < http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, status)
	}
}

func TestBucketInvalidName(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL(t)
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

func TestLocalMirror(t *testing.T) {
	tests := []struct {
		numCopies []int // each of the number in the list represents the number of copies enforced on the bucket
		skipArgs  tutils.SkipTestArgs
	}{
		// set number `copies = 1` - no copies should be created
		{numCopies: []int{1}},
		// set number `copies = 2` - one additional copy for each object should be created
		{numCopies: []int{2}},
		// first set number of copies to 2, then to 3
		{numCopies: []int{2, 3}, skipArgs: tutils.SkipTestArgs{Long: true}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.numCopies), func(t *testing.T) {
			tutils.CheckSkip(t, test.skipArgs)
			testLocalMirror(t, test.numCopies)
		})
	}
}

func testLocalMirror(t *testing.T, numCopies []int) {
	m := ioContext{
		t:               t,
		num:             10000,
		numGetsEachFile: 5,
		bck: cmn.Bck{
			Name:     cmn.RandString(10),
			Provider: cmn.ProviderAIS,
		},
	}

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

	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActMakeNCopies}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)
}

func TestCloudMirror(t *testing.T) {
	var (
		m = &ioContext{
			t:      t,
			num:    64,
			bck:    cliBck,
			prefix: t.Name(),
		}
		baseParams = tutils.BaseAPIParams()
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: m.bck})

	m.init()
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
	m := ioContext{
		t:               t,
		num:             10,
		numGetsEachFile: 2,
	}
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

func TestRenameBucketEmpty(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	var (
		m = ioContext{
			t: t,
		}
		baseParams = tutils.BaseAPIParams()
		dstBck     = cmn.Bck{
			Name:     testBucketName + "_new",
			Provider: cmn.ProviderAIS,
		}
	)

	m.saveClusterState()
	m.expectTargets(1)

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

	args := api.XactReqArgs{ID: uuid, Kind: cmn.ActRenameLB, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
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

func TestRenameBucketNonEmpty(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	var (
		m = ioContext{
			t:               t,
			num:             1000,
			numGetsEachFile: 2,
		}
		baseParams = tutils.BaseAPIParams()
		dstBck     = cmn.Bck{
			Name:     testBucketName + "_new",
			Provider: cmn.ProviderAIS,
		}
	)

	m.saveClusterState()
	m.proxyURL = tutils.RandomProxyURL(t)
	m.expectTargets(1)

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

	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActRenameLB, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
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

func TestRenameBucketAlreadyExistingDst(t *testing.T) {
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

	m.saveClusterState()
	m.expectTargets(1)

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
			Name:     testBucketName + "_new1",
			Provider: cmn.ProviderAIS,
		}
		dstBck2 = cmn.Bck{
			Name:     testBucketName + "_new2",
			Provider: cmn.ProviderAIS,
		}
	)

	m.saveClusterState()
	m.proxyURL = tutils.RandomProxyURL(t)
	m.expectTargets(1)

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
	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActRenameLB, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
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

func TestRenameBucketNonExistentSrc(t *testing.T) {
	var (
		m = ioContext{
			t: t,
		}
		baseParams = tutils.BaseAPIParams()
		dstBck     = cmn.Bck{
			Name:     cmn.RandString(10),
			Provider: cmn.ProviderAIS,
		}
		srcBcks = []cmn.Bck{
			{
				Name:     cmn.RandString(10),
				Provider: cmn.ProviderAIS,
			},
			{
				Name:     cmn.RandString(10),
				Provider: cmn.ProviderAmazon,
			},
		}
	)

	m.saveClusterState()
	m.expectTargets(1)

	for _, srcBck := range srcBcks {
		_, err := api.RenameBucket(baseParams, srcBck, dstBck)
		tutils.CheckErrIsNotFound(t, err)
		_, err = api.HeadBucket(baseParams, dstBck)
		tutils.CheckErrIsNotFound(t, err)
	}
}

func TestCopyBucket(t *testing.T) {
	tests := []struct {
		srcCloud         bool
		dstCloud         bool
		dstBckExist      bool // determines if destination bucket exists before copy or not
		dstBckHasObjects bool // determines if destination bucket contains any objects before copy or not
		multipleDests    bool // determines if there are multiple destinations to which objects are copied
	}{
		// ais -> ais
		{srcCloud: false, dstCloud: false, dstBckExist: false, dstBckHasObjects: false, multipleDests: false},
		{srcCloud: false, dstCloud: false, dstBckExist: true, dstBckHasObjects: false, multipleDests: false},
		{srcCloud: false, dstCloud: false, dstBckExist: true, dstBckHasObjects: true, multipleDests: false},
		{srcCloud: false, dstCloud: false, dstBckExist: false, dstBckHasObjects: false, multipleDests: true},
		{srcCloud: false, dstCloud: false, dstBckExist: true, dstBckHasObjects: true, multipleDests: true},

		// cloud -> ais
		{srcCloud: true, dstCloud: false, dstBckExist: false, dstBckHasObjects: false},
		{srcCloud: true, dstCloud: false, dstBckExist: true, dstBckHasObjects: false},
		{srcCloud: true, dstCloud: false, dstBckExist: true, dstBckHasObjects: true},
		{srcCloud: true, dstCloud: false, dstBckExist: false, dstBckHasObjects: false, multipleDests: true},
		{srcCloud: true, dstCloud: false, dstBckExist: true, dstBckHasObjects: true, multipleDests: true},

		// ais -> cloud
		{srcCloud: false, dstCloud: true, dstBckExist: true, dstBckHasObjects: false},
	}

	for _, test := range tests {
		// Bucket must exist when we require it to have objects.
		cmn.Assert(test.dstBckExist || !test.dstBckHasObjects)

		// We only have 1 cloud bucket available (cliBck), coping from the same bucket to the same bucket would fail.
		// TODO: remove this limitation and add cloud -> cloud test cases.
		cmn.Assert(!test.srcCloud || !test.dstCloud)

		testName := fmt.Sprintf("src-cloud=%t/dst-cloud=%t/", test.srcCloud, test.dstCloud)
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

				objCnt = 100
				srcm   = &ioContext{
					t:   t,
					num: objCnt,
					bck: cmn.Bck{
						Name:     "src_copy_bck",
						Provider: cmn.ProviderAIS,
					},
				}
				dstms = []*ioContext{
					{
						t:   t,
						num: objCnt,
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
					num: objCnt,
					bck: cmn.Bck{
						Name:     "dst_copy_bck_2",
						Provider: cmn.ProviderAIS,
					},
				})
			}
			bckTest := cmn.Bck{Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
			if test.srcCloud {
				srcm.bck = cliBck
				bckTest.Provider = cliBck.Provider
				tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: srcm.bck})
			}
			if test.dstCloud {
				dstms = []*ioContext{
					{
						t:   t,
						num: 0, // Make sure to not put anything new to destination cloud bucket
						bck: cliBck,
					},
				}
				tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: dstms[0].bck})
			}

			srcm.saveClusterState()
			srcm.expectTargets(1)

			for _, dstm := range dstms {
				dstm.init()

				if dstm.bck.IsCloud() {
					// Remove unnecessary local objects.
					tassert.CheckFatal(t, api.EvictCloudBucket(baseParams, dstm.bck))
				}
			}

			if bckTest.IsAIS() {
				tutils.CreateFreshBucket(t, srcm.proxyURL, srcm.bck)
				defer tutils.DestroyBucket(t, srcm.proxyURL, srcm.bck)
				srcm.setRandBucketProps()
			}

			if test.dstBckExist {
				for _, dstm := range dstms {
					if !dstm.bck.IsCloud() {
						tutils.CreateFreshBucket(t, dstm.proxyURL, dstm.bck)
						dstm.setRandBucketProps()
					}
				}
			} else { // cleanup
				for _, dstm := range dstms {
					if !dstm.bck.IsCloud() {
						tutils.DestroyBucket(t, dstm.proxyURL, dstm.bck)
					}
				}
			}

			defer func() {
				for _, dstm := range dstms {
					if !dstm.bck.IsCloud() {
						tutils.DestroyBucket(t, dstm.proxyURL, dstm.bck)
					} else {
						msg := &cmn.SelectMsg{Flags: cmn.SelectCached}
						entries, err := api.ListObjects(baseParams, dstm.bck, msg, 0)
						tassert.CheckFatal(t, err)
						for _, e := range entries.Entries {
							if err := api.DeleteObject(baseParams, dstm.bck, e.Name); err != nil {
								tutils.Logf("failed to delete object %s/%s\n", dstm.bck, e.Name)
							}
						}
					}
				}
			}()

			srcProps, err := api.HeadBucket(baseParams, srcm.bck)
			tassert.CheckFatal(t, err)

			if test.dstBckHasObjects {
				for _, dstm := range dstms {
					// Don't make PUTs to cloud bucket
					if !dstm.bck.IsCloud() {
						dstm.puts()
					}
				}
			}

			if bckTest.IsAIS() {
				srcm.puts()

				srcBckList, err = api.ListObjects(baseParams, srcm.bck, nil, 0)
				tassert.CheckFatal(t, err)
			} else if bckTest.IsCloud() {
				srcm.cloudPuts(false /*evict*/)
				defer srcm.del()

				srcBckList, err = api.ListObjects(baseParams, srcm.bck, nil, 0)
				tassert.CheckFatal(t, err)
			} else {
				panic(bckTest)
			}

			xactIDs := make([]string, len(dstms))
			for idx, dstm := range dstms {
				tutils.Logf("copying %s => %s\n", srcm.bck, dstm.bck)
				uuid, err := api.CopyBucket(baseParams, srcm.bck, dstm.bck)
				xactIDs[idx] = uuid
				tassert.CheckFatal(t, err)
			}

			for _, uuid := range xactIDs {
				args := api.XactReqArgs{ID: uuid, Kind: cmn.ActCopyBucket, Timeout: copyBucketTimeout}
				_, err = api.WaitForXaction(baseParams, args)
				tassert.CheckFatal(t, err)
			}

			for _, dstm := range dstms {
				if dstm.bck.IsCloud() {
					continue
				}

				tutils.Logf("checking and comparing bucket %s props\n", dstm.bck)
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

			for _, dstm := range dstms {
				tutils.Logf("checking and comparing objects of bucket %s\n", dstm.bck)
				expectedObjCount := srcm.num
				if test.dstBckHasObjects {
					expectedObjCount += dstm.num
				}

				_, err := api.HeadBucket(baseParams, srcm.bck)
				tassert.CheckFatal(t, err)

				msg := &cmn.SelectMsg{}
				if test.dstCloud {
					msg.Flags = cmn.SelectCached
				}
				dstBckList, err := api.ListObjects(baseParams, dstm.bck, msg, 0)
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

// TODO: make some of those long-only, after they are considered stable.
func TestCopyBucketSimple(t *testing.T) {
	var (
		srcBck = cmn.Bck{Name: "cpybck_src", Provider: cmn.ProviderAIS}

		m = &ioContext{
			t:         t,
			num:       1000,
			fileSize:  512,
			fixedSize: true,
			bck:       srcBck,
		}
	)

	if testing.Short() {
		m.num = 10
	}

	tutils.Logln("Preparing a source bucket")
	tutils.CreateFreshBucket(t, proxyURL, srcBck)
	defer tutils.DestroyBucket(t, proxyURL, srcBck)
	m.init()

	tutils.Logln("Putting objects to the source bucket")
	m.puts()

	t.Run("Stats", func(t *testing.T) { testCopyBucketStats(t, srcBck, m) })
	t.Run("Prefix", func(t *testing.T) { testCopyBucketPrefix(t, srcBck, m) })
	t.Run("Abort", func(t *testing.T) { testCopyBucketAbort(t, srcBck, m) })
	t.Run("DryRun", func(t *testing.T) { testCopyBucketDryRun(t, srcBck, m) })
}

func testCopyBucketAbort(t *testing.T, srcBck cmn.Bck, m *ioContext) {
	dstBck := cmn.Bck{
		Name:     testBucketName + "_new1",
		Provider: cmn.ProviderAIS,
	}

	xactID, err := api.CopyBucket(baseParams, srcBck, dstBck)
	tassert.CheckError(t, err)
	defer tutils.DestroyBucket(t, m.proxyURL, dstBck)

	err = api.AbortXaction(baseParams, api.XactReqArgs{ID: xactID})
	tassert.CheckError(t, err)

	stats, err := api.GetXactionStatsByID(baseParams, xactID)
	tassert.CheckError(t, err)
	tassert.Errorf(t, stats.Aborted(), "failed to abort copy bucket (%s)", xactID)

	// TODO -- FIXME : Aborting a CopyBucket xaction should cleanup the bucket
	// Uncomment this part after the backend logic is ready

	/* // golint:ignore // should be uncommented after fixing backend
	// bck, err := api.ListBuckets(baseParams, cmn.QueryBcks(dstBck))
	// tassert.CheckError(t, err)
	// tassert.Errorf(t, !bck.Contains(cmn.QueryBcks(dstBck)), "should not contains bucket %s", dstBck)
	*/
}

func testCopyBucketStats(t *testing.T, srcBck cmn.Bck, m *ioContext) {
	dstBck := cmn.Bck{Name: "cpybck_dst", Provider: cmn.ProviderAIS}

	xactID, err := api.CopyBucket(baseParams, srcBck, dstBck)
	tassert.CheckFatal(t, err)
	defer tutils.DestroyBucket(t, proxyURL, dstBck)

	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActCopyBucket, Timeout: time.Minute}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	stats, err := api.GetXactionStatsByID(baseParams, xactID)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, stats.ObjCount() == int64(m.num), "Stats expected to return %d objects in total", m.num)
	expectedBytesCnt := int64(m.fileSize * uint64(m.num))
	tassert.Errorf(t, stats.BytesCount() == expectedBytesCnt, "Stats expected to return %d bytes, got %d",
		expectedBytesCnt, stats.BytesCount())
}

func testCopyBucketPrefix(t *testing.T, srcBck cmn.Bck, m *ioContext) {
	var (
		cpyPrefix = "cpyprefix" + cmn.RandString(5)
		dstBck    = cmn.Bck{Name: "cpybck_dst", Provider: cmn.ProviderAIS}
	)

	xactID, err := api.CopyBucket(baseParams, srcBck, dstBck, &cmn.CopyBckMsg{Prefix: cpyPrefix})
	tassert.CheckFatal(t, err)
	defer tutils.DestroyBucket(t, proxyURL, dstBck)

	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActCopyBucket, Timeout: time.Minute}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	list, err := api.ListObjects(baseParams, dstBck, nil, 0)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(list.Entries) == m.num, "expected %d to be copied, got %d", m.num, len(list.Entries))
	for _, e := range list.Entries {
		tassert.Fatalf(t, strings.HasPrefix(e.Name, cpyPrefix), "expected %q to have prefix %q", e.Name, cpyPrefix)
	}
}

func testCopyBucketDryRun(t *testing.T, srcBck cmn.Bck, m *ioContext) {
	dstBck := cmn.Bck{Name: "cpybck_dst" + cmn.RandString(5), Provider: cmn.ProviderAIS}

	xactID, err := api.CopyBucket(baseParams, srcBck, dstBck, &cmn.CopyBckMsg{DryRun: true})
	tassert.CheckFatal(t, err)
	defer tutils.DestroyBucket(t, proxyURL, dstBck)

	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActCopyBucket, Timeout: time.Minute}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	stats, err := api.GetXactionStatsByID(baseParams, xactID)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, stats.ObjCount() == int64(m.num), "dry run stats expected to return %d objects", m.num)
	expectedBytesCnt := int64(m.fileSize * uint64(m.num))
	tassert.Errorf(t, stats.BytesCount() == expectedBytesCnt, "dry run stats expected to return %d bytes, got %d", expectedBytesCnt, stats.BytesCount())

	exists, err := api.DoesBucketExist(baseParams, cmn.QueryBcks(dstBck))
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, exists == false, "expected destination bucket to not be created")
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
			Name:     testBucketName + "_new1",
			Provider: cmn.ProviderAIS,
		}
		dstBck2 = cmn.Bck{
			Name:     testBucketName + "_new2",
			Provider: cmn.ProviderAIS,
		}
	)

	m.saveClusterState()
	m.expectTargets(1)

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
	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActCopyBucket, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
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
			Name:     testBucketName + "_new1",
			Provider: cmn.ProviderAIS,
		}
		dstBck2 = cmn.Bck{
			Name:     testBucketName + "_new2",
			Provider: cmn.ProviderAIS,
		}
	)

	m.saveClusterState()
	m.expectTargets(1)

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
	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActRenameLB, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
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
		cloudBck = cliBck
		aisBck   = cmn.Bck{
			Name:     cmn.RandString(10),
			Provider: cmn.ProviderAIS,
		}
		m = ioContext{
			t:      t,
			num:    10,
			bck:    cloudBck,
			prefix: t.Name(),
		}

		proxyURL   = tutils.RandomProxyURL(t)
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

	msg := &cmn.SelectMsg{Prefix: m.prefix}
	cloudObjList, err := api.ListObjects(baseParams, cloudBck, msg, 0)
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
	msg.AddProps(cmn.GetPropsAll...)
	aisObjList, err := api.ListObjects(baseParams, aisBck, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(
		t, len(cloudObjList.Entries) == len(aisObjList.Entries),
		"object lists cloud vs ais does not match (got: %+v, expected: %+v)",
		aisObjList.Entries, cloudObjList.Entries,
	)

	// Check if cached listing works correctly.
	cacheMsg := &cmn.SelectMsg{Flags: cmn.SelectCached, Prefix: m.prefix}
	aisObjList, err = api.ListObjects(baseParams, aisBck, cacheMsg, 0)
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

	aisObjList, err = api.ListObjects(baseParams, aisBck, msg, 0)
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

func TestBucketListAndSummary(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	type test struct {
		provider string
		summary  bool
		cached   bool
		fast     bool // TODO: it makes sense only for summary
	}

	providers := []string{cmn.ProviderAIS}
	if cliBck.IsCloud() {
		providers = append(providers, cliBck.Provider)
	}

	var tests []test
	for _, provider := range providers {
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
				baseParams = tutils.BaseAPIParams()
			)
			bckTest := cmn.Bck{Provider: test.provider, Ns: cmn.NsGlobal}
			if !bckTest.IsAIS() {
				m.num = 123
			}
			cacheSize := m.num / 2 // determines number of objects which should be cached

			m.saveClusterState()
			m.expectTargets(2)

			expectedFiles := m.num
			if bckTest.IsAIS() {
				tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
				defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

				m.puts()
			} else if bckTest.IsCloud() {
				m.bck.Name = cliBck.Name

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

			if test.summary {
				msg := &cmn.BucketSummaryMsg{Cached: test.cached, Fast: test.fast}
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
				msg := &cmn.SelectMsg{}
				if test.cached {
					msg.Flags = cmn.SelectCached
				}
				objList, err := api.ListObjects(baseParams, m.bck, msg, 0)
				tassert.CheckFatal(t, err)

				if len(objList.Entries) != expectedFiles {
					t.Errorf("number of listed objects (%d) is different than expected (%d)", len(objList.Entries), expectedFiles)
				}
			}
		})
	}
}
