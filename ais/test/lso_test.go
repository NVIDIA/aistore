// Package integration_test.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"fmt"
	"math/rand/v2"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

func TestLso(t *testing.T) {
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
			Provider: apc.AIS,
			Ns:       genBucketNs(),
		}
		wg = &sync.WaitGroup{}

		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	if testing.Short() {
		iterations = 3
	}

	tests := []struct {
		pageSize int64
	}{
		{pageSize: 0},
		{pageSize: 2000},
		{pageSize: rand.Int64N(15000)},
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

			tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

			p := bck.DefaultProps(initialClusterConfig)

			totalObjects := 0
			for iter := 1; iter <= iterations; iter++ {
				tlog.Logfln("listing iteration: %d/%d (total_objs: %d)", iter, iterations, totalObjects)
				objectCount := rand.IntN(800) + 1010
				totalObjects += objectCount
				for wid := range workerCount {
					wg.Add(1)
					go func(wid int) {
						defer wg.Done()
						objectSize := int64(rand.IntN(256) + 20)
						objDir := tools.RandomObjDir(dirLen, 5)
						objectsToPut := objectCount / workerCount
						if wid == workerCount-1 { // last worker puts leftovers
							objectsToPut += objectCount % workerCount
						}
						objNames := tools.PutRR(t, baseParams, objectSize, p.Cksum.Type, bck, objDir, objectsToPut)
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
				msg := &apc.LsoMsg{PageSize: test.pageSize}
				msg.AddProps(apc.GetPropsChecksum, apc.GetPropsAtime, apc.GetPropsVersion, apc.GetPropsCopies, apc.GetPropsSize)
				lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)

				if lst.ContinuationToken != "" {
					t.Errorf("continuation token was unexpectedly set to: %s", lst.ContinuationToken)
				}

				empty := &cmn.LsoEnt{}
				for _, en := range lst.Entries {
					e, exists := objs.Load(en.Name)
					if !exists {
						t.Errorf("failed to locate %s", bck.Cname(en.Name))
						continue
					}

					obj := e.(objEntry)
					if obj.size != en.Size {
						t.Errorf(
							"sizes do not match for object %s, expected: %d, got: %d",
							obj.name, obj.size, en.Size,
						)
					}

					if en.Version == empty.Version {
						t.Errorf("%s version is empty (not set)", bck.Cname(en.Name))
					} else if en.Checksum == empty.Checksum ||
						en.Atime == empty.Atime ||
						en.Flags == empty.Flags ||
						en.Copies == empty.Copies {
						t.Errorf("some fields of %s are empty (not set): %#v", bck.Cname(en.Name), en)
					}
				}

				// Check if names in the entries are unique.
				objs.Range(func(key, _ any) bool {
					objName := key.(string)
					i := sort.Search(len(lst.Entries), func(i int) bool {
						return lst.Entries[i].Name >= objName
					})
					if i == len(lst.Entries) || lst.Entries[i].Name != objName {
						t.Errorf("object %s was not found in the result of bucket listing", objName)
					}
					return true
				})

				if len(lst.Entries) != totalObjects {
					t.Fatalf("actual objects %d, expected: %d", len(lst.Entries), totalObjects)
				}

				// Check listing bucket with predefined prefix.
				prefixes.Range(func(key, value any) bool {
					prefix := key.(string)
					expectedObjCount := value.(int)

					msg := &apc.LsoMsg{
						Prefix: prefix,
					}
					lst, err = api.ListObjects(baseParams, bck, msg, api.ListArgs{})
					tassert.CheckFatal(t, err)

					if expectedObjCount != len(lst.Entries) {
						t.Errorf(
							"(prefix: %s), actual objects %d, expected: %d",
							prefix, len(lst.Entries), expectedObjCount,
						)
					}

					for _, en := range lst.Entries {
						if !strings.HasPrefix(en.Name, prefix) {
							t.Errorf("object %q does not have expected prefix: %q", en.Name, prefix)
						}
					}
					return true
				})
			}
		})
	}
}

func TestLsoRemoteBucketVersions(t *testing.T) {
	var (
		m = ioContext{
			t:        t,
			bck:      cliBck,
			num:      50,
			fileSize: 128,
			prefix:   trand.String(6) + "-",
		}
		baseParams = tools.BaseAPIParams()
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, RemoteBck: true, Bck: m.bck})

	m.init(true /*cleanup*/)

	p, err := api.HeadBucket(baseParams, m.bck, false /* don't add */)
	tassert.CheckFatal(t, err)

	if !p.Versioning.Enabled {
		t.Skipf("%s requires a remote bucket with enabled versioning", t.Name())
	}

	m.puts()

	tlog.Logfln("Listing %q objects", m.bck.String())
	msg := &apc.LsoMsg{Prefix: m.prefix}
	msg.AddProps(apc.GetPropsVersion, apc.GetPropsSize)
	bckObjs, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)

	tlog.Logfln("Checking %q object versions [total: %d]", m.bck.String(), len(bckObjs.Entries))
	for _, en := range bckObjs.Entries {
		tassert.Errorf(t, en.Size != 0, "object %s does not have size", en.Name)
		tassert.Errorf(t, en.Version != "", "object %s does not have version", en.Name)
	}
}

// Minimalistic list objects test to check that everything works correctly.
func TestLsoSmoke(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			baseParams = tools.BaseAPIParams()
			m          = ioContext{
				t:                   t,
				num:                 100,
				bck:                 bck.Clone(),
				deleteRemoteBckObjs: true,
				fileSize:            5 * cos.KiB,
			}

			iters = 5
			msg   = &apc.LsoMsg{PageSize: 10}
		)

		m.init(true /*cleanup*/)
		m.puts()

		// Run couple iterations to see that we get deterministic results.
		tlog.Logfln("run %d list objects iterations", iters)
		for iter := range iters {
			lst, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tassert.Errorf(
				t, len(lst.Entries) == m.num,
				"unexpected number of entries (got: %d, expected: %d) on iter: %d",
				len(lst.Entries), m.num, iter,
			)
		}
	})
}

func TestLsoGoBack(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			baseParams = tools.BaseAPIParams()
			m          = ioContext{
				t:        t,
				num:      2000,
				bck:      bck.Clone(),
				fileSize: 128,
			}

			msg = &apc.LsoMsg{PageSize: 50}
		)

		if !bck.IsAIS() {
			m.num = 300
		}

		m.init(true /*cleanup*/)
		m.puts()
		if m.bck.IsRemote() {
			defer m.del()
		}
		var (
			tokens          []string
			entries         cmn.LsoEntries
			expectedEntries cmn.LsoEntries
		)
		tlog.Logln("listing couple pages to move iterator on targets")
		for range m.num / int(msg.PageSize) {
			tokens = append(tokens, msg.ContinuationToken)
			objPage, err := api.ListObjectsPage(baseParams, m.bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			expectedEntries = append(expectedEntries, objPage.Entries...)
		}

		tlog.Logln("list bucket's content in reverse order")

		for i := len(tokens) - 1; i >= 0; i-- {
			msg.ContinuationToken = tokens[i]
			objPage, err := api.ListObjectsPage(baseParams, m.bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			entries = append(entries, objPage.Entries...)
		}

		cmn.SortLso(entries)
		cmn.SortLso(expectedEntries)

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
				"unexpected en (got: %q, expected: %q)",
				entries[idx], expectedEntries[idx],
			)
		}
	})
}

func TestLsoRerequestPage(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			baseParams = tools.BaseAPIParams()
			m          = ioContext{
				t:                   t,
				bck:                 bck.Clone(),
				deleteRemoteBckObjs: true,
				num:                 500,
				fileSize:            128,
			}
			rerequests = 5
		)

		if !bck.IsAIS() {
			m.num = 50
		}

		m.init(true /*cleanup*/)
		m.puts()
		if m.bck.IsRemote() {
			defer m.del()
		}
		var (
			err error
			lst *cmn.LsoRes

			totalCnt = 0
			msg      = &apc.LsoMsg{PageSize: 10}
		)
		tlog.Logln("starting rerequesting routine...")
		for {
			prevToken := msg.ContinuationToken
			for range rerequests {
				msg.ContinuationToken = prevToken
				lst, err = api.ListObjectsPage(baseParams, m.bck, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)
			}
			totalCnt += len(lst.Entries)
			if lst.ContinuationToken == "" {
				break
			}
		}
		tassert.Fatalf(
			t, totalCnt == m.num,
			"unexpected total number of objects (got: %d, expected: %d)", totalCnt, m.num,
		)
	})
}

func TestLsoStartAfter(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			baseParams = tools.BaseAPIParams()
			m          = ioContext{
				t:        t,
				num:      200,
				bck:      bck.Clone(),
				fileSize: 128,
			}
		)

		if !bck.IsAIS() {
			m.num = 20
		}

		m.init(true /*cleanup*/)
		m.puts()
		if m.bck.IsRemote() {
			defer m.del()
		}
		lst, err := api.ListObjects(baseParams, m.bck, nil, api.ListArgs{})
		tassert.CheckFatal(t, err)

		middleObjName := lst.Entries[m.num/2-1].Name
		tlog.Logfln("start listing bucket after: %q...", middleObjName)

		msg := &apc.LsoMsg{PageSize: 10, StartAfter: middleObjName}
		lst, err = api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})

		switch {
		case bck.IsAIS():
			tassert.CheckFatal(t, err)
			tassert.Errorf(
				t, len(lst.Entries) == m.num/2,
				"unexpected number of entries (got: %d, expected: %d)",
				len(lst.Entries), m.num/2,
			)
		case err != nil:
			herr := cmn.AsErrHTTP(err)
			tlog.Logfln("Error is expected here, got %q", herr)
		default:
			tassert.Errorf(t, false, "expected an error, got nil")
		}
	})
}

func TestLsoProps(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			baseParams = tools.BaseAPIParams()
			m          = ioContext{
				t:                   t,
				num:                 rand.IntN(5000) + 1000,
				bck:                 bck.Clone(),
				fileSize:            128,
				deleteRemoteBckObjs: true,
			}
			remoteVersioning bool
		)

		if !bck.IsAIS() {
			m.num = rand.IntN(250) + 100
		}

		m.init(true /*cleanup*/)
		m.puts()
		if m.bck.IsRemote() {
			defer m.del()

			s := "disabled"
			p, err := api.HeadBucket(baseParams, m.bck, false /* don't add */)
			tassert.CheckFatal(t, err)
			if remoteVersioning = p.Versioning.Enabled; remoteVersioning {
				s = "enabled"
			}
			tlog.Logfln("%s: versioning is %s", m.bck.Cname(""), s)
		}
		checkProps := func(props []string, f func(en *cmn.LsoEnt)) {
			msg := &apc.LsoMsg{PageSize: 100}
			msg.AddProps(props...)
			lst, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tassert.Errorf(
				t, len(lst.Entries) == m.num,
				"unexpected number of entries (got: %d, expected: %d)", len(lst.Entries), m.num,
			)
			for _, en := range lst.Entries {
				tassert.Errorf(t, en.Name != "", "name is not set")
				f(en)
			}
		}

		tlog.Logfln("trying empty (minimal) subset of props...")
		checkProps([]string{}, func(en *cmn.LsoEnt) {
			tassert.Errorf(t, en.Name != "", "name is not set")
			tassert.Errorf(t, en.Size != 0, "size is not set")

			tassert.Errorf(t, en.Atime == "", "atime is set")
			tassert.Errorf(t, en.Location == "", "target location is set %q", en.Location)
			tassert.Errorf(t, en.Copies == 0, "copies is set")
		})

		tlog.Logfln("trying ais-default subset of props...")
		checkProps(apc.GetPropsDefaultAIS, func(en *cmn.LsoEnt) {
			tassert.Errorf(t, en.Size != 0, "size is not set")
			tassert.Errorf(t, en.Checksum != "", "checksum is not set")
			tassert.Errorf(t, en.Atime != "", "atime is not set")

			tassert.Errorf(t, en.Location == "", "target location is set %q", en.Location)
			tassert.Errorf(t, en.Copies == 0, "copies is set")
		})

		tlog.Logfln("trying cloud-default subset of props...")
		checkProps(apc.GetPropsDefaultCloud, func(en *cmn.LsoEnt) {
			tassert.Errorf(t, en.Size != 0, "size is not set")
			tassert.Errorf(t, en.Checksum != "", "checksum is not set")
			if bck.IsAIS() || remoteVersioning {
				tassert.Errorf(t, en.Version != "", "version is not set")
			}
			tassert.Errorf(t, !m.bck.IsCloud() || en.Custom != "", "custom is not set")

			tassert.Errorf(t, en.Atime == "", "atime is set")
			tassert.Errorf(t, en.Copies == 0, "copies is set")
		})

		tlog.Logfln("trying specific subset of props...")
		checkProps(
			[]string{apc.GetPropsChecksum, apc.GetPropsVersion, apc.GetPropsCopies}, func(en *cmn.LsoEnt) {
				tassert.Errorf(t, en.Checksum != "", "checksum is not set")
				if bck.IsAIS() || remoteVersioning {
					tassert.Error(t, en.Version != "", "version is not set: "+m.bck.Cname(en.Name))
				}
				tassert.Error(t, en.Copies > 0, "copies is not set")

				tassert.Error(t, en.Atime == "", "atime is set")
				tassert.Errorf(t, en.Location == "", "target location is set %q", en.Location)
			})

		tlog.Logfln("trying small subset of props...")
		checkProps([]string{apc.GetPropsSize}, func(en *cmn.LsoEnt) {
			tassert.Errorf(t, en.Size != 0, "size is not set")

			tassert.Errorf(t, en.Atime == "", "atime is set")
			tassert.Errorf(t, en.Location == "", "target location is set %q", en.Location)
			tassert.Errorf(t, en.Copies == 0, "copies is set")
		})

		tlog.Logfln("trying all props...")
		checkProps(apc.GetPropsAll, func(en *cmn.LsoEnt) {
			tassert.Errorf(t, en.Size != 0, "size is not set")
			if bck.IsAIS() || remoteVersioning {
				tassert.Error(t, en.Version != "", "version is not set: "+m.bck.Cname(en.Name))
			}
			tassert.Errorf(t, en.Checksum != "", "checksum is not set")
			tassert.Errorf(t, en.Atime != "", "atime is not set")
			tassert.Errorf(t, en.Location != "", "target location is not set [%#v]", en)
			tassert.Errorf(t, en.Copies != 0, "copies is not set")
		})
	})
}

// Runs remote list objects with `cached == true` (for both evicted and not evicted objects).
func TestLsoRemoteCached(t *testing.T) {
	var (
		baseParams = tools.BaseAPIParams()
		m          = ioContext{
			t:        t,
			bck:      cliBck,
			num:      rand.IntN(100) + 10,
			fileSize: 128,
		}

		remoteVersioning bool
		s                = "disabled"
	)
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})

	p, err := api.HeadBucket(baseParams, m.bck, false /* don't add */)
	tassert.CheckFatal(t, err)
	if remoteVersioning = p.Versioning.Enabled; remoteVersioning {
		s = "enabled"
	}
	tlog.Logfln("%s: versioning is %s", m.bck.Cname(""), s)

	m.init(true /*cleanup*/)

	for _, evict := range []bool{false, true} {
		tlog.Logfln("list remote objects with evict=%t", evict)
		m.remotePuts(evict)

		msg := &apc.LsoMsg{PageSize: 10, Flags: apc.LsCached}
		msg.AddProps(apc.GetPropsDefaultAIS...)
		msg.AddProps(apc.GetPropsVersion)

		lst, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
		tassert.CheckFatal(t, err)
		if evict {
			tassert.Errorf(
				t, len(lst.Entries) == 0,
				"unexpected number of entries (got: %d, expected: 0)", len(lst.Entries),
			)
		} else {
			tassert.Errorf(
				t, len(lst.Entries) == m.num,
				"unexpected number of entries (got: %d, expected: %d)", len(lst.Entries), m.num,
			)
			for _, en := range lst.Entries {
				tassert.Errorf(t, en.Name != "", "name is not set")
				tassert.Errorf(t, en.Size != 0, "size is not set")
				tassert.Errorf(t, en.Checksum != "", "checksum is not set")
				tassert.Errorf(t, en.Atime != "", "atime is not set")
				if remoteVersioning {
					tassert.Errorf(t, en.Version != "", "version is not set")
				}
				tassert.Errorf(t, en.Location == "", "target location is set %q", en.Location)
				tassert.Errorf(t, en.Copies == 0, "copies is set")
			}
		}
	}
}

// Runs standard list objects but selects new random proxy every page.
func TestLsoRandProxy(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			m = ioContext{
				t:                   t,
				bck:                 bck.Clone(),
				num:                 rand.IntN(5000) + 1000,
				fileSize:            5 * cos.KiB,
				deleteRemoteBckObjs: true,
			}

			totalCnt = 0
			msg      = &apc.LsoMsg{PageSize: 100}
		)

		if !bck.IsAIS() {
			m.num = rand.IntN(300) + 100
		}

		m.init(true /*cleanup*/)
		m.puts()
		if m.bck.IsRemote() {
			defer m.del()
		}
		for {
			baseParams := tools.BaseAPIParams()
			lst, err := api.ListObjectsPage(baseParams, m.bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			totalCnt += len(lst.Entries)
			if lst.ContinuationToken == "" {
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
func TestLsoRandPageSize(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			totalCnt   int
			baseParams = tools.BaseAPIParams()
			m          = ioContext{
				t:        t,
				bck:      bck.Clone(),
				num:      rand.IntN(5000) + 1000,
				fileSize: 128,
			}
			msg = &apc.LsoMsg{Flags: apc.LsCached}
		)

		if !bck.IsAIS() {
			m.num = rand.IntN(200) + 100
		}

		m.init(true /*cleanup*/)
		m.puts()
		if m.bck.IsRemote() {
			defer m.del()
		}
		for {
			msg.PageSize = rand.Int64N(50) + 50

			lst, err := api.ListObjectsPage(baseParams, m.bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			totalCnt += len(lst.Entries)
			if lst.ContinuationToken == "" {
				break
			}
			tassert.Errorf(t, len(lst.Entries) == int(msg.PageSize), "wrong page size %d (expected %d)",
				len(lst.Entries), msg.PageSize,
			)
		}
		tassert.Fatalf(
			t, totalCnt == m.num,
			"unexpected total number of objects (got: %d, expected: %d)", totalCnt, m.num,
		)
	})
}

func TestLsoPrefix(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	providers := []string{apc.AIS}
	if cliBck.IsRemote() {
		providers = append(providers, cliBck.Provider)
	}

	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			const objCnt = 30
			var (
				customPage = true
				bck        cmn.Bck
			)
			bckTest := cmn.Bck{Provider: provider, Ns: cmn.NsGlobal}
			if bckTest.IsRemote() {
				bck = cliBck

				tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: bck})

				bckProp, err := api.HeadBucket(baseParams, bck, false /* don't add */)
				tassert.CheckFatal(t, err)
				customPage = bckProp.Provider != apc.Azure

				tlog.Logfln("Cleaning up the remote bucket %s", bck.String())
				lst, err := api.ListObjects(baseParams, bck, nil, api.ListArgs{})
				tassert.CheckFatal(t, err)
				for _, en := range lst.Entries {
					err := tools.Del(proxyURL, bck, en.Name, nil, nil, false /*silent*/)
					tassert.CheckFatal(t, err)
				}
			} else {
				bck = cmn.Bck{Name: testBucketName, Provider: provider}
				tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
			}

			objNames := make([]string, 0, objCnt)

			t.Cleanup(func() {
				for _, objName := range objNames {
					err := tools.Del(proxyURL, bck, objName, nil, nil, true /*silent*/)
					tassert.CheckError(t, err)
				}
			})

			for i := range objCnt {
				objName := fmt.Sprintf("prefix/obj%d", i+1)
				objNames = append(objNames, objName)

				r, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: fileSize, CksumType: cos.ChecksumNone})
				_, err := api.PutObject(&api.PutArgs{
					BaseParams: baseParams,
					Bck:        bck,
					ObjName:    objName,
					Reader:     r,
					Size:       fileSize,
				})
				tassert.CheckFatal(t, err)
			}

			tests := []struct {
				name     string
				prefix   string
				pageSize int64
				limit    int64
				expected int
			}{
				{
					"full_list_default_pageSize_no_limit",
					"prefix", 0, 0,
					objCnt,
				},
				{
					"full_list_small_pageSize_no_limit",
					"prefix", objCnt / 7, 0,
					objCnt,
				},
				{
					"full_list_limited",
					"prefix", 0, 8,
					8,
				},
				{
					"full_list_prefixed",
					"prefix/obj1", 0, 0,
					11, // obj1 and obj10..obj19
				},
				{
					"full_list_overlimited_prefixed",
					"prefix/obj1", 0, 20,
					11, // obj1 and obj10..obj19
				},
				{
					"full_list_limited_prefixed",
					"prefix/obj1", 0, 2,
					2, // obj1 and obj10
				},
				{
					"empty_list_prefixed",
					"prefix/nothing", 0, 0,
					0,
				},
			}

			for _, test := range tests {
				if test.pageSize != 0 && !customPage {
					tlog.Logfln("Bucket %s does not support custom paging, skipping...", bck.String())
					continue
				}
				t.Run(test.name, func(t *testing.T) {
					tlog.Logfln("Prefix: %q, Expected objects: %d", test.prefix, test.expected)
					msg := &apc.LsoMsg{PageSize: test.pageSize, Prefix: test.prefix}
					tlog.Logf(
						"list_objects %s [prefix: %q, page_size: %d]\n",
						bck.String(), msg.Prefix, msg.PageSize,
					)

					lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{Limit: test.limit})
					tassert.CheckFatal(t, err)

					tlog.Logfln("list_objects output: %d objects", len(lst.Entries))

					if len(lst.Entries) != test.expected {
						t.Errorf("returned %d objects instead of %d", len(lst.Entries), test.expected)
					}
				})
			}
		})
	}
}

func TestLsoCache(t *testing.T) {
	var (
		baseParams = tools.BaseAPIParams()
		m          = ioContext{
			t:        t,
			num:      rand.IntN(3000) + 1481,
			fileSize: cos.KiB,
		}
		totalIters = 10
	)

	if testing.Short() {
		m.num = 250 + rand.IntN(500)
		totalIters = 5
	}

	m.init(true /*cleanup*/)

	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)
	m.puts()

	// Do it N times - first: fill the cache; next calls: use it.
	for iter := range totalIters {
		var (
			started = time.Now()
			msg     = &apc.LsoMsg{PageSize: rand.Int64N(20) + 4}
		)
		lst, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{})
		tassert.CheckFatal(t, err)

		tlog.Logf(
			"[iter: %d] page_size: %d, time: %s\n",
			iter, msg.PageSize, time.Since(started),
		)

		tassert.Errorf(
			t, len(lst.Entries) == m.num,
			"unexpected number of entries (got: %d, expected: %d)", len(lst.Entries), m.num,
		)
	}
}

//
// with rebalance -------------------------------------------------
//

func TestLsoWithRebalance(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		baseParams = tools.BaseAPIParams()
		wg         = &sync.WaitGroup{}
		m          = ioContext{
			t:        t,
			num:      10000,
			fileSize: 128,
		}
		rebID string
	)

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(2)

	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

	target := m.startMaintenanceNoRebalance()

	m.puts()

	wg.Add(1)
	go func() {
		defer wg.Done()
		rebID = m.stopMaintenance(target)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 15 {
			tlog.Logfln("listing all objects, iter: %d", i)
			lst, err := api.ListObjects(baseParams, m.bck, nil, api.ListArgs{})
			tassert.CheckFatal(t, err)
			if lst.Flags == 0 {
				tassert.Errorf(t, len(lst.Entries) == m.num, "entries mismatch (%d vs %d)", len(lst.Entries), m.num)
			} else if len(lst.Entries) != m.num {
				tlog.Logfln("List objects while rebalancing: %d vs %d", len(lst.Entries), m.num)
			}

			time.Sleep(time.Second)
		}
	}()

	wg.Wait()
	m.waitAndCheckCluState()
	tools.WaitForRebalanceByID(t, baseParams, rebID)
}

//
// with location
//

func TestLsoLocalGetLocation(t *testing.T) {
	var (
		m = ioContext{
			t:         t,
			num:       1000,
			fileSize:  cos.KiB,
			fixedSize: true,
		}

		targets    = make(map[string]struct{})
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		smap       = tools.GetClusterMap(t, proxyURL)
	)

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(1)

	tools.CreateBucket(t, proxyURL, m.bck, nil, true /*cleanup*/)

	m.puts()

	msg := &apc.LsoMsg{Props: apc.GetPropsLocation}
	lst, err := api.ListObjects(baseParams, m.bck, msg, api.ListArgs{Limit: int64(m.num)})
	tassert.CheckFatal(t, err)

	if len(lst.Entries) != m.num {
		t.Errorf("Expected %d bucket list entries, found %d\n", m.num, len(lst.Entries))
	}

	j := 10
	if len(lst.Entries) >= 200 {
		j = 100
	}
	for i, e := range lst.Entries {
		if e.Location == "" {
			t.Fatalf("[%#v]: location is empty", e)
		}
		tname, _ := core.ParseObjLoc(e.Location)
		tid := meta.N2ID(tname)
		targets[tid] = struct{}{}
		tsi := smap.GetTarget(tid)
		url := tsi.URL(cmn.NetPublic)
		baseParams := tools.BaseAPIParams(url)

		oah, err := api.GetObject(baseParams, m.bck, e.Name, nil)
		tassert.CheckFatal(t, err)
		if uint64(oah.Size()) != m.fileSize {
			t.Errorf("Expected filesize: %d, actual filesize: %d\n", m.fileSize, oah.Size())
		}

		if i%j == 0 {
			if i == 0 {
				tlog.Logln("Modifying config to enforce intra-cluster access, expecting errors...\n")
			}
			tools.SetClusterConfig(t, cos.StrKVs{"features": feat.EnforceIntraClusterAccess.String()})
			t.Cleanup(func() {
				tools.SetClusterConfig(t, cos.StrKVs{"features": "0"})
			})

			_, err = api.GetObject(baseParams, m.bck, e.Name, nil)
			if err == nil {
				tlog.Logln("Warning: expected error, got nil")
			}
			tools.SetClusterConfig(t, cos.StrKVs{"features": "0"})
		}
	}

	if smap.CountActiveTs() != len(targets) { // The objects should have been distributed to all targets
		t.Errorf("Expected %d different target URLs, actual: %d different target URLs",
			smap.CountActiveTs(), len(targets))
	}

	// Ensure no target URLs are returned when the property is not requested
	msg.Props = ""
	lst, err = api.ListObjects(baseParams, m.bck, msg, api.ListArgs{Limit: int64(m.num)})
	tassert.CheckFatal(t, err)

	if len(lst.Entries) != m.num {
		t.Errorf("Expected %d bucket list entries, found %d\n", m.num, len(lst.Entries))
	}

	for _, e := range lst.Entries {
		if e.Location != "" {
			t.Fatalf("[%#v]: location expected to be empty\n", e)
		}
	}
}

func TestLsoCloudGetLocation(t *testing.T) {
	var (
		m = ioContext{
			t:         t,
			bck:       cliBck,
			num:       100,
			fileSize:  cos.KiB,
			fixedSize: true,
		}
		targets    = make(map[string]struct{})
		bck        = cliBck
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		smap       = tools.GetClusterMap(t, proxyURL)
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: bck})

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(2)

	m.puts()

	listObjectsMsg := &apc.LsoMsg{Props: apc.GetPropsLocation, Flags: apc.LsCached}
	lst, err := api.ListObjects(baseParams, bck, listObjectsMsg, api.ListArgs{})
	tassert.CheckFatal(t, err)

	if len(lst.Entries) < m.num {
		t.Errorf("Bucket %s has %d objects, expected %d", m.bck.String(), len(lst.Entries), m.num)
	}
	j := 10
	if len(lst.Entries) >= 200 {
		j = 100
	}
	for i, e := range lst.Entries {
		if e.Location == "" {
			t.Fatalf("[%#v]: location is empty", e)
		}
		tmp := strings.Split(e.Location, apc.LocationPropSepa)
		tid := meta.N2ID(tmp[0])
		targets[tid] = struct{}{}
		tsi := smap.GetTarget(tid)
		url := tsi.URL(cmn.NetPublic)
		baseParams := tools.BaseAPIParams(url)

		oah, err := api.GetObject(baseParams, bck, e.Name, nil)
		tassert.CheckFatal(t, err)
		if uint64(oah.Size()) != m.fileSize {
			t.Errorf("Expected fileSize: %d, actual fileSize: %d\n", m.fileSize, oah.Size())
		}

		if i%j == 0 {
			if i == 0 {
				tlog.Logln("Modifying config to enforce intra-cluster access, expecting errors...\n")
			}
			tools.SetClusterConfig(t, cos.StrKVs{"features": feat.EnforceIntraClusterAccess.String()})
			_, err = api.GetObject(baseParams, m.bck, e.Name, nil)

			if err == nil {
				tlog.Logln("Warning: expected error, got nil")
			}

			tools.SetClusterConfig(t, cos.StrKVs{"features": "0"})
		}
	}

	// The objects should have been distributed to all targets
	if m.originalTargetCount != len(targets) {
		t.Errorf("Expected %d different target URLs, actual: %d different target URLs", m.originalTargetCount, len(targets))
	}

	// Ensure no target URLs are returned when the property is not requested
	listObjectsMsg.Props = ""
	lst, err = api.ListObjects(baseParams, bck, listObjectsMsg, api.ListArgs{})
	tassert.CheckFatal(t, err)

	if len(lst.Entries) != m.num {
		t.Errorf("Expected %d bucket list entries, found %d\n", m.num, len(lst.Entries))
	}

	for _, e := range lst.Entries {
		if e.Location != "" {
			t.Fatalf("[%#v]: location expected to be empty\n", e)
		}
	}
}

//
// no recursion --------------------------------------------------------------------
//

func TestLsoNoRecursion(t *testing.T) {
	type test struct {
		prefix string
		count  int
	}
	var (
		bck = cmn.Bck{
			Name:     t.Name() + "Bucket",
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		objs       = []string{
			"img001", "vid001",
			"img-test/obj1", "img-test/vid1", "img-test/pics/obj01",
			"img003", "img-test/pics/vid01"}
		tests = []test{
			{prefix: "", count: 4},         // img001, vid001, img003, img-test/ (dir)
			{prefix: "img-test", count: 3}, // obj1, vid1, pics/ (dir)
			{prefix: "img-test/", count: 3},
			{prefix: "img-test/pics", count: 2},  // obj01, vid01 (no subdirs)
			{prefix: "img-test/pics/", count: 2}, // obj01, vid01 (no subdirs)
		}
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	for _, nm := range objs {
		objectSize := int64(rand.IntN(256) + 20)
		reader, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: objectSize, CksumType: cos.ChecksumNone})
		_, err := api.PutObject(&api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    nm,
			Reader:     reader,
		})
		tassert.CheckFatal(t, err)
	}

	msg := &apc.LsoMsg{Props: apc.GetPropsName}
	lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == len(objs), "Invalid number of objects %d vs %d", len(lst.Entries), len(objs))

	for idx, tst := range tests {
		msg := &apc.LsoMsg{Flags: apc.LsNoRecursion | apc.LsNameSize, Prefix: tst.prefix}
		lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
		tassert.CheckFatal(t, err)

		if tst.count != len(lst.Entries) {
			tlog.Logfln("Failed test #%d (prefix %s). Expected %d, got %d",
				idx, tst.prefix, tst.count, len(lst.Entries))
			for idx, en := range lst.Entries {
				tlog.Logfln("%d. %s (%v)", idx, en.Name, en.Flags)
			}
			tassert.Fatalf(t, false, "[%s] expected %d entries, got %d", tst.prefix, tst.count, len(lst.Entries))
		}
	}
}

// test non-recursive listing with pagination,
// specifically covering edge cases where directories and files interact with continuation tokens.
func TestLsoNoRecursionPagination(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	t.Run("MoreSubdirsThanPageSize", func(t *testing.T) {
		bck := cmn.Bck{
			Name:     "NonRecurs_" + path.Base(t.Name()) + cos.GenTie(),
			Provider: apc.AIS,
		}
		tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
		// Scenario: 5 subdirectories, 2 files, pageSize=3
		// Expected: Page1=[d1,d2,d3], Page2=[d4,d5,f1], Page3=[f2]
		// This tests that directories don't get duplicated across pages
		objs := []string{
			"prefix/d1/file1",
			"prefix/d2/file1",
			"prefix/d3/file1",
			"prefix/d4/file1",
			"prefix/d5/file1",
			"prefix/file1",
			"prefix/file2",
		}
		for _, nm := range objs {
			reader, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: 128, CksumType: cos.ChecksumNone})
			_, err := api.PutObject(&api.PutArgs{
				BaseParams: baseParams,
				Bck:        bck,
				ObjName:    nm,
				Reader:     reader,
			})
			tassert.CheckFatal(t, err)
		}

		// List with small page size
		msg := &apc.LsoMsg{
			Prefix:   "prefix/",
			Flags:    apc.LsNoRecursion,
			PageSize: 3,
		}

		var allEntries []*cmn.LsoEnt
		pageCount := 0
		for {
			lst, err := api.ListObjectsPage(baseParams, bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			allEntries = append(allEntries, lst.Entries...)
			pageCount++
			if lst.ContinuationToken == "" {
				break
			}
			msg.ContinuationToken = lst.ContinuationToken
		}

		// Should have 5 dirs + 2 files = 7 entries, no duplicates
		expectedCount := 7
		tassert.Fatalf(t, len(allEntries) == expectedCount,
			"expected %d entries, got %d", expectedCount, len(allEntries))

		// Check for duplicates and count dirs/files
		var dirCount, fileCount int
		seen := make(map[string]bool)
		for _, e := range allEntries {
			tassert.Fatalf(t, !seen[e.Name], "duplicate entry %q", e.Name)
			seen[e.Name] = true
			if e.IsAnyFlagSet(apc.EntryIsDir) {
				dirCount++
				// Directory entries must have trailing slash
				tassert.Fatalf(t, cos.IsLastB(e.Name, '/'),
					"directory entry %q missing trailing slash", e.Name)
			} else {
				fileCount++
				// File entries must NOT have trailing slash
				tassert.Fatalf(t, !cos.IsLastB(e.Name, '/'),
					"file entry %q should not have trailing slash", e.Name)
			}
		}
		tassert.Fatalf(t, dirCount == 5, "expected 5 directories, got %d", dirCount)
		tassert.Fatalf(t, fileCount == 2, "expected 2 files, got %d", fileCount)
	})

	t.Run("TokenFromFileDirsAlreadyShown", func(t *testing.T) {
		// Scenario: 1 subdirectory, 2 files, pageSize=3
		// Expected: Page1=[subdir,file1,file2], token=file2, Page2=[]
		// This tests that when token is from a file, directories don't reappear
		bck := cmn.Bck{
			Name:     "NonRecurs_" + path.Base(t.Name()) + cos.GenTie(),
			Provider: apc.AIS,
		}
		tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
		objs := []string{
			"test2/subdir/nested",
			"test2/aaa",
			"test2/zzz",
		}
		for _, nm := range objs {
			reader, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: 128, CksumType: cos.ChecksumNone})
			_, err := api.PutObject(&api.PutArgs{
				BaseParams: baseParams,
				Bck:        bck,
				ObjName:    nm,
				Reader:     reader,
			})
			tassert.CheckFatal(t, err)
		}

		msg := &apc.LsoMsg{
			Prefix:   "test2/",
			Flags:    apc.LsNoRecursion,
			PageSize: 3,
		}

		var allEntries []*cmn.LsoEnt
		for {
			lst, err := api.ListObjectsPage(baseParams, bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			allEntries = append(allEntries, lst.Entries...)
			if lst.ContinuationToken == "" {
				break
			}
			msg.ContinuationToken = lst.ContinuationToken
		}

		// Should have 1 dir + 2 files = 3 entries
		expectedCount := 3
		tassert.Fatalf(t, len(allEntries) == expectedCount,
			"expected %d entries, got %d", expectedCount, len(allEntries))

		// Check no duplicates and validate dir/file counts
		var dirCount, fileCount int
		seen := make(map[string]bool)
		for _, e := range allEntries {
			tassert.Fatalf(t, !seen[e.Name], "duplicate entry %q", e.Name)
			seen[e.Name] = true
			if e.IsAnyFlagSet(apc.EntryIsDir) {
				dirCount++
				// Directory entries must have trailing slash
				tassert.Fatalf(t, cos.IsLastB(e.Name, '/'),
					"directory entry %q missing trailing slash", e.Name)
			} else {
				fileCount++
				// File entries must NOT have trailing slash
				tassert.Fatalf(t, !cos.IsLastB(e.Name, '/'),
					"file entry %q should not have trailing slash", e.Name)
			}
		}
		tassert.Fatalf(t, dirCount == 1, "expected 1 directory, got %d", dirCount)
		tassert.Fatalf(t, fileCount == 2, "expected 2 files, got %d", fileCount)
	})

	t.Run("DeepNestingSmallPages", func(t *testing.T) {
		// Scenario: Deeply nested structure with small page size
		// Tests that nested directories are properly skipped and direct children shown
		bck := cmn.Bck{
			Name:     "NonRecurs_" + path.Base(t.Name()) + cos.GenTie(),
			Provider: apc.AIS,
		}
		tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
		objs := []string{
			"deep/a/file1",
			"deep/b/c/file1",
			"deep/b/c/d/file1",
			"deep/file1",
			"deep/file2",
			"deep/file3",
		}
		for _, nm := range objs {
			reader, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: 128, CksumType: cos.ChecksumNone})
			_, err := api.PutObject(&api.PutArgs{
				BaseParams: baseParams,
				Bck:        bck,
				ObjName:    nm,
				Reader:     reader,
			})
			tassert.CheckFatal(t, err)
		}

		msg := &apc.LsoMsg{
			Prefix:   "deep/",
			Flags:    apc.LsNoRecursion,
			PageSize: 2,
		}

		var allEntries []*cmn.LsoEnt
		for {
			lst, err := api.ListObjectsPage(baseParams, bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			allEntries = append(allEntries, lst.Entries...)
			if lst.ContinuationToken == "" {
				break
			}
			msg.ContinuationToken = lst.ContinuationToken
		}

		// Should have: deep/a/ (dir), deep/b/ (dir), deep/file1, deep/file2, deep/file3 = 5 entries
		expectedCount := 5
		tassert.Fatalf(t, len(allEntries) == expectedCount,
			"expected %d entries, got %d", expectedCount, len(allEntries))

		// Check no duplicates, verify expected entries, and count dirs/files
		expected := map[string]bool{
			"deep/a/": true, "deep/b/": true,
			"deep/file1": true, "deep/file2": true, "deep/file3": true,
		}
		var dirCount, fileCount int
		seen := make(map[string]bool)
		for _, e := range allEntries {
			tassert.Fatalf(t, !seen[e.Name], "duplicate entry %q", e.Name)
			seen[e.Name] = true
			tassert.Fatalf(t, expected[e.Name], "unexpected entry %q", e.Name)
			if e.IsAnyFlagSet(apc.EntryIsDir) {
				dirCount++
				// Directory entries must have trailing slash
				tassert.Fatalf(t, cos.IsLastB(e.Name, '/'),
					"directory entry %q missing trailing slash", e.Name)
			} else {
				fileCount++
				// File entries must NOT have trailing slash
				tassert.Fatalf(t, !cos.IsLastB(e.Name, '/'),
					"file entry %q should not have trailing slash", e.Name)
			}
		}
		tassert.Fatalf(t, dirCount == 2, "expected 2 directories, got %d", dirCount)
		tassert.Fatalf(t, fileCount == 3, "expected 3 files, got %d", fileCount)
	})

	// NOTE:
	// objname (e.g.) "aaa/bbb" will collide (`cos.ErrMv`) with a same-name
	// virtual directory when both land on the same mountpath
	t.Run("DirNameIsPrefixOfFileName", func(t *testing.T) {
		bck := cmn.Bck{
			Name:     "NonRecurs_" + path.Base(t.Name()) + cos.GenTie(),
			Provider: apc.AIS,
		}
		tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

		prefix := "dirprefix/"
		objs := []string{
			prefix + "aaa/nested",  // makes prefix+"aaa/" a directory
			prefix + "aaa111",      // file with name that starts with "aaa" but is NOT inside "aaa/"
			prefix + "aaa222",      // another sibling file with similar prefix
			prefix + "bbb/deep/f1", // makes prefix+"bbb/" a directory
			prefix + "bbb123",      // file with name that starts with "bbb" but is NOT inside "bbb/"
			prefix + "zzz",         // trailing file
		}
		for _, nm := range objs {
			reader, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: 128, CksumType: cos.ChecksumNone})
			_, err := api.PutObject(&api.PutArgs{
				BaseParams: baseParams,
				Bck:        bck,
				ObjName:    nm,
				Reader:     reader,
			})
			tassert.CheckFatal(t, err)
		}

		// page size of 1 to stress the continuation token mechanism
		msg := &apc.LsoMsg{
			Prefix:   prefix,
			Flags:    apc.LsNoRecursion,
			PageSize: 1,
		}

		var allEntries []*cmn.LsoEnt
		pageCount := 0
		for {
			lst, err := api.ListObjectsPage(baseParams, bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			allEntries = append(allEntries, lst.Entries...)
			pageCount++
			if lst.ContinuationToken == "" {
				break
			}
			msg.ContinuationToken = lst.ContinuationToken
		}

		// expected lexicographically:
		// 1. prefix+aaa/     (directory)
		// 2. prefix+aaa111   (file)
		// 3. prefix+aaa222   (file)
		// 4. prefix+bbb/     (directory)
		// 5. prefix+bbb123   (file)
		// 6. prefix+zzz      (file)
		expectedCount := 6
		tassert.Fatalf(t, len(allEntries) == expectedCount,
			"expected %d entries, got %d", expectedCount, len(allEntries))

		seen := make(map[string]bool, expectedCount)
		for _, e := range allEntries {
			tassert.Fatalf(t, !seen[e.Name], "duplicate entry %q", e.Name)
			seen[e.Name] = true
		}

		expectedEntries := map[string]bool{
			prefix + "aaa/":   true,
			prefix + "aaa111": false,
			prefix + "aaa222": false,
			prefix + "bbb/":   true,
			prefix + "bbb123": false,
			prefix + "zzz":    false,
		}

		for _, e := range allEntries {
			expectedIsDir, exists := expectedEntries[e.Name]
			tassert.Fatalf(t, exists, "unexpected entry %q", e.Name)

			isDir := e.IsAnyFlagSet(apc.EntryIsDir)
			tassert.Fatalf(t, isDir == expectedIsDir,
				"entry %q: expected isDir=%v, got isDir=%v", e.Name, expectedIsDir, isDir)

			if isDir {
				tassert.Fatalf(t, cos.IsLastB(e.Name, '/'),
					"directory entry %q missing trailing slash", e.Name)
			} else {
				tassert.Fatalf(t, !cos.IsLastB(e.Name, '/'),
					"file entry %q should not have trailing slash", e.Name)
			}
		}

		for i := 1; i < len(allEntries); i++ {
			tassert.Fatalf(t, allEntries[i-1].Name < allEntries[i].Name,
				"entries not in lexicographical order: %q >= %q",
				allEntries[i-1].Name, allEntries[i].Name)
		}

		tlog.Logf("DirNameIsPrefixOfFileName: %d pages for %d entries\n", pageCount, len(allEntries))
	})
}

// test non-recursive listing with a larger dataset
// using default page size, ensuring correctness at scale with complex directory structures.
func TestLsoNoRecursionLargeScale(t *testing.T) {
	var (
		bck = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	// Create a complex directory structure with more entries than default page size (10000):
	// - 5500 top-level directories (dir00001-dir05500), each with 1 file
	// - 5500 top-level files (file00001-file05500)
	// Total: 11000 objects, expecting 11000 entries at root level (5500 dirs + 5500 files)
	const (
		numDirs  = 5500
		numFiles = 5500
	)
	var objs = make([]string, 0, numDirs+numFiles)

	// Create directories with nested files
	for i := 1; i <= numDirs; i++ {
		dirName := fmt.Sprintf("dir%05d", i)
		objs = append(objs, dirName+"/file")
	}

	// Create top-level files
	for i := 1; i <= numFiles; i++ {
		objs = append(objs, fmt.Sprintf("file%05d", i))
	}

	tlog.Logf("Creating %d objects...\n", len(objs))
	for _, nm := range objs {
		reader, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: 32, CksumType: cos.ChecksumNone})
		_, err := api.PutObject(&api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    nm,
			Reader:     reader,
		})
		tassert.CheckFatal(t, err)
	}

	t.Run("DefaultPageSizeRootLevel", func(t *testing.T) {
		// List with default page size (0 = apc.MaxPageSizeAIS = 10000)
		// We have 11000 entries, so this should require multiple pages
		msg := &apc.LsoMsg{
			Flags: apc.LsNoRecursion,
		}

		var allEntries []*cmn.LsoEnt
		pageCount := 0
		for {
			lst, err := api.ListObjectsPage(baseParams, bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			allEntries = append(allEntries, lst.Entries...)
			pageCount++
			if lst.ContinuationToken == "" {
				break
			}
			msg.ContinuationToken = lst.ContinuationToken
		}

		tlog.Logf("DefaultPageSizeRootLevel: %d pages, %d total entries\n", pageCount, len(allEntries))

		// Should have 5500 directories + 5500 files = 11000 entries
		expectedCount := numDirs + numFiles
		tassert.Fatalf(t, len(allEntries) == expectedCount,
			"expected %d entries, got %d", expectedCount, len(allEntries))

		// Should require at least 2 pages (11000 / 10000 = 2 pages)
		tassert.Fatalf(t, pageCount >= 2, "expected at least 2 pages, got %d", pageCount)

		// Verify no duplicates and count dirs/files
		var dirCount, fileCount int
		seen := make(map[string]bool)
		for _, e := range allEntries {
			tassert.Fatalf(t, !seen[e.Name], "duplicate entry %q", e.Name)
			seen[e.Name] = true

			if e.IsAnyFlagSet(apc.EntryIsDir) {
				dirCount++
				// Directory entries must have trailing slash
				tassert.Fatalf(t, cos.IsLastB(e.Name, '/'),
					"directory entry %q missing trailing slash", e.Name)
			} else {
				fileCount++
				// File entries must NOT have trailing slash
				tassert.Fatalf(t, !cos.IsLastB(e.Name, '/'),
					"file entry %q should not have trailing slash", e.Name)
			}
		}
		tassert.Fatalf(t, dirCount == numDirs, "expected %d directories, got %d", numDirs, dirCount)
		tassert.Fatalf(t, fileCount == numFiles, "expected %d files, got %d", numFiles, fileCount)
	})

	t.Run("SmallPageSizeManyPages", func(t *testing.T) {
		// Use very small page size to force many pagination iterations
		const (
			pageSize = 77 // Prime number to create uneven pages
		)
		msg := &apc.LsoMsg{
			Flags:    apc.LsNoRecursion,
			PageSize: pageSize,
		}

		var allEntries []*cmn.LsoEnt
		pageCount := 0
		for {
			lst, err := api.ListObjectsPage(baseParams, bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			allEntries = append(allEntries, lst.Entries...)
			pageCount++
			if lst.ContinuationToken == "" {
				break
			}
			msg.ContinuationToken = lst.ContinuationToken
		}

		tlog.Logf("SmallPageSizeManyPages: %d pages, %d total entries\n", pageCount, len(allEntries))

		// Should have 5500 directories + 5500 files = 11000 entries
		expectedCount := numDirs + numFiles
		tassert.Fatalf(t, len(allEntries) == expectedCount,
			"expected %d entries, got %d", expectedCount, len(allEntries))

		// Should have taken multiple pages
		expectedMinPages := expectedCount / pageSize // at least 142 pages
		tassert.Fatalf(t, pageCount >= expectedMinPages,
			"expected at least %d pages, got %d", expectedMinPages, pageCount)

		// Verify no duplicates and count dirs/files
		var dirCount, fileCount int
		seen := make(map[string]bool)
		for _, e := range allEntries {
			tassert.Fatalf(t, !seen[e.Name], "duplicate entry %q", e.Name)
			seen[e.Name] = true

			if e.IsAnyFlagSet(apc.EntryIsDir) {
				dirCount++
				// Directory entries must have trailing slash
				tassert.Fatalf(t, cos.IsLastB(e.Name, '/'),
					"directory entry %q missing trailing slash", e.Name)
			} else {
				fileCount++
				// File entries must NOT have trailing slash
				tassert.Fatalf(t, !cos.IsLastB(e.Name, '/'),
					"file entry %q should not have trailing slash", e.Name)
			}
		}
		tassert.Fatalf(t, dirCount == numDirs, "expected %d directories, got %d", numDirs, dirCount)
		tassert.Fatalf(t, fileCount == numFiles, "expected %d files, got %d", numFiles, fileCount)
	})

	t.Run("SubdirectoryListing", func(t *testing.T) {
		// List inside a specific directory
		msg := &apc.LsoMsg{
			Prefix: "dir00025/",
			Flags:  apc.LsNoRecursion,
		}

		lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
		tassert.CheckFatal(t, err)

		// dir00025 has 1 file, no subdirectories
		tassert.Fatalf(t, len(lst.Entries) == 1, "expected 1 entry, got %d", len(lst.Entries))

		// The entry should be a file, not a directory
		tassert.Fatalf(t, !lst.Entries[0].IsAnyFlagSet(apc.EntryIsDir),
			"expected file, got directory %q", lst.Entries[0].Name)
	})

	t.Run("ConsistencyCheck", func(t *testing.T) {
		// Run the same query multiple times to ensure consistency
		msg := &apc.LsoMsg{
			Flags:    apc.LsNoRecursion,
			PageSize: 1000,
		}

		var firstRun []*cmn.LsoEnt
		for {
			lst, err := api.ListObjectsPage(baseParams, bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			firstRun = append(firstRun, lst.Entries...)
			if lst.ContinuationToken == "" {
				break
			}
			msg.ContinuationToken = lst.ContinuationToken
		}

		// Validate first run has expected counts
		expectedCount := numDirs + numFiles
		tassert.Fatalf(t, len(firstRun) == expectedCount,
			"first run: expected %d entries, got %d", expectedCount, len(firstRun))

		// Second run
		msg.ContinuationToken = ""
		var secondRun []*cmn.LsoEnt
		for {
			lst, err := api.ListObjectsPage(baseParams, bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			secondRun = append(secondRun, lst.Entries...)
			if lst.ContinuationToken == "" {
				break
			}
			msg.ContinuationToken = lst.ContinuationToken
		}

		// Both runs should produce identical results
		tassert.Fatalf(t, len(firstRun) == len(secondRun),
			"runs differ in length: %d vs %d", len(firstRun), len(secondRun))

		for i := range firstRun {
			tassert.Fatalf(t, firstRun[i].Name == secondRun[i].Name,
				"entry %d differs: %q vs %q", i, firstRun[i].Name, secondRun[i].Name)
		}

		// Validate counts and trailing slash convention
		var dirCount, fileCount int
		for _, e := range firstRun {
			if e.IsAnyFlagSet(apc.EntryIsDir) {
				dirCount++
				// Directory entries must have trailing slash
				tassert.Fatalf(t, cos.IsLastB(e.Name, '/'),
					"directory entry %q missing trailing slash", e.Name)
			} else {
				fileCount++
				// File entries must NOT have trailing slash
				tassert.Fatalf(t, !cos.IsLastB(e.Name, '/'),
					"file entry %q should not have trailing slash", e.Name)
			}
		}
		tassert.Fatalf(t, dirCount == numDirs, "expected %d directories, got %d", numDirs, dirCount)
		tassert.Fatalf(t, fileCount == numFiles, "expected %d files, got %d", numFiles, fileCount)
	})
}

// stress test non-recursive listing with randomized inputs:
// random page sizes, random directory names, and random file names.
func TestLsoNoRecursionRandom(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	// Use deterministic structure to avoid any path conflicts:
	// - Directories use "D_" prefix with sequential numbers
	// - Top-level files use "F_" prefix with sequential numbers
	// This guarantees no conflicts between files and directory paths.
	//
	// Note: The edge case where a file and directory share the same name
	// (e.g., "aaa" as file and "aaa/" as directory) is tested separately
	// in DirNameIsPrefixOfFileName subtest. That scenario has filesystem
	// constraints on local AIS buckets.
	numTopDirs := 50 + rand.IntN(51)  // 50-100
	numTopFiles := 50 + rand.IntN(51) // 50-100

	var (
		objs          = make([]string, 0, numTopDirs+numTopFiles) // all objects
		expectedDirs  = make(map[string]bool)                     // top-level dirs (with trailing slash)
		topLevelFiles = make(map[string]bool)                     // top-level files
		allDirInfos   = make([]struct{ name string }, 0, numTopDirs)
	)

	// Create directories: D_0001, D_0002, etc. with nested files
	for i := range numTopDirs {
		dirName := fmt.Sprintf("D_%04d", i)
		expectedDirs[dirName+"/"] = true
		allDirInfos = append(allDirInfos, struct{ name string }{dirName})

		// Add 1-3 files inside each directory
		numFiles := 1 + rand.IntN(3)
		for f := range numFiles {
			objs = append(objs, fmt.Sprintf("%s/file_%03d", dirName, f))
		}
	}

	// Create top-level files: F_0001, F_0002, etc.
	for i := range numTopFiles {
		fileName := fmt.Sprintf("F_%04d", i)
		topLevelFiles[fileName] = true
		objs = append(objs, fileName)
	}

	tlog.Logf("Creating %d objects (%d top-level dirs, %d top-level files)...\n",
		len(objs), len(expectedDirs), len(topLevelFiles))

	// Create all objects
	for _, nm := range objs {
		reader, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: 32, CksumType: cos.ChecksumNone})
		_, err := api.PutObject(&api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    nm,
			Reader:     reader,
		})
		tassert.CheckFatal(t, err)
	}

	expectedTotal := len(expectedDirs) + len(topLevelFiles)

	t.Run("RandomPageSizes", func(t *testing.T) {
		// Run multiple iterations with different random page sizes
		for iter := range 5 {
			pageSize := int64(1 + rand.IntN(50)) // Random page size 1-50
			msg := &apc.LsoMsg{
				Flags:    apc.LsNoRecursion,
				PageSize: pageSize,
			}

			var allEntries []*cmn.LsoEnt
			pageCount := 0
			for {
				lst, err := api.ListObjectsPage(baseParams, bck, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)
				allEntries = append(allEntries, lst.Entries...)
				pageCount++
				if lst.ContinuationToken == "" {
					break
				}
				msg.ContinuationToken = lst.ContinuationToken
			}

			tlog.Logf("Iter %d: pageSize=%d, pages=%d, entries=%d\n", iter, pageSize, pageCount, len(allEntries))

			// Verify count
			tassert.Fatalf(t, len(allEntries) == expectedTotal,
				"iter %d: expected %d entries, got %d", iter, expectedTotal, len(allEntries))

			// Verify no duplicates and correct types
			seen := make(map[string]bool)
			var dirCount, fileCount int
			for _, e := range allEntries {
				tassert.Fatalf(t, !seen[e.Name], "iter %d: duplicate entry %q", iter, e.Name)
				seen[e.Name] = true

				if e.IsAnyFlagSet(apc.EntryIsDir) {
					dirCount++
					tassert.Fatalf(t, cos.IsLastB(e.Name, '/'),
						"iter %d: directory %q missing trailing slash", iter, e.Name)
					tassert.Fatalf(t, expectedDirs[e.Name],
						"iter %d: unexpected directory %q", iter, e.Name)
				} else {
					fileCount++
					tassert.Fatalf(t, !cos.IsLastB(e.Name, '/'),
						"iter %d: file %q has trailing slash", iter, e.Name)
					tassert.Fatalf(t, topLevelFiles[e.Name],
						"iter %d: unexpected file %q", iter, e.Name)
				}
			}
			tassert.Fatalf(t, dirCount == len(expectedDirs),
				"iter %d: expected %d dirs, got %d", iter, len(expectedDirs), dirCount)
			tassert.Fatalf(t, fileCount == len(topLevelFiles),
				"iter %d: expected %d files, got %d", iter, len(topLevelFiles), fileCount)

			// Verify lexicographical order
			for i := 1; i < len(allEntries); i++ {
				tassert.Fatalf(t, allEntries[i-1].Name < allEntries[i].Name,
					"iter %d: not in order: %q >= %q", iter, allEntries[i-1].Name, allEntries[i].Name)
			}
		}
	})

	t.Run("RandomPrefixes", func(t *testing.T) {
		// Test listing with random prefixes from existing directories
		for _, di := range allDirInfos[:min(10, len(allDirInfos))] {
			pageSize := int64(1 + rand.IntN(20)) // Random small page size
			msg := &apc.LsoMsg{
				Prefix:   di.name + "/",
				Flags:    apc.LsNoRecursion,
				PageSize: pageSize,
			}

			var allEntries []*cmn.LsoEnt
			for {
				lst, err := api.ListObjectsPage(baseParams, bck, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)
				allEntries = append(allEntries, lst.Entries...)
				if lst.ContinuationToken == "" {
					break
				}
				msg.ContinuationToken = lst.ContinuationToken
			}

			// Should have at least 1 entry (files or subdirs)
			tassert.Fatalf(t, len(allEntries) >= 1,
				"prefix %q: expected at least 1 entry, got %d", di.name+"/", len(allEntries))

			// Verify no duplicates and all entries have correct prefix
			seen := make(map[string]bool)
			for _, e := range allEntries {
				tassert.Fatalf(t, !seen[e.Name], "prefix %q: duplicate entry %q", di.name+"/", e.Name)
				seen[e.Name] = true
				tassert.Fatalf(t, strings.HasPrefix(e.Name, di.name+"/"),
					"prefix %q: entry %q doesn't have expected prefix", di.name+"/", e.Name)

				// Verify trailing slash convention
				if e.IsAnyFlagSet(apc.EntryIsDir) {
					tassert.Fatalf(t, cos.IsLastB(e.Name, '/'),
						"prefix %q: directory %q missing trailing slash", di.name+"/", e.Name)
				} else {
					tassert.Fatalf(t, !cos.IsLastB(e.Name, '/'),
						"prefix %q: file %q has trailing slash", di.name+"/", e.Name)
				}
			}

			// Verify lexicographical order
			for i := 1; i < len(allEntries); i++ {
				tassert.Fatalf(t, allEntries[i-1].Name < allEntries[i].Name,
					"prefix %q: not in order: %q >= %q", di.name+"/", allEntries[i-1].Name, allEntries[i].Name)
			}
		}
	})
}

func TestLsoNoRecursion_PageSizesUpTo1000(t *testing.T) {
	var (
		bck        = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	// Deterministic, mixed dataset:
	// - 40 top-level dirs (each with 2 nested files)
	// - 40 top-level files
	const (
		numDirs  = 40
		numFiles = 40
	)
	for i := range numDirs {
		dir := fmt.Sprintf("d%03d", i)
		for j := range 2 {
			nm := fmt.Sprintf("%s/f%02d", dir, j)
			reader, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: 16, CksumType: cos.ChecksumNone})
			_, err := api.PutObject(&api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: nm, Reader: reader})
			tassert.CheckFatal(t, err)
		}
	}
	for i := range numFiles {
		nm := fmt.Sprintf("f%03d", i)
		reader, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: 16, CksumType: cos.ChecksumNone})
		_, err := api.PutObject(&api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: nm, Reader: reader})
		tassert.CheckFatal(t, err)
	}

	expectedTotal := numDirs + numFiles

	// random page sizes in [1..1000]
	rng := cos.NowRand()
	pageSizes := []int64{1, 2, 3, 7, 31, 77, 128, 255, 512, 999, 1000}
	for range 10 {
		pageSizes = append(pageSizes, int64(1+rng.IntN(1000)))
	}

	for _, ps := range pageSizes {
		t.Run(fmt.Sprintf("pagesize_%d", ps), func(t *testing.T) {
			msg := &apc.LsoMsg{Flags: apc.LsNoRecursion, PageSize: ps}

			var (
				allEntries []*cmn.LsoEnt
				pageCount  int
			)
			for {
				lst, err := api.ListObjectsPage(baseParams, bck, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)

				allEntries = append(allEntries, lst.Entries...)
				pageCount++
				if lst.ContinuationToken == "" {
					break
				}
				msg.ContinuationToken = lst.ContinuationToken
			}

			tassert.Fatalf(t, len(allEntries) == expectedTotal,
				"pageSize=%d: expected %d entries, got %d (pages=%d)",
				ps, expectedTotal, len(allEntries), pageCount)

			seen := make(map[string]bool, expectedTotal)
			var dirCount, fileCount int
			for _, e := range allEntries {
				tassert.Fatalf(t, !seen[e.Name], "pageSize=%d: duplicate %q", ps, e.Name)
				seen[e.Name] = true

				if e.IsAnyFlagSet(apc.EntryIsDir) {
					dirCount++
					tassert.Fatalf(t, cos.IsLastB(e.Name, '/'), "dir %q missing trailing slash", e.Name)
				} else {
					fileCount++
					tassert.Fatalf(t, !cos.IsLastB(e.Name, '/'), "file %q must not have trailing slash", e.Name)
				}
			}
			tassert.Fatalf(t, dirCount == numDirs, "pageSize=%d: expected %d dirs, got %d", ps, numDirs, dirCount)
			tassert.Fatalf(t, fileCount == numFiles, "pageSize=%d: expected %d files, got %d", ps, numFiles, fileCount)

			// Must be lexicographical for token correctness.
			for i := 1; i < len(allEntries); i++ {
				tassert.Fatalf(t, allEntries[i-1].Name < allEntries[i].Name,
					"pageSize=%d: not lex-ordered: %q >= %q", ps, allEntries[i-1].Name, allEntries[i].Name)
			}
		})
	}
}

func TestLsoNoRecursion_NonExistingAndPartialPrefixes(t *testing.T) {
	var (
		bck        = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	// populate a few well-known names to probe prefix behavior
	objs := []string{
		"alpha/file1",
		"alpha/file2",
		"alpha/beta/x1",
		"alpha/beta/x2",
		"alpha2/top",
		"alphabet/soup",
		"zeta",
	}
	for _, nm := range objs {
		reader, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: 16, CksumType: cos.ChecksumNone})
		_, err := api.PutObject(&api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: nm, Reader: reader})
		tassert.CheckFatal(t, err)
	}

	type tc struct {
		name   string
		prefix string
		// expected entry names (dirs must include trailing slash)
		want map[string]bool
	}

	tests := []tc{
		{
			name:   "non_existing_prefix_no_matches",
			prefix: "does-not-exist/",
			want:   map[string]bool{}, // expect empty
		},
		{
			name: "partial_prefix_without_slash_matches_multiple_groups",

			// match alpha/, alpha2, alphabet/
			// it should still be lex-ordered; entries returned must start with "alp".
			prefix: "alp",

			// validate via invariants below
			want: nil,
		},
		{
			name:   "prefix_is_not_dir_boundary",
			prefix: "alpha/b", // matches alpha/beta/... but isn't "alpha/b/"
			want:   nil,
		},
		{
			name:   "existing_dir_prefix_with_slash",
			prefix: "alpha/",
			want: map[string]bool{
				"alpha/beta/": true,
				"alpha/file1": true,
				"alpha/file2": true,
			},
		},
		{
			name:   "existing_nested_dir_prefix_with_slash",
			prefix: "alpha/beta/",
			want: map[string]bool{
				"alpha/beta/x1": true,
				"alpha/beta/x2": true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &apc.LsoMsg{Prefix: tt.prefix, Flags: apc.LsNoRecursion, PageSize: 3}

			var allEntries []*cmn.LsoEnt
			for {
				lst, err := api.ListObjectsPage(baseParams, bck, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)
				allEntries = append(allEntries, lst.Entries...)
				if lst.ContinuationToken == "" {
					break
				}
				msg.ContinuationToken = lst.ContinuationToken
			}

			// check invariants: no dups, lex order, trailing slash for dirs, all names must have the prefix
			seen := make(map[string]bool)
			for i, e := range allEntries {
				tassert.Fatalf(t, strings.HasPrefix(e.Name, tt.prefix),
					"entry %d %q does not have prefix %q", i, e.Name, tt.prefix)

				tassert.Fatalf(t, !seen[e.Name], "duplicate %q", e.Name)
				seen[e.Name] = true

				if e.IsAnyFlagSet(apc.EntryIsDir) {
					tassert.Fatalf(t, cos.IsLastB(e.Name, '/'), "dir %q missing trailing slash", e.Name)
				} else {
					tassert.Fatalf(t, !cos.IsLastB(e.Name, '/'), "file %q must not have trailing slash", e.Name)
				}
			}
			for i := 1; i < len(allEntries); i++ {
				tassert.Fatalf(t, allEntries[i-1].Name < allEntries[i].Name,
					"not lex-ordered: %q >= %q", allEntries[i-1].Name, allEntries[i].Name)
			}

			// validate
			if tt.want != nil {
				tassert.Fatalf(t, len(allEntries) == len(tt.want),
					"expected %d entries, got %d", len(tt.want), len(allEntries))
				for _, e := range allEntries {
					tassert.Fatalf(t, tt.want[e.Name], "unexpected entry %q", e.Name)
				}
			}
		})
	}
}

func TestLsoNoRecursion_DirBoundaryVsUnderscoreCollision(t *testing.T) {
	var (
		bck        = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	// corner cases including:
	//   p/aaa/bbb/1
	//   p/aaa/bbb/2
	//   p/aaa/bbb_object
	objs := []string{
		"p/aaa/bbb/1",
		"p/aaa/bbb/2",
		"p/aaa/bbb_object_name",
		"p/aaa/bbb0",
		"p/aaa/ccc/1",
		"p/aaa/ccc_object",
	}
	for _, nm := range objs {
		reader, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: 16, CksumType: cos.ChecksumNone})
		_, err := api.PutObject(&api.PutArgs{BaseParams: baseParams, Bck: bck, ObjName: nm, Reader: reader})
		tassert.CheckFatal(t, err)
	}

	msg := &apc.LsoMsg{
		Prefix:   "p/aaa/",
		Flags:    apc.LsNoRecursion,
		PageSize: 1, // maximize token pressure + ordering dependency
	}

	var allEntries []*cmn.LsoEnt
	for {
		lst, err := api.ListObjectsPage(baseParams, bck, msg, api.ListArgs{})
		tassert.CheckFatal(t, err)
		allEntries = append(allEntries, lst.Entries...)
		if lst.ContinuationToken == "" {
			break
		}
		msg.ContinuationToken = lst.ContinuationToken
	}

	// expected:
	want := map[string]bool{
		"p/aaa/bbb/":            true,
		"p/aaa/ccc/":            true,
		"p/aaa/bbb0":            true,
		"p/aaa/bbb_object_name": true,
		"p/aaa/ccc_object":      true,
	}
	tassert.Fatalf(t, len(allEntries) == len(want), "expected %d entries, got %d", len(want), len(allEntries))

	seen := make(map[string]bool)
	for _, e := range allEntries {
		tassert.Fatalf(t, want[e.Name], "unexpected entry %q", e.Name)
		tassert.Fatalf(t, !seen[e.Name], "duplicate entry %q", e.Name)
		seen[e.Name] = true

		if e.IsAnyFlagSet(apc.EntryIsDir) {
			tassert.Fatalf(t, cos.IsLastB(e.Name, '/'), "dir %q missing trailing slash", e.Name)
		} else {
			tassert.Fatalf(t, !cos.IsLastB(e.Name, '/'), "file %q must not have trailing slash", e.Name)
		}
	}

	// lex order must hold
	for i := 1; i < len(allEntries); i++ {
		tassert.Fatalf(t, allEntries[i-1].Name < allEntries[i].Name,
			"not lex-ordered: %q >= %q", allEntries[i-1].Name, allEntries[i].Name)
	}
}
