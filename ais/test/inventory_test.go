// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
)

// TODO -- FIXME:
// - support multi-target - remove Skip(MaxTargets == 1)
// - if bck.IsRemoteAIS() m.num *= 100
// - support ais://

//
// create inventory
//

func TestCreateInventorySimple(t *testing.T) {
	var (
		m = &ioContext{
			t:      t,
			num:    20, // given PageSize (below)
			bck:    cliBck,
			prefix: t.Name(),
		}
		bp = tools.BaseAPIParams()
	)
	tools.CheckSkip(t, &tools.SkipTestArgs{MaxTargets: 1, RemoteBck: true, Bck: m.bck})

	m.init(true /*cleanup*/)
	m.remotePuts(true /*evict*/)

	msg := &apc.CreateInvMsg{
		LsoMsg: apc.LsoMsg{
			Prefix:   m.prefix,
			Props:    apc.GetPropsName,
			PageSize: 3, // forces multiple pages
		},
	}

	xid, err := api.CreateBucketInventory(bp, m.bck, msg)
	tassert.CheckFatal(t, err)

	tlog.Logfln("%s[%s] started", apc.ActCreateNBI, xid)

	args := xact.ArgsMsg{ID: xid, Kind: apc.ActCreateNBI, Timeout: tools.ListRemoteBucketTimeout}
	_, err = api.WaitForXactionIC(bp, &args)
	tassert.CheckFatal(t, err)
}

func TestCreateInventoryPermuteOnDisk(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MaxTargets: 1, RemoteBck: true, Bck: cliBck})

	type test struct {
		num           int
		pageSize      int64
		pagesPerChunk int64
		name          string
	}
	tests := []test{
		{num: 5, pageSize: 10, pagesPerChunk: 1, name: "few-objects-large-page"},
		{num: 20, pageSize: 3, pagesPerChunk: 10, name: "multi-page-one-chunk"},
		{num: 50, pageSize: 3, pagesPerChunk: 2, name: "multi-page-multi-chunk"},
		{num: 12, pageSize: 4, pagesPerChunk: 1, name: "exact-page-boundary"},
		{num: 40, pageSize: 2, pagesPerChunk: 2, name: "small-pages-many-chunks"},
		{num: 30, pageSize: 5, pagesPerChunk: 3, name: "medium-pages-medium-chunks"},
	}

	sysBck := meta.SysBckInv().Clone()

	proxyURL := tools.GetPrimaryURL()
	initMountpaths(t, proxyURL)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				m = &ioContext{
					t:      t,
					num:    tc.num,
					bck:    cliBck,
					prefix: t.Name(),
				}
				bp = tools.BaseAPIParams()
			)
			m.remotePuts(true /*evict*/)

			msg := &apc.CreateInvMsg{
				LsoMsg: apc.LsoMsg{
					Prefix:   m.prefix,
					Props:    apc.GetPropsName,
					PageSize: tc.pageSize,
				},
				PagesPerChunk: tc.pagesPerChunk,
			}

			xid, err := api.CreateBucketInventory(bp, m.bck, msg)
			tassert.CheckFatal(t, err)

			tlog.Logfln("%s[%s] started (%s: num=%d pageSize=%d ppc=%d)",
				apc.ActCreateNBI, xid, tc.name, tc.num, tc.pageSize, tc.pagesPerChunk)

			args := xact.ArgsMsg{ID: xid, Kind: apc.ActCreateNBI, Timeout: tools.ListRemoteBucketTimeout}
			_, err = api.WaitForXactionIC(bp, &args)
			tassert.CheckFatal(t, err)

			// expected chunks: ceil(num / (pageSize * pagesPerChunk))
			entriesPerChunk := tc.pageSize * tc.pagesPerChunk
			expectedChunks := int((int64(tc.num) + entriesPerChunk - 1) / entriesPerChunk)

			tlog.Logfln("expecting %d chunk(s)", expectedChunks)

			// validate chunks on disk (local deployments only)
			if expectedChunks > 1 {
				srcUname := string(m.bck.MakeUname(""))
				invObjName := srcUname + xid

				m.validateChunksOnDisk(sysBck, invObjName, expectedChunks)
			}
		})
	}
}

//
// create and list inventory
//

func TestListInventory(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MaxTargets: 1, RemoteBck: true, Bck: cliBck})

	type test struct {
		name             string
		num              int
		pageSize         int64 // CreateInvMsg.PageSize (inventory creation)
		pagesPerChunk    int64
		maxEntriesPerChk int64
		listPageSize     int64  // LsoMsg.PageSize (listing)
		props            string // used for both create and list
		invName          string
	}
	tests := []test{
		// A x B: vary chunk granularity vs list page size
		{
			name: "small-chunks-small-pages", num: 30,
			pageSize: 3, pagesPerChunk: 2, listPageSize: 4,
			props: apc.GetPropsName, invName: "inv-sc-sp-" + cos.GenTie(),
		},
		{
			name: "small-chunks-large-pages", num: 30,
			pageSize: 3, pagesPerChunk: 2, listPageSize: 50,
			props: apc.GetPropsName, invName: "inv-sc-lp-" + cos.GenTie(),
		},
		{
			name: "large-chunks-small-pages", num: 40,
			pageSize: 5, pagesPerChunk: 10, listPageSize: 3,
			props: apc.GetPropsName, invName: "inv-lc-sp-" + cos.GenTie(),
		},
		{
			name: "one-chunk-multi-pages", num: 20,
			pageSize: 4, pagesPerChunk: 10, listPageSize: 7,
			props: apc.GetPropsName, invName: "inv-1c-mp-" + cos.GenTie(),
		},
		// B: MaxEntriesPerChunk
		{
			name: "max-entries-per-chunk", num: 25,
			pageSize: 10, pagesPerChunk: 50, maxEntriesPerChk: 8, listPageSize: 5,
			props: apc.GetPropsName, invName: "inv-maxent-" + cos.GenTie(),
		},
		// C: props coverage
		{
			name: "props-name-size", num: 15,
			pageSize: 5, pagesPerChunk: 3, listPageSize: 6,
			props: apc.GetPropsNameSize, invName: "inv-ns-" + cos.GenTie(),
		},
		{
			name: "props-all", num: 15,
			pageSize: 5, pagesPerChunk: 3, listPageSize: 6,
			props: strings.Join(apc.GetPropsAll, apc.LsPropsSepa), invName: "inv-all-" + cos.GenTie(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if cliBck.IsRemoteAIS() {
				a := max(rand.IntN(100), 10)
				tc.num *= a
				tc.pageSize *= int64(a)
				tc.pagesPerChunk = min(tc.pagesPerChunk*int64(a), apc.MaxInvPagesPerChunk)
				tc.maxEntriesPerChk *= int64(a)
				tc.listPageSize *= int64(a)
			}
			var (
				m = &ioContext{
					t:      t,
					num:    tc.num,
					bck:    cliBck,
					prefix: "subdir-" + cos.GenTie() + "/",
				}
				bp = tools.BaseAPIParams()
			)
			m.init(true /*cleanup*/)
			m.remotePuts(true /*evict*/)

			// 1. create inventory
			createMsg := &apc.CreateInvMsg{
				Name: tc.invName,
				LsoMsg: apc.LsoMsg{
					Prefix:   m.prefix,
					Props:    tc.props,
					PageSize: tc.pageSize,
				},
				PagesPerChunk:      tc.pagesPerChunk,
				MaxEntriesPerChunk: tc.maxEntriesPerChk,
			}

			xid, err := api.CreateBucketInventory(bp, m.bck, createMsg)
			tassert.CheckFatal(t, err)

			tlog.Logfln("%s[%s] started (%s: num=%d invPage=%d ppc=%d maxEnt=%d listPage=%d props=%q)",
				apc.ActCreateNBI, xid, tc.name, tc.num, tc.pageSize,
				tc.pagesPerChunk, tc.maxEntriesPerChk, tc.listPageSize, tc.props)

			wargs := xact.ArgsMsg{ID: xid, Kind: apc.ActCreateNBI, Timeout: tools.ListRemoteBucketTimeout}
			_, err = api.WaitForXactionIC(bp, &wargs)
			tassert.CheckFatal(t, err)

			// 2. list inventory (paginated)
			var (
				allEntries []*cmn.LsoEnt
				token      string
				numPages   int
			)
			for {
				lsmsg := &apc.LsoMsg{
					Prefix:            m.prefix,
					Props:             tc.props,
					PageSize:          tc.listPageSize,
					Flags:             apc.LsNBI,
					ContinuationToken: token,
				}
				args := api.ListArgs{
					Header: http.Header{apc.HdrInvName: []string{createMsg.Name}},
				}
				lst, err := api.ListObjects(bp, m.bck, lsmsg, args)
				tassert.CheckFatal(t, err)

				numPages++
				allEntries = append(allEntries, lst.Entries...)

				if lst.ContinuationToken == "" {
					break
				}
				token = lst.ContinuationToken

				// sanity: each page must respect page size
				tassert.Fatalf(t, len(lst.Entries) <= int(tc.listPageSize),
					"page too large: got %d, max %d", len(lst.Entries), tc.listPageSize)
			}

			// 3. validate entry count
			tassert.Fatalf(t, len(allEntries) == tc.num,
				"expected %d entries, got %d", tc.num, len(allEntries))

			// 4. validate sorted order and no duplicates
			for i := 1; i < len(allEntries); i++ {
				prev, curr := allEntries[i-1].Name, allEntries[i].Name
				tassert.Fatalf(t, prev < curr, "entries not sorted or duplicate at [%d]: %q >= %q", i, prev, curr)
			}

			// 5. validate names match what was PUT
			listedNames := make(cos.StrSet, len(allEntries))
			for _, e := range allEntries {
				listedNames.Set(e.Name)
			}
			for _, name := range m.objNames {
				tassert.Fatalf(t, listedNames.Contains(name),
					"PUT'd object %q not found in inventory listing", name)
			}

			// 6. validate props
			if tc.props != apc.GetPropsName {
				for _, e := range allEntries {
					tassert.Fatalf(t, e.Size > 0,
						"expected non-zero size for %q", e.Name)
				}
			}

			tlog.Logfln("listed %d entries in %d page(s) - OK", len(allEntries), numPages)
		})
	}
}

// prefix filtering over NBI
func TestListInventoryPrefix(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MaxTargets: 1, RemoteBck: true, Bck: cliBck})

	var (
		parent = "pfx-" + cos.GenTie() + "/"
		m1     = &ioContext{
			t:      t,
			num:    20,
			bck:    cliBck,
			prefix: parent + "aaa/",
		}
		m2 = &ioContext{
			t:      t,
			num:    15,
			bck:    cliBck,
			prefix: parent + "bbb/",
		}
		bp = tools.BaseAPIParams()
	)
	m1.init(true /*cleanup*/)
	m1.remotePuts(true /*evict*/)

	m2.init(true /*cleanup*/)
	m2.remotePuts(true /*evict*/)

	// create inventory covering both sub-prefixes
	createMsg := &apc.CreateInvMsg{
		Name: "inv-prefix-" + cos.GenTie(),
		LsoMsg: apc.LsoMsg{
			Prefix:   parent,
			Props:    apc.GetPropsName,
			PageSize: 5,
		},
		PagesPerChunk: 2,
	}

	xid, err := api.CreateBucketInventory(bp, m1.bck, createMsg)
	tassert.CheckFatal(t, err)

	wargs := xact.ArgsMsg{ID: xid, Kind: apc.ActCreateNBI, Timeout: tools.ListRemoteBucketTimeout}
	_, err = api.WaitForXactionIC(bp, &wargs)
	tassert.CheckFatal(t, err)

	// list with each sub-prefix
	for _, tc := range []struct {
		prefix string
		num    int
		names  []string
	}{
		{m1.prefix, m1.num, m1.objNames},
		{m2.prefix, m2.num, m2.objNames},
	} {
		var (
			allEntries []*cmn.LsoEnt
			token      string
			numPages   int
		)
		for {
			lsmsg := &apc.LsoMsg{
				Prefix:            tc.prefix,
				Props:             apc.GetPropsName,
				PageSize:          3,
				Flags:             apc.LsNBI,
				ContinuationToken: token,
			}
			args := api.ListArgs{
				Header: http.Header{apc.HdrInvName: []string{createMsg.Name}},
			}
			lst, err := api.ListObjects(bp, m1.bck, lsmsg, args)
			tassert.CheckFatal(t, err)

			numPages++
			allEntries = append(allEntries, lst.Entries...)
			if lst.ContinuationToken == "" {
				break
			}
			token = lst.ContinuationToken
		}

		tassert.Fatalf(t, len(allEntries) == tc.num,
			"prefix %q: expected %d, got %d", tc.prefix, tc.num, len(allEntries))

		for _, e := range allEntries {
			tassert.Fatalf(t, strings.HasPrefix(e.Name, tc.prefix),
				"entry %q doesn't match prefix %q", e.Name, tc.prefix)
		}

		tlog.Logfln("prefix %q: listed %d out of %d in %d page(s) - OK",
			tc.prefix, len(allEntries), m1.num+m2.num, numPages)
	}
}
