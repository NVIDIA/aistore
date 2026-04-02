// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
)

//
// create inventory
//

const nbiCreateTimeout = 15 * time.Second

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

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})

	_nbiIniCln(t, m.bck)
	m.init(true /*cleanup*/)
	m.remotePuts(true /*evict*/)

	msg := &apc.CreateNBIMsg{
		LsoMsg: apc.LsoMsg{
			Prefix:   m.prefix,
			Props:    apc.GetPropsName,
			PageSize: 3, // forces multiple pages
		},
	}

	xid, err := api.CreateNBI(bp, m.bck, msg)
	tassert.CheckFatal(t, err)
	tlog.Logfln("%s[%s] started", apc.ActCreateNBI, xid)

	args := xact.ArgsMsg{ID: xid, Kind: apc.ActCreateNBI, Timeout: nbiCreateTimeout}
	_, err = api.WaitForXactionIC(bp, &args)
	tassert.CheckFatal(t, err)
}

//
// create and list inventory
//

func TestListInventory(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck})

	type test struct {
		name          string
		num           int
		pageSize      int64 // CreateNBIMsg.PageSize (inventory creation)
		namesPerChunk int64
		listPageSize  int64  // LsoMsg.PageSize (listing)
		props         string // used for both create and list
		invName       string
		smallBucket   bool
	}
	tests := []test{
		// A x B: vary chunk granularity vs list page size
		{
			name: "small-chunks-pages-empty-name", num: 30,
			pageSize: 3, namesPerChunk: 6, listPageSize: 4,
			props: apc.GetPropsName, invName: "",
		},
		{
			name: "small-chunks-small-pages", num: 30,
			pageSize: 3, namesPerChunk: 6, listPageSize: 4,
			props: apc.GetPropsName, invName: "inv-sc-sp-" + cos.GenTie(),
		},
		{
			name: "small-chunks-small-pages-small-bucket-empty-invName", num: 3,
			pageSize: 3, namesPerChunk: 6, listPageSize: 4,
			props:       apc.GetPropsName,
			smallBucket: true,
		},
		{
			name: "small-chunks-large-pages", num: 30,
			pageSize: 3, namesPerChunk: 6, listPageSize: 50,
			props: apc.GetPropsName, invName: "inv-sc-lp-" + cos.GenTie(),
		},
		{
			name: "large-chunks-small-pages", num: 40,
			pageSize: 5, namesPerChunk: 50, listPageSize: 3,
			props: apc.GetPropsName, invName: "inv-lc-sp-" + cos.GenTie(),
		},
		{
			name: "one-chunk-multi-pages", num: 20,
			pageSize: 4, namesPerChunk: 40, listPageSize: 7,
			props: apc.GetPropsName, invName: "inv-1c-mp-" + cos.GenTie(),
		},
		// B: NamesPerChunk (small, forces many chunks)
		{
			name: "names-per-chunk", num: 25,
			pageSize: 10, namesPerChunk: 8, listPageSize: 5,
			props: apc.GetPropsName, invName: "inv-maxent-" + cos.GenTie(),
		},
		// C: props coverage
		{
			name: "props-name-size", num: 15,
			pageSize: 5, namesPerChunk: 15, listPageSize: 6,
			props: apc.GetPropsNameSize, invName: "inv-ns-" + cos.GenTie(),
		},
		{
			name: "props-all", num: 15,
			pageSize: 5, namesPerChunk: 15, listPageSize: 6,
			props: strings.Join(apc.GetPropsAll, apc.LsPropsSepa), invName: "inv-all-" + cos.GenTie(),
		},
	}

	_nbiIniCln(t, cliBck)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if cliBck.IsRemoteAIS() && !tc.smallBucket {
				a := max(rand.IntN(100), 10)
				tc.num *= a
				tc.pageSize *= int64(a)
				tc.namesPerChunk *= int64(a)
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
			createMsg := &apc.CreateNBIMsg{
				Name: tc.invName,
				LsoMsg: apc.LsoMsg{
					Prefix:   m.prefix,
					Props:    tc.props,
					PageSize: tc.pageSize,
				},
				NamesPerChunk: tc.namesPerChunk,
			}

			xid, err := api.CreateNBI(bp, m.bck, createMsg)
			tassert.CheckFatal(t, err)
			tlog.Logfln("%s[%s] started (%s: num=%d invPage=%d npc=%d listPage=%d props=%q)",
				apc.ActCreateNBI, xid, tc.name, tc.num, tc.pageSize,
				tc.namesPerChunk, tc.listPageSize, tc.props)

			wargs := xact.ArgsMsg{ID: xid, Kind: apc.ActCreateNBI, Timeout: nbiCreateTimeout}
			_, err = api.WaitForXactionIC(bp, &wargs)
			tassert.CheckFatal(t, err)

			// 2. list inventory (paginated)
			var (
				allEntries []*cmn.LsoEnt
				token      string
				numPages   int
				args       api.ListArgs
			)
			if createMsg.Name != "" {
				args.Header = http.Header{apc.HdrInvName: []string{createMsg.Name}}
			}
			for {
				lsmsg := &apc.LsoMsg{
					Prefix:            m.prefix,
					Props:             tc.props,
					PageSize:          tc.listPageSize,
					Flags:             apc.LsNBI,
					ContinuationToken: token,
				}
				lst, err := api.ListObjects(bp, m.bck, lsmsg, args)
				tassert.CheckFatal(t, err)
				numPages++ // FIXME: implies api.ListObjectsPage()
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
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck})

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
	if cliBck.IsRemoteAIS() {
		a := max(rand.IntN(100), 10)
		m1.num *= a
		m2.num *= a
	}

	m1.init(true /*cleanup*/)
	m1.puts()
	m2.puts()
	_nbiIniCln(t, cliBck)

	// create inventory covering both sub-prefixes
	createMsg := &apc.CreateNBIMsg{
		Name: "inv-prefix-" + cos.GenTie(),
		LsoMsg: apc.LsoMsg{
			Prefix:   parent,
			Props:    apc.GetPropsName,
			PageSize: max(int64(m1.num/10), 4),
		},
		NamesPerChunk: int64(max(rand.IntN(40)+4, 4)),
	}

	xid, err := api.CreateNBI(bp, m1.bck, createMsg)
	tassert.CheckFatal(t, err)
	wargs := xact.ArgsMsg{ID: xid, Kind: apc.ActCreateNBI, Timeout: nbiCreateTimeout}
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
			args       api.ListArgs
		)
		if createMsg.Name != "" {
			args.Header = http.Header{apc.HdrInvName: []string{createMsg.Name}}
		}
		for {
			lsmsg := &apc.LsoMsg{
				Prefix:            tc.prefix,
				Props:             apc.GetPropsName,
				PageSize:          max(int64(m1.num/10), 4),
				Flags:             apc.LsNBI,
				ContinuationToken: token,
			}
			lst, err := api.ListObjectsPage(bp, m1.bck, lsmsg, args)
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

func TestListInventoryPrefixPermute(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck})

	type test struct {
		name             string
		numA, numM, numZ int
		pageSize         int64 // CreateNBIMsg.PageSize
		namesPerChunk    int64
		listPageSize     int64 // listing PageSize (best-effort for NBI)
		invName          string
	}
	tests := []test{
		{
			name: "small-chunks-small-pages",
			numA: 40, numM: 30, numZ: 50,
			pageSize: 3, namesPerChunk: 6, listPageSize: 4,
			invName: "inv-sc-sp",
		},
		{
			name: "small-chunks-pages-empty-invName",
			numA: 40, numM: 30, numZ: 50,
			pageSize: 3, namesPerChunk: 6, listPageSize: 4,
		},
		{
			name: "small-chunks-large-pages",
			numA: 40, numM: 30, numZ: 50,
			pageSize: 3, namesPerChunk: 6, listPageSize: 1000,
			invName: "inv-sc-lp",
		},
		{
			name: "large-chunks-small-pages",
			numA: 70, numM: 60, numZ: 80,
			pageSize: 6, namesPerChunk: 60, listPageSize: 3,
			invName: "inv-lc-sp",
		},
		{
			name: "names-per-chunk",
			numA: 30, numM: 25, numZ: 35,
			pageSize: 10, namesPerChunk: 8, listPageSize: 5,
			invName: "inv-maxent",
		},
		{
			name: "zero-page-size",
			numA: 40, numM: 30, numZ: 50,
			pageSize: 3, namesPerChunk: 6, listPageSize: 0,
			invName: "inv-zero-ps",
		},
		{
			name: "tiny-pages-large-chunks",
			numA: 60, numM: 80, numZ: 40,
			pageSize: 5, namesPerChunk: 100, listPageSize: 2,
			invName: "inv-tiny-ps",
		},
		{
			name: "many-chunks-tiny-pages",
			numA: 50, numM: 50, numZ: 50,
			pageSize: 5, namesPerChunk: 10, listPageSize: 2,
		},
	}

	bck := cliBck
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_nbiIniCln(t, bck)

			var (
				parent = "p-" + cos.GenTie() + "/"
				bp     = tools.BaseAPIParams()
			)

			m0 := &ioContext{t: t, bck: bck, prefix: parent}
			m0.init(true /*cleanup*/)

			ma := &ioContext{t: t, bck: bck, prefix: parent + "a/"}
			mm := &ioContext{t: t, bck: bck, prefix: parent + "m/"}
			mz := &ioContext{t: t, bck: bck, prefix: parent + "z/"}

			ma.num = tc.numA
			mm.num = tc.numM
			mz.num = tc.numZ

			ma.puts()
			mm.puts()
			mz.puts()

			createMsg := &apc.CreateNBIMsg{
				Name: tc.invName,
				LsoMsg: apc.LsoMsg{
					Prefix:   parent,
					Props:    apc.GetPropsName,
					PageSize: tc.pageSize,
				},
				NamesPerChunk: tc.namesPerChunk,
			}

			xid, err := api.CreateNBI(bp, bck, createMsg)
			tassert.CheckFatal(t, err)

			wargs := xact.ArgsMsg{ID: xid, Kind: apc.ActCreateNBI, Timeout: nbiCreateTimeout}
			_, err = api.WaitForXactionIC(bp, &wargs)
			tassert.CheckFatal(t, err)

			var args api.ListArgs
			if createMsg.Name != "" {
				args.Header = http.Header{apc.HdrInvName: []string{createMsg.Name}}
			}

			listNBI := func(prefix string) (entries []*cmn.LsoEnt) {
				var (
					token   string
					seenTok = make(cos.StrSet, 16)
				)
				for {
					lsmsg := &apc.LsoMsg{
						Prefix:            prefix,
						Props:             apc.GetPropsName,
						PageSize:          tc.listPageSize,
						Flags:             apc.LsNBI,
						ContinuationToken: token,
					}
					lst, err := api.ListObjectsPage(bp, bck, lsmsg, args)
					tassert.CheckFatal(t, err)

					tassert.Fatalf(t, len(lst.Entries) > 0 || lst.ContinuationToken == "",
						"empty page with continuation token %q for prefix %q", lst.ContinuationToken, prefix)

					entries = append(entries, lst.Entries...)
					if lst.ContinuationToken == "" {
						break
					}
					tassert.Fatalf(t, !seenTok.Contains(lst.ContinuationToken),
						"repeated continuation token %q", lst.ContinuationToken)
					seenTok.Set(lst.ContinuationToken)
					token = lst.ContinuationToken
				}

				for i := range entries {
					tassert.Fatalf(t, strings.HasPrefix(entries[i].Name, prefix),
						"entry %q doesn't match prefix %q", entries[i].Name, prefix)
					if i > 0 {
						prev, curr := entries[i-1].Name, entries[i].Name
						tassert.Fatalf(t, prev < curr,
							"entries not sorted/unique at [%d]: %q >= %q", i, prev, curr)
					}
				}
				return entries
			}

			missingFromEntries := func(expected []string, got []*cmn.LsoEnt) []string {
				gotSet := make(cos.StrSet, len(got))
				for _, e := range got {
					gotSet.Set(e.Name)
				}
				missed := make([]string, 0, 4)
				for _, name := range expected {
					if _, ok := gotSet[name]; !ok {
						missed = append(missed, name)
					}
				}
				return missed
			}

			gotM := listNBI(mm.prefix)
			missed := missingFromEntries(mm.objNames, gotM)
			tassert.Fatalf(t, len(missed) == 0,
				"missing %v for prefix %q - %d out of %d expected",
				missed, mm.prefix, len(missed), len(mm.objNames))
			tassert.Fatalf(t, len(gotM) == mm.num,
				"prefix %q: expected %d, got %d", mm.prefix, mm.num, len(gotM))

			gotParent := listNBI(parent)
			expTotal := ma.num + mm.num + mz.num

			expSet := make(cos.StrSet, expTotal)
			expSet.Add(ma.objNames...)
			expSet.Add(mm.objNames...)
			expSet.Add(mz.objNames...)
			debug.Assert(len(expSet) == expTotal)

			gotParentSet := make(cos.StrSet, len(gotParent))
			for _, en := range gotParent {
				gotParentSet.Set(en.Name)
			}
			missed = missed[:0]
			for name := range expSet {
				if _, ok := gotParentSet[name]; !ok {
					missed = append(missed, name)
				}
			}
			tassert.Fatalf(t, len(missed) == 0,
				"missing %v for parent prefix %q - %d out of %d expected",
				missed, parent, len(missed), expTotal)
			tassert.Fatalf(t, len(gotParent) == expTotal,
				"parent prefix %q: expected %d, got %d", parent, expTotal, len(gotParent))

			missing := parent + "zzzz/"
			gotMissing := listNBI(missing)
			tassert.Fatalf(t, len(gotMissing) == 0,
				"prefix %q: expected 0, got %d", missing, len(gotMissing))
		})
	}
}

//
// non-recursive listing over NBI
//

func TestListInventoryNoRecursion(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck})

	var (
		bck    = cliBck
		bp     = tools.BaseAPIParams()
		parent = "nrtest-" + cos.GenTie() + "/"
	)

	_nbiIniCln(t, bck)

	// Hierarchical object layout:
	//   parent/file-a.txt
	//   parent/file-b.txt
	//   parent/sub1/obj1 .. obj3
	//   parent/sub1/deep/obj4
	//   parent/sub2/obj5, obj6
	//   parent/sub3/obj7
	objNames := []string{
		parent + "file-a.txt",
		parent + "file-b.txt",
		parent + "sub1/obj1",
		parent + "sub1/obj2",
		parent + "sub1/obj3",
		parent + "sub1/deep/obj4",
		parent + "sub2/obj5",
		parent + "sub2/obj6",
		parent + "sub3/obj7",
	}

	for _, name := range objNames {
		r, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: 128, CksumType: cos.ChecksumNone})
		_, err := api.PutObject(&api.PutArgs{
			BaseParams: bp,
			Bck:        bck,
			ObjName:    name,
			Reader:     r,
		})
		tassert.CheckFatal(t, err)
	}
	t.Cleanup(func() {
		for _, name := range objNames {
			api.DeleteObject(bp, bck, name)
		}
	})

	invName := "inv-nr-" + cos.GenTie()
	createMsg := &apc.CreateNBIMsg{
		Name: invName,
		LsoMsg: apc.LsoMsg{
			Prefix:   parent,
			Props:    apc.GetPropsName,
			PageSize: 3,
		},
		NamesPerChunk: 4,
		Force:         true,
	}

	xid, err := api.CreateNBI(bp, bck, createMsg)
	tassert.CheckFatal(t, err)
	tlog.Logfln("%s[%s] started (9 objects, hierarchical)", apc.ActCreateNBI, xid)

	wargs := xact.ArgsMsg{ID: xid, Kind: apc.ActCreateNBI, Timeout: nbiCreateTimeout}
	_, err = api.WaitForXactionIC(bp, &wargs)
	tassert.CheckFatal(t, err)

	largs := api.ListArgs{
		Header: http.Header{apc.HdrInvName: []string{invName}},
	}

	listNBI := func(t *testing.T, prefix string, flags uint64, pageSize int64) []*cmn.LsoEnt {
		var (
			allEntries []*cmn.LsoEnt
			token      string
			seenTok    = make(cos.StrSet, 16)
		)
		for {
			lsmsg := &apc.LsoMsg{
				Prefix:            prefix,
				Props:             apc.GetPropsName,
				PageSize:          pageSize,
				Flags:             flags,
				ContinuationToken: token,
			}
			lst, err := api.ListObjectsPage(bp, bck, lsmsg, largs)
			tassert.CheckFatal(t, err)

			tassert.Fatalf(t, len(lst.Entries) > 0 || lst.ContinuationToken == "",
				"empty page with non-empty token %q (prefix %q)", lst.ContinuationToken, prefix)

			allEntries = append(allEntries, lst.Entries...)
			if lst.ContinuationToken == "" {
				break
			}
			tassert.Fatalf(t, !seenTok.Contains(lst.ContinuationToken),
				"repeated continuation token %q", lst.ContinuationToken)
			seenTok.Set(lst.ContinuationToken)
			token = lst.ContinuationToken
		}

		for i := 1; i < len(allEntries); i++ {
			tassert.Fatalf(t, allEntries[i-1].Name < allEntries[i].Name,
				"entries not sorted at [%d]: %q >= %q", i, allEntries[i-1].Name, allEntries[i].Name)
		}
		return allEntries
	}

	// 1. non-recursive at root prefix
	t.Run("root-nr", func(t *testing.T) {
		entries := listNBI(t, parent, apc.LsNBI|apc.LsNoRecursion, 2)

		exp := []string{
			parent + "file-a.txt",
			parent + "file-b.txt",
			parent + "sub1/",
			parent + "sub2/",
			parent + "sub3/",
		}
		tassert.Fatalf(t, len(entries) == len(exp),
			"expected %d, got %d", len(exp), len(entries))

		for i, e := range entries {
			tassert.Fatalf(t, e.Name == exp[i],
				"[%d] expected %q, got %q", i, exp[i], e.Name)
		}

		for _, e := range entries {
			isDir := strings.HasSuffix(e.Name, "/")
			hasFlag := e.Flags&apc.EntryIsDir != 0
			tassert.Fatalf(t, isDir == hasFlag,
				"%q: trailing-slash=%v, EntryIsDir=%v", e.Name, isDir, hasFlag)
		}
		tlog.Logfln("root-nr: %d entries OK", len(entries))
	})

	// 2. non-recursive at sub-prefix (one level deeper)
	t.Run("sub1-nr", func(t *testing.T) {
		entries := listNBI(t, parent+"sub1/", apc.LsNBI|apc.LsNoRecursion, 2)

		exp := []string{
			parent + "sub1/deep/",
			parent + "sub1/obj1",
			parent + "sub1/obj2",
			parent + "sub1/obj3",
		}
		tassert.Fatalf(t, len(entries) == len(exp),
			"expected %d, got %d", len(exp), len(entries))
		for i, e := range entries {
			tassert.Fatalf(t, e.Name == exp[i],
				"[%d] expected %q, got %q", i, exp[i], e.Name)
		}

		tassert.Fatalf(t, entries[0].Flags&apc.EntryIsDir != 0,
			"%q: expected EntryIsDir", entries[0].Name)

		tlog.Logfln("sub1-nr: %d entries OK", len(entries))
	})

	// 3. non-recursive at leaf prefix (no subdirectories at this level)
	t.Run("leaf-nr", func(t *testing.T) {
		entries := listNBI(t, parent+"sub1/deep/", apc.LsNBI|apc.LsNoRecursion, 10)

		tassert.Fatalf(t, len(entries) == 1,
			"expected 1, got %d", len(entries))
		tassert.Fatalf(t, entries[0].Name == parent+"sub1/deep/obj4",
			"expected %q, got %q", parent+"sub1/deep/obj4", entries[0].Name)
		tassert.Fatalf(t, entries[0].Flags&apc.EntryIsDir == 0,
			"%q: expected file, not dir", entries[0].Name)

		tlog.Logfln("leaf-nr: OK")
	})

	// 4. regular NBI (no --nr) still returns all flat objects
	t.Run("flat-nbi", func(t *testing.T) {
		entries := listNBI(t, parent, apc.LsNBI, 4)

		tassert.Fatalf(t, len(entries) == len(objNames),
			"expected %d, got %d", len(objNames), len(entries))

		nameSet := make(cos.StrSet, len(entries))
		for _, e := range entries {
			nameSet.Set(e.Name)
		}
		for _, name := range objNames {
			tassert.Fatalf(t, nameSet.Contains(name),
				"PUT'd object %q missing from inventory", name)
		}
		tlog.Logfln("flat-nbi: %d entries OK", len(entries))
	})

	// 5. sweep page sizes to stress pagination boundaries
	t.Run("page-sweep", func(t *testing.T) {
		for _, ps := range []int64{1, 2, 3, 5, 10, 0} {
			entries := listNBI(t, parent, apc.LsNBI|apc.LsNoRecursion, ps)

			tassert.Fatalf(t, len(entries) == 5,
				"pageSize=%d: expected 5, got %d", ps, len(entries))
		}
		tlog.Logfln("page-sweep: all page sizes OK")
	})

	// 6. non-recursive with prefix that has only dirs (no files at this level)
	t.Run("dirs-only-nr", func(t *testing.T) {
		entries := listNBI(t, parent+"sub2/", apc.LsNBI|apc.LsNoRecursion, 10)

		tassert.Fatalf(t, len(entries) == 2,
			"expected 2, got %d", len(entries))
		tassert.Fatalf(t, entries[0].Name == parent+"sub2/obj5", "got %q", entries[0].Name)
		tassert.Fatalf(t, entries[1].Name == parent+"sub2/obj6", "got %q", entries[1].Name)

		for _, e := range entries {
			tassert.Fatalf(t, e.Flags&apc.EntryIsDir == 0,
				"%q: unexpected dir flag", e.Name)
		}
		tlog.Logfln("dirs-only-nr: OK")
	})
}

// Cross-validates NR listing against flat listing at multiple prefix levels
// with page-size sweep, using an object layout that covers edge cases:
//   - adjacent names (aaa-readme vs aaa/)
//   - deep nesting (bbb/deep/x/y/, zzz/a/b/c/)
//   - single-object and many-object directories
//   - root files interleaved with directories
//   - dir-only prefix levels (no files)
//   - file-only prefix levels (no dirs)
//   - non-existent prefix
func TestListInventoryNoRecursionPagination(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck})

	var (
		bck    = cliBck
		bp     = tools.BaseAPIParams()
		parent = "nrpag-" + cos.GenTie() + "/"
	)

	_nbiIniCln(t, bck)

	var objNames []string
	add := func(pattern string, n int) {
		for i := range n {
			objNames = append(objNames, fmt.Sprintf(pattern, parent, i))
		}
	}
	// 31 objects total
	objNames = append(objNames, parent+"aaa-readme")     // adjacent to aaa/
	add("%saaa/%02d", 10)                                // 10 under aaa/
	add("%sbbb/%02d", 5)                                 // 5 direct under bbb/
	add("%sbbb/deep/x/y/%02d", 3)                        // 3 deeply nested under bbb/
	objNames = append(objNames, parent+"ccc/only")       // single-object dir
	add("%sroot-%02d", 10)                               // 10 root files
	objNames = append(objNames, parent+"zzz/a/b/c/deep") // deeply nested → zzz/

	for _, name := range objNames {
		r, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: 128, CksumType: cos.ChecksumNone})
		_, err := api.PutObject(&api.PutArgs{
			BaseParams: bp,
			Bck:        bck,
			ObjName:    name,
			Reader:     r,
		})
		tassert.CheckFatal(t, err)
	}
	t.Cleanup(func() {
		for _, name := range objNames {
			api.DeleteObject(bp, bck, name)
		}
	})

	invName := "inv-nrpag-" + cos.GenTie()
	createMsg := &apc.CreateNBIMsg{
		Name: invName,
		LsoMsg: apc.LsoMsg{
			Prefix:   parent,
			Props:    apc.GetPropsName,
			PageSize: 5,
		},
		NamesPerChunk: 3, // very small chunks → many chunks per target
		Force:         true,
	}

	xid, err := api.CreateNBI(bp, bck, createMsg)
	tassert.CheckFatal(t, err)
	tlog.Logfln("%s[%s] started (31 objects, edge-case layout)", apc.ActCreateNBI, xid)

	wargs := xact.ArgsMsg{ID: xid, Kind: apc.ActCreateNBI, Timeout: nbiCreateTimeout}
	_, err = api.WaitForXactionIC(bp, &wargs)
	tassert.CheckFatal(t, err)

	largs := api.ListArgs{
		Header: http.Header{apc.HdrInvName: []string{invName}},
	}

	listNBI := func(t *testing.T, prefix string, flags uint64, pageSize int64) []*cmn.LsoEnt {
		var (
			all     []*cmn.LsoEnt
			token   string
			seenTok = make(cos.StrSet, 16)
		)
		for {
			lsmsg := &apc.LsoMsg{
				Prefix:            prefix,
				Props:             apc.GetPropsName,
				PageSize:          pageSize,
				Flags:             flags,
				ContinuationToken: token,
			}
			lst, err := api.ListObjectsPage(bp, bck, lsmsg, largs)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, len(lst.Entries) > 0 || lst.ContinuationToken == "",
				"empty page with token %q (prefix %q ps %d)", lst.ContinuationToken, prefix, pageSize)
			all = append(all, lst.Entries...)
			if lst.ContinuationToken == "" {
				break
			}
			tassert.Fatalf(t, !seenTok.Contains(lst.ContinuationToken),
				"repeated token %q (prefix %q ps %d)", lst.ContinuationToken, prefix, pageSize)
			seenTok.Set(lst.ContinuationToken)
			token = lst.ContinuationToken
		}
		for i := 1; i < len(all); i++ {
			tassert.Fatalf(t, all[i-1].Name < all[i].Name,
				"not sorted at [%d]: %q >= %q (prefix %q ps %d)",
				i, all[i-1].Name, all[i].Name, prefix, pageSize)
		}
		return all
	}

	// ground truth: flat NBI listing
	flat := listNBI(t, parent, apc.LsNBI, 0)
	tassert.Fatalf(t, len(flat) == len(objNames),
		"flat: expected %d, got %d", len(objNames), len(flat))

	flatNames := make([]string, len(flat))
	for i, e := range flat {
		flatNames[i] = e.Name
	}

	// cross-validate at various prefix levels with page-size sweep
	prefixes := []struct {
		tag    string
		prefix string
	}{
		{"root", parent},                       // 10 files + 3 dirs = 13
		{"aaa", parent + "aaa/"},               // 10 files, 0 dirs
		{"bbb", parent + "bbb/"},               // 5 files + 1 dir = 6
		{"bbb-deep", parent + "bbb/deep/"},     // 0 files + 1 dir = 1
		{"bbb-leaf", parent + "bbb/deep/x/y/"}, // 3 files, 0 dirs (leaf)
		{"ccc", parent + "ccc/"},               // 1 file, 0 dirs
		{"zzz", parent + "zzz/"},               // 0 files + 1 dir = 1
		{"zzz-deep", parent + "zzz/a/b/c/"},    // 1 file, 0 dirs (leaf)
		{"miss", parent + "nonexistent/"},      // 0 entries
	}

	pageSizes := []int64{1, 2, 3, 5, 10, 0}

	for _, pc := range prefixes {
		t.Run(pc.tag, func(t *testing.T) {
			exp := _expectedNR(flatNames, pc.prefix)

			for _, ps := range pageSizes {
				got := listNBI(t, pc.prefix, apc.LsNBI|apc.LsNoRecursion, ps)

				tassert.Fatalf(t, len(got) == len(exp),
					"prefix=%q ps=%d: expected %d, got %d\n  exp: %v\n  got: %v",
					pc.prefix, ps, len(exp), len(got),
					exp, _lsoNames(got))

				for i, e := range got {
					tassert.Fatalf(t, e.Name == exp[i],
						"prefix=%q ps=%d [%d]: expected %q, got %q",
						pc.prefix, ps, i, exp[i], e.Name)

					// dir entries must have EntryIsDir; files must not
					isDir := strings.HasSuffix(e.Name, "/")
					hasFlag := e.Flags&apc.EntryIsDir != 0
					tassert.Fatalf(t, isDir == hasFlag,
						"prefix=%q ps=%d %q: trail-slash=%v flag=%v",
						pc.prefix, ps, e.Name, isDir, hasFlag)
				}
			}

			tlog.Logfln("%s: %d entries, %d page-sizes OK", pc.tag, len(exp), len(pageSizes))
		})
	}
}

//
// local helpers
//

// _expectedNR computes the expected non-recursive listing from a sorted flat name list.
// Since flatNames is sorted and directory entries replace a contiguous run of children,
// the output is also sorted — no separate sort step needed.
func _expectedNR(flatNames []string, prefix string) []string {
	seen := make(cos.StrSet, 8)
	var result []string
	for _, name := range flatNames {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		rel := strings.TrimPrefix(name, prefix)
		if idx := strings.IndexByte(rel, '/'); idx >= 0 {
			dirName := prefix + rel[:idx+1]
			if !seen.Contains(dirName) {
				seen.Set(dirName)
				result = append(result, dirName)
			}
		} else {
			result = append(result, name)
		}
	}
	sort.Strings(result) // defensive; should already be sorted
	return result
}

func _lsoNames(entries []*cmn.LsoEnt) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	return names
}

func _nbiIniCln(t *testing.T, bck cmn.Bck) {
	if !bck.IsRemote() {
		return
	}
	proxyURL := tools.GetPrimaryURL()
	t.Cleanup(func() {
		tools.EvictRemoteBucket(t, proxyURL, bck, false /*keepMD*/)
	})
}
