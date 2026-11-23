// Package lpi_test: local page iterator
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package lpi_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/lpi"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"
)

const lpiPrefix = "p/"

type rpage struct {
	owned       []string // remote names owned by this target (page.Entries after filtering)
	first, last string   // remote page bounds before filtering
	token       string   // empty: last remote page
	removed     []string // expected remotely deleted
}

// remote names that this target does not own are present only as page bounds
func TestLpisDo(t *testing.T) {
	tests := []struct {
		name  string
		local []string
		pages []rpage
	}{
		{
			name:  "no-owned-names",
			local: []string{"p/b", "p/x", "p/y"},
			pages: []rpage{
				{first: "p/a", last: "p/m", token: "t1", removed: []string{"p/b"}},
				{owned: []string{"p/x"}, first: "p/x", last: "p/z", removed: []string{"p/y"}},
			},
		},
		{
			name:  "empty-remote-page",
			local: []string{"p/b", "p/c"},
			pages: []rpage{
				{first: "p/a", last: "p/a", token: "t1"},
				{token: "t2"},
				{owned: []string{"p/c"}, first: "p/c", last: "p/c", token: "t3", removed: []string{"p/b"}},
				{},
			},
		},
		{
			name:  "dir-siblings",
			local: []string{"p/d-1", "p/d.json", "p/d/000", "p/d/001", "p/e"},
			pages: []rpage{
				{first: "p/c", last: "p/c", token: "t1"},
				{owned: []string{"p/d/000"}, first: "p/d/000", last: "p/d/000", token: "t2", removed: []string{"p/d-1", "p/d.json"}},
				{first: "p/f", last: "p/f", removed: []string{"p/d/001", "p/e"}},
			},
		},
	}

	mpaths := tools.PrepareMountPaths(t, 2)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bck := newBck(t, mpaths)
			for i, name := range test.local {
				putLocal(t, mpaths, i, &bck, name)
			}
			lpis := &lpi.Lpis{}
			lpis.Init(&bck, lpiPrefix, nil)

			for i, rp := range test.pages {
				page := rp.lsoRes()
				lpis.Do(page, rp.first, rp.last, test.name)
				tassert.Fatalf(t, lpis.Enabled(), "page %d: unexpectedly disabled", i)
				checkPage(t, i, page, &rp)
			}
		})
	}
}

func TestLpisDoNotMonotonic(t *testing.T) {
	mpaths := tools.PrepareMountPaths(t, 2)
	bck := newBck(t, mpaths)
	putLocal(t, mpaths, 0, &bck, "p/b")

	lpis := &lpi.Lpis{}
	lpis.Init(&bck, lpiPrefix, nil)

	rp := rpage{first: "p/m", last: "p/m", token: "t1", removed: []string{"p/b"}}
	page := rp.lsoRes()
	lpis.Do(page, rp.first, rp.last, t.Name())
	checkPage(t, 0, page, &rp)

	page = &cmn.LsoRes{ContinuationToken: "t2"}
	lpis.Do(page, "p/a", "p/z", t.Name())
	tassert.Fatalf(t, !lpis.Enabled(), "expected auto-disable on non-monotonic remote page")
	tassert.Fatalf(t, len(page.Entries) == 0, "disabled: unexpected entries %v", page.Entries)
}

//
// helpers
//

func newBck(t *testing.T, mpaths fs.MPI) cmn.Bck {
	bck := cmn.Bck{Name: trand.String(10), Provider: apc.AWS, Ns: cmn.NsGlobal}
	for _, mi := range mpaths {
		tassert.CheckFatal(t, cos.CreateDir(mi.MakePathCT(&bck, fs.ObjCT)))
	}
	return bck
}

// spread local names across mountpaths
func putLocal(t *testing.T, mpaths fs.MPI, i int, bck *cmn.Bck, name string) {
	mis := make([]*fs.Mountpath, 0, len(mpaths))
	for _, mi := range mpaths {
		mis = append(mis, mi)
	}
	slices.SortFunc(mis, func(a, b *fs.Mountpath) int { return strings.Compare(a.Path, b.Path) })
	fqn := mis[i%len(mis)].MakePathFQN(bck, fs.ObjCT, name)
	tassert.CheckFatal(t, cos.CreateDir(filepath.Dir(fqn)))
	tassert.CheckFatal(t, os.WriteFile(fqn, []byte(name), cos.PermRWR))
}

func (rp *rpage) lsoRes() *cmn.LsoRes {
	page := &cmn.LsoRes{ContinuationToken: rp.token}
	for _, name := range rp.owned {
		page.Entries = append(page.Entries, &cmn.LsoEnt{Name: name})
	}
	return page
}

func checkPage(t *testing.T, i int, page *cmn.LsoRes, rp *rpage) {
	var owned, removed []string
	for _, en := range page.Entries {
		if en.IsAnyFlagSet(apc.EntryVerRemoved) {
			removed = append(removed, en.Name)
		} else {
			owned = append(owned, en.Name)
		}
	}
	slices.Sort(removed)
	tassert.Fatalf(t, slices.Equal(owned, rp.owned), "page %d: owned %v, expected %v", i, owned, rp.owned)
	tassert.Fatalf(t, slices.Equal(removed, rp.removed), "page %d: removed %v, expected %v", i, removed, rp.removed)
}
