// Package lpi: local page iterator
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package lpi

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
)

type (
	milpi struct {
		page Page
		it   *Iter
		mi   *fs.Mountpath
	}
	Lpis struct {
		bck *cmn.Bck
		a   []milpi

		// guard state against remote out-of-sorting-order; auto-disables otherwise
		// - err on the side of caution
		// - see extended comment below on how to re-enable at runtime
		prevRemoteLast string
		disabled       bool
	}
)

// TODO: re-enable after a certain window of "forgiveness" ======================================
//
// Currently, LPI self-disables on detecting non-monotonic remote pages (first <= prevRemoteLast)
//
// Consider adding a small "forgiveness window":
// - when disabled, x-list keeps calling Do();
// - the latter should not walk local mountpaths; it should only observe remote page
//   boundaries (first/last) and track a sequence of monotonic pages;
// - maintain a counter of consecutive "good" pages where first > prevRemoteLast (and last >= first).
// - auto-re-enable after N good pages (e.g. N=5);
// - and reset/realign local iterator state (Clear() + restart, or re-init Lpis) so that
//   local cursor cannot remain out of sync with remote progression. ============================

func (lpis *Lpis) Enabled() bool { return !lpis.disabled }

func (lpis *Lpis) Init(bck *cmn.Bck, prefix string, smap *meta.Smap) {
	var (
		avail = fs.GetAvail()
	)
	{
		lpis.a = make([]milpi, 0, len(avail))
		lpis.bck = bck
	}
	for _, mi := range avail {
		it, err := New(mi.MakePathCT(bck, fs.ObjCT) /*root*/, prefix, smap)
		debug.AssertNoErr(err)
		milpi := milpi{
			page: make(Page),
			it:   it,
			mi:   mi,
		}
		lpis.a = append(lpis.a, milpi)
	}
}

// first and last are the remote page bounds before filtering by target
func (lpis *Lpis) Do(page *cmn.LsoRes, first, last, tag string) {
	var (
		prevLast = lpis.prevRemoteLast
		wg       = &sync.WaitGroup{}
	)
	debug.AssertFunc(func() bool { return lpis.Enabled() })
	if first != "" {
		if prevLast != "" && first <= prevLast {
			lpis.disabled = true
			nlog.Warningln(tag, "auto-disabling: remote page not monotonic: prev-last=", prevLast, "first-current=", first)
			return
		}
		lpis.prevRemoteLast = last
	} else if page.ContinuationToken != "" {
		return // empty remote page, no upper bound
	}

	if page.ContinuationToken == "" {
		last = AllPages // drain remaining local names
	}
	// 1. all mountpaths: next page
	for _, milpi := range lpis.a {
		wg.Add(1)
		go func(bck *cmn.Bck, lastName, tag string) {
			milpi.run(bck, lastName, tag)
			wg.Done()
		}(lpis.bck, last, tag)
	}
	wg.Wait()

	// 2. last page as a (mem-pooled) map
	lastPageMap := allocPage()
	for _, en := range page.Entries {
		lastPageMap[en.Name] = 0
	}

	// 3. find and add 'remotely-deleted'
	for _, milpi := range lpis.a {
		for lname, size := range milpi.page {
			if prevLast != "" && lname <= prevLast {
				continue
			}
			if _, ok := lastPageMap[lname]; ok {
				delete(milpi.page, lname)
				continue
			}
			en := &cmn.LsoEnt{Name: lname, Size: size}
			en.SetFlag(apc.EntryVerRemoved | apc.EntryIsCached)
			page.Entries = append(page.Entries, en)
		}
	}
	freePage(lastPageMap)
}

///////////
// milpi //
///////////

func (milpi *milpi) run(bck *cmn.Bck, lastName, tag string) {
	eop := AllPages
	if milpi.it.Pos() == "" {
		// iterated to the end, exhausted local content
		milpi.it.Clear()
		return
	}
	if lastName != "" {
		eop = milpi.mi.MakePathFQN(bck, fs.ObjCT, lastName)
	}

	// next local page "until"
	lpiMsg := Msg{EOP: eop}
	if err := milpi.it.Next(lpiMsg, milpi.page); err != nil {
		if cmn.Rom.V(4, cos.ModFS) {
			nlog.Warningln(tag, err)
		}
	}
}
