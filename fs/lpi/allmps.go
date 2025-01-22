// Package lpi: local page iterator
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package lpi

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
)

type (
	milpi struct {
		page Page
		it   *Iter
		mi   *fs.Mountpath
	}
	Lpis struct {
		a   []milpi
		bck *cmn.Bck
	}
)

func (lpis *Lpis) Init(bck *cmn.Bck, prefix string) {
	var (
		avail = fs.GetAvail()
	)
	{
		lpis.a = make([]milpi, 0, len(avail))
		lpis.bck = bck
	}
	for _, mi := range avail {
		it, err := New(mi.MakePathPrefix(bck, fs.ObjectType, prefix))
		debug.AssertNoErr(err)
		milpi := milpi{
			page: make(Page),
			it:   it,
			mi:   mi,
		}
		lpis.a = append(lpis.a, milpi)
	}
}

// TODO: consider jogger-per-mountpath

func (lpis *Lpis) Do(lastPage cmn.LsoEntries, outPage *cmn.LsoRes, tag string) {
	var (
		lastName string
		eop      = AllPages
		num      = len(lastPage) // num entries
	)
	if num > 0 {
		lastName = lastPage[num-1].Name
	}
	// 1. all mountpaths: next page
	for _, milpi := range lpis.a {
		if milpi.it.Pos() == "" {
			// iterated to the end, exhausted local content
			milpi.it.Clear()
			continue
		}
		if lastName != "" {
			eop = milpi.mi.MakePathPrefix(lpis.bck, fs.ObjectType, lastName)
		}

		// next local page "until"
		lpiMsg := Msg{EOP: eop}
		if err := milpi.it.Next(lpiMsg, milpi.page); err != nil {
			if cmn.Rom.FastV(4, cos.SmoduleXs) {
				nlog.Warningln(tag, err)
			}
		}
	}

	// 2. last page as a map
	lastPageMap := allocPage()
	for _, en := range lastPage {
		lastPageMap[en.Name] = struct{}{}
	}

	// 3. find and add 'remotely-deleted'
	for _, milpi := range lpis.a {
		for lname := range milpi.page {
			if _, ok := lastPageMap[lname]; ok {
				delete(milpi.page, lname)
				continue
			}
			en := &cmn.LsoEnt{Name: lname}
			en.SetFlag(apc.EntryVerRemoved | apc.EntryIsCached)
			outPage.Entries = append(outPage.Entries, en)
		}
	}
	freePage(lastPageMap)
}
