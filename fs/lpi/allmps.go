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
		it, err := New(mi.MakePathCT(bck, fs.ObjectType) /*root*/, prefix)
		debug.AssertNoErr(err)
		milpi := milpi{
			page: make(Page),
			it:   it,
			mi:   mi,
		}
		lpis.a = append(lpis.a, milpi)
	}
}

func (lpis *Lpis) Do(lastPage cmn.LsoEntries, outPage *cmn.LsoRes, tag string, last bool) {
	const (
		limitNumGor = 2 // TODO: calibrate
	)
	var (
		lastName string
		num      = len(lastPage) // num entries
		wg       = cos.NewLimitedWaitGroup(limitNumGor, len(lpis.a))
	)
	if num > 0 && !last {
		lastName = lastPage[num-1].Name
	}

	// 1. all mountpaths: next page
	for _, milpi := range lpis.a {
		wg.Add(1)
		go func(bck *cmn.Bck, lastName, tag string) {
			milpi.run(bck, lastName, tag)
			wg.Done()
		}(lpis.bck, lastName, tag)
	}
	wg.Wait()

	// 2. last page as a (mem-pooled) map
	lastPageMap := allocPage()
	for _, en := range lastPage {
		lastPageMap[en.Name] = 0
	}

	// 3. find and add 'remotely-deleted'
	for _, milpi := range lpis.a {
		for lname, size := range milpi.page {
			if _, ok := lastPageMap[lname]; ok {
				delete(milpi.page, lname)
				continue
			}
			en := &cmn.LsoEnt{Name: lname, Size: size}
			en.SetFlag(apc.EntryVerRemoved | apc.EntryIsCached)
			outPage.Entries = append(outPage.Entries, en)
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
		eop = milpi.mi.MakePathFQN(bck, fs.ObjectType, lastName)
	}

	// next local page "until"
	lpiMsg := Msg{EOP: eop}
	if err := milpi.it.Next(lpiMsg, milpi.page); err != nil {
		if cmn.Rom.FastV(4, cos.SmoduleXs) {
			nlog.Warningln(tag, err)
		}
	}
}
