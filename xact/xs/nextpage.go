// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs

// core next-page and next-remote-page methods for object listing

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
)

type npgCtx struct {
	bck  *meta.Bck
	wi   walkInfo
	page cmn.LsoResult
	off  *int64
	idx  int
}

func newNpgCtx(bck *meta.Bck, msg *apc.LsoMsg, cb lomVisitedCb, off *int64) (npg *npgCtx) {
	npg = &npgCtx{
		bck: bck,
		wi: walkInfo{
			msg:          msg.Clone(),
			lomVisitedCb: cb,
			wanted:       wanted(msg),
			smap:         core.T.Sowner().Get(),
		},
		off: off,
	}
	return
}

// limited usage: bucket summary, multi-obj
func (npg *npgCtx) nextPageA() error {
	npg.page.UUID = npg.wi.msg.UUID
	npg.idx = 0
	opts := &fs.WalkBckOpts{
		WalkOpts: fs.WalkOpts{CTs: []string{fs.ObjectType}, Callback: npg.cb, Sorted: true},
	}
	opts.WalkOpts.Bck.Copy(npg.bck.Bucket())
	opts.ValidateCallback = func(fqn string, de fs.DirEntry) error {
		if de.IsDir() {
			return npg.wi.processDir(fqn)
		}
		return nil
	}
	err := fs.WalkBck(opts)
	if err != nil {
		freeLsoEntries(npg.page.Entries)
	} else {
		npg.page.Entries = npg.page.Entries[:npg.idx]
	}
	return err
}

func (npg *npgCtx) cb(fqn string, de fs.DirEntry) error {
	entry, err := npg.wi.callback(fqn, de)
	if entry == nil && err == nil {
		return nil
	}
	if err != nil {
		return cmn.NewErrAborted(core.T.String()+" ResultSetXact", "query", err)
	}
	if npg.idx < len(npg.page.Entries) {
		*npg.page.Entries[npg.idx] = *entry
	} else {
		debug.Assert(npg.idx == len(npg.page.Entries))
		npg.page.Entries = append(npg.page.Entries, entry)
	}
	npg.idx++
	return nil
}

// Returns the next page from the remote bucket's "list-objects" result set.
func (npg *npgCtx) nextPageR(nentries cmn.LsoEntries, inclStatusLocalMD bool) (lst *cmn.LsoResult, err error) {
	debug.Assert(!npg.wi.msg.IsFlagSet(apc.LsObjCached))
	lst = &cmn.LsoResult{Entries: nentries}
	if npg.wi.msg.IsFlagSet(apc.LsInventory) {
		debug.Assert(npg.off != nil)
		_, err = core.T.Backend(npg.bck).ListObjectsInv(npg.bck, npg.wi.msg, lst, npg.off)
	} else {
		_, err = core.T.Backend(npg.bck).ListObjects(npg.bck, npg.wi.msg, lst)
	}
	if err != nil {
		freeLsoEntries(nentries)
		return nil, err
	}
	debug.Assert(lst.UUID == "" || lst.UUID == npg.wi.msg.UUID)
	lst.UUID = npg.wi.msg.UUID

	if inclStatusLocalMD {
		err = npg.populate(lst)
	}
	return lst, err
}

func (npg *npgCtx) populate(lst *cmn.LsoResult) error {
	post := npg.wi.lomVisitedCb
	for _, obj := range lst.Entries {
		si, err := npg.wi.smap.HrwName2T(npg.bck.MakeUname(obj.Name))
		if err != nil {
			return err
		}
		if si.ID() != core.T.SID() {
			continue
		}
		lom := core.AllocLOM(obj.Name)
		if err := lom.InitBck(npg.bck.Bucket()); err != nil {
			core.FreeLOM(lom)
			if cmn.IsErrBucketNought(err) {
				return err
			}
			continue
		}
		if err := lom.Load(true /* cache it*/, false /*locked*/); err != nil {
			core.FreeLOM(lom)
			continue
		}

		npg.wi.setWanted(obj, lom)
		obj.SetPresent()

		if post != nil {
			post(lom)
		}
		core.FreeLOM(lom)
	}
	return nil
}
