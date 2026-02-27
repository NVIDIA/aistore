// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

// core next-page and next-remote-page methods for object listing

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
)

type npgCtx struct {
	bp    core.Backend
	bck   *meta.Bck
	s3ctx *core.LsoS3InvCtx // Deprecated; remove by April-May 2026
	wi    walkInfo
	page  cmn.LsoRes
	idx   int
}

func newNpgCtx(bck *meta.Bck, msg *apc.LsoMsg, cb lomVisitedCb, s3ctx *core.LsoS3InvCtx, bp core.Backend) (npg *npgCtx) {
	npg = &npgCtx{
		bp:  bp,
		bck: bck,
		wi: walkInfo{
			msg:          msg.Clone(),
			lomVisitedCb: cb,
			wanted:       wanted(msg),
			smap:         core.T.Sowner().Get(),
		},
		s3ctx: s3ctx,
	}
	if msg.IsFlagSet(apc.LsDiff) {
		npg.wi.custom = make(cos.StrKVs) // TODO: move to parent x-lso; clear and reuse here
	}
	return npg
}

// limited usage: lrit (compare w/ LsoXact.doWalk)
func (npg *npgCtx) nextPageA() error {
	npg.page.UUID = npg.wi.msg.UUID
	npg.idx = 0
	opts := &fs.WalkBckOpts{
		ValidateCb: npg.validateCb,
		WalkOpts:   fs.WalkOpts{CTs: []string{fs.ObjCT}, Callback: npg.cb, Sorted: true},
	}
	opts.WalkOpts.Bck.Copy(npg.bck.Bucket())
	err := fs.WalkBck(opts)
	if err != nil {
		freeLsoEntries(npg.page.Entries)
	} else {
		npg.page.Entries = npg.page.Entries[:npg.idx]
	}
	return err
}

func (npg *npgCtx) validateCb(fqn string, de fs.DirEntry) error {
	if !de.IsDir() {
		return nil
	}
	ct, err := npg.wi.processDir(fqn)
	if err != nil || ct == nil {
		return err
	}
	if !npg.wi.msg.IsFlagSet(apc.LsNoRecursion) {
		return nil
	}
	// multi-object (lrit) operation: skip virtual dir entries
	// ie., always ignore returned `addDirEntry`
	_, err = cmn.CheckDirNoRecurs(npg.wi.msg.Prefix, ct.ObjectName())
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
func (npg *npgCtx) nextPageR(nentries cmn.LsoEntries) (lst *cmn.LsoRes, err error) {
	debug.Assert(!npg.wi.msg.IsFlagSet(apc.LsCached))
	lst = &cmn.LsoRes{Entries: nentries}
	if npg.s3ctx != nil {
		if npg.s3ctx.Lom == nil {
			_, err = npg.bp.GetBucketInv(npg.bck, npg.s3ctx)
		}
		if err == nil {
			err = npg.bp.ListObjectsInv(npg.bck, npg.wi.msg, lst, npg.s3ctx)
		}
	} else {
		_, err = npg.bp.ListObjects(npg.bck, npg.wi.msg, lst)
	}
	if err != nil {
		freeLsoEntries(nentries)
		return nil, err
	}

	debug.Assert(lst.UUID == "" || lst.UUID == npg.wi.msg.UUID)
	lst.UUID = npg.wi.msg.UUID
	return lst, err
}

// - filter entries to keep only mine
// - add or set local metadata
// - see also: cmn.ConcatLso
func (npg *npgCtx) filterAddLmeta(lst *cmn.LsoRes) error {
	var (
		bck  = npg.bck
		post = npg.wi.lomVisitedCb
		msg  = npg.wi.msg
		i    int
	)

	for _, en := range lst.Entries {
		si, err := npg.wi.smap.HrwName2T(npg.bck.MakeUname(en.Name))
		if err != nil {
			return err
		}
		if si.ID() != core.T.SID() {
			continue
		}

		// [NOTE]
		// in re: `apc.LsNoDirs` and `apc.LsNoRecursion`, see:
		// * https://github.com/NVIDIA/aistore/blob/main/docs/howto_virt_dirs.md

		if en.IsAnyFlagSet(apc.EntryIsDir) && !msg.IsFlagSet(apc.LsNoDirs) {
			// collect virtual dir-s aka "common prefixes"
			lst.Entries[i] = en
			i++
			continue
		}

		en.ClrFlag(apc.EntryIsCached) // always clear remote (ie, remais) 'is-cached' bit
		lom := core.AllocLOM(en.Name)
		if err := lom.InitBck(bck); err != nil {
			if cmn.IsErrBucketNought(err) {
				core.FreeLOM(lom)
				return err
			}
			goto keep
		}
		if err := lom.Load(true /* cache it*/, false /*locked*/); err != nil {
			goto keep
		}
		if msg.IsFlagSet(apc.LsNotCached) {
			core.FreeLOM(lom)
			continue
		}

		npg.wi.setWanted(en, lom)
		en.SetFlag(apc.EntryIsCached) // formerly, SetPresent

		if lom.IsChunked() {
			en.SetFlag(apc.EntryIsChunked)
		}
		if post != nil {
			post(lom)
		}

	keep:
		core.FreeLOM(lom)
		lst.Entries[i] = en
		i++
	}

	lst.Entries = lst.Entries[:i]
	return nil
}
