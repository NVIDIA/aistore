// Package xs contains most of the supported eXtended actions (xactions) with some
// exceptions that include certain storage services (mirror, EC) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package xs

// core next-page and next-remote-page methods for object listing

import (
	"context"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
)

type npgCtx struct {
	ctx    context.Context
	t      cluster.Target
	bck    *cluster.Bck
	msg    *apc.LsoMsg
	wi     *walkInfo
	wanted cos.BitFlags
	page   cmn.LsoResult
}

func newNpgCtx(ctx context.Context, t cluster.Target, bck *cluster.Bck, msg *apc.LsoMsg) (w *npgCtx) {
	w = &npgCtx{ctx: ctx, t: t, bck: bck, msg: msg}
	w.wanted = wanted(msg)
	return
}

// limited usage: bucket summary, multi-obj
func (npg *npgCtx) nextPageA() error {
	debug.Assert(npg.page.Entries == nil)
	npg.page.UUID = npg.msg.UUID
	npg.wi = newWalkInfo(npg.ctx, npg.t, npg.msg.Clone())

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
	return fs.WalkBck(opts)
}

func (npg *npgCtx) cb(fqn string, de fs.DirEntry) error {
	entry, err := npg.wi.callback(fqn, de)
	if entry == nil && err == nil {
		return nil
	}
	if err != nil {
		return cmn.NewErrAborted(npg.t.String()+" ResultSetXact", "query", err)
	}
	npg.page.Entries = append(npg.page.Entries, entry)
	return nil
}

// Returns the next page from the remote bucket's "list-objects" result set.
func (npg *npgCtx) nextPageR() (*cmn.LsoResult, error) {
	debug.Assert(!npg.msg.IsFlagSet(apc.LsObjCached))
	lst, _, err := npg.t.Backend(npg.bck).ListObjects(npg.bck, npg.msg)
	if err != nil {
		return nil, err
	}
	err = npg.populate(lst)
	return lst, err
}

func (npg *npgCtx) populate(lst *cmn.LsoResult) error {
	var (
		smap            = npg.t.Sowner().Get()
		postCallback, _ = npg.ctx.Value(ctxPostCallbackKey).(postCallbackFunc)
	)
	for _, obj := range lst.Entries {
		si, err := cluster.HrwTarget(npg.bck.MakeUname(obj.Name), smap)
		if err != nil {
			return err
		}
		if si.ID() != npg.t.SID() {
			continue
		}
		lom := cluster.AllocLOM(obj.Name)
		if err := lom.InitBck(npg.bck.Bucket()); err != nil {
			cluster.FreeLOM(lom)
			if cmn.IsErrBucketNought(err) {
				return err
			}
			continue
		}
		if err := lom.Load(true /* cache it*/, false /*locked*/); err != nil {
			cluster.FreeLOM(lom)
			continue
		}

		setWanted(obj, lom, npg.msg.TimeFormat, npg.wanted)
		obj.SetPresent()

		if postCallback != nil {
			postCallback(lom)
		}
		cluster.FreeLOM(lom)
	}
	return nil
}
