// Package objwalk provides common context and helper methods for object listing and
// object querying.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package objwalk

// This source contains core next-page and next-remote-page methods for list-objects operations.

import (
	"context"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
)

type Walk struct {
	ctx    context.Context
	t      cluster.Target
	bck    *cluster.Bck
	msg    *apc.LsoMsg
	wanted cos.BitFlags
}

func NewWalk(ctx context.Context, t cluster.Target, bck *cluster.Bck, msg *apc.LsoMsg) (w *Walk) {
	w = &Walk{ctx: ctx, t: t, bck: bck, msg: msg}
	w.wanted = wanted(msg)
	return
}

// limited usage: bucket summary, multi-obj
func (w *Walk) NextPageA() (*cmn.LsoResult, error) {
	var (
		lst = &cmn.LsoResult{}
		wi  = NewWalkInfo(w.ctx, w.t, w.msg.Clone())
	)

	cb := func(fqn string, de fs.DirEntry) error {
		entry, err := wi.Callback(fqn, de)
		if entry == nil && err == nil {
			return nil
		}
		if err != nil {
			return cmn.NewErrAborted(w.t.String()+" ResultSetXact", "query", err)
		}
		lst.Entries = append(lst.Entries, entry)
		return nil
	}

	opts := &fs.WalkBckOpts{
		WalkOpts: fs.WalkOpts{CTs: []string{fs.ObjectType}, Callback: cb, Sorted: true},
	}
	opts.WalkOpts.Bck.Copy(w.bck.Bucket())
	opts.ValidateCallback = func(fqn string, de fs.DirEntry) error {
		if de.IsDir() {
			return wi.ProcessDir(fqn)
		}
		return nil
	}

	if err := fs.WalkBck(opts); err != nil {
		return nil, err
	}

	return lst, nil
}

// Returns the next page from the remote bucket's "list-objects" result set.
func (w *Walk) NextPageR() (*cmn.LsoResult, error) {
	debug.Assert(!w.msg.IsFlagSet(apc.LsObjCached))
	lst, _, err := w.t.Backend(w.bck).ListObjects(w.bck, w.msg)
	if err != nil {
		return nil, err
	}
	err = w.Populate(lst)
	return lst, err
}

func (w *Walk) Populate(lst *cmn.LsoResult) error {
	var (
		smap            = w.t.Sowner().Get()
		postCallback, _ = w.ctx.Value(CtxPostCallbackKey).(PostCallbackFunc)
	)
	for _, obj := range lst.Entries {
		si, err := cluster.HrwTarget(w.bck.MakeUname(obj.Name), smap)
		if err != nil {
			return err
		}
		if si.ID() != w.t.SID() {
			continue
		}
		lom := cluster.AllocLOM(obj.Name)
		if err := lom.InitBck(w.bck.Bucket()); err != nil {
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

		setWanted(obj, lom, w.msg.TimeFormat, w.wanted)
		obj.SetPresent()

		if postCallback != nil {
			postCallback(lom)
		}
		cluster.FreeLOM(lom)
	}
	return nil
}
