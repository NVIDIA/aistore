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
	msg    *apc.ListObjsMsg
	wanted cos.BitFlags
}

func NewWalk(ctx context.Context, t cluster.Target, bck *cluster.Bck, msg *apc.ListObjsMsg) (w *Walk) {
	w = &Walk{ctx: ctx, t: t, bck: bck, msg: msg}
	w.wanted = wanted(msg)
	return
}

// NextObjPage can be used when there's no need to retain results for a longer period of time
// (use it when the results are needed immediately)
func (w *Walk) NextObjPage() (*cmn.ListObjects, error) {
	var (
		lst = &cmn.ListObjects{}
		wi  = NewWalkInfo(w.ctx, w.t, w.msg)
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

// Gets next page from the remote bucket's "list-objects" result set.
func (w *Walk) NextRemoteObjPage() (*cmn.ListObjects, error) {
	var (
		smap            = w.t.Sowner().Get()
		postCallback, _ = w.ctx.Value(CtxPostCallbackKey).(PostCallbackFunc)
	)
	debug.Assert(!w.msg.IsFlagSet(apc.LsObjCached))
	lst, _, err := w.t.Backend(w.bck).ListObjects(w.bck, w.msg)
	if err != nil {
		return nil, err
	}
	for _, obj := range lst.Entries {
		si, err := cluster.HrwTarget(w.bck.MakeUname(obj.Name), smap)
		if err != nil {
			return nil, err
		}
		if si.ID() != w.t.SID() {
			continue
		}
		lom := cluster.AllocLOM(obj.Name)
		if err := lom.InitBck(w.bck.Bucket()); err != nil {
			cluster.FreeLOM(lom)
			if cmn.IsErrBucketNought(err) {
				return nil, err
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

	return lst, nil
}
