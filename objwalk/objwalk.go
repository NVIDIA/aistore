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

// NextRemoteObjPage reads a page of objects in a cloud bucket. If cached object requested,
// the function returns only local data without talking to the backend provider.
// After reading cloud object list, the function fills it with information
// that is available only locally(copies, targetURL etc).
func (w *Walk) NextRemoteObjPage() (*cmn.ListObjects, error) {
	debug.Assert(!w.msg.IsFlagSet(apc.LsObjCached))

	// TODO -- FIXME: needed?
	msg := &apc.ListObjsMsg{}
	cos.CopyStruct(msg, w.msg)

	lst, _, err := w.t.Backend(w.bck).ListObjects(w.bck, msg)
	if err != nil {
		return nil, err
	}
	var (
		localID         = w.t.SID()
		smap            = w.t.Sowner().Get()
		postCallback, _ = w.ctx.Value(CtxPostCallbackKey).(PostCallbackFunc)
	)
	for _, e := range lst.Entries {
		si, err := cluster.HrwTarget(w.bck.MakeUname(e.Name), smap)
		if err != nil {
			return nil, err
		}

		// TODO -- FIXME: check
		if si.ID() != localID {
			continue
		}
		lom := cluster.AllocLOM(e.Name)
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

		setWanted(e, w.t, lom, w.msg.TimeFormat, w.wanted)
		e.SetPresent()

		if postCallback != nil {
			postCallback(lom)
		}
		cluster.FreeLOM(lom)
	}

	return lst, nil
}
