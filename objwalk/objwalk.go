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
	"github.com/NVIDIA/aistore/fs"
)

type (
	Walk struct {
		ctx context.Context
		t   cluster.Target
		bck *cluster.Bck
		msg *apc.ListObjsMsg
	}
)

func NewWalk(ctx context.Context, t cluster.Target, bck *cluster.Bck, msg *apc.ListObjsMsg) *Walk {
	return &Walk{ctx: ctx, t: t, bck: bck, msg: msg}
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
	if w.msg.IsFlagSet(apc.LsObjCached) {
		return w.NextObjPage()
	}
	msg := &apc.ListObjsMsg{}
	cos.CopyStruct(msg, w.msg)
	lst, _, err := w.t.Backend(w.bck).ListObjects(w.bck, msg)
	if err != nil {
		return nil, err
	}
	var (
		localURL        = w.t.Snode().URL(cmn.NetPublic)
		localID         = w.t.SID()
		smap            = w.t.Sowner().Get()
		postCallback, _ = w.ctx.Value(CtxPostCallbackKey).(PostCallbackFunc)
		needURL         = w.msg.WantProp(apc.GetTargetURL)
		needAtime       = w.msg.WantProp(apc.GetPropsAtime)
		needCksum       = w.msg.WantProp(apc.GetPropsChecksum)
		needVersion     = w.msg.WantProp(apc.GetPropsVersion)
		needCopies      = w.msg.WantProp(apc.GetPropsCopies)
	)
	for _, e := range lst.Entries {
		si, _ := cluster.HrwTarget(w.bck.MakeUname(e.Name), smap)
		if si.ID() != localID {
			continue
		}

		if needURL {
			e.TargetURL = localURL
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
		e.SetExists()
		if needAtime {
			if lom.AtimeUnix() < 0 {
				// Prefetched object - return zero time
				e.Atime = cos.FormatUnixNano(0, w.msg.TimeFormat)
			} else {
				e.Atime = cos.FormatUnixNano(lom.AtimeUnix(), w.msg.TimeFormat)
			}
		}
		if needCksum && lom.Checksum() != nil {
			e.Checksum = lom.Checksum().Value()
		}
		if needVersion && lom.Version() != "" {
			e.Version = lom.Version()
		}
		if needCopies {
			e.Copies = int16(lom.NumCopies())
		}
		if postCallback != nil {
			postCallback(lom)
		}
		cluster.FreeLOM(lom)
	}

	return lst, nil
}
