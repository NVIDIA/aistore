// Package objwalk provides core functionality for listing bucket objects in pages.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package objwalk

import (
	"context"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/objwalk/walkinfo"
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

// DefaultLocalObjPage should be used when there's no need to persist results for a longer period of time.
// It's supposed to be used when results are needed immediately.
func (w *Walk) DefaultLocalObjPage() (*cmn.BucketList, error) {
	var (
		bckList = &cmn.BucketList{}
		wi      = walkinfo.NewWalkInfo(w.ctx, w.t, w.msg)
	)

	cb := func(fqn string, de fs.DirEntry) error {
		entry, err := wi.Callback(fqn, de)
		if entry == nil && err == nil {
			return nil
		}
		if err != nil {
			return cmn.NewErrAborted(w.t.String()+" ResultSetXact", "query", err)
		}
		bckList.Entries = append(bckList.Entries, entry)
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

	return bckList, nil
}

// RemoteObjPage reads a page of objects in a cloud bucket. NOTE: if a request
// wants cached object list, the function returns only local data without
// talking to backend provider.
// After reading cloud object list, the function fills it with information
// that is available only locally(copies, targetURL etc).
func (w *Walk) RemoteObjPage() (*cmn.BucketList, error) {
	if w.msg.IsFlagSet(apc.LsPresent) {
		return w.DefaultLocalObjPage()
	}
	msg := &apc.ListObjsMsg{}
	*msg = *w.msg
	objList, _, err := w.t.Backend(w.bck).ListObjects(w.bck, msg)
	if err != nil {
		return nil, err
	}
	var (
		localURL        = w.t.Snode().URL(cmn.NetPublic)
		localID         = w.t.SID()
		smap            = w.t.Sowner().Get()
		postCallback, _ = w.ctx.Value(walkinfo.CtxPostCallbackKey).(walkinfo.PostCallbackFunc)
		needURL         = w.msg.WantProp(apc.GetTargetURL)
		needAtime       = w.msg.WantProp(apc.GetPropsAtime)
		needCksum       = w.msg.WantProp(apc.GetPropsChecksum)
		needVersion     = w.msg.WantProp(apc.GetPropsVersion)
		needCopies      = w.msg.WantProp(apc.GetPropsCopies)
	)
	for _, e := range objList.Entries {
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

	return objList, nil
}
