// Package objwalk provides core functionality for reading the list of a bucket objects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package objwalk

import (
	"context"
	"io"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/objwalk/walkinfo"
	"github.com/NVIDIA/aistore/query"
)

type (
	Walk struct {
		ctx context.Context
		t   cluster.Target
		bck *cluster.Bck
		msg *cmn.SelectMsg
	}
)

func NewWalk(ctx context.Context, t cluster.Target, bck *cluster.Bck, msg *cmn.SelectMsg) *Walk {
	return &Walk{
		ctx: ctx,
		t:   t,
		bck: bck,
		msg: msg,
	}
}

// DefaultLocalObjPage should be used when there's no need to persist results for a longer period of time.
// It's supposed to be used when results are needed immediately.
func (w *Walk) DefaultLocalObjPage(msg *cmn.SelectMsg) (*cmn.BucketList, error) {
	var (
		objSrc = &query.ObjectsSource{}
		bckSrc = &query.BucketSource{Bck: w.bck}
		q      = query.NewQuery(objSrc, bckSrc, nil)
	)

	msg.UUID = cos.GenUUID()
	xact := query.NewObjectsListing(w.ctx, w.t, q, msg)
	go xact.Run()

	cos.Assert(!xact.TokenUnsatisfiable(msg.ContinuationToken))
	return LocalObjPage(xact, msg.PageSize)
}

// LocalObjPage walks local filesystems and collects objects for a given
// bucket based on query.ObjectsListingXact. The bucket can be local or cloud one. In latter case the
// function returns the list of cloud objects cached locally
func LocalObjPage(xact *query.ObjectsListingXact, objectsCnt uint) (*cmn.BucketList, error) {
	var err error

	entries, err := xact.NextN(objectsCnt)
	list := &cmn.BucketList{Entries: entries}
	if err != nil && err != io.EOF {
		return nil, err
	}

	return list, nil
}

// RemoteObjPage reads a page of objects in a cloud bucket. NOTE: if a request
// wants cached object list, the function returns only local data without
// talking to backend provider.
// After reading cloud object list, the function fills it with information
// that is available only locally(copies, targetURL etc).
func (w *Walk) RemoteObjPage() (*cmn.BucketList, error) {
	if w.msg.IsFlagSet(cmn.SelectCached) {
		return w.DefaultLocalObjPage(w.msg)
	}
	msg := &cmn.SelectMsg{}
	*msg = *w.msg
	objList, _, err := w.t.Backend(w.bck).ListObjects(w.ctx, w.bck, msg)
	if err != nil {
		return nil, err
	}
	var (
		localURL        = w.t.Snode().URL(cmn.NetworkPublic)
		localID         = w.t.SID()
		smap            = w.t.Sowner().Get()
		postCallback, _ = w.ctx.Value(walkinfo.CtxPostCallbackKey).(walkinfo.PostCallbackFunc)
		needURL         = w.msg.WantProp(cmn.GetTargetURL)
		needAtime       = w.msg.WantProp(cmn.GetPropsAtime)
		needCksum       = w.msg.WantProp(cmn.GetPropsChecksum)
		needVersion     = w.msg.WantProp(cmn.GetPropsVersion)
		needCopies      = w.msg.WantProp(cmn.GetPropsCopies)
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
		if err := lom.Init(w.bck.Bck); err != nil {
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
