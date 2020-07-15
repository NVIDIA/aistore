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
func (w *Walk) DefaultLocalObjPage(objectsCnt uint, wi *walkinfo.WalkInfo) (*cmn.BucketList, error) {
	bckSrc := query.BckSource(w.bck.Bck)
	objSrc := &query.ObjectsSource{}
	q := query.NewQuery(objSrc, bckSrc, nil)

	xact := query.NewObjectsListing(w.t, q, wi, cmn.GenUUID())
	go xact.Start()

	cmn.Assert(!xact.PageMarkerUnsatisfiable(wi.Marker))
	return LocalObjPage(xact, objectsCnt)
}

// LocalObjPage walks local filesystems and collects objects for a given
// bucket based on query.ObjectsListingXact. The bucket can be local or cloud one. In latter case the
// function returns the list of cloud objects cached locally
func LocalObjPage(xact *query.ObjectsListingXact, objectsCnt uint) (*cmn.BucketList, error) {
	var (
		err error
	)

	entries, err := xact.NextN(objectsCnt)
	list := &cmn.BucketList{Entries: entries}
	if err != nil && err != io.EOF {
		return nil, err
	}

	return list, nil
}

// CloudObjPage reads a page of objects in a cloud bucket. NOTE: if a request
// wants cached object list, the function returns only local data without
// talking to cloud provider.
// After reading cloud object list, the function fills it with information
// that is available only locally(copies, targetURL etc).
func (w *Walk) CloudObjPage() (*cmn.BucketList, error) {
	if w.msg.Cached {
		return w.DefaultLocalObjPage(w.msg.WantObjectsCnt(), walkinfo.NewDefaultWalkInfo(w.t))
	}

	msg := &cmn.SelectMsg{}
	*msg = *w.msg
	msg.UUID = ""

	objList, err, _ := w.t.Cloud(w.bck).ListObjects(w.ctx, w.bck, msg)
	if err != nil {
		return nil, err
	}

	var (
		config          = cmn.GCO.Get()
		localURL        = w.t.Snode().URL(cmn.NetworkPublic)
		localID         = w.t.Snode().ID()
		smap            = w.t.GetSowner().Get()
		postCallback, _ = w.ctx.Value(walkinfo.CtxPostCallbackKey).(walkinfo.PostCallbackFunc)

		needURL     = w.msg.WantProp(cmn.GetTargetURL)
		needAtime   = w.msg.WantProp(cmn.GetPropsAtime)
		needCksum   = w.msg.WantProp(cmn.GetPropsChecksum)
		needVersion = w.msg.WantProp(cmn.GetPropsVersion)
		needCopies  = w.msg.WantProp(cmn.GetPropsCopies)
	)

	for _, e := range objList.Entries {
		si, _ := cluster.HrwTarget(w.bck.MakeUname(e.Name), smap)
		if si.ID() != localID {
			continue
		}

		if needURL {
			e.TargetURL = localURL
		}
		lom := &cluster.LOM{T: w.t, ObjName: e.Name}
		err := lom.Init(w.bck.Bck, config)
		if err != nil {
			if cmn.IsErrBucketNought(err) {
				return nil, err
			}
			continue
		}
		err = lom.Load()
		if err != nil {
			continue
		}

		e.SetExists()
		if needAtime {
			e.Atime = cmn.FormatUnixNano(lom.AtimeUnix(), w.msg.TimeFormat)
		}
		if needCksum && lom.Cksum() != nil {
			_, storedCksum := lom.Cksum().Get()
			e.Checksum = storedCksum
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
	}

	return objList, nil
}
