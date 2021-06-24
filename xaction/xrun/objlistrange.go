// Package runners provides implementation for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xrun

import (
	"errors"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/objwalk"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

func isLocalObject(smap *cluster.Smap, b cmn.Bck, objName, sid string) (bool, error) {
	bck := cluster.NewBckEmbed(b)
	si, err := cluster.HrwTarget(bck.MakeUname(objName), smap)
	if err != nil {
		return false, err
	}
	return si.ID() == sid, nil
}

// Try to parse string as template:
// 1. As bash-style: `file-{0..100}`
// 2. As at-style: `file-@100`
// 3. Falls back to just a prefix without number ranges
func parseTemplate(template string) (cos.ParsedTemplate, error) {
	if template == "" {
		return cos.ParsedTemplate{}, errors.New("empty range template")
	}
	if parsed, err := cos.ParseBashTemplate(template); err == nil {
		return parsed, nil
	}
	if parsed, err := cos.ParseAtTemplate(template); err == nil {
		return parsed, nil
	}
	return cos.ParsedTemplate{Prefix: template}, nil
}

//
// Evict/Delete/Prefect
//

func (r *evictDelete) doObjEvictDelete(args *xreg.DeletePrefetchArgs, objName string) error {
	lom := &cluster.LOM{ObjName: objName}
	if err := lom.Init(r.Bck()); err != nil {
		glog.Error(err)
		return nil
	}
	errCode, err := r.t.DeleteObject(args.Ctx, lom, args.Evict)
	if errCode == http.StatusNotFound {
		return nil
	}
	if err != nil {
		if cmn.IsObjNotExist(err) || cmn.IsStatusNotFound(err) {
			return nil
		}
		return err
	}
	r.ObjectsInc()
	r.BytesAdd(lom.SizeBytes(true)) // was loaded and evicted
	return nil
}

func (r *evictDelete) listOperation(args *xreg.DeletePrefetchArgs, listMsg *cmn.ListMsg) error {
	return r.iterateList(args, listMsg, r.doObjEvictDelete)
}

func (r *evictDelete) iterateBucketRange(args *xreg.DeletePrefetchArgs) error {
	return r.iterateRange(args, r.doObjEvictDelete)
}

func (r *prefetch) prefetchMissing(args *xreg.DeletePrefetchArgs, objName string) error {
	lom := &cluster.LOM{ObjName: objName}
	err := lom.Init(r.Bck())
	if err != nil {
		return err
	}
	var coldGet bool
	if err = lom.Load(true /*cache it*/, false /*locked*/); err != nil {
		coldGet = cmn.IsErrObjNought(err)
		if !coldGet {
			return err
		}
	}
	if lom.Bck().IsAIS() { // must not come here
		if coldGet {
			glog.Errorf("prefetch: %s", lom)
		}
		return nil
	}
	if !coldGet && lom.Version() != "" && lom.VersionConf().ValidateWarmGet {
		if coldGet, _, err = r.t.CheckRemoteVersion(args.Ctx, lom); err != nil {
			return err
		}
	}
	if !coldGet {
		return nil
	}
	// Do not set Atime to current time as prefetching does not mean the object
	// was used. At the same time, zero Atime make the lom life-span in the cache
	// too short - the first housekeeping removes it. Set the special value:
	// negatve Now() for correct processing the LOM while housekeeping
	lom.SetAtimeUnix(-time.Now().UnixNano())
	if _, err = r.t.GetCold(args.Ctx, lom, cluster.Prefetch); err != nil {
		if err != cmn.ErrSkip {
			return err
		}
		return nil
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("prefetch: %s", lom)
	}
	r.ObjectsInc()
	r.BytesAdd(lom.SizeBytes())
	return nil
}

func (r *prefetch) listOperation(args *xreg.DeletePrefetchArgs, listMsg *cmn.ListMsg) error {
	return r.iterateList(args, listMsg, r.prefetchMissing)
}

func (r *prefetch) iterateBucketRange(args *xreg.DeletePrefetchArgs) error {
	return r.iterateRange(args, r.prefetchMissing)
}

//
// Common methods
//

func (r *listRangeBase) iterateRange(args *xreg.DeletePrefetchArgs, cb objCallback) error {
	cos.Assert(args.RangeMsg != nil)
	pt, err := parseTemplate(args.RangeMsg.Template)
	if err != nil {
		return err
	}

	smap := r.t.Sowner().Get()
	if len(pt.Ranges) != 0 {
		return r.iterateTemplate(args, smap, &pt, cb)
	}
	return r.iteratePrefix(args, smap, pt.Prefix, cb)
}

func (r *listRangeBase) iterateTemplate(args *xreg.DeletePrefetchArgs, smap *cluster.Smap, pt *cos.ParsedTemplate, cb objCallback) error {
	var (
		getNext = pt.Iter()
		sid     = r.t.SID()
	)
	for objName, hasNext := getNext(); !r.Aborted() && hasNext; objName, hasNext = getNext() {
		if r.Aborted() {
			return nil
		}
		local, err := isLocalObject(smap, r.Bck(), objName, sid)
		if err != nil {
			return err
		}
		if !local {
			continue
		}
		if err := cb(args, objName); err != nil {
			return err
		}
	}
	return nil
}

func (r *listRangeBase) iteratePrefix(args *xreg.DeletePrefetchArgs, smap *cluster.Smap, prefix string, cb objCallback) error {
	var (
		objList *cmn.BucketList
		sid     = r.t.SID()
		err     error
	)

	bck := cluster.NewBckEmbed(r.Bck())
	if err := bck.Init(r.t.Bowner()); err != nil {
		return err
	}

	msg := &cmn.SelectMsg{Prefix: prefix, Props: cmn.GetPropsStatus}
	for !r.Aborted() {
		if bck.IsAIS() {
			walk := objwalk.NewWalk(args.Ctx, r.t, bck, msg)
			objList, err = walk.DefaultLocalObjPage(msg)
		} else {
			objList, _, err = r.t.Backend(bck).ListObjects(args.Ctx, bck, msg)
		}
		if err != nil {
			return err
		}
		for _, be := range objList.Entries {
			if !be.IsStatusOK() {
				continue
			}
			if r.Aborted() {
				return nil
			}
			if bck.IsRemote() {
				local, err := isLocalObject(smap, r.Bck(), be.Name, sid)
				if err != nil {
					return err
				}
				if !local {
					continue
				}
			}

			if err := cb(args, be.Name); err != nil {
				return err
			}
		}

		// Stop when the last page is reached.
		if objList.ContinuationToken == "" {
			break
		}

		// Update `ContinuationToken` for the next request.
		msg.ContinuationToken = objList.ContinuationToken
	}
	return nil
}

func (r *listRangeBase) iterateList(args *xreg.DeletePrefetchArgs, listMsg *cmn.ListMsg, cb objCallback) error {
	var (
		smap = r.t.Sowner().Get()
		sid  = r.t.SID()
	)
	for _, obj := range listMsg.ObjNames {
		if r.Aborted() {
			break
		}
		local, err := isLocalObject(smap, r.Bck(), obj, sid)
		if err != nil {
			return err
		}
		if !local {
			continue
		}
		if err := cb(args, obj); err != nil {
			return err
		}
	}
	return nil
}
