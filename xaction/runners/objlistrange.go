// Package runners provides implementation for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package runners

import (
	"errors"
	"net/http"
	"os"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/objwalk"
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
func parseTemplate(template string) (cmn.ParsedTemplate, error) {
	if template == "" {
		return cmn.ParsedTemplate{}, errors.New("empty range template")
	}

	if parsed, err := cmn.ParseBashTemplate(template); err == nil {
		return parsed, nil
	}
	if parsed, err := cmn.ParseAtTemplate(template); err == nil {
		return parsed, nil
	}
	return cmn.ParsedTemplate{Prefix: template}, nil
}

//
// Evict/Delete/Prefect
//

func (r *EvictDelete) objDelete(args *DeletePrefetchArgs, lom *cluster.LOM) error {
	var (
		cloudErr   error
		delFromAIS bool
	)
	lom.Lock(true)
	defer lom.Unlock(true)

	var (
		bck          = lom.Bck()
		delFromCloud = bck.IsRemote() && !args.Evict
	)
	if err := lom.Load(false); err == nil {
		delFromAIS = true
	} else if !cmn.IsErrObjNought(err) {
		return err
	}

	if delFromCloud {
		if err, _ := r.t.Cloud(bck).DeleteObj(args.Ctx, lom); err != nil {
			cloudErr = err
		}
	}
	if delFromAIS {
		errRet := lom.Remove()
		if errRet != nil {
			if !os.IsNotExist(errRet) {
				if cloudErr != nil {
					glog.Errorf("%s: failed to delete from cloud: %v", lom, cloudErr)
				}
				return errRet
			}
		}
		if args.Evict {
			cmn.Assert(bck.IsRemote())
		}
	}
	return cloudErr
}

func (r *EvictDelete) doObjEvictDelete(args *DeletePrefetchArgs, objName string) error {
	lom := &cluster.LOM{T: r.t, ObjName: objName}
	err := lom.Init(r.Bck())
	if err != nil {
		glog.Error(err)
		return nil
	}
	err = r.objDelete(args, lom)
	if err != nil {
		if cmn.IsObjNotExist(err) {
			return nil
		}
		httpErr, ok := err.(*cmn.HTTPError)
		if ok && httpErr.Status == http.StatusNotFound {
			return nil
		}
		return err
	}
	r.ObjectsInc()
	r.BytesAdd(lom.Size())
	return nil
}

func (r *EvictDelete) listOperation(args *DeletePrefetchArgs, listMsg *cmn.ListMsg) error {
	return r.iterateList(args, listMsg, r.doObjEvictDelete)
}

func (r *EvictDelete) iterateBucketRange(args *DeletePrefetchArgs) error {
	return r.iterateRange(args, r.doObjEvictDelete)
}

func (r *Prefetch) prefetchMissing(args *DeletePrefetchArgs, objName string) error {
	var coldGet bool
	lom := &cluster.LOM{T: r.t, ObjName: objName}
	err := lom.Init(r.Bck())
	if err != nil {
		return err
	}
	if err = lom.Load(); err != nil {
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
		if coldGet, err, _ = r.t.CheckCloudVersion(args.Ctx, lom); err != nil {
			return err
		}
	}
	if !coldGet {
		return nil
	}
	if err, _ = r.t.GetCold(args.Ctx, lom, true); err != nil {
		if !errors.Is(err, cmn.ErrSkip) {
			return err
		}
		return nil
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("prefetch: %s", lom)
	}
	r.ObjectsInc()
	r.BytesAdd(lom.Size())
	return nil
}

func (r *Prefetch) listOperation(args *DeletePrefetchArgs, listMsg *cmn.ListMsg) error {
	return r.iterateList(args, listMsg, r.prefetchMissing)
}

func (r *Prefetch) iterateBucketRange(args *DeletePrefetchArgs) error {
	return r.iterateRange(args, r.prefetchMissing)
}

//
// Common methods
//

func (r *listRangeBase) iterateRange(args *DeletePrefetchArgs, cb objCallback) error {
	cmn.Assert(args.RangeMsg != nil)
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

func (r *listRangeBase) iterateTemplate(args *DeletePrefetchArgs, smap *cluster.Smap, pt *cmn.ParsedTemplate, cb objCallback) error {
	var (
		getNext = pt.Iter()
		sid     = r.t.Snode().ID()
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

func (r *listRangeBase) iteratePrefix(args *DeletePrefetchArgs, smap *cluster.Smap, prefix string, cb objCallback) error {
	var (
		objList *cmn.BucketList
		sid     = r.t.Snode().ID()
		err     error
	)

	bck := cluster.NewBckEmbed(r.Bck())
	if err := bck.Init(r.t.Bowner(), r.t.Snode()); err != nil {
		return err
	}

	msg := &cmn.SelectMsg{Prefix: prefix, Props: cmn.GetPropsStatus}
	for !r.Aborted() {
		if bck.IsAIS() {
			walk := objwalk.NewWalk(args.Ctx, r.t, bck, msg)
			objList, err = walk.DefaultLocalObjPage(msg)
		} else {
			objList, err, _ = r.t.Cloud(bck).ListObjects(args.Ctx, bck, msg)
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

func (r *listRangeBase) iterateList(args *DeletePrefetchArgs, listMsg *cmn.ListMsg, cb objCallback) error {
	var (
		smap = r.t.Sowner().Get()
		sid  = r.t.Snode().ID()
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
