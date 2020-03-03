// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/objwalk"
)

func parseRange(rangestr string) (min, max int64, err error) {
	if rangestr != "" {
		ranges := strings.Split(rangestr, ":")
		if ranges[0] == "" {
			// Min was not set
			min = 0
		} else {
			min, err = strconv.ParseInt(ranges[0], 10, 64)
			if err != nil {
				return
			}
		}

		if ranges[1] == "" {
			// Max was not set
			max = 0
		} else {
			max, err = strconv.ParseInt(ranges[1], 10, 64)
			if err != nil {
				return
			}
		}
	} else {
		min = 0
		max = 0
	}
	return
}

func acceptRegexRange(name, prefix string, regex *regexp.Regexp, min, max int64) bool {
	oname := strings.TrimPrefix(name, prefix)
	s := regex.FindStringSubmatch(oname)
	if s == nil {
		return false
	}
	// If the regex matches:
	if i, err := strconv.ParseInt(s[0], 10, 64); err != nil && s[0] != "" {
		// If the regex matched a non-empty non-number
		return false
	} else if s[0] == "" || ((min == 0 || i >= min) && (max == 0 || i <= max)) {
		// Either the match is empty, or the match is a number.
		// If the match is a number, either min=0 (unset) or it must be above the minimum, and
		// either max=0 (unset) or ir must be below the maximum
		return true
	}
	return false
}

func (r *EvictDelete) objDelete(ctx context.Context, lom *cluster.LOM, evict bool) error {
	var (
		cloudErr   error
		errRet     error
		delFromAIS bool
	)
	lom.Lock(true)
	defer lom.Unlock(true)

	delFromCloud := lom.Bck().IsCloud() && !evict
	if err := lom.Load(false); err == nil {
		delFromAIS = true
	} else if !cmn.IsObjNotExist(err) || (!delFromCloud && cmn.IsObjNotExist(err)) {
		return err
	}

	if delFromCloud {
		if err, _ := r.t.Cloud().DeleteObj(ctx, lom); err != nil {
			cloudErr = fmt.Errorf("%s: DELETE failed, err: %v", lom, err)
		}
	}
	if delFromAIS {
		errRet = lom.Remove()
		if errRet != nil {
			if !os.IsNotExist(errRet) {
				if cloudErr != nil {
					glog.Errorf("%s: failed to delete from cloud: %v", lom, cloudErr)
				}
				return errRet
			}
		}
		if evict {
			cmn.Assert(lom.Bck().IsCloud())
		}
	}
	if cloudErr != nil {
		return fmt.Errorf("%s: failed to delete from cloud: %v", lom, cloudErr)
	}
	return errRet
}

func (r *EvictDelete) doListEvictDelete(ctx context.Context, objs []string, evict bool) error {
	for _, objName := range objs {
		if r.Aborted() {
			return nil
		}
		lom := &cluster.LOM{T: r.t, Objname: objName}
		err := lom.Init(r.bck.Bck)
		if err != nil {
			glog.Error(err)
			continue
		}
		err = r.objDelete(ctx, lom, evict)
		if err != nil {
			if evict && cmn.IsObjNotExist(err) {
				continue
			}
			return err
		}
		r.ObjectsInc()
		r.BytesAdd(lom.Size())
	}
	return nil
}

func (r *EvictDelete) listOperation(ctx context.Context, listMsg *cmn.ListMsg, evict bool) error {
	var (
		objs = make([]string, 0, len(listMsg.Objnames))
		smap = r.t.GetSowner()
	)
	for _, obj := range listMsg.Objnames {
		si, err := cluster.HrwTarget(r.bck.MakeUname(obj), smap.Get())
		if err != nil {
			return err
		}
		if si.ID() == r.t.Snode().ID() {
			objs = append(objs, obj)
		}
	}

	if len(objs) == 0 {
		return nil
	}

	return r.doListEvictDelete(ctx, objs, evict)
}

func (r *EvictDelete) iterateBucketRange(args *EvictDeleteArgs) error {
	cmn.Assert(args.RangeMsg != nil)
	var (
		bucketListPage *cmn.BucketList
		prefix         = args.RangeMsg.Prefix
		msg            = &cmn.SelectMsg{Prefix: prefix, Props: cmn.GetPropsStatus}
	)

	if err := r.bck.Init(r.t.GetBowner(), r.t.Snode()); err != nil {
		return err
	}

	min, max, err := parseRange(args.RangeMsg.Range)
	if err != nil {
		return fmt.Errorf("error parsing range string (%s): %v", args.RangeMsg.Range, err)
	}

	re, err := regexp.Compile(args.RangeMsg.Regex)
	if err != nil {
		return fmt.Errorf("could not compile range regex: %v", err)
	}

	for !r.Aborted() {
		if r.bck.IsAIS() {
			walk := objwalk.NewWalk(context.Background(), r.t, r.bck.Bck, msg)
			bucketListPage, err = walk.LocalObjPage()
		} else {
			bucketListPage, err, _ = r.t.Cloud().ListBucket(args.Ctx, r.bck.Name, msg)
		}
		if err != nil {
			return err
		}
		if len(bucketListPage.Entries) == 0 {
			break
		}

		matchingEntries := make([]string, 0, len(bucketListPage.Entries))
		for _, be := range bucketListPage.Entries {
			if !be.IsStatusOK() {
				continue
			}
			if !acceptRegexRange(be.Name, prefix, re, min, max) {
				continue
			}
			if r.Aborted() {
				return nil
			}
			matchingEntries = append(matchingEntries, be.Name)
		}

		if len(matchingEntries) != 0 {
			// Create a ListMsg with a single page of BucketList containing BucketEntries
			listMsg := &cmn.ListMsg{
				ListRangeMsgBase: args.RangeMsg.ListRangeMsgBase,
				Objnames:         matchingEntries,
			}

			// Call listrange function with paged chunk of entries
			if err := r.listOperation(args.Ctx, listMsg, args.Evict); err != nil {
				return err
			}
		}
		// Stop when the last page of BucketList is reached
		if bucketListPage.PageMarker == "" {
			break
		}

		// Update PageMarker for the next request
		msg.PageMarker = bucketListPage.PageMarker
	}

	return nil
}
