// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/stats"
	"github.com/json-iterator/go"
)

const (
	prefetchChanSize = 200
	defaultDeadline  = 0
	defaultWait      = false

	//list range message keys
	rangePrefix = "prefix"
	rangeRegex  = "regex"
	rangeKey    = "range"
)

type filesWithDeadline struct {
	ctx      context.Context
	objnames []string
	bucket   string
	deadline time.Time
	done     chan struct{}
}

type listf func(ct context.Context, objects []string, bucket string, deadline time.Duration, done chan struct{}) error

func getCloudBucketPage(ct context.Context, bucket string, msg *cmn.GetMsg) (bucketList *cmn.BucketList, err error) {
	jsbytes, errstr, errcode := getcloudif().listbucket(ct, bucket, msg)
	if errstr != "" {
		return nil, fmt.Errorf("Error listing cloud bucket %s: %d(%s)", bucket, errcode, errstr)
	}
	bucketList = &cmn.BucketList{}
	if err := jsoniter.Unmarshal(jsbytes, bucketList); err != nil {
		return nil, fmt.Errorf("Error unmarshalling BucketList: %v", err)
	}
	return
}

func (t *targetrunner) getOpFromActionMsg(action string) listf {
	switch action {
	case cmn.ActPrefetch:
		return t.addPrefetchList
	case cmn.ActEvict:
		return t.doListEvict
	case cmn.ActDelete:
		return t.doListDelete
	default:
		return nil
	}
}

//======================
//
// Regex Matching Method
//
//======================

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

//=============
//
// Delete/Evict
//
//=============

func (t *targetrunner) doListEvictDelete(ct context.Context, evict bool, objs []string, bucket string, deadline time.Duration, done chan struct{}) error {
	xdel := t.xactions.newEvictDelete(evict)
	defer func() {
		if done != nil {
			done <- struct{}{}
		}
		xdel.EndTime(time.Now())
	}()

	var absdeadline time.Time
	if deadline != 0 {
		// 0 is no deadline - if deadline == 0, the absolute deadline is 0 time.
		absdeadline = time.Now().Add(deadline)
	}

	for _, objname := range objs {
		select {
		case <-xdel.ChanAbort():
			return nil
		default:
		}
		// skip when deadline has expired
		if !absdeadline.IsZero() && time.Now().After(absdeadline) {
			continue
		}
		err := t.fildelete(ct, bucket, objname, evict)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *targetrunner) doListDelete(ct context.Context, objs []string, bucket string, deadline time.Duration, done chan struct{}) error {
	return t.doListEvictDelete(ct, false /* evict */, objs, bucket, deadline, done)
}

func (t *targetrunner) doListEvict(ct context.Context, objs []string, bucket string, deadline time.Duration, done chan struct{}) error {
	return t.doListEvictDelete(ct, true /* evict */, objs, bucket, deadline, done)
}

//=========
//
// Prefetch
//
//=========

func (t *targetrunner) prefetchMissing(ct context.Context, objname, bucket string) {
	var (
		errstr            string
		vchanged, coldget bool
		lom               = &cluster.LOM{T: t, Bucket: bucket, Objname: objname}
		versioncfg        = &cmn.GCO.Get().Ver
	)
	if errstr = lom.Fill(cluster.LomFstat | cluster.LomVersion | cluster.LomCksum); errstr != "" {
		glog.Error(errstr)
		return
	}
	if lom.Bislocal { // must not come here
		if lom.Doesnotexist {
			glog.Errorf("prefetch: %s", lom)
		}
		return
	}
	coldget = lom.Doesnotexist
	if lom.Exists() && versioncfg.ValidateWarmGet && lom.Version != "" && versioningConfigured(false) {
		if vchanged, errstr, _ = t.checkCloudVersion(ct, bucket, objname, lom.Version); errstr != "" {
			return
		}
		coldget = vchanged
	}
	if !coldget {
		return
	}
	if errstr, _ = t.getCold(ct, lom, true); errstr != "" {
		if errstr != "skip" {
			glog.Errorln(errstr)
		}
		return
	}
	if glog.V(4) {
		glog.Infof("prefetch: %s", lom)
	}
	t.statsif.Add(stats.PrefetchCount, 1)
	t.statsif.Add(stats.PrefetchSize, lom.Size)
	if vchanged {
		t.statsif.Add(stats.VerChangeSize, lom.Size)
		t.statsif.Add(stats.VerChangeCount, 1)
	}
}

func (t *targetrunner) addPrefetchList(ct context.Context, objs []string, bucket string,
	deadline time.Duration, done chan struct{}) error {
	if t.bmdowner.get().IsLocal(bucket) {
		return fmt.Errorf("Cannot prefetch from a local bucket: %s", bucket)
	}
	var absdeadline time.Time
	if deadline != 0 {
		// 0 is no deadline - if deadline == 0, the absolute deadline is 0 time.
		absdeadline = time.Now().Add(deadline)
	}
	t.prefetchQueue <- filesWithDeadline{ctx: ct, objnames: objs, bucket: bucket, deadline: absdeadline, done: done}
	return nil
}

//================
//
// Message Parsing
//
//================

func unmarshalMsgValue(jsmap map[string]interface{}, key string) (val string, errstr string) {
	v, ok := jsmap[key]
	if !ok {
		errstr = fmt.Sprintf("no %s field in map", key)
		return
	}
	if val, ok = v.(string); !ok {
		errstr = fmt.Sprintf("value ((%+v, %T) corresponding to key (%s) in map is not of string type", v, v, key)
	}
	return
}

func parseBaseMsg(jsmap map[string]interface{}) (pbm *cmn.ListRangeMsgBase, errstr string) {
	const s = "Error parsing BaseMsg:"
	pbm = &cmn.ListRangeMsgBase{Deadline: defaultDeadline, Wait: defaultWait}
	if v, ok := jsmap["deadline"]; ok {
		deadline, err := time.ParseDuration(v.(string))
		if err != nil {
			return pbm, fmt.Sprintf("%s (Deadline: %v, %T, %v)", s, v, v, err)
		}
		pbm.Deadline = deadline
	}
	if v, ok := jsmap["wait"]; ok {
		wait, ok := v.(bool)
		if !ok {
			return pbm, fmt.Sprintf("%s (Wait: %v, %T)", s, v, v)
		}
		pbm.Wait = wait
	}
	return
}

func parseListMsg(jsmap map[string]interface{}) (pm *cmn.ListMsg, errstr string) {
	const s = "Error parsing ListMsg: "
	pbm, errstr := parseBaseMsg(jsmap)
	if errstr != "" {
		return
	}
	pm = &cmn.ListMsg{ListRangeMsgBase: *pbm}
	v, ok := jsmap["objnames"]
	if !ok {
		return pm, s + "No objnames field"
	}
	if objnames, ok := v.([]interface{}); ok {
		pm.Objnames = make([]string, 0, len(objnames))
		for _, obj := range objnames {
			objname, ok := obj.(string)
			if !ok {
				return pm, s + "Non-string Object Name"
			}
			pm.Objnames = append(pm.Objnames, objname)
		}
	} else {
		return pm, s + "Couldn't parse objnames"
	}
	return
}

func parseRangeMsg(jsmap map[string]interface{}) (pm *cmn.RangeMsg, errstr string) {
	const s = "Error parsing RangeMsg: %s"
	pbm, errstr := parseBaseMsg(jsmap)
	if errstr != "" {
		return
	}
	pm = &cmn.RangeMsg{ListRangeMsgBase: *pbm}

	prefix, errstr := unmarshalMsgValue(jsmap, rangePrefix)
	if errstr != "" {
		return pm, fmt.Sprintf(s, errstr)
	}
	pm.Prefix = prefix

	regex, errstr := unmarshalMsgValue(jsmap, rangeRegex)
	if errstr != "" {
		return pm, fmt.Sprintf(s, errstr)
	}
	pm.Regex = regex

	r, errstr := unmarshalMsgValue(jsmap, rangeKey)
	if errstr != "" {
		return pm, fmt.Sprintf(s, errstr)
	}
	pm.Range = r

	return
}

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

//=======================================================================
//
// Method called by target to execute 1) prefetch, 2) evict, or 3) delete
//
//=======================================================================

func (t *targetrunner) listRangeOperation(r *http.Request, apitems []string, msg cmn.ActionMsg) error {
	operation := t.getOpFromActionMsg(msg.Action)
	if operation == nil {
		return fmt.Errorf("Invalid Operation")
	}

	detail := fmt.Sprintf(" (%s, %s, %T)", msg.Action, msg.Name, msg.Value)
	jsmap, ok := msg.Value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid cmn.ActionMsg.Value format" + detail)
	}
	if _, ok := jsmap["objnames"]; !ok {
		// Parse map into RangeMsg, convert to and process ListMsg page-by-page
		rangeMsg, errstr := parseRangeMsg(jsmap)
		if errstr != "" {
			return fmt.Errorf(errstr + detail)
		}
		return t.iterateBucketListPages(r, apitems, rangeMsg, operation)
	}
	// Parse map into ListMsg
	listMsg, errstr := parseListMsg(jsmap)
	if errstr != "" {
		return fmt.Errorf(errstr + detail)
	}
	return t.listOperation(r, apitems, listMsg, operation)
}

func (t *targetrunner) listOperation(r *http.Request, apitems []string, listMsg *cmn.ListMsg, f listf) error {
	var err error
	bucket := apitems[0]
	objs := make([]string, 0, len(listMsg.Objnames))
	for _, obj := range listMsg.Objnames {
		si, errstr := hrwTarget(bucket, obj, t.smapowner.get())
		if errstr != "" {
			return fmt.Errorf(errstr)
		}
		if si.DaemonID == t.si.DaemonID {
			objs = append(objs, obj)
		}
	}

	if len(objs) != 0 {
		done := make(chan struct{}, 1)
		defer close(done)

		errCh := make(chan error)
		defer close(errCh)

		// Asynchronously perform function
		go func() {
			err := f(t.contextWithAuth(r), objs, bucket, listMsg.Deadline, done)
			if err != nil {
				glog.Errorf("Error performing list function: %v", err)
				t.statsif.Add(stats.ErrListCount, 1)
			}
			errCh <- err
		}()

		if listMsg.Wait {
			<-done
			err = <-errCh
		}
	}
	return err
}

func (t *targetrunner) iterateBucketListPages(r *http.Request, apitems []string, rangeMsg *cmn.RangeMsg, operation listf) error {
	var (
		bucketListPage *cmn.BucketList
		err            error
		bucket         = apitems[0]
		prefix         = rangeMsg.Prefix
		ct             = t.contextWithAuth(r)
		msg            = &cmn.GetMsg{GetPrefix: prefix, GetProps: cmn.GetPropsStatus}
		islocal        = t.bmdowner.get().IsLocal(bucket)
	)

	min, max, err := parseRange(rangeMsg.Range)
	if err != nil {
		return fmt.Errorf("Error parsing range string (%s): %v", rangeMsg.Range, err)
	}

	re, err := regexp.Compile(rangeMsg.Regex)
	if err != nil {
		return fmt.Errorf("Could not compile regex: %v", err)
	}

	for {
		if islocal {
			bucketListPage, err = t.prepareLocalObjectList(bucket, msg)
		} else {
			bucketListPage, err = getCloudBucketPage(ct, bucket, msg)
		}
		if err != nil {
			return err
		}
		if len(bucketListPage.Entries) == 0 {
			break
		}

		matchingEntries := make([]string, 0, len(bucketListPage.Entries))
		for _, be := range bucketListPage.Entries {
			if be.Status != cmn.ObjStatusOK {
				continue
			}
			if !acceptRegexRange(be.Name, prefix, re, min, max) {
				continue
			}
			matchingEntries = append(matchingEntries, be.Name)
		}

		if len(matchingEntries) != 0 {
			// Create a ListMsg with a single page of BucketList containing BucketEntries
			listMsg := &cmn.ListMsg{
				ListRangeMsgBase: rangeMsg.ListRangeMsgBase,
				Objnames:         matchingEntries,
			}

			// Call listrange function with paged chunk of entries
			if err := t.listOperation(r, apitems, listMsg, operation); err != nil {
				return err
			}
		}
		// Stop when the last page of BucketList is reached
		if bucketListPage.PageMarker == "" {
			break
		}

		// Update PageMarker for the next request
		msg.GetPageMarker = bucketListPage.PageMarker
	}
	return nil
}
