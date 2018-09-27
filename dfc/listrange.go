// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
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
	"github.com/NVIDIA/dfcpub/api"
	"github.com/json-iterator/go"
)

const (
	prefetchChanSize = 200
	defaultDeadline  = 0
	defaultWait      = false
	maxPrefetchPages = 10 // FIXME: Pagination for PREFETCH

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

type xactPrefetch struct {
	xactBase
	targetrunner *targetrunner
}

type xactEvictDelete struct {
	xactBase
	targetrunner *targetrunner
}

//===========================
//
// Generic List/Range Methods
//
//===========================

func (t *targetrunner) getListFromRangeCloud(ct context.Context, bucket string, msg *api.GetMsg) (bucketList *api.BucketList, err error) {
	bucketList = &api.BucketList{Entries: make([]*api.BucketEntry, 0)}
	for i := 0; i < maxPrefetchPages; i++ {
		jsbytes, errstr, errcode := getcloudif().listbucket(ct, bucket, msg)
		if errstr != "" {
			return nil, fmt.Errorf("Error listing cloud bucket %s: %d(%s)", bucket, errcode, errstr)
		}
		reslist := &api.BucketList{}
		if err := jsoniter.Unmarshal(jsbytes, reslist); err != nil {
			return nil, fmt.Errorf("Error unmarshalling BucketList: %v", err)
		}
		bucketList.Entries = append(bucketList.Entries, reslist.Entries...)
		if reslist.PageMarker == "" {
			break
		} else if i == maxPrefetchPages-1 {
			glog.Warningf("Did not prefetch all keys (More than %d pages)", maxPrefetchPages)
		}
		msg.GetPageMarker = reslist.PageMarker
	}

	return
}

func (t *targetrunner) getListFromRange(ct context.Context, bucket, prefix, regex string, min, max int64) ([]string, error) {
	msg := &api.GetMsg{GetPrefix: prefix}
	var (
		fullbucketlist *api.BucketList
		err            error
	)
	islocal := t.bmdowner.get().islocal(bucket)
	if islocal {
		fullbucketlist, err = t.prepareLocalObjectList(bucket, msg)
	} else {
		fullbucketlist, err = t.getListFromRangeCloud(ct, bucket, msg)
	}
	if err != nil {
		return nil, err
	}

	objs := make([]string, 0, len(fullbucketlist.Entries))
	re, err := regexp.Compile(regex)
	if err != nil {
		return nil, fmt.Errorf("Could not compile regex: %v", err)
	}
	for _, be := range fullbucketlist.Entries {
		if !acceptRegexRange(be.Name, prefix, re, min, max) {
			continue
		}
		if si, errstr := HrwTarget(bucket, be.Name, t.smapowner.get()); si == nil || si.DaemonID == t.si.DaemonID {
			if errstr != "" {
				return nil, fmt.Errorf(errstr)
			}
			objs = append(objs, be.Name)
		}
	}

	return objs, nil
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

type listf func(ct context.Context, objects []string, bucket string, deadline time.Duration, done chan struct{}) error

func (t *targetrunner) listOperation(r *http.Request, apitems []string, listMsg *api.ListMsg, operation listf) error {
	bucket := apitems[0]
	objs := make([]string, 0, len(listMsg.Objnames))
	for _, obj := range listMsg.Objnames {
		si, errstr := HrwTarget(bucket, obj, t.smapowner.get())
		if errstr != "" {
			return fmt.Errorf(errstr)
		}
		if si.DaemonID == t.si.DaemonID {
			objs = append(objs, obj)
		}
	}
	if len(objs) != 0 {
		var done chan struct{}
		if listMsg.Wait {
			done = make(chan struct{}, 1)
		}

		// Asynchronously perform operation
		go func() {
			if err := operation(t.contextWithAuth(r), objs, bucket, listMsg.Deadline, done); err != nil {
				glog.Errorf("Error performing list operation: %v", err)
				t.statsif.add(statErrListCount, 1)
			}
		}()

		if listMsg.Wait {
			<-done
			close(done)
		}
	}
	return nil
}

//=============
//
// Delete/Evict
//
//=============

func (t *targetrunner) doListEvictDelete(ct context.Context, evict bool, objs []string, bucket string, deadline time.Duration, done chan struct{}) error {
	xdel := t.xactinp.newEvictDelete(evict)
	defer func() {
		if done != nil {
			done <- struct{}{}
		}
		t.xactinp.del(xdel.id)
	}()

	var absdeadline time.Time
	if deadline != 0 {
		// 0 is no deadline - if deadline == 0, the absolute deadline is 0 time.
		absdeadline = time.Now().Add(deadline)
	}

	for _, objname := range objs {
		select {
		case <-xdel.abrt:
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

// Creates and returns a new extended action after appending it to an array of extended actions in progress
func (q *xactInProgress) newEvictDelete(evict bool) *xactEvictDelete {
	q.lock.Lock()
	defer q.lock.Unlock()

	xact := api.ActDelete
	if evict {
		xact = api.ActEvict
	}

	id := q.uniqueid()
	xpre := &xactEvictDelete{xactBase: *newxactBase(id, xact)}
	q.add(xpre)
	return xpre
}

func (xact *xactEvictDelete) tostring() string {
	start := xact.stime.Sub(xact.targetrunner.starttime())
	if !xact.finished() {
		return fmt.Sprintf("xaction %s:%d started %v", xact.kind, xact.id, start)
	}
	fin := time.Since(xact.targetrunner.starttime())
	return fmt.Sprintf("xaction %s:%d started %v finished %v", xact.kind, xact.id, start, fin)
}

//=========
//
// Prefetch
//
//=========

func (t *targetrunner) doPrefetch() {
	xpre := t.xactinp.renewPrefetch(t)
	if xpre == nil {
		return
	}
loop:
	for {
		select {
		case fwd := <-t.prefetchQueue:
			if !fwd.deadline.IsZero() && time.Now().After(fwd.deadline) {
				continue
			}
			bucket := fwd.bucket
			for _, objname := range fwd.objnames {
				t.prefetchMissing(fwd.ctx, objname, bucket)
			}

			// Signal completion of prefetch
			if fwd.done != nil {
				fwd.done <- struct{}{}
			}
		default:
			// When there is nothing left to fetch, the prefetch routine ends
			break loop

		}
	}

	xpre.etime = time.Now()
	t.xactinp.del(xpre.id)
}

func (t *targetrunner) prefetchMissing(ct context.Context, objname, bucket string) {
	var (
		errstr, version   string
		vchanged, coldget bool
		props             *objectProps
	)
	versioncfg := &ctx.config.Ver
	islocal := t.bmdowner.get().islocal(bucket)
	fqn, errstr := t.fqn(bucket, objname, islocal)
	if errstr != "" {
		glog.Error(errstr)
		return
	}
	//
	// NOTE: lockless
	//
	coldget, _, version, errstr = t.lookupLocally(bucket, objname, fqn)
	if (errstr != "" && !coldget) || (errstr != "" && coldget && islocal) {
		glog.Errorln(errstr)
		return
	}
	if !coldget && !islocal && versioncfg.ValidateWarmGet && version != "" && t.versioningConfigured(bucket) {
		if vchanged, errstr, _ = t.checkCloudVersion(ct, bucket, objname, version); errstr != "" {
			return
		}
		coldget = vchanged
	}
	if !coldget {
		return
	}
	if props, errstr, _ = t.coldget(ct, bucket, objname, true); errstr != "" {
		if errstr != "skip" {
			glog.Errorln(errstr)
		}
		return
	}
	if glog.V(4) {
		glog.Infof("PREFETCH: %s/%s", bucket, objname)
	}
	t.statsif.add(statPrefetchCount, 1)
	t.statsif.add(statPrefetchSize, props.size)
	if vchanged {
		t.statsif.add(statVerChangeSize, props.size)
		t.statsif.add(statVerChangeCount, 1)
	}
}

func (t *targetrunner) addPrefetchList(ct context.Context, objs []string, bucket string,
	deadline time.Duration, done chan struct{}) error {
	if t.bmdowner.get().islocal(bucket) {
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

func (q *xactInProgress) renewPrefetch(t *targetrunner) *xactPrefetch {
	q.lock.Lock()
	defer q.lock.Unlock()
	_, xx := q.findU(api.ActPrefetch)
	if xx != nil {
		xpre := xx.(*xactPrefetch)
		glog.Infof("%s already running, nothing to do", xpre.tostring())
		return nil
	}
	id := q.uniqueid()
	xpre := &xactPrefetch{xactBase: *newxactBase(id, api.ActPrefetch)}
	xpre.targetrunner = t
	q.add(xpre)
	return xpre
}

func (xact *xactPrefetch) tostring() string {
	start := xact.stime.Sub(xact.targetrunner.starttime())
	if !xact.finished() {
		return fmt.Sprintf("xaction %s:%d started %v", xact.kind, xact.id, start)
	}
	fin := time.Since(xact.targetrunner.starttime())
	return fmt.Sprintf("xaction %s:%d started %v finished %v", xact.kind, xact.id, start, fin)
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

func parseBaseMsg(jsmap map[string]interface{}) (pbm *api.ListRangeMsgBase, errstr string) {
	const s = "Error parsing BaseMsg:"
	pbm = &api.ListRangeMsgBase{Deadline: defaultDeadline, Wait: defaultWait}
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

func parseListMsg(jsmap map[string]interface{}) (pm *api.ListMsg, errstr string) {
	const s = "Error parsing ListMsg: "
	pbm, errstr := parseBaseMsg(jsmap)
	if errstr != "" {
		return
	}
	pm = &api.ListMsg{ListRangeMsgBase: *pbm}
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

func parseRangeMsg(jsmap map[string]interface{}) (pm *api.RangeMsg, errstr string) {
	const s = "Error parsing RangeMsg: %s"
	pbm, errstr := parseBaseMsg(jsmap)
	if errstr != "" {
		return
	}
	pm = &api.RangeMsg{ListRangeMsgBase: *pbm}

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

// Converts a parsed rangeMsg to a listMsg to reduce redundancy of methods
func (t *targetrunner) convertMsg(r *http.Request, apitems []string, rangeMsg *api.RangeMsg) (listMsg *api.ListMsg, err error) {
	bucket := apitems[0]
	min, max, err := parseRange(rangeMsg.Range)
	if err != nil {
		err = fmt.Errorf("Error parsing range string (%s): %v", rangeMsg.Range, err)
		return
	}

	objs, err := t.getListFromRange(t.contextWithAuth(r), bucket, rangeMsg.Prefix, rangeMsg.Regex, min, max)
	if err != nil {
		err = fmt.Errorf("Error converting range to list: %v", err)
		return
	}

	listMsg = &api.ListMsg{
		ListRangeMsgBase: rangeMsg.ListRangeMsgBase,
		Objnames:         objs,
	}
	return
}

//=======================================================================
//
// Method called by target to execute 1) prefetch, 2) evict, or 3) delete
//
//=======================================================================

func (t *targetrunner) listRangeOperation(r *http.Request, apitems []string, msg api.ActionMsg) error {
	var listMsg *api.ListMsg

	detail := fmt.Sprintf(" (%s, %s, %T)", msg.Action, msg.Name, msg.Value)
	jsmap, ok := msg.Value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid api.ActionMsg.Value format" + detail)
	}
	if _, ok := jsmap["objnames"]; !ok {
		// Parse map into RangeMsg and convert to ListMsg
		actionMsg, errstr := parseRangeMsg(jsmap)
		if errstr != "" {
			return fmt.Errorf(errstr + detail)
		}
		lm, err := t.convertMsg(r, apitems, actionMsg)
		if err != nil {
			return err
		}
		listMsg = lm
	} else {
		// Parse map into ListMsg
		actionMsg, errstr := parseListMsg(jsmap)
		if errstr != "" {
			return fmt.Errorf(errstr + detail)
		}
		listMsg = actionMsg
	}

	// Execute prefetch operation
	if msg.Action == api.ActPrefetch {
		return t.listOperation(r, apitems, listMsg, t.addPrefetchList)
	}

	// Assigns the operation type based on evict flag and executes the listOperation
	operation := t.doListDelete
	if msg.Action == api.ActEvict {
		operation = t.doListEvict
	}
	// Execute evict/delete operation
	return t.listOperation(r, apitems, listMsg, operation)
}
