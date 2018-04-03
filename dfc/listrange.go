// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
)

const (
	prefetchChanSize = 200
	defaultDeadline  = 0
	defaultWait      = false
	maxPrefetchPages = 10 // FIXME: Pagination for PREFETCH
)

type filesWithDeadline struct {
	objnames []string
	bucket   string
	deadline time.Time
	done     chan struct{}
}

type xactPrefetch struct {
	xactBase
	targetrunner *targetrunner
}

type xactDeleteEvict struct {
	xactBase
	targetrunner *targetrunner
}

//===========================
//
// Generic List/Range Methods
//
//===========================

func (t *targetrunner) getListFromRangeCloud(bucket string, msg *GetMsg) (bucketList *BucketList, err error) {
	bucketList = &BucketList{Entries: make([]*BucketEntry, 0)}
	for i := 0; i < maxPrefetchPages; i++ {
		jsbytes, errstr, errcode := getcloudif().listbucket(bucket, msg)
		if errstr != "" {
			return nil, fmt.Errorf("Error listing cloud bucket %s: %d(%s)", bucket, errcode, errstr)
		}
		reslist := &BucketList{}
		if err := json.Unmarshal(jsbytes, reslist); err != nil {
			return nil, fmt.Errorf("Error unmarshalling BucketList: %v", err)
		}
		bucketList.Entries = append(bucketList.Entries, reslist.Entries...)
		if reslist.PageMarker == "" {
			break
		} else if i == maxPrefetchPages {
			glog.Warningf("Did not prefetch all keys (More than %d pages)", maxPrefetchPages)
		}
		msg.GetPageMarker = reslist.PageMarker
	}

	return
}

func (t *targetrunner) getListFromRange(bucket, prefix, regex string, min, max int64) ([]string, error) {
	msg := &GetMsg{GetPrefix: prefix}
	var (
		fullbucketlist *BucketList
		err            error
	)
	if t.islocalBucket(bucket) {
		fullbucketlist, err = t.prepareLocalObjectList(bucket, msg)
	} else {
		fullbucketlist, err = t.getListFromRangeCloud(bucket, msg)
	}
	if err != nil {
		return nil, err
	}

	objs := make([]string, 0)
	re, err := regexp.Compile(regex)
	if err != nil {
		return nil, fmt.Errorf("Could not compile regex: %v", err)
	}
	for _, be := range fullbucketlist.Entries {
		if !acceptRegexRange(be.Name, prefix, re, min, max) {
			continue
		}
		if si, errstr := hrwTarget(bucket+"/"+be.Name, t.smap); si.DaemonID == t.si.DaemonID {
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

type listf func(objects []string, bucket string, deadline time.Duration, done chan struct{}) error
type rangef func(bucket, prefix, regex string, min, max int64, deadline time.Duration, done chan struct{}) error

func (t *targetrunner) listOperation(w http.ResponseWriter, r *http.Request, listMsg ListMsg, operation listf) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		return
	}
	bucket := apitems[0]
	objs := make([]string, 0)
	for _, obj := range listMsg.Objnames {
		si, errstr := hrwTarget(bucket+"/"+obj, t.smap)
		if errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
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
			if err := operation(objs, bucket, listMsg.Deadline, done); err != nil {
				glog.Errorf("Error performing list operation: %v", err)
				t.statsif.add("numerr", 1)
			}
		}()

		if listMsg.Wait {
			<-done
			close(done)
		}
	}
}

func (t *targetrunner) rangeOperation(w http.ResponseWriter, r *http.Request, rangeMsg RangeMsg, operation rangef) {
	var (
		err error
	)
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		return
	}
	bucket := apitems[0]
	min, max, err := parseRange(rangeMsg.Range)
	if err != nil {
		s := fmt.Sprintf("Error parsing range string (%s): %v", rangeMsg.Range, err)
		t.invalmsghdlr(w, r, s)
	}

	var done chan struct{}
	if rangeMsg.Wait {
		done = make(chan struct{}, 1)
	}

	// Asynchronously perform operation
	go func() {
		if err := operation(bucket, rangeMsg.Prefix, rangeMsg.Regex,
			min, max, rangeMsg.Deadline, done); err != nil {
			glog.Errorf("Error performing range operation: %v", err)
			t.statsif.add("numerr", 1)
		}
	}()

	if rangeMsg.Wait {
		<-done
		close(done)
	}
}

//============
//
// Delete/Evict
//
//=============

func (t *targetrunner) deleteList(w http.ResponseWriter, r *http.Request, deleteMsg ListMsg) {
	t.listOperation(w, r, deleteMsg, t.doListDelete)
}

func (t *targetrunner) evictList(w http.ResponseWriter, r *http.Request, evictMsg ListMsg) {
	t.listOperation(w, r, evictMsg, t.doListEvict)
}

func (t *targetrunner) deleteRange(w http.ResponseWriter, r *http.Request, deleteRangeMsg RangeMsg) {
	t.rangeOperation(w, r, deleteRangeMsg, t.doRangeDelete)
}

func (t *targetrunner) evictRange(w http.ResponseWriter, r *http.Request, evictMsg RangeMsg) {
	t.rangeOperation(w, r, evictMsg, t.doRangeEvict)
}

func (t *targetrunner) doListEvictDelete(evict bool, objs []string, bucket string, deadline time.Duration, done chan struct{}) error {
	var xdel *xactDeleteEvict
	if evict {
		xdel = t.xactinp.newEvict()
	} else {
		xdel = t.xactinp.newDelete()
	}
	defer func() {
		if done != nil {
			var v struct{}
			done <- v
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
		if !absdeadline.IsZero() && time.Now().After(absdeadline) {
			continue
		}
		err := t.fildelete(bucket, objname, evict)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *targetrunner) doRangeEvictDelete(evict bool, bucket, prefix, regex string, min, max int64,
	deadline time.Duration, done chan struct{}) error {

	objs, err := t.getListFromRange(bucket, prefix, regex, min, max)
	if err != nil {
		return err
	}

	return t.doListEvictDelete(evict, objs, bucket, deadline, done)
}

func (t *targetrunner) doListDelete(objs []string, bucket string, deadline time.Duration, done chan struct{}) error {
	return t.doListEvictDelete(false /* evict */, objs, bucket, deadline, done)
}

func (t *targetrunner) doListEvict(objs []string, bucket string, deadline time.Duration, done chan struct{}) error {
	return t.doListEvictDelete(true /* evict */, objs, bucket, deadline, done)
}

func (t *targetrunner) doRangeDelete(bucket, prefix, regex string, min, max int64,
	deadline time.Duration, done chan struct{}) error {
	return t.doRangeEvictDelete(false /* evict */, bucket, prefix, regex, min, max, deadline, done)
}
func (t *targetrunner) doRangeEvict(bucket, prefix, regex string, min, max int64,
	deadline time.Duration, done chan struct{}) error {
	return t.doRangeEvictDelete(true /* evict */, bucket, prefix, regex, min, max, deadline, done)
}

func (q *xactInProgress) newDelete() *xactDeleteEvict {
	q.lock.Lock()
	defer q.lock.Unlock()
	id := q.uniqueid()
	xpre := &xactDeleteEvict{xactBase: *newxactBase(id, ActDelete)}
	q.add(xpre)
	return xpre
}

func (q *xactInProgress) newEvict() *xactDeleteEvict {
	q.lock.Lock()
	defer q.lock.Unlock()
	id := q.uniqueid()
	xpre := &xactDeleteEvict{xactBase: *newxactBase(id, ActEvict)}
	q.add(xpre)
	return xpre
}

func (xact *xactDeleteEvict) tostring() string {
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

func (t *targetrunner) prefetchList(w http.ResponseWriter, r *http.Request, prefetchMsg ListMsg) {
	t.listOperation(w, r, prefetchMsg, t.addPrefetchList)
}

func (t *targetrunner) prefetchRange(w http.ResponseWriter, r *http.Request, prefetchRangeMsg RangeMsg) {
	t.rangeOperation(w, r, prefetchRangeMsg, t.addPrefetchRange)
}

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
				t.prefetchMissing(objname, bucket)
			}

			// Signal completion of prefetch
			if fwd.done != nil {
				var v struct{}
				fwd.done <- v
			}
		default:
			// When there is nothing left to fetch, the prefetch routine ends
			break loop

		}
	}
	t.xactinp.del(xpre.id)
}

func (t *targetrunner) prefetchMissing(objname, bucket string) {
	var (
		errstr, version   string
		vchanged, coldget bool
		props             *objectProps
	)
	versioncfg := &ctx.config.VersionConfig
	fqn := t.fqn(bucket, objname)
	islocal := t.islocalBucket(bucket)
	//
	// NOTE: lockless
	//
	if coldget, _, version, errstr = t.isObjectCached(bucket, objname, fqn); errstr != "" {
		glog.Errorln(errstr)
		return
	}
	if !coldget && !islocal && versioncfg.ValidateWarmGet && version != "" && t.versioningConfigured(bucket) {
		if vchanged, errstr, _ = t.checkCloudVersion(bucket, objname, version); errstr != "" {
			return
		}
		coldget = vchanged
	}
	if !coldget {
		return
	}
	if props, errstr, _ = t.coldget(bucket, objname, true); errstr != "" {
		if errstr != "skip" {
			glog.Errorln(errstr)
		}
		return
	}
	glog.Infof("PREFETCH done: %s/%s", bucket, objname)
	t.statsif.add("numprefetch", 1)
	t.statsif.add("bytesprefetched", props.size)
	if vchanged {
		t.statsif.add("bytesvchanged", props.size)
		t.statsif.add("numvchanged", 1)
	}
}

func (t *targetrunner) addPrefetchList(objs []string, bucket string, deadline time.Duration, done chan struct{}) error {
	if t.islocalBucket(bucket) {
		return fmt.Errorf("Cannot prefetch from a local bucket: %s", bucket)
	}
	var absdeadline time.Time
	if deadline != 0 {
		// 0 is no deadline - if deadline == 0, the absolute deadline is 0 time.
		absdeadline = time.Now().Add(deadline)
	}
	t.prefetchQueue <- filesWithDeadline{objnames: objs, bucket: bucket, deadline: absdeadline, done: done}
	return nil
}

func (t *targetrunner) addPrefetchRange(bucket, prefix, regex string, min, max int64, deadline time.Duration, done chan struct{}) error {
	if t.islocalBucket(bucket) {
		return fmt.Errorf("Cannot prefetch from a local bucket: %s", bucket)
	}

	objs, err := t.getListFromRange(bucket, prefix, regex, min, max)
	if err != nil {
		return err
	}

	return t.addPrefetchList(objs, bucket, deadline, done)
}

func (q *xactInProgress) renewPrefetch(t *targetrunner) *xactPrefetch {
	q.lock.Lock()
	defer q.lock.Unlock()
	_, xx := q.find(ActPrefetch)
	if xx != nil {
		xpre := xx.(*xactPrefetch)
		glog.Infof("%s already running, nothing to do", xpre.tostring())
		return nil
	}
	id := q.uniqueid()
	xpre := &xactPrefetch{xactBase: *newxactBase(id, ActPrefetch)}
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

func parseRangeListMsgBase(jsmap map[string]interface{}) (RangeListMsgBase, error) {
	pmb := RangeListMsgBase{Deadline: defaultDeadline, Wait: defaultWait}
	if v, ok := jsmap["deadline"]; ok {
		deadline, err := time.ParseDuration(v.(string))
		if err != nil {
			return pmb, fmt.Errorf("Error parsing PrefetchMsgBase Deadline: %v", err)
		}
		pmb.Deadline = deadline
	}
	if v, ok := jsmap["wait"]; ok {
		wait, ok := v.(bool)
		if !ok {
			return pmb, fmt.Errorf("Error parsing PrefetchMsgBase Wait: Not a boolean")
		}
		pmb.Wait = wait
	}
	return pmb, nil
}

func parseListMsg(jsmap map[string]interface{}) (ListMsg, error) {
	pm := ListMsg{}
	pmb, err := parseRangeListMsgBase(jsmap)
	if err != nil {
		return pm, err
	}
	pm.RangeListMsgBase = pmb
	v, ok := jsmap["objnames"]
	if !ok {
		return pm, fmt.Errorf("Error parsing PrefetchMsg: No objnames field")
	}
	if objnames, ok := v.([]interface{}); ok {
		pm.Objnames = make([]string, 0)
		for _, obj := range objnames {
			objname, ok := obj.(string)
			if !ok {
				return pm, fmt.Errorf("Error parsing PrefetchMsg: Non-string Object Name")
			}
			pm.Objnames = append(pm.Objnames, objname)
		}
	} else {
		return pm, fmt.Errorf("Error parsing PrefetchMsg: Couldn't parse objnames")
	}
	return pm, nil
}

func parseRangeMsg(jsmap map[string]interface{}) (RangeMsg, error) {
	pm := RangeMsg{}
	pmb, err := parseRangeListMsgBase(jsmap)
	if err != nil {
		return pm, err
	}
	pm.RangeListMsgBase = pmb
	v, ok := jsmap["prefix"]
	if !ok {
		return pm, fmt.Errorf("Error parsing PrefetchRangeMsg: no prefix field")
	}
	if prefix, ok := v.(string); ok {
		pm.Prefix = prefix
	} else {
		return pm, fmt.Errorf("Error parsing PrefetchMsg: couldn't parse prefix")
	}

	v, ok = jsmap["regex"]
	if !ok {
		return pm, fmt.Errorf("Error parsing PrefetchRangeMsg: no regex field")
	}
	if regex, ok := v.(string); ok {
		pm.Regex = regex
	} else {
		return pm, fmt.Errorf("Error parsing PrefetchMsg: couldn't parse regex")
	}

	v, ok = jsmap["range"]
	if !ok {
		return pm, fmt.Errorf("Error parsing PrefetchRangeMsg: no range field")
	}
	if rng, ok := v.(string); ok {
		pm.Range = rng
	} else {
		return pm, fmt.Errorf("Error parsing PrefetchMsg: couldn't parse range")
	}
	return pm, nil
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
