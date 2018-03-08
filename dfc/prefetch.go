package dfc

import (
	"encoding/json"
	"fmt"
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
				t.prefetchIfMissing(objname, bucket)
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

func (t *targetrunner) prefetchIfMissing(objname, bucket string) {
	fqn, uname := t.fqn(bucket, objname), bucket+"/"+objname
	// The first check does not take the lock, preventing get from starving
	// from repeated prefetches on the same cached file
	if coldget, _, _ := t.getchecklocal(bucket, objname, fqn); !coldget {
		return
	}
	// The second check takes the lock, preventing interference between get and prefetch
	t.rtnamemap.lockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	defer func() { t.rtnamemap.unlockname(uname, true) }()
	coldget, _, errstr := t.getchecklocal(bucket, objname, fqn)
	if errstr != "" {
		glog.Errorln(errstr)
		return
	}
	if coldget {
		//FIXME: Revisit potential use of timeout for prefetch deadline
		if _, size, errstr, errcode := getcloudif().getobj(fqn, bucket, objname); errstr != "" {
			glog.Errorf("Error retrieving object %s/%s: Error Code %d: %s",
				bucket, objname, errcode, errstr)
		} else {
			glog.Infof("Prefetched %s", fqn)
			t.statsif.add("numprefetch", 1)
			t.statsif.add("bytesprefetched", size)
		}
	}
}

func (t *targetrunner) addPrefetch(objs []string, bucket string, deadline time.Duration, done chan struct{}) error {
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
	msg := &GetMsg{GetPrefix: prefix}
	jsbytes, errstr, errcode := getcloudif().listbucket(bucket, msg)
	if errstr != "" {
		return fmt.Errorf("Error listing cloud bucket %s: %d(%s)", bucket, errcode, errstr)
	}
	reslist := &BucketList{}
	if err := json.Unmarshal(jsbytes, reslist); err != nil {
		return fmt.Errorf("Error unmarshalling BucketListL %v", err)
	}
	objs, err := t.getObjsPrefixRegex(reslist, bucket, prefix, regex, min, max)
	if err != nil {
		return err
	}
	return t.addPrefetch(objs, bucket, deadline, done)
}

func (t *targetrunner) getObjsPrefixRegex(reslist *BucketList, bucket, prefix, regex string, min, max int64) ([]string, error) {
	objs := make([]string, 0)
	re, err := regexp.Compile(regex)
	if err != nil {
		return nil, fmt.Errorf("Could not compile regex: %v", err)
	}
	for _, be := range reslist.Entries {
		if !acceptRegexRange(be.Name, prefix, re, min, max) {
			continue
		}
		if si := hrwTarget(bucket+"/"+be.Name, t.smap); si.DaemonID == t.si.DaemonID {
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
	} else if s[0] == "" || (min == 0 && max == 0) || (i >= min && i <= max) {
		// Either the match is empty, or the match is a number.
		// If the match is a number, then either the range must have been empty (both zero)
		// or the number must be within the given range.
		return true
	}
	return false
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
	start := xact.stime.Sub(xact.targetrunner.starttime)
	if !xact.finished() {
		return fmt.Sprintf("xaction %s:%d started %v", xact.kind, xact.id, start)
	}
	fin := time.Since(xact.targetrunner.starttime)
	return fmt.Sprintf("xaction %s:%d started %v finished %v", xact.kind, xact.id, start, fin)
}

//================
//
// Message Parsing
//
//================

func parsePrefetchMsgBase(jsmap map[string]interface{}) (PrefetchMsgBase, error) {
	pmb := PrefetchMsgBase{Deadline: defaultDeadline, Wait: defaultWait}
	if v, ok := jsmap["deadline"]; ok {
		if deadline, err := time.ParseDuration(v.(string)); err != nil {
			return pmb, fmt.Errorf("Error parsing PrefetchMsgBase Deadline: %v", err)
		} else {
			pmb.Deadline = deadline
		}
	}
	if v, ok := jsmap["wait"]; ok {
		if wait, ok := v.(bool); !ok {
			return pmb, fmt.Errorf("Error parsing PrefetchMsgBase Wait: Not a boolean")
		} else {
			pmb.Wait = wait
		}
	}
	return pmb, nil
}

func parsePrefetchMsg(jsmap map[string]interface{}) (PrefetchMsg, error) {
	pm := PrefetchMsg{}
	pmb, err := parsePrefetchMsgBase(jsmap)
	if err != nil {
		return pm, err
	}
	pm.PrefetchMsgBase = pmb
	if v, ok := jsmap["objnames"]; !ok {
		return pm, fmt.Errorf("Error parsing PrefetchMsg: No objnames field")
	} else {
		if objnames, ok := v.([]interface{}); ok {
			pm.Objnames = make([]string, 0)
			for _, obj := range objnames {
				if objname, ok := obj.(string); !ok {
					return pm, fmt.Errorf("Error parsing PrefetchMsg: Non-string Object Name")
				} else {
					pm.Objnames = append(pm.Objnames, objname)
				}
			}
		} else {
			return pm, fmt.Errorf("Error parsing PrefetchMsg: Couldn't parse objnames")
		}
	}
	return pm, nil
}

func parsePrefetchRangeMsg(jsmap map[string]interface{}) (PrefetchRangeMsg, error) {
	pm := PrefetchRangeMsg{}
	pmb, err := parsePrefetchMsgBase(jsmap)
	if err != nil {
		return pm, err
	}
	pm.PrefetchMsgBase = pmb
	if v, ok := jsmap["prefix"]; !ok {
		return pm, fmt.Errorf("Error parsing PrefetchRangeMsg: no prefix field")
	} else {
		if prefix, ok := v.(string); ok {
			pm.Prefix = prefix
		} else {
			return pm, fmt.Errorf("Error parsing PrefetchMsg: couldn't parse prefix")
		}
	}
	if v, ok := jsmap["regex"]; !ok {
		return pm, fmt.Errorf("Error parsing PrefetchRangeMsg: no regex field")
	} else {
		if regex, ok := v.(string); ok {
			pm.Regex = regex
		} else {
			return pm, fmt.Errorf("Error parsing PrefetchMsg: couldn't parse regex")
		}
	}
	if v, ok := jsmap["range"]; !ok {
		return pm, fmt.Errorf("Error parsing PrefetchRangeMsg: no range field")
	} else {
		if rng, ok := v.(string); ok {
			pm.Range = rng
		} else {
			return pm, fmt.Errorf("Error parsing PrefetchMsg: couldn't parse range")
		}
	}
	return pm, nil
}
