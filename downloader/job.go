// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"errors"
	"path"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	// Determines the size of single batch size generated in `genNext`.
	downloadBatchSize = 10_000
)

var (
	_ DlJob = &sliceDlJob{}
	_ DlJob = &cloudBucketDlJob{}
	_ DlJob = &rangeDlJob{}
)

var (
	errAISBckReq = errors.New("regular download requires ais bucket")
)

type (
	dlObj struct {
		objName   string
		link      string
		fromCloud bool
	}

	DlJob interface {
		ID() string
		Bck() cmn.Bck
		Description() string
		Timeout() time.Duration

		// If total length (size) of download job is not known, -1 should be returned.
		Len() int

		// Determines if it requires also syncing.
		Sync() bool

		// Checks if object name matches the request.
		checkObj(objName string) bool

		// genNext is supposed to fulfill the following protocol:
		//  `ok` is set to `true` if there is batch to process, `false` otherwise
		genNext() (objs []dlObj, ok bool, err error)

		throttler() *throttler

		cleanup()
	}

	baseDlJob struct {
		id          string
		bck         *cluster.Bck
		timeout     time.Duration
		description string
		t           *throttler
	}

	sliceDlJob struct {
		baseDlJob
		objs    []dlObj
		current int
	}

	singleDlJob struct {
		*sliceDlJob
	}

	multiDlJob struct {
		*sliceDlJob
	}

	rangeDlJob struct {
		baseDlJob
		t     cluster.Target
		objs  []dlObj               // objects' metas which are ready to be downloaded
		iter  func() (string, bool) // links iterator
		count int                   // total number object to download by a target
		dir   string                // objects directory(prefix) from request
		done  bool                  // true = the iterator is exhausted, nothing left to read
	}

	cloudBucketDlJob struct {
		baseDlJob
		t   cluster.Target
		ctx context.Context // context for the request, user etc...

		prefix string
		suffix string
		sync   bool

		done              bool
		objs              []dlObj // objects' metas which are ready to be downloaded
		continuationToken string
	}

	downloadJobInfo struct {
		ID          string `json:"id"`
		Description string `json:"description"`

		FinishedCnt  atomic.Int32 `json:"finished"` // also includes skipped
		ScheduledCnt atomic.Int32 `json:"scheduled"`
		SkippedCnt   atomic.Int32 `json:"skipped"`
		ErrorCnt     atomic.Int32 `json:"errors"`
		Total        int          `json:"total"`

		Aborted       atomic.Bool `json:"aborted"`
		AllDispatched atomic.Bool `json:"all_dispatched"`

		StartedTime  time.Time   `json:"started_time"`
		FinishedTime atomic.Time `json:"finished_time"`
	}
)

func (j *baseDlJob) ID() string             { return j.id }
func (j *baseDlJob) Bck() cmn.Bck           { return j.bck.Bck }
func (j *baseDlJob) Timeout() time.Duration { return j.timeout }
func (j *baseDlJob) Description() string    { return j.description }
func (j *baseDlJob) Sync() bool             { return false }
func (j *baseDlJob) checkObj(string) bool   { cmn.Assert(false); return false }
func (j *baseDlJob) throttler() *throttler  { return j.t }
func (j *baseDlJob) cleanup() {
	dlStore.markFinished(j.ID())
	dlStore.flush(j.ID())
	j.throttler().stop()
}

func newBaseDlJob(t cluster.Target, id string, bck *cluster.Bck, timeout, desc string, limits DlLimits) *baseDlJob {
	// TODO: this might be inaccurate if we download 1 or 2 objects because then
	//  other targets will have limits but will not use them.
	if limits.BytesPerHour > 0 {
		limits.BytesPerHour /= t.GetSowner().Get().CountTargets()
	}

	td, _ := time.ParseDuration(timeout)
	return &baseDlJob{
		id:          id,
		bck:         bck,
		timeout:     td,
		description: desc,
		t:           newThrottler(limits),
	}
}

func (j *sliceDlJob) Len() int { return len(j.objs) }
func (j *sliceDlJob) genNext() (objs []dlObj, ok bool, err error) {
	if j.current == len(j.objs) {
		return nil, false, nil
	}

	if j.current+downloadBatchSize >= len(j.objs) {
		objs = j.objs[j.current:]
		j.current = len(j.objs)
		return objs, true, nil
	}

	objs = j.objs[j.current : j.current+downloadBatchSize]
	j.current += downloadBatchSize
	return objs, true, nil
}

func newMultiDlJob(t cluster.Target, id string, bck *cluster.Bck, payload *DlMultiBody) (*multiDlJob, error) {
	if !bck.IsAIS() {
		return nil, errAISBckReq
	}
	var (
		objs cmn.SimpleKVs
		err  error
	)
	base := newBaseDlJob(t, id, bck, payload.Timeout, payload.Describe(), payload.Limits)
	if objs, err = payload.ExtractPayload(); err != nil {
		return nil, err
	}
	sliceDlJob, err := newSliceDlJob(t, bck, base, objs)
	if err != nil {
		return nil, err
	}
	return &multiDlJob{sliceDlJob}, nil
}

func newSingleDlJob(t cluster.Target, id string, bck *cluster.Bck, payload *DlSingleBody) (*singleDlJob, error) {
	if !bck.IsAIS() {
		return nil, errAISBckReq
	}

	var (
		objs cmn.SimpleKVs
		err  error
	)
	base := newBaseDlJob(t, id, bck, payload.Timeout, payload.Describe(), payload.Limits)
	if objs, err = payload.ExtractPayload(); err != nil {
		return nil, err
	}
	sliceDlJob, err := newSliceDlJob(t, bck, base, objs)
	if err != nil {
		return nil, err
	}
	return &singleDlJob{sliceDlJob}, nil
}

func newSliceDlJob(t cluster.Target, bck *cluster.Bck, base *baseDlJob, objects cmn.SimpleKVs) (*sliceDlJob, error) {
	objs, err := buildDlObjs(t, bck, objects)
	if err != nil {
		return nil, err
	}
	return &sliceDlJob{
		baseDlJob: *base,
		objs:      objs,
	}, nil
}

func (j *cloudBucketDlJob) Len() int   { return -1 }
func (j *cloudBucketDlJob) Sync() bool { return j.sync }
func (j *cloudBucketDlJob) checkObj(objName string) bool {
	return strings.HasPrefix(objName, j.prefix) && strings.HasSuffix(objName, j.suffix)
}
func (j *cloudBucketDlJob) genNext() (objs []dlObj, ok bool, err error) {
	if j.done {
		return nil, false, nil
	}
	if err := j.getNextObjs(); err != nil {
		return nil, false, err
	}
	return j.objs, true, nil
}

// Reads the content of a cloud bucket page by page until any objects to
// download found or the bucket list is over.
func (j *cloudBucketDlJob) getNextObjs() error {
	var (
		sid   = j.t.Snode().ID()
		smap  = j.t.GetSowner().Get()
		cloud = j.t.Cloud(j.bck)
	)
	j.objs = j.objs[:0]
	for len(j.objs) < downloadBatchSize {
		msg := &cmn.SelectMsg{
			Prefix:            j.prefix,
			ContinuationToken: j.continuationToken,
			PageSize:          cloud.MaxPageSize(),
		}
		bckList, err, _ := cloud.ListObjects(j.ctx, j.bck, msg)
		if err != nil {
			return err
		}
		j.continuationToken = bckList.ContinuationToken

		for _, entry := range bckList.Entries {
			if !j.checkObj(entry.Name) {
				continue
			}
			obj, err := makeDlObj(smap, sid, j.bck, entry.Name, "")
			if err != nil {
				if err == errInvalidTarget {
					continue
				}
				return err
			}
			j.objs = append(j.objs, obj)
		}
		if j.continuationToken == "" {
			j.done = true
			break
		}
	}
	return nil
}

func (j *rangeDlJob) SrcBck() cmn.Bck { return j.bck.Bck }
func (j *rangeDlJob) Len() int        { return j.count }
func (j *rangeDlJob) genNext() ([]dlObj, bool, error) {
	if j.done {
		return nil, false, nil
	}
	if err := j.getNextObjs(); err != nil {
		return nil, false, err
	}
	return j.objs, true, nil
}

func (j *rangeDlJob) getNextObjs() error {
	var (
		smap = j.t.GetSowner().Get()
		sid  = j.t.Snode().ID()
	)
	j.objs = j.objs[:0]
	for len(j.objs) < downloadBatchSize {
		link, ok := j.iter()
		if !ok {
			j.done = true
			break
		}
		name := path.Join(j.dir, path.Base(link))
		obj, err := makeDlObj(smap, sid, j.bck, name, link)
		if err != nil {
			if err == errInvalidTarget {
				continue
			}
			return err
		}
		j.objs = append(j.objs, obj)
	}
	return nil
}

func newCloudBucketDlJob(ctx context.Context, t cluster.Target, id string, bck *cluster.Bck, payload *DlCloudBody) (*cloudBucketDlJob, error) {
	if !bck.IsCloud() {
		return nil, errors.New("bucket download requires a cloud bucket")
	}
	base := newBaseDlJob(t, id, bck, payload.Timeout, payload.Describe(), payload.Limits)
	job := &cloudBucketDlJob{
		baseDlJob: *base,
		t:         t,
		ctx:       ctx,
		sync:      payload.Sync,
		prefix:    payload.Prefix,
		suffix:    payload.Suffix,
	}
	return job, nil
}

func countObjects(t cluster.Target, pt cmn.ParsedTemplate, dir string, bck *cluster.Bck) (cnt int, err error) {
	var (
		smap = t.GetSowner().Get()
		sid  = t.Snode().ID()
		iter = pt.Iter()
		si   *cluster.Snode
	)

	for link, ok := iter(); ok; link, ok = iter() {
		name := path.Join(dir, path.Base(link))
		name, err = normalizeObjName(name)
		if err != nil {
			return
		}
		si, err = cluster.HrwTarget(bck.MakeUname(name), smap)
		if err != nil {
			return
		}
		if si.ID() == sid {
			cnt++
		}
	}
	return cnt, nil
}

func newRangeDlJob(t cluster.Target, id string, bck *cluster.Bck, payload *DlRangeBody) (*rangeDlJob, error) {
	if !bck.IsAIS() {
		return nil, errors.New("regular download requires ais bucket")
	}

	var (
		pt  cmn.ParsedTemplate
		err error
	)
	// NOTE: Size of objects to be downloaded by a target will be unknown.
	//  So proxy won't be able to sum sizes from all targets when calculating total size.
	//  This should be taken care of somehow, as total is easy to know from range template anyway.
	if pt, err = cmn.ParseBashTemplate(payload.Template); err != nil {
		return nil, err
	}

	base := newBaseDlJob(t, id, bck, payload.Timeout, payload.Describe(), payload.Limits)
	cnt, err := countObjects(t, pt, payload.Subdir, base.bck)
	if err != nil {
		return nil, err
	}
	job := &rangeDlJob{
		baseDlJob: *base,
		t:         t,
		iter:      pt.Iter(),
		dir:       payload.Subdir,
		count:     cnt,
	}
	return job, nil
}

func (d *downloadJobInfo) ToDlJobInfo() DlJobInfo {
	return DlJobInfo{
		ID:            d.ID,
		Description:   d.Description,
		FinishedCnt:   int(d.FinishedCnt.Load()),
		ScheduledCnt:  int(d.ScheduledCnt.Load()),
		SkippedCnt:    int(d.SkippedCnt.Load()),
		ErrorCnt:      int(d.ErrorCnt.Load()),
		Total:         d.Total,
		AllDispatched: d.AllDispatched.Load(),
		Aborted:       d.Aborted.Load(),
		StartedTime:   d.StartedTime,
		FinishedTime:  d.FinishedTime.Load(),
	}
}
