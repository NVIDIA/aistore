// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

const sliceDownloadBatchSize = 1000

var (
	_ DlJob = &sliceDlJob{}
	_ DlJob = &cloudBucketDlJob{}
	_ DlJob = &rangeDlJob{}
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

		// genNext is supposed to fulfill the following protocol:
		// ok is set to true if there is batch to process, false otherwise
		genNext() (objs []dlObj, ok bool)

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

		objs       []dlObj // objects' metas which are ready to be downloaded
		pageMarker string
		prefix     string
		suffix     string
		mtx        sync.Mutex
		pagesCnt   int
	}

	downloadJobInfo struct {
		ID          string `json:"id"`
		Description string `json:"description"`

		FinishedCnt  atomic.Int32 `json:"finished"`
		ScheduledCnt atomic.Int32 `json:"scheduled"`
		Total        int          `json:"total"`
		ErrorCnt     atomic.Int32 `json:"errors"`

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
func (j *baseDlJob) throttler() *throttler  { return j.t }
func (j *baseDlJob) cleanup() {
	if err := dlStore.markFinished(j.ID()); err != nil {
		glog.Error(err)
	}
	dlStore.flush(j.ID())
	j.throttler().stop()
}

func newBaseDlJob(id string, bck *cluster.Bck, timeout, desc string, limits DlLimits) *baseDlJob {
	t, _ := time.ParseDuration(timeout)
	return &baseDlJob{
		id:          id,
		bck:         bck,
		timeout:     t,
		description: desc,
		t:           newThrottler(limits),
	}
}

func (j *sliceDlJob) Len() int { return len(j.objs) }
func (j *sliceDlJob) genNext() (objs []dlObj, ok bool) {
	if j.current == len(j.objs) {
		return []dlObj{}, false
	}

	if j.current+sliceDownloadBatchSize >= len(j.objs) {
		objs = j.objs[j.current:]
		j.current = len(j.objs)
		return objs, true
	}

	objs = j.objs[j.current : j.current+sliceDownloadBatchSize]
	j.current += sliceDownloadBatchSize
	return objs, true
}

func newSliceDlJob(base *baseDlJob, objs []dlObj) *sliceDlJob {
	return &sliceDlJob{
		baseDlJob: *base,
		objs:      objs,
	}
}

func (j *cloudBucketDlJob) Len() int { return -1 }
func (j *cloudBucketDlJob) genNext() (objs []dlObj, ok bool) {
	j.mtx.Lock()
	defer j.mtx.Unlock()

	if len(j.objs) == 0 {
		return nil, false
	}

	readyToDownloadObjs := j.objs
	if err := j.getNextObjs(); err != nil {
		glog.Error(err)
		// this makes genNext return ok = false in the next
		// loop iteration
		j.objs, j.pageMarker = []dlObj{}, ""
	}

	return readyToDownloadObjs, true
}

// Reads the content of a cloud bucket page by page until any objects to
// download found or the bucket list is over.
func (j *cloudBucketDlJob) getNextObjs() error {
	j.objs = []dlObj{}
	if j.pagesCnt > 0 && j.pageMarker == "" {
		// Cloud ListObjects returned empty pageMarker after at least one request
		// this means there are no more objects in to list
		return nil
	}
	smap := j.t.GetSowner().Get()
	sid := j.t.Snode().ID()

	for {
		j.pagesCnt++
		msg := &cmn.SelectMsg{
			Prefix:     j.prefix,
			PageMarker: j.pageMarker,
			Fast:       true,
		}

		bckList, err, _ := j.t.Cloud(j.bck).ListObjects(j.ctx, j.bck, msg)
		if err != nil {
			return err
		}
		j.pageMarker = msg.PageMarker

		for _, entry := range bckList.Entries {
			if !strings.HasSuffix(entry.Name, j.suffix) {
				continue
			}
			job, err := jobForObject(smap, sid, j.bck, entry.Name, "")
			if err != nil {
				if err == errInvalidTarget {
					continue
				}
				return err
			}
			j.objs = append(j.objs, job)
		}
		if j.pageMarker == "" || len(j.objs) != 0 {
			break
		}
	}

	return nil
}

func (j *rangeDlJob) Len() int { return j.count }

func (j *rangeDlJob) genNext() ([]dlObj, bool) {
	readyToDownloadObjs := j.objs
	if j.done {
		j.objs = []dlObj{}
		return readyToDownloadObjs, len(readyToDownloadObjs) != 0
	}
	if err := j.getNextObjs(); err != nil {
		return []dlObj{}, false
	}
	return readyToDownloadObjs, true
}

func (j *rangeDlJob) getNextObjs() error {
	var (
		link string
		smap = j.t.GetSowner().Get()
		sid  = j.t.Snode().ID()
		ok   = true
	)
	j.objs = []dlObj{}

	for len(j.objs) < sliceDownloadBatchSize && ok {
		link, ok = j.iter()
		if !ok {
			break
		}
		name := path.Join(j.dir, path.Base(link))
		dlJob, err := jobForObject(smap, sid, j.bck, name, link)
		if err != nil {
			if err == errInvalidTarget {
				continue
			}
			return err
		}
		j.objs = append(j.objs, dlJob)
	}
	j.done = !ok
	return nil
}

func newCloudBucketDlJob(ctx context.Context, t cluster.Target, base *baseDlJob, prefix, suffix string) (*cloudBucketDlJob, error) {
	job := &cloudBucketDlJob{
		baseDlJob:  *base,
		pageMarker: "",
		t:          t,
		ctx:        ctx,
		prefix:     prefix,
		suffix:     suffix,
	}

	err := job.getNextObjs()
	return job, err
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

func newRangeDlJob(t cluster.Target, base *baseDlJob, pt cmn.ParsedTemplate, subdir string) (*rangeDlJob, error) {
	cnt, err := countObjects(t, pt, subdir, base.bck)
	if err != nil {
		return nil, err
	}
	job := &rangeDlJob{
		baseDlJob: *base,
		t:         t,
		iter:      pt.Iter(),
		dir:       subdir,
		count:     cnt,
	}

	err = job.getNextObjs()
	return job, err
}

func (d *downloadJobInfo) ToDlJobInfo() DlJobInfo {
	return DlJobInfo{
		ID:            d.ID,
		Description:   d.Description,
		FinishedCnt:   int(d.FinishedCnt.Load()),
		ScheduledCnt:  int(d.ScheduledCnt.Load()),
		ErrorCnt:      int(d.ErrorCnt.Load()),
		Total:         d.Total,
		AllDispatched: d.AllDispatched.Load(),
		Aborted:       d.Aborted.Load(),
		StartedTime:   d.StartedTime,
		FinishedTime:  d.FinishedTime.Load(),
	}
}
