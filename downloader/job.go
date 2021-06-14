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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/nl"
)

const (
	// Determines the size of single batch size generated in `genNext`.
	downloadBatchSize = 10_000
)

// interface guard
var (
	_ DlJob = (*sliceDlJob)(nil)
	_ DlJob = (*backendDlJob)(nil)
	_ DlJob = (*rangeDlJob)(nil)
)

type (
	dlObj struct {
		objName    string
		link       string
		fromRemote bool
	}

	DlJob interface {
		ID() string
		Bck() cmn.Bck
		Description() string
		Timeout() time.Duration
		ActiveStats() (*DlStatusResp, error)

		Notif() cluster.Notif // notifications
		AddNotif(n cluster.Notif, job DlJob)

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
		dlXact      *Downloader

		// notif
		notif *NotifDownload
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

	backendDlJob struct {
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
func (*baseDlJob) Sync() bool               { return false }

// Notifications
func (j *baseDlJob) Notif() cluster.Notif { return j.notif }

func (j *baseDlJob) AddNotif(n cluster.Notif, job DlJob) {
	var ok bool
	cos.Assert(j.notif == nil) // currently, "add" means "set"
	j.notif, ok = n.(*NotifDownload)
	cos.Assert(ok)
	j.notif.DlJob = job
	cos.Assert(j.notif.F != nil)
	if n.Upon(cluster.UponProgress) {
		cos.Assert(j.notif.P != nil)
	}
}

func (j *baseDlJob) ActiveStats() (*DlStatusResp, error) {
	resp, _, err := j.dlXact.JobStatus(j.ID(), true /*onlyActive*/)
	if err != nil {
		return nil, err
	}
	return resp.(*DlStatusResp), nil
}

func (*baseDlJob) checkObj(string) bool    { debug.Assert(false); return false }
func (j *baseDlJob) throttler() *throttler { return j.t }

func (j *baseDlJob) cleanup() {
	j.throttler().stop()
	dlStore.markFinished(j.ID())
	dlStore.flush(j.ID())
	nl.OnFinished(j.Notif(), nil)
}

func newBaseDlJob(t cluster.Target, id string, bck *cluster.Bck, timeout, desc string, limits DlLimits, dlXact *Downloader) *baseDlJob {
	// TODO: this might be inaccurate if we download 1 or 2 objects because then
	//  other targets will have limits but will not use them.
	if limits.BytesPerHour > 0 {
		limits.BytesPerHour /= t.Sowner().Get().CountActiveTargets()
	}

	td, _ := time.ParseDuration(timeout)
	return &baseDlJob{
		id:          id,
		bck:         bck,
		timeout:     td,
		description: desc,
		t:           newThrottler(limits),
		dlXact:      dlXact,
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

func newMultiDlJob(t cluster.Target, id string, bck *cluster.Bck, payload *DlMultiBody, dlXact *Downloader) (*multiDlJob, error) {
	var (
		objs cos.SimpleKVs
		err  error
	)
	base := newBaseDlJob(t, id, bck, payload.Timeout, payload.Describe(), payload.Limits, dlXact)
	if objs, err = payload.ExtractPayload(); err != nil {
		return nil, err
	}
	sliceDlJob, err := newSliceDlJob(t, bck, base, objs)
	if err != nil {
		return nil, err
	}
	return &multiDlJob{sliceDlJob}, nil
}

func newSingleDlJob(t cluster.Target, id string, bck *cluster.Bck, payload *DlSingleBody, dlXact *Downloader) (*singleDlJob, error) {
	var (
		objs cos.SimpleKVs
		err  error
	)
	base := newBaseDlJob(t, id, bck, payload.Timeout, payload.Describe(), payload.Limits, dlXact)
	if objs, err = payload.ExtractPayload(); err != nil {
		return nil, err
	}
	sliceDlJob, err := newSliceDlJob(t, bck, base, objs)
	if err != nil {
		return nil, err
	}
	return &singleDlJob{sliceDlJob}, nil
}

func newSliceDlJob(t cluster.Target, bck *cluster.Bck, base *baseDlJob, objects cos.SimpleKVs) (*sliceDlJob, error) {
	objs, err := buildDlObjs(t, bck, objects)
	if err != nil {
		return nil, err
	}
	return &sliceDlJob{
		baseDlJob: *base,
		objs:      objs,
	}, nil
}

func (*backendDlJob) Len() int     { return -1 }
func (j *backendDlJob) Sync() bool { return j.sync }

func (j *backendDlJob) checkObj(objName string) bool {
	return strings.HasPrefix(objName, j.prefix) && strings.HasSuffix(objName, j.suffix)
}

func (j *backendDlJob) genNext() (objs []dlObj, ok bool, err error) {
	if j.done {
		return nil, false, nil
	}
	if err := j.getNextObjs(); err != nil {
		return nil, false, err
	}
	return j.objs, true, nil
}

// Reads the content of a remote bucket page by page until any objects to
// download found or the bucket list is over.
func (j *backendDlJob) getNextObjs() error {
	var (
		sid     = j.t.SID()
		smap    = j.t.Sowner().Get()
		backend = j.t.Backend(j.bck)
	)
	j.objs = j.objs[:0]
	for len(j.objs) < downloadBatchSize {
		msg := &cmn.SelectMsg{
			Prefix:            j.prefix,
			ContinuationToken: j.continuationToken,
			PageSize:          backend.MaxPageSize(),
		}
		bckList, _, err := backend.ListObjects(j.ctx, j.bck, msg)
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
		smap = j.t.Sowner().Get()
		sid  = j.t.SID()
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

func newBackendDlJob(ctx context.Context, t cluster.Target, id string, bck *cluster.Bck, payload *DlBackendBody, dlXact *Downloader) (*backendDlJob, error) {
	if !bck.IsRemote() {
		return nil, errors.New("bucket download requires a remote bucket")
	} else if bck.IsHTTP() {
		return nil, errors.New("bucket download does not support HTTP buckets")
	}
	base := newBaseDlJob(t, id, bck, payload.Timeout, payload.Describe(), payload.Limits, dlXact)
	job := &backendDlJob{
		baseDlJob: *base,
		t:         t,
		ctx:       ctx,
		sync:      payload.Sync,
		prefix:    payload.Prefix,
		suffix:    payload.Suffix,
	}
	return job, nil
}

func countObjects(t cluster.Target, pt cos.ParsedTemplate, dir string, bck *cluster.Bck) (cnt int, err error) {
	var (
		smap = t.Sowner().Get()
		sid  = t.SID()
		iter = pt.Iter()
		si   *cluster.Snode
	)

	for link, ok := iter(); ok; link, ok = iter() {
		name := path.Join(dir, path.Base(link))
		name, err = NormalizeObjName(name)
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

func newRangeDlJob(t cluster.Target, id string, bck *cluster.Bck, payload *DlRangeBody, dlXact *Downloader) (*rangeDlJob, error) {
	var (
		pt  cos.ParsedTemplate
		err error
	)
	// NOTE: Size of objects to be downloaded by a target will be unknown.
	//  So proxy won't be able to sum sizes from all targets when calculating total size.
	//  This should be taken care of somehow, as total is easy to know from range template anyway.
	if pt, err = cos.ParseBashTemplate(payload.Template); err != nil {
		return nil, err
	}

	base := newBaseDlJob(t, id, bck, payload.Timeout, payload.Describe(), payload.Limits, dlXact)
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

// Used for debugging purposes to ensure integrity of the struct.
func (d *downloadJobInfo) valid() bool {
	if d.Aborted.Load() {
		return true
	}
	if !d.AllDispatched.Load() {
		return true
	}
	return d.ScheduledCnt.Load() == d.FinishedCnt.Load()+d.ErrorCnt.Load()
}
