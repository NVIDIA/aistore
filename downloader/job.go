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

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

const sliceDownloadBatchSize = 1000

var (
	_ DlJob = &SliceDlJob{}
	_ DlJob = &CloudBucketDlJob{}
	_ DlJob = &RangeDlJob{}
)

type (
	DlJob interface {
		ID() string
		Bck() cmn.Bck

		// FIXME: change to time.Duration
		Timeout() string

		// genNext is supposed to fulfill the following protocol:
		// ok is set to true if there is batch to process, false otherwise
		genNext() (objs []DlObj, ok bool)

		Description() string
		// if total length (size) of download job is not known, -1 should be returned
		Len() int
	}

	BaseDlJob struct {
		id          string
		bck         *cluster.Bck
		timeout     string
		description string
	}

	SliceDlJob struct {
		BaseDlJob
		objs    []DlObj
		timeout string
		current int
	}

	RangeDlJob struct {
		BaseDlJob
		t     cluster.Target
		objs  []DlObj               // objects' metas which are ready to be downloaded
		iter  func() (string, bool) // links iterator
		count int                   // total number object to download by a target
		dir   string                // objects directory(prefix) from request
		done  bool                  // true = the iterator is exhausted, nothing left to read
	}

	CloudBucketDlJob struct {
		BaseDlJob
		t   cluster.Target
		ctx context.Context // context for the request, user etc...

		objs       []DlObj // objects' metas which are ready to be downloaded
		pageMarker string
		prefix     string
		suffix     string
		mtx        sync.Mutex
		pagesCnt   int
	}

	DownloadJobInfo struct {
		ID          string `json:"id"`
		Description string `json:"description"`

		FinishedCnt  atomic.Int32 `json:"finished"`
		ScheduledCnt atomic.Int32 `json:"scheduled"`
		Total        int          `json:"total"`
		ErrorCnt     atomic.Int32 `json:"errors"`

		Aborted       atomic.Bool `json:"aborted"`
		AllDispatched atomic.Bool `json:"all_dispatched"`

		FinishedTime atomic.Time `json:"-"`
	}

	ListBucketPageCb func(bucket, pageMarker string) (*cmn.BucketList, error)
	TargetObjsCb     func(objects cmn.SimpleKVs, bucket string, cloud bool) ([]DlObj, error)
)

func (j *BaseDlJob) ID() string          { return j.id }
func (j *BaseDlJob) Bck() cmn.Bck        { return j.bck.Bck }
func (j *BaseDlJob) Timeout() string     { return j.timeout }
func (j *BaseDlJob) Description() string { return j.description }

func newBaseDlJob(id string, bck *cluster.Bck, timeout, desc string) *BaseDlJob {
	return &BaseDlJob{id: id, bck: bck, timeout: timeout, description: desc}
}

func (j *SliceDlJob) Len() int { return len(j.objs) }
func (j *SliceDlJob) genNext() (objs []DlObj, ok bool) {
	if j.current == len(j.objs) {
		return []DlObj{}, false
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

func newSliceDlJob(id string, objs []DlObj, bck *cluster.Bck, timeout, description string) *SliceDlJob {
	return &SliceDlJob{
		BaseDlJob: BaseDlJob{id: id, bck: bck, timeout: timeout, description: description},
		objs:      objs,
		timeout:   timeout,
	}
}

func (j *CloudBucketDlJob) Len() int { return -1 }
func (j *CloudBucketDlJob) genNext() (objs []DlObj, ok bool) {
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
		j.objs, j.pageMarker = []DlObj{}, ""
	}

	return readyToDownloadObjs, true
}

// Reads the content of a cloud bucket page by page until any objects to
// download found or the bucket list is over.
func (j *CloudBucketDlJob) getNextObjs() error {
	j.objs = []DlObj{}
	if j.pagesCnt > 0 && j.pageMarker == "" {
		// Cloud ListBucket returned empty pageMarker after at least one reqest
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
			PageSize:   cmn.DefaultListPageSize,
		}

		bckList, err, _ := j.t.Cloud().ListBucket(j.ctx, j.bck.Name, msg)
		if err != nil {
			return err
		}
		j.pageMarker = msg.PageMarker

		for _, entry := range bckList.Entries {
			if !strings.HasSuffix(entry.Name, j.suffix) {
				continue
			}
			dlJob, err := jobForObject(smap, sid, j.bck, entry.Name, "")
			if err != nil {
				if err == errInvalidTarget {
					continue
				}
				return err
			}
			j.objs = append(j.objs, dlJob)
		}
		if j.pageMarker == "" || len(j.objs) != 0 {
			break
		}
	}

	return nil
}

func (j *RangeDlJob) Len() int { return j.count }

func (j *RangeDlJob) genNext() ([]DlObj, bool) {
	readyToDownloadObjs := j.objs
	if j.done {
		j.objs = []DlObj{}
		return readyToDownloadObjs, len(readyToDownloadObjs) != 0
	}
	if err := j.getNextObjs(); err != nil {
		return []DlObj{}, false
	}
	return readyToDownloadObjs, true
}

func (j *RangeDlJob) getNextObjs() error {
	var (
		link string
		smap = j.t.GetSowner().Get()
		sid  = j.t.Snode().ID()
		ok   = true
	)
	j.objs = []DlObj{}

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

func newCloudBucketDlJob(ctx context.Context, t cluster.Target, base *BaseDlJob, prefix, suffix string) (*CloudBucketDlJob, error) {
	job := &CloudBucketDlJob{
		BaseDlJob:  *base,
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

func newRangeDlJob(t cluster.Target, base *BaseDlJob, pt cmn.ParsedTemplate, subdir string) (*RangeDlJob, error) {
	cnt, err := countObjects(t, pt, subdir, base.bck)
	if err != nil {
		return nil, err
	}
	job := &RangeDlJob{
		BaseDlJob: *base,
		t:         t,
		iter:      pt.Iter(),
		dir:       subdir,
		count:     cnt,
	}

	err = job.getNextObjs()
	return job, err
}
