// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
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
)

type (
	DlJob interface {
		ID() string
		Bck() cmn.Bck

		// FIXME: change to time.Duration
		Timeout() string

		// GenNext is supposed to fulfill the following protocol:
		// ok is set to true if there is batch to process, false otherwise
		GenNext() (objs []DlObj, ok bool)

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

func NewBaseDlJob(id string, bck *cluster.Bck, timeout, desc string) *BaseDlJob {
	return &BaseDlJob{id: id, bck: bck, timeout: timeout, description: desc}
}

func (j *SliceDlJob) Len() int { return len(j.objs) }
func (j *SliceDlJob) GenNext() (objs []DlObj, ok bool) {
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

func NewSliceDlJob(id string, objs []DlObj, bck *cluster.Bck, timeout, description string) *SliceDlJob {
	return &SliceDlJob{
		BaseDlJob: BaseDlJob{id: id, bck: bck, timeout: timeout, description: description},
		objs:      objs,
		timeout:   timeout,
	}
}

func (j *CloudBucketDlJob) Len() int { return -1 }
func (j *CloudBucketDlJob) GenNext() (objs []DlObj, ok bool) {
	j.mtx.Lock()
	defer j.mtx.Unlock()

	if len(j.objs) == 0 {
		return nil, false
	}

	readyToDownloadObjs := j.objs
	if err := j.GetNextObjs(); err != nil {
		glog.Error(err)
		// this makes GenNext return ok = false in the next
		// loop iteration
		j.objs, j.pageMarker = []DlObj{}, ""
	}

	return readyToDownloadObjs, true
}

// TODO: the function can generate empty list and stop iterating even if there
// more objects in the bucket. Possible case:
// 1. Bucket with 2000 objects ending with 'tar' and 2000 objects ending with 'tgz'
// 2. Requesting objects with suffix 'tgz' loads the first page, filters out
//    all objects from the result and returns empty array
// 3. L150 does check and stops traversing
// 4. Result -> no object downloaded
func (j *CloudBucketDlJob) GetNextObjs() error {
	if j.pagesCnt > 0 && j.pageMarker == "" {
		// Cloud ListBucket returned empty pageMarker after at least one reqest
		// this means there are no more objects in to list
		j.objs = []DlObj{}
		return nil
	}
	smap := j.t.GetSowner().Get()
	sid := j.t.Snode().ID()

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

	return nil
}

func NewCloudBucketDlJob(ctx context.Context, t cluster.Target, base *BaseDlJob, prefix, suffix string) (*CloudBucketDlJob, error) {
	job := &CloudBucketDlJob{
		BaseDlJob:  *base,
		pageMarker: "",
		t:          t,
		ctx:        ctx,
		prefix:     prefix,
		suffix:     suffix,
	}

	err := job.GetNextObjs()
	return job, err
}
