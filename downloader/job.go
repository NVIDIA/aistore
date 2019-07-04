// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

const sliceDownloadBatchSize = 1000

var (
	_ DownloadJob = &SliceDownloadJob{}
	_ DownloadJob = &CloudBucketDownloadJob{}
)

type (
	DownloadJob interface {
		ID() string
		Bucket() string
		BckProvider() string

		// FIXME: change to time.Duration
		Timeout() string

		// GenNext is supposed to fulfill the following protocol:
		// ok is set to true if there is batch to process, false otherwise
		GenNext() (objs []cmn.DlObj, ok bool)

		Description() string
		// if total length (size) of download job is not known, -1 should be returned
		Len() int
	}

	BaseDownloadJob struct {
		id          string
		bucket      string
		bckProvider string
		timeout     string
		description string
	}

	SliceDownloadJob struct {
		BaseDownloadJob
		objs    []cmn.DlObj
		timeout string
		current int
	}

	CloudBucketDownloadJob struct {
		BaseDownloadJob
		t   cluster.Target
		ctx context.Context // context for the request, user etc...

		objs       []cmn.DlObj // objects' metas which are ready to be downloaded
		pageMarker string
		prefix     string
		suffix     string
		wg         *sync.WaitGroup
		pagesCnt   int
	}

	DownloadJobInfo struct {
		ID          string `json:"id"`
		Description string `json:"description"`

		FinishedCnt  atomic.Int32 `json:"finished"`
		ScheduledCnt atomic.Int32 `json:"scheduled"`
		Total        int          `json:"total"`

		Aborted       atomic.Bool `json:"aborted"`
		AllDispatched atomic.Bool `json:"all_dispatched"`
	}

	ListBucketPageCb func(bucket, pageMarker string) (*cmn.BucketList, error)
	TargetObjsCb     func(objects cmn.SimpleKVs, bucket string, cloud bool) ([]cmn.DlObj, error)
)

func (j *BaseDownloadJob) ID() string          { return j.id }
func (j *BaseDownloadJob) Bucket() string      { return j.bucket }
func (j *BaseDownloadJob) BckProvider() string { return j.bckProvider }
func (j *BaseDownloadJob) Timeout() string     { return j.timeout }
func (j *BaseDownloadJob) Description() string { return j.description }

func NewBaseDownloadJob(id, bucket, bckprovider, timeout, desc string) *BaseDownloadJob {
	return &BaseDownloadJob{
		id:          id,
		bucket:      bucket,
		bckProvider: bckprovider,
		timeout:     timeout,
		description: desc,
	}
}

func (j *SliceDownloadJob) Len() int { return len(j.objs) }
func (j *SliceDownloadJob) GenNext() (objs []cmn.DlObj, ok bool) {
	if j.current == len(j.objs) {
		return []cmn.DlObj{}, false
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

func NewSliceDownloadJob(id string, objs []cmn.DlObj, bucket, bckProvider, timeout, description string) *SliceDownloadJob {
	return &SliceDownloadJob{
		BaseDownloadJob: BaseDownloadJob{
			id:          id,
			bucket:      bucket,
			bckProvider: bckProvider,
			timeout:     timeout,
			description: description,
		},
		objs:    objs,
		timeout: timeout,
	}
}

func (j *CloudBucketDownloadJob) Len() int { return -1 }
func (j *CloudBucketDownloadJob) GenNext() (objs []cmn.DlObj, ok bool) {
	j.wg.Wait()

	if len(j.objs) == 0 {
		return nil, false
	}

	readyToDownloadObjs := j.objs
	j.wg.Add(1)
	go func() {
		if err := j.GetNextObjs(); err != nil {
			glog.Error(err)
			// this makes GenNext return ok = false in the next
			// loop iteration
			j.objs, j.pageMarker = []cmn.DlObj{}, ""
		}
		j.wg.Done()
	}()

	return readyToDownloadObjs, true
}

func (j *CloudBucketDownloadJob) GetNextObjs() error {
	if j.pagesCnt > 0 && j.pageMarker == "" {
		// Cloud ListBucket returned empty pageMarker after at least one reqest
		// this means there are no more objects in to list
		j.objs = []cmn.DlObj{}
		return nil
	}

	j.pagesCnt++
	msg := &cmn.SelectMsg{
		Prefix:     j.prefix,
		PageMarker: j.pageMarker,
		Fast:       true,
		PageSize:   cmn.DefaultPageSize,
	}

	bckList, err, _ := j.t.Cloud().ListBucket(j.ctx, j.bucket, msg)
	if err != nil {
		return err
	}
	j.pageMarker = msg.PageMarker

	objects := make(cmn.SimpleKVs, cmn.DefaultPageSize)
	for _, entry := range bckList.Entries {
		si, errstr := j.t.HRWTarget(j.bucket, entry.Name)
		if errstr != "" {
			return errors.New(errstr)
		}

		if !strings.HasSuffix(entry.Name, j.suffix) || si.ID() != j.t.Snode().ID() {
			continue
		}
		objects[entry.Name] = ""
	}

	dl, err := GetTargetDlObjs(j.t, objects, j.bucket, true)
	if err != nil {
		return err
	}
	j.objs = dl

	return nil
}

func NewCloudBucketDownloadJob(ctx context.Context, t cluster.Target, base *BaseDownloadJob, prefix, suffix string) (*CloudBucketDownloadJob, error) {
	job := &CloudBucketDownloadJob{
		BaseDownloadJob: *base,
		pageMarker:      "",
		t:               t,
		ctx:             ctx,
		prefix:          prefix,
		suffix:          suffix,
		wg:              &sync.WaitGroup{},
	}

	err := job.GetNextObjs()
	return job, err
}
