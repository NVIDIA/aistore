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
		GenNext() (objs []cmn.DlObj, ok bool)

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
		objs    []cmn.DlObj
		timeout string
		current int
	}

	CloudBucketDlJob struct {
		BaseDlJob
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
		ErrorCnt     atomic.Int32 `json:"errors"`

		Aborted       atomic.Bool `json:"aborted"`
		AllDispatched atomic.Bool `json:"all_dispatched"`

		FinishedTime atomic.Time `json:"-"`
	}

	ListBucketPageCb func(bucket, pageMarker string) (*cmn.BucketList, error)
	TargetObjsCb     func(objects cmn.SimpleKVs, bucket string, cloud bool) ([]cmn.DlObj, error)
)

func (j *BaseDlJob) ID() string          { return j.id }
func (j *BaseDlJob) Bck() cmn.Bck        { return j.bck.Bck }
func (j *BaseDlJob) Timeout() string     { return j.timeout }
func (j *BaseDlJob) Description() string { return j.description }

func NewBaseDlJob(id string, bck *cluster.Bck, timeout, desc string) *BaseDlJob {
	return &BaseDlJob{id: id, bck: bck, timeout: timeout, description: desc}
}

func (j *SliceDlJob) Len() int { return len(j.objs) }
func (j *SliceDlJob) GenNext() (objs []cmn.DlObj, ok bool) {
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

func NewSliceDlJob(id string, objs []cmn.DlObj, bck *cluster.Bck, timeout, description string) *SliceDlJob {
	return &SliceDlJob{
		BaseDlJob: BaseDlJob{id: id, bck: bck, timeout: timeout, description: description},
		objs:      objs,
		timeout:   timeout,
	}
}

func (j *CloudBucketDlJob) Len() int { return -1 }
func (j *CloudBucketDlJob) GenNext() (objs []cmn.DlObj, ok bool) {
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

func (j *CloudBucketDlJob) GetNextObjs() error {
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
		PageSize:   cmn.DefaultListPageSize,
	}

	bckList, err, _ := j.t.Cloud().ListBucket(j.ctx, j.bck.Name, msg)
	if err != nil {
		return err
	}
	j.pageMarker = msg.PageMarker

	objects := make(cmn.SimpleKVs, cmn.DefaultListPageSize)
	smap := j.t.GetSowner().Get()
	for _, entry := range bckList.Entries {
		si, err := cluster.HrwTarget(j.bck.MakeUname(entry.Name), smap)
		if err != nil {
			return err
		}

		if !strings.HasSuffix(entry.Name, j.suffix) || si.ID() != j.t.Snode().ID() {
			continue
		}
		objects[entry.Name] = ""
	}

	dl, err := GetTargetDlObjs(j.t, objects, j.bck)
	if err != nil {
		return err
	}
	j.objs = dl

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
		wg:         &sync.WaitGroup{},
	}

	err := job.GetNextObjs()
	return job, err
}
