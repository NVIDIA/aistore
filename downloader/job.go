// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn"
)

const sliceDownloadBatchSize = 1000

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
		// TODO: we might not know len in some cases, like cloud buckets
		Len() int
	}

	SliceDownloadJob struct {
		id          string
		objs        []cmn.DlObj
		bucket      string
		bckProvider string
		timeout     string
		current     int
		description string
	}

	DownloadJobInfo struct {
		ID          string `json:"id"`
		Bucket      string `json:"bck"`
		BckProvider string `json:"bckprovider"`
		Description string `json:"description"`

		FinishedCnt  *atomic.Int32 `json:"finished"`
		ScheduledCnt *atomic.Int32 `json:"scheduled"`
		Total        int           `json:"total"`

		Aborted       *atomic.Bool `json:"aborted"`
		AllDispatched *atomic.Bool `json:"all_dispatched"`
	}
)

func (j *SliceDownloadJob) ID() string          { return j.id }
func (j *SliceDownloadJob) Bucket() string      { return j.bucket }
func (j *SliceDownloadJob) BckProvider() string { return j.bckProvider }
func (j *SliceDownloadJob) Len() int            { return len(j.objs) }
func (j *SliceDownloadJob) Timeout() string     { return j.timeout }
func (j *SliceDownloadJob) Description() string { return j.description }
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
		id:          id,
		objs:        objs,
		bucket:      bucket,
		bckProvider: bckProvider,
		timeout:     timeout,
		description: description,
	}
}
