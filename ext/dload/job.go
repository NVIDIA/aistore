// Package dload implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package dload

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/nl"
)

const (
	// Determines the size of single batch size generated in `genNext`.
	downloadBatchSize = 10_000
)

// interface guard
var (
	_ jobif = (*sliceDlJob)(nil)
	_ jobif = (*backendDlJob)(nil)
	_ jobif = (*rangeDlJob)(nil)
)

type (
	dlObj struct {
		objName    string
		link       string
		fromRemote bool
	}

	jobif interface {
		ID() string
		XactID() string
		Bck() *cmn.Bck
		Description() string
		Timeout() time.Duration
		ActiveStats() (*StatusResp, error)
		String() string
		Notif() cluster.Notif // notifications
		AddNotif(n cluster.Notif, job jobif)

		// If total length (size) of download job is not known, -1 should be returned.
		Len() int

		// Determines if it requires also syncing.
		Sync() bool

		// Checks if object name matches the request.
		checkObj(objName string) bool

		// genNext is supposed to fulfill the following protocol:
		//  `ok` is set to `true` if there is batch to process, `false` otherwise
		genNext() (objs []dlObj, ok bool, err error)

		// via tryAcquire and release
		throttler() *throttler

		// job cleanup
		cleanup()
	}

	baseDlJob struct {
		bck         *meta.Bck
		notif       *NotifDownload
		xdl         *Xact
		id          string
		description string
		timeout     time.Duration
		throt       throttler
	}

	sliceDlJob struct {
		baseDlJob
		objs    []dlObj
		current int
	}
	multiDlJob struct {
		sliceDlJob
	}
	singleDlJob struct {
		sliceDlJob
	}

	rangeDlJob struct {
		baseDlJob
		objs  []dlObj            // objects' metas which are ready to be downloaded
		pt    cos.ParsedTemplate // range template
		dir   string             // objects directory(prefix) from request
		count int                // total number object to download by a target
		done  bool               // true when iterator is finished, nothing left to read
	}

	backendDlJob struct {
		baseDlJob
		prefix            string
		suffix            string
		continuationToken string
		objs              []dlObj // objects' metas which are ready to be downloaded
		sync              bool
		done              bool
	}

	dljob struct {
		id            string
		xid           string
		description   string
		startedTime   time.Time
		finishedTime  atomic.Time
		finishedCnt   atomic.Int32
		scheduledCnt  atomic.Int32
		skippedCnt    atomic.Int32
		errorCnt      atomic.Int32
		total         int
		aborted       atomic.Bool
		allDispatched atomic.Bool
	}
)

///////////////
// baseDlJob //
///////////////

func (j *baseDlJob) init(id string, bck *meta.Bck, timeout, desc string, limits Limits, xdl *Xact) {
	// TODO: this might be inaccurate if we download 1 or 2 objects because then
	//  other targets will have limits but will not use them.
	if limits.BytesPerHour > 0 {
		limits.BytesPerHour /= cluster.T.Sowner().Get().CountActiveTs()
	}
	td, _ := time.ParseDuration(timeout)
	{
		j.id = id
		j.bck = bck
		j.timeout = td
		j.description = desc
		j.throt.init(limits)
		j.xdl = xdl
	}
}

func (j *baseDlJob) ID() string             { return j.id }
func (j *baseDlJob) XactID() string         { return j.xdl.ID() }
func (j *baseDlJob) Bck() *cmn.Bck          { return j.bck.Bucket() }
func (j *baseDlJob) Timeout() time.Duration { return j.timeout }
func (j *baseDlJob) Description() string    { return j.description }
func (*baseDlJob) Sync() bool               { return false }

func (j *baseDlJob) String() (s string) {
	s = fmt.Sprintf("dl-job[%s]-%s", j.ID(), j.Bck())
	if j.Description() == "" {
		return
	}
	return s + "-" + j.Description()
}

func (j *baseDlJob) Notif() cluster.Notif { return j.notif }

func (j *baseDlJob) AddNotif(n cluster.Notif, job jobif) {
	var ok bool
	debug.Assert(j.notif == nil) // currently, "add" means "set"
	j.notif, ok = n.(*NotifDownload)
	debug.Assert(ok)
	j.notif.job = job
	debug.Assert(j.notif.F != nil)
	if n.Upon(cluster.UponProgress) {
		debug.Assert(j.notif.P != nil)
	}
}

func (j *baseDlJob) ActiveStats() (*StatusResp, error) {
	resp, _, err := j.xdl.JobStatus(j.ID(), true /*onlyActive*/)
	if err != nil {
		return nil, err
	}
	return resp.(*StatusResp), nil
}

func (*baseDlJob) checkObj(string) bool    { debug.Assert(false); return false }
func (j *baseDlJob) throttler() *throttler { return &j.throt }

func (j *baseDlJob) cleanup() {
	j.throttler().stop()
	err := g.store.markFinished(j.ID())
	if err != nil {
		nlog.Errorf("%s: %v", j, err)
	}
	g.store.flush(j.ID())
	nl.OnFinished(j.Notif(), err)
}

//
// sliceDlJob -- multiDlJob -- singleDlJob
//

func (j *sliceDlJob) init(bck *meta.Bck, objects cos.StrKVs) error {
	objs, err := buildDlObjs(bck, objects)
	if err != nil {
		return err
	}
	j.objs = objs
	return nil
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

func newMultiDlJob(id string, bck *meta.Bck, payload *MultiBody, xdl *Xact) (mj *multiDlJob, err error) {
	var objs cos.StrKVs

	mj = &multiDlJob{}
	mj.baseDlJob.init(id, bck, payload.Timeout, payload.Describe(), payload.Limits, xdl)

	if objs, err = payload.ExtractPayload(); err != nil {
		return nil, err
	}
	err = mj.sliceDlJob.init(bck, objs)
	return
}

func (j *multiDlJob) String() (s string) { return "multi-" + j.baseDlJob.String() }

func newSingleDlJob(id string, bck *meta.Bck, payload *SingleBody, xdl *Xact) (sj *singleDlJob, err error) {
	var objs cos.StrKVs

	sj = &singleDlJob{}
	sj.baseDlJob.init(id, bck, payload.Timeout, payload.Describe(), payload.Limits, xdl)

	if objs, err = payload.ExtractPayload(); err != nil {
		return nil, err
	}
	err = sj.sliceDlJob.init(bck, objs)
	return
}

func (j *singleDlJob) String() (s string) {
	return "single-" + j.baseDlJob.String()
}

////////////////
// rangeDlJob //
////////////////

// NOTE: the sizes of objects to be downloaded will be unknown.
func newRangeDlJob(id string, bck *meta.Bck, payload *RangeBody, xdl *Xact) (rj *rangeDlJob, err error) {
	rj = &rangeDlJob{}
	if rj.pt, err = cos.ParseBashTemplate(payload.Template); err != nil {
		return nil, err
	}
	rj.baseDlJob.init(id, bck, payload.Timeout, payload.Describe(), payload.Limits, xdl)

	if rj.count, err = countObjects(rj.pt, payload.Subdir, rj.bck); err != nil {
		return nil, err
	}
	rj.pt.InitIter()
	rj.dir = payload.Subdir
	return
}

func (j *rangeDlJob) SrcBck() *cmn.Bck { return j.bck.Bucket() }
func (j *rangeDlJob) Len() int         { return j.count }

func (j *rangeDlJob) genNext() ([]dlObj, bool, error) {
	if j.done {
		return nil, false, nil
	}
	if err := j.getNextObjs(); err != nil {
		return nil, false, err
	}
	return j.objs, true, nil
}

func (j *rangeDlJob) String() (s string) {
	return fmt.Sprintf("range-%s-%d-%s", &j.baseDlJob, j.count, j.dir)
}

func (j *rangeDlJob) getNextObjs() error {
	var (
		smap = cluster.T.Sowner().Get()
		sid  = cluster.T.SID()
	)
	j.objs = j.objs[:0]
	for len(j.objs) < downloadBatchSize {
		link, ok := j.pt.Next()
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

//////////////////
// backendDlJob //
//////////////////

func newBackendDlJob(id string, bck *meta.Bck, payload *BackendBody, xdl *Xact) (bj *backendDlJob, err error) {
	if !bck.IsRemote() {
		return nil, errors.New("bucket download requires a remote bucket")
	} else if bck.IsHTTP() {
		return nil, errors.New("bucket download does not support HTTP buckets")
	}
	bj = &backendDlJob{}
	bj.baseDlJob.init(id, bck, payload.Timeout, payload.Describe(), payload.Limits, xdl)
	{
		bj.sync = payload.Sync
		bj.prefix = payload.Prefix
		bj.suffix = payload.Suffix
	}
	return
}

func (*backendDlJob) Len() int     { return -1 }
func (j *backendDlJob) Sync() bool { return j.sync }

func (j *backendDlJob) String() (s string) {
	return fmt.Sprintf("backend-%s-%s-%s", &j.baseDlJob, j.prefix, j.suffix)
}

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
		sid     = cluster.T.SID()
		smap    = cluster.T.Sowner().Get()
		backend = cluster.T.Backend(j.bck)
	)
	j.objs = j.objs[:0]
	for len(j.objs) < downloadBatchSize {
		var (
			lst = &cmn.LsoResult{}
			msg = &apc.LsoMsg{Prefix: j.prefix, ContinuationToken: j.continuationToken, PageSize: backend.MaxPageSize()}
		)
		_, err := backend.ListObjects(j.bck, msg, lst)
		if err != nil {
			return err
		}
		j.continuationToken = lst.ContinuationToken

		for _, entry := range lst.Entries {
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

///////////
// dljob //
///////////

func (j *dljob) clone() Job {
	return Job{
		ID:            j.id,
		XactID:        j.xid,
		Description:   j.description,
		FinishedCnt:   int(j.finishedCnt.Load()),
		ScheduledCnt:  int(j.scheduledCnt.Load()),
		SkippedCnt:    int(j.skippedCnt.Load()),
		ErrorCnt:      int(j.errorCnt.Load()),
		Total:         j.total,
		AllDispatched: j.allDispatched.Load(),
		Aborted:       j.aborted.Load(),
		StartedTime:   j.startedTime,
		FinishedTime:  j.finishedTime.Load(),
	}
}

// Used for debugging purposes to ensure integrity of the struct.
func (j *dljob) valid() (err error) {
	if j.aborted.Load() {
		return
	}
	if !j.allDispatched.Load() {
		return
	}
	if a, b, c := j.scheduledCnt.Load(), j.finishedCnt.Load(), j.errorCnt.Load(); a != b+c {
		err = fmt.Errorf("invalid: %d != %d + %d", a, b, c)
	}
	return
}
