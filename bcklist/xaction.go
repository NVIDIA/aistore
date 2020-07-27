// Package bcklist provides xaction and utilities for listing bucket objects.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package bcklist

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sort"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/objwalk"
	"github.com/NVIDIA/aistore/objwalk/walkinfo"
	"github.com/NVIDIA/aistore/xaction/demand"
)

// Xaction is on-demand one to avoid creating a new xaction per page even
// in passthrough mode. It just restarts `walk` if needed.
// Xaction is created once per bucket list request (per UUID)
type BckListTask struct {
	demand.XactDemandBase
	ctx        context.Context
	t          cluster.Target
	msg        *cmn.SelectMsg
	walkStopCh *cmn.StopCh           // to abort file walk
	cluBck     *cluster.Bck          // cluster.Bck constructed from the original cmn.Bck
	remoteBck  *cluster.Bck          // differs from cluBck
	workCh     chan *bckListReq      // incoming requests
	objCache   chan *cmn.BucketEntry // local cache filled when idle
	walkWg     sync.WaitGroup        // to wait until walk finishes
	pageError  error                 // when requesting a page from Cloud bucket
	lastMarker string                // last requested PageMarker (to detect re-requesting)
	lastPage   []*cmn.BucketEntry    // last sent page and a little more
	inProgress atomic.Bool           // the page is in progress
	walkDone   bool                  // true: done walking or Cloud returned all objects
	fromRemote bool                  // whether to request remote data (Cloud/Remote/Backend)
}

type bckListReq struct {
	action string
	msg    *cmn.SelectMsg
	ch     chan *BckListResp
}
type BckListResp struct {
	BckList *cmn.BucketList
	Status  int
	Err     error
}
type BckListCallback = func(resp *BckListResp)

const (
	bckListReqSize = 32  // the size of xaction request queue
	cacheSize      = 128 // the size of local cache filled in advance when idle
)

var (
	errStopped = errors.New("stopped")
	ErrGone    = errors.New("gone")
)

func NewBckListTask(ctx context.Context, t cluster.Target, bck cmn.Bck,
	smsg *cmn.SelectMsg, uuid string) *BckListTask {
	idleTime := cmn.GCO.Get().Timeout.SendFile
	xact := &BckListTask{
		ctx:      ctx,
		t:        t,
		msg:      smsg,
		workCh:   make(chan *bckListReq, bckListReqSize),
		objCache: make(chan *cmn.BucketEntry, cacheSize),
		lastPage: make([]*cmn.BucketEntry, 0, cacheSize),
	}
	xact.XactDemandBase = *demand.NewXactDemandBaseBckUUID(uuid, cmn.ActListObjects, bck, idleTime)
	return xact
}

func (r *BckListTask) String() string {
	return fmt.Sprintf("%s: %s", r.t.Snode(), &r.XactDemandBase)
}

func (r *BckListTask) Do(action string, msg *cmn.SelectMsg, ch chan *BckListResp) {
	if r.Finished() {
		ch <- &BckListResp{Err: ErrGone}
		close(ch)
		return
	}
	req := &bckListReq{
		action: action,
		msg:    msg,
		ch:     ch,
	}
	r.workCh <- req
}

// Starts fs.Walk beforehand if needed so that by the time we read the next page
// local cache is populated.
func (r *BckListTask) init() error {
	r.cluBck = cluster.NewBckEmbed(r.Bck())
	if err := r.cluBck.Init(r.t.GetBowner(), r.t.Snode()); err != nil {
		return err
	}
	r.fromRemote = !r.cluBck.IsAIS() && !r.msg.Cached
	// remote bucket listing is always paginated
	if r.fromRemote && r.msg.WantObjectsCnt() == 0 {
		r.msg.PageSize = cmn.DefaultListPageSize
	}
	if r.cluBck.IsAIS() && r.cluBck.HasBackendBck() {
		r.remoteBck = cluster.NewBckEmbed(r.cluBck.CloudBck())
		if err := r.remoteBck.Init(r.t.GetBowner(), r.t.Snode()); err != nil {
			return err
		}
	} else if !r.cluBck.IsAIS() {
		r.remoteBck = r.cluBck
	}
	if r.fromRemote {
		return nil
	}

	r.walkStopCh = cmn.NewStopCh()
	r.walkWg.Add(1)
	go r.traverseBucket()
	return nil
}

func (r *BckListTask) Run() (err error) {
	glog.Infoln(r.String())
	if err := r.init(); err != nil {
		return err
	}

	for {
		select {
		case req := <-r.workCh:
			// Copy only the values that can change between calls
			debug.Assert(r.msg.Passthrough == req.msg.Passthrough)
			debug.Assert(r.msg.Prefix == req.msg.Prefix)
			debug.Assert(r.msg.Cached == req.msg.Cached)
			r.msg.PageMarker = req.msg.PageMarker
			if !r.fromRemote || req.msg.PageSize != 0 {
				r.msg.PageSize = req.msg.PageSize
			}
			resp := r.dispatchRequest(req.action)
			req.ch <- resp
			close(req.ch)
		case <-r.IdleTimer():
			r.Stop(nil)
			return nil
		case <-r.ChanAbort():
			r.Stop(nil)
			return cmn.NewAbortedError(r.String())
		}
	}
}

func (r *BckListTask) stopWalk() {
	if r.walkStopCh != nil {
		r.walkStopCh.Close()
		r.walkWg.Wait()
	}
}

func (r *BckListTask) Stop(err error) {
	r.XactDemandBase.Stop()
	r.Finish()
	close(r.workCh)
	r.stopWalk()
	if err == nil {
		glog.Infoln(r.String())
	} else {
		glog.Errorf("%s: stopped with err %v", r, err)
	}
}

func (r *BckListTask) dispatchRequest(action string) *BckListResp {
	cnt := int(r.msg.WantObjectsCnt())
	marker := r.msg.PageMarker
	switch action {
	case cmn.TaskStart:
		if err := r.genNextPage(marker, cnt); err != nil {
			return &BckListResp{
				Status: http.StatusInternalServerError,
				Err:    err,
			}
		}
		return &BckListResp{Status: http.StatusAccepted}
	case cmn.TaskStatus, cmn.TaskResult:
		defer r.DecPending()
		if r.pageInProgress() {
			return &BckListResp{Status: http.StatusAccepted}
		}
		if !r.pageIsValid(marker, cnt) {
			return &BckListResp{
				Status: http.StatusBadRequest,
				Err:    fmt.Errorf("%s: the page for %s is not initialized", r, marker),
			}
		}
		if r.pageError != nil {
			return &BckListResp{
				Status: http.StatusInternalServerError,
				Err:    r.pageError,
			}
		}
		if action == cmn.TaskStatus {
			return &BckListResp{Status: http.StatusOK}
		}
		list, err := r.getPage(marker, cnt)
		status := http.StatusOK
		if err != nil {
			status = http.StatusInternalServerError
		}
		return &BckListResp{
			BckList: list,
			Status:  status,
			Err:     err,
		}
	default:
		r.DecPending()
		return &BckListResp{
			Status: http.StatusBadRequest,
			Err:    fmt.Errorf("%s: invalid action %s", r, action),
		}
	}
}

func (r *BckListTask) IsMountpathXact() bool { return true }

func (r *BckListTask) walkCallback(lom *cluster.LOM) {
	r.ObjectsInc()
	r.BytesAdd(lom.Size())
}

func (r *BckListTask) walkCtx() context.Context {
	return context.WithValue(
		context.Background(),
		walkinfo.CtxPostCallbackKey,
		walkinfo.PostCallbackFunc(r.walkCallback),
	)
}

func (r *BckListTask) nextPageAIS(marker string, cnt int) {
	defer r.DecPending()
	if r.isPageCached(marker, cnt) {
		if !r.inProgress.CAS(true, false) {
			cmn.Assert(false)
		}
		return
	}
	read := 0
	for read < cnt || cnt == 0 {
		obj, ok := <-r.objCache
		if !ok {
			r.walkDone = true
			break
		}
		read++
		r.lastPage = append(r.lastPage, obj)
	}
	if !r.inProgress.CAS(true, false) {
		cmn.Assert(false)
	}
}

// Retunrs an index of the first objects in the cache that follows marker
func (r *BckListTask) findMarker(marker string) int {
	cond := func(i int) bool { return !cmn.PageMarkerIncludesObject(marker, r.lastPage[i].Name) }
	return sort.Search(len(r.lastPage), cond)
}

func (r *BckListTask) isPageCached(marker string, cnt int) bool {
	if r.walkDone {
		return true
	}
	if cnt == 0 {
		return false
	}
	idx := r.findMarker(marker)
	return idx+cnt < len(r.lastPage)
}

func (r *BckListTask) nextPageCloud(marker string, cnt int) {
	defer r.DecPending()
	if r.isPageCached(marker, cnt) {
		return
	}

	walk := objwalk.NewWalk(r.walkCtx(), r.t, r.remoteBck, r.msg)
	bckList, err := walk.CloudObjPage()
	if err != nil {
		r.pageError = err
	} else {
		if bckList.PageMarker == "" {
			r.walkDone = true
		}
		r.lastPage = append(r.lastPage, bckList.Entries...)
	}
	if !r.inProgress.CAS(true, false) {
		cmn.Assert(false)
	}
}

func (r *BckListTask) pageInProgress() bool {
	return r.inProgress.Load()
}

// Called before generating a page for a proxy. It is OK if the page is
// still in progress. If the page is done, the function ensures that the
// local cache contains the requested data.
func (r *BckListTask) pageIsValid(marker string, cnt int) bool {
	if r.pageInProgress() {
		return true
	}
	// The same page is re-requested
	if r.lastMarker == marker {
		return true
	}
	if cmn.PageMarkerIncludesObject(r.lastMarker, marker) {
		// Requested a status about page returned a few pages ago
		return false
	}
	idx := r.findMarker(marker)
	inCache := idx+cnt <= len(r.lastPage)
	return inCache || r.walkDone
}

func (r *BckListTask) getPage(marker string, cnt int) (*cmn.BucketList, error) {
	var (
		idx  = r.findMarker(marker)
		list = r.lastPage[idx:]
	)
	if len(list) < cnt && !r.walkDone {
		return nil, errors.New("page is not loaded yet")
	}
	cmn.Assert(!r.pageInProgress())
	cmn.Assert(r.msg.UUID != "")
	if cnt != 0 && len(list) >= cnt {
		entries := list[:cnt]
		return &cmn.BucketList{
			Entries:    entries,
			PageMarker: entries[cnt-1].Name,
			UUID:       r.msg.UUID,
		}, nil
	}
	return &cmn.BucketList{Entries: list, UUID: r.msg.UUID}, nil
}

// TODO: support arbitrary page marker (do restart in this case).
// genNextPage calls DecPending either immediately on error or inside
// a goroutine if some work must be done.
func (r *BckListTask) genNextPage(marker string, cnt int) error {
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s: marker [%s]", r, r.msg.PageMarker)
	}
	if marker != "" && marker == r.lastMarker {
		r.DecPending()
		return nil
	}
	if r.walkDone {
		r.DecPending()
		return nil
	}
	if !r.inProgress.CAS(false, true) {
		r.DecPending()
		return errors.New("another page is in progress")
	}

	r.discardObsolete(r.lastMarker)
	if r.lastMarker < marker {
		r.lastMarker = marker
	}
	if r.fromRemote {
		go r.nextPageCloud(marker, cnt)
		return nil
	}

	go r.nextPageAIS(marker, cnt)
	return nil
}

// Removes from local cache, the objects that have been already sent
func (r *BckListTask) discardObsolete(marker string) {
	if marker == "" || len(r.lastPage) == 0 {
		return
	}
	j := r.findMarker(marker)
	// Entire cache is "after" page marker, keep the whole cache
	if j == 0 {
		return
	}
	l := len(r.lastPage)
	// All the cache data have been sent to clients, clean it up
	if j == l {
		r.lastPage = r.lastPage[:0]
		return
	}
	// To reuse local cache, copy items and fix the slice length
	copy(r.lastPage[0:], r.lastPage[j:])
	r.lastPage = r.lastPage[:l-j]
}

func (r *BckListTask) traverseBucket() {
	wi := walkinfo.NewWalkInfo(r.walkCtx(), r.t, r.msg)
	defer r.walkWg.Done()
	cb := func(fqn string, de fs.DirEntry) error {
		entry, err := wi.Callback(fqn, de)
		if err != nil || entry == nil {
			return err
		}
		select {
		case r.objCache <- entry:
			/* do nothing */
		case <-r.walkStopCh.Listen():
			return errStopped
		}
		return nil
	}
	opts := &fs.WalkBckOptions{
		Options: fs.Options{
			Bck:      r.Bck(),
			CTs:      []string{fs.ObjectType},
			Callback: cb,
			Sorted:   true,
		},
		ValidateCallback: func(fqn string, de fs.DirEntry) error {
			if de.IsDir() {
				return wi.ProcessDir(fqn)
			}
			return nil
		},
	}

	if err := fs.WalkBck(opts); err != nil {
		if err != filepath.SkipDir && err != errStopped {
			glog.Errorf("%s walk failed, err %v", r, err)
		}
	}
	close(r.objCache)
}
