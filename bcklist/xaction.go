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
	bck        *cluster.Bck
	workCh     chan *bckListReq      // incoming requests
	objCache   chan *cmn.BucketEntry // local cache filled when idle
	lastPage   []*cmn.BucketEntry    // last sent page and a little more
	msg        *cmn.SelectMsg
	walkStopCh *cmn.StopCh    // to abort file walk
	token      string         // the continuation token for the last sent page (for re-requests)
	nextToken  string         // continuation token returned by Cloud to get the next page
	walkWg     sync.WaitGroup // to wait until walk finishes
	walkDone   bool           // true: done walking or Cloud returned all objects
	fromRemote bool           // whether to request remote data (Cloud/Remote/Backend)
}

type bckListReq struct {
	msg *cmn.SelectMsg
	ch  chan *BckListResp
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

func (r *BckListTask) Do(msg *cmn.SelectMsg, ch chan *BckListResp) {
	if r.Finished() {
		ch <- &BckListResp{Err: ErrGone}
		close(ch)
		return
	}
	req := &bckListReq{
		msg: msg,
		ch:  ch,
	}
	r.workCh <- req
}

// Starts fs.Walk beforehand if needed so that by the time we read the next page
// local cache is populated.
func (r *BckListTask) init() error {
	r.bck = cluster.NewBckEmbed(r.Bck())
	if err := r.bck.Init(r.t.GetBowner(), r.t.Snode()); err != nil {
		return err
	}
	r.fromRemote = !r.bck.IsAIS() && !r.msg.Cached
	// remote bucket listing is always paginated
	if r.fromRemote && r.msg.WantObjectsCnt() == 0 {
		r.msg.PageSize = cmn.DefaultListPageSize
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
			debug.Assert(r.msg.UseCache == req.msg.UseCache)
			debug.Assert(r.msg.Prefix == req.msg.Prefix)
			debug.Assert(r.msg.Cached == req.msg.Cached)
			r.msg.ContinuationToken = req.msg.ContinuationToken
			if !r.fromRemote || req.msg.PageSize != 0 {
				r.msg.PageSize = req.msg.PageSize
			}
			resp := r.dispatchRequest()
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

func (r *BckListTask) dispatchRequest() *BckListResp {
	var (
		cnt   = int(r.msg.WantObjectsCnt())
		token = r.msg.ContinuationToken
	)

	r.IncPending()
	defer r.DecPending()

	if err := r.genNextPage(token, cnt); err != nil {
		return &BckListResp{
			Status: http.StatusInternalServerError,
			Err:    err,
		}
	}
	if !r.pageIsValid(token, cnt) {
		return &BckListResp{
			Status: http.StatusBadRequest,
			Err:    fmt.Errorf("the page for %q was not initialized", token),
		}
	}

	list, err := r.getPage(token, cnt)
	status := http.StatusOK
	if err != nil {
		status = http.StatusInternalServerError
	}
	return &BckListResp{
		BckList: list,
		Status:  status,
		Err:     err,
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

func (r *BckListTask) nextPageAIS(marker string, cnt int) error {
	if r.isPageCached(marker, cnt) {
		return nil
	}
	read := 0
	for read < cnt || cnt == 0 {
		obj, ok := <-r.objCache
		if !ok {
			r.walkDone = true
			break
		}
		// Skip all objects until the requested marker.
		if cmn.TokenIncludesObject(marker, obj.Name) {
			continue
		}
		read++
		r.lastPage = append(r.lastPage, obj)
	}
	return nil
}

// Returns an index of the first objects in the cache that follows marker
func (r *BckListTask) findMarker(marker string) int {
	if r.fromRemote && r.token == marker {
		return 0
	}
	cond := func(i int) bool { return !cmn.TokenIncludesObject(marker, r.lastPage[i].Name) }
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

func (r *BckListTask) nextPageCloud() error {
	walk := objwalk.NewWalk(r.walkCtx(), r.t, r.bck, r.msg)
	bckList, err := walk.CloudObjPage()
	if err != nil {
		r.nextToken = ""
		return err
	}
	if bckList.ContinuationToken == "" {
		r.walkDone = true
	}
	r.lastPage = bckList.Entries
	r.nextToken = bckList.ContinuationToken
	r.lastPage = append(r.lastPage, bckList.Entries...)
	return nil
}

// Called before generating a page for a proxy. It is OK if the page is
// still in progress. If the page is done, the function ensures that the
// local cache contains the requested data.
func (r *BckListTask) pageIsValid(marker string, cnt int) bool {
	// The same page is re-requested
	if r.token == marker {
		return true
	}
	if r.fromRemote {
		return r.walkDone || len(r.lastPage) >= cnt
	}
	if cmn.TokenIncludesObject(r.token, marker) {
		// Requested a status about page returned a few pages ago
		return false
	}
	idx := r.findMarker(marker)
	inCache := idx+cnt <= len(r.lastPage)
	return inCache || r.walkDone
}

func (r *BckListTask) getPage(marker string, cnt int) (*cmn.BucketList, error) {
	cmn.Assert(r.msg.UUID != "")
	if r.fromRemote {
		return &cmn.BucketList{
			UUID:              r.msg.UUID,
			Entries:           r.lastPage,
			ContinuationToken: r.nextToken,
		}, nil
	}

	var (
		idx  = r.findMarker(marker)
		list = r.lastPage[idx:]
	)
	if len(list) < cnt && !r.walkDone {
		return nil, errors.New("page is not loaded yet")
	}
	if cnt != 0 && len(list) >= cnt {
		entries := list[:cnt]
		return &cmn.BucketList{
			UUID:              r.msg.UUID,
			Entries:           entries,
			ContinuationToken: entries[cnt-1].Name,
		}, nil
	}
	return &cmn.BucketList{Entries: list, UUID: r.msg.UUID}, nil
}

// TODO: support arbitrary page marker (do restart in this case).
// genNextPage calls DecPending either immediately on error or inside
// a goroutine if some work must be done.
func (r *BckListTask) genNextPage(token string, cnt int) error {
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[%s] token: %q", r, r.msg.ContinuationToken)
	}
	if token != "" && token == r.token {
		return nil
	}
	if r.walkDone {
		return nil
	}

	// Due to impossibility of getting object name from continuation token,
	// in case of Cloud, a target keeps only the entire last sent page.
	// The page is replaced with a new one when a client asks for next page.
	if r.fromRemote {
		r.token = token
		return r.nextPageCloud()
	}
	r.discardObsolete(r.token)
	if r.token < token {
		r.token = token
	}
	return r.nextPageAIS(token, cnt)
}

// Removes from local cache, the objects that have been already sent.
// Use only for AIS buckets(or Cached:true requests) - in other cases
// the marker, in general, is not an object name
func (r *BckListTask) discardObsolete(token string) {
	if token == "" || len(r.lastPage) == 0 {
		return
	}
	j := r.findMarker(token)
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
