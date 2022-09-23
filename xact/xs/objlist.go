// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/objwalk"
	"github.com/NVIDIA/aistore/objwalk/walkinfo"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/vmihailenco/msgpack"
)

// Xaction is on-demand one to avoid creating a new xaction per page even
// in passthrough mode. It just restarts `walk` if needed.
// Xaction is created once per bucket list request (per UUID)
type (
	olFactory struct {
		xreg.RenewBase
		xctn *ObjListXact
		msg  *apc.ListObjsMsg
	}
	ObjListXact struct {
		xact.DemandBase
		t   cluster.Target
		bck *cluster.Bck
		msg *apc.ListObjsMsg

		workCh chan *apc.ListObjsMsg // Incoming requests.
		respCh chan *Resp            // Outgoing responses.
		stopCh *cos.StopCh           // Informs about stopped xact.

		objCache   chan *cmn.ObjEntry // local cache filled when idle
		lastPage   []*cmn.ObjEntry    // last sent page and a little more
		walkStopCh *cos.StopCh        // to abort file walk
		token      string             // the continuation token for the last sent page (for re-requests)
		nextToken  string             // continuation token returned by Cloud to get the next page
		walkWg     sync.WaitGroup     // to wait until walk finishes
		walkDone   bool               // true: done walking or Cloud returned all objects
		fromRemote bool               // whether to request remote data
	}
	Resp struct {
		BckList *cmn.ListObjects
		Status  int
		Err     error
	}
	archEntry struct { // File inside an archive
		name string
		size uint64 // uncompressed size if possible
	}
)

const (
	cacheSize = 128 // the size of local cache filled in advance when idle
)

var (
	errStopped = errors.New("stopped")
	ErrGone    = errors.New("gone")
)

// interface guard
var (
	_ cluster.Xact   = (*ObjListXact)(nil)
	_ xreg.Renewable = (*olFactory)(nil)
)

func (*olFactory) New(args xreg.Args, bck *cluster.Bck) xreg.Renewable {
	p := &olFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, msg: args.Custom.(*apc.ListObjsMsg)}
	return p
}

func (p *olFactory) Start() error {
	p.xctn = newXact(p.T, p.Bck, p.msg, p.UUID())
	return nil
}

func (*olFactory) Kind() string        { return apc.ActList }
func (p *olFactory) Get() cluster.Xact { return p.xctn }

func (p *olFactory) WhenPrevIsRunning(xprev xreg.Renewable) (xreg.WPR, error) {
	debug.Assertf(false, "%s vs %s", p.Str(p.Kind()), xprev) // xreg.usePrev() must've returned true
	return xreg.WprUse, nil
}

func newXact(t cluster.Target, bck *cluster.Bck, lsmsg *apc.ListObjsMsg, uuid string) *ObjListXact {
	totallyIdle := cmn.GCO.Get().Timeout.MaxHostBusy.D()
	xctn := &ObjListXact{
		t:        t,
		bck:      bck,
		msg:      lsmsg,
		workCh:   make(chan *apc.ListObjsMsg),
		respCh:   make(chan *Resp),
		stopCh:   cos.NewStopCh(),
		lastPage: make([]*cmn.ObjEntry, 0, cacheSize),
	}
	debug.Assert(xctn.bck.Props != nil)
	xctn.DemandBase.Init(uuid, apc.ActList, bck, totallyIdle)
	return xctn
}

func (r *ObjListXact) String() string { return fmt.Sprintf("%s: %s", r.t, &r.DemandBase) }

// skip on-demand idle-ness check
func (r *ObjListXact) Abort(err error) (ok bool) {
	if ok = r.Base.Abort(err); ok {
		r.Finish(err)
	}
	return
}

func (r *ObjListXact) Do(msg *apc.ListObjsMsg) *Resp {
	// The guarantee here is that we either put something on the channel and our
	// request will be processed (since the `workCh` is unbuffered) or we receive
	// message that the xaction has been stopped.
	select {
	case r.workCh <- msg:
		return <-r.respCh
	case <-r.stopCh.Listen():
		return &Resp{Err: ErrGone}
	}
}

func (r *ObjListXact) init() {
	r.fromRemote = !r.bck.IsAIS() && !r.msg.IsFlagSet(apc.LsCached)
	if r.fromRemote {
		return
	}

	// Start fs.Walk beforehand if needed so that by the time we read
	// the next page local cache is populated.
	r._initTraverse()
}

func (r *ObjListXact) _initTraverse() {
	if r.walkStopCh != nil {
		r.walkStopCh.Close()
		r.walkWg.Wait()
	}

	r.objCache = make(chan *cmn.ObjEntry, cacheSize)
	r.walkDone = false
	r.walkStopCh = cos.NewStopCh()
	r.walkWg.Add(1)

	go r.traverseBucket(r.msg.Clone())
}

func (r *ObjListXact) Run(*sync.WaitGroup) {
	if verbose {
		glog.Infoln(r.String())
	}
	r.init()

	for {
		select {
		case msg := <-r.workCh:
			// Copy only the values that can change between calls
			debug.Assert(r.msg.Prefix == msg.Prefix && r.msg.Flags == msg.Flags)
			r.msg.ContinuationToken = msg.ContinuationToken
			r.msg.PageSize = msg.PageSize
			r.respCh <- r.dispatchRequest()
		case <-r.IdleTimer():
			r.stop(nil)
			return
		case errCause := <-r.ChanAbort():
			r.stop(errCause)
			return
		}
	}
}

func (r *ObjListXact) stopWalk() {
	if r.walkStopCh != nil {
		r.walkStopCh.Close()
		r.walkWg.Wait()
	}
}

func (r *ObjListXact) stop(err error) {
	r.DemandBase.Stop()
	r.stopCh.Close()
	// NOTE: Not closing `r.workCh` as it potentially could result in "sending on closed channel" panic.
	close(r.respCh)
	r.stopWalk()
	r.Finish(err)
}

func (r *ObjListXact) dispatchRequest() *Resp {
	var (
		cnt   = r.msg.PageSize
		token = r.msg.ContinuationToken
	)

	debug.Assert(cnt != 0)

	r.IncPending()
	defer r.DecPending()

	if err := r.genNextPage(token, cnt); err != nil {
		return &Resp{
			Status: http.StatusInternalServerError,
			Err:    err,
		}
	}

	objList := r.getPage(token, cnt)
	return &Resp{
		BckList: objList,
		Status:  http.StatusOK,
	}
}

func (r *ObjListXact) walkCallback(*cluster.LOM) { r.ObjsAdd(1, 0) }

func (r *ObjListXact) walkCtx() context.Context {
	return context.WithValue(
		context.Background(),
		walkinfo.CtxPostCallbackKey,
		walkinfo.PostCallbackFunc(r.walkCallback),
	)
}

func (r *ObjListXact) nextPageAIS(cnt uint) error {
	if r.isPageCached(r.token, cnt) {
		return nil
	}
	for read := uint(0); read < cnt; {
		obj, ok := <-r.objCache
		if !ok {
			r.walkDone = true
			break
		}
		// Skip all objects until the requested marker.
		if cmn.TokenIncludesObject(r.token, obj.Name) {
			continue
		}
		read++
		r.lastPage = append(r.lastPage, obj)
	}
	return nil
}

// Returns an index of the first objects in the cache that follows marker
func (r *ObjListXact) findMarker(marker string) uint {
	if r.fromRemote && r.token == marker {
		return 0
	}
	return uint(sort.Search(len(r.lastPage), func(i int) bool {
		return !cmn.TokenIncludesObject(marker, r.lastPage[i].Name)
	}))
}

func (r *ObjListXact) isPageCached(marker string, cnt uint) bool {
	if r.walkDone {
		return true
	}
	idx := r.findMarker(marker)
	return idx+cnt < uint(len(r.lastPage))
}

func (r *ObjListXact) nextPageRemote() error {
	walk := objwalk.NewWalk(r.walkCtx(), r.t, r.bck, r.msg)
	bckList, err := walk.NextRemoteObjPage()
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

func (r *ObjListXact) getPage(marker string, cnt uint) *cmn.ListObjects {
	debug.Assert(cos.IsValidUUID(r.msg.UUID))
	if r.fromRemote {
		return &cmn.ListObjects{
			UUID:              r.msg.UUID,
			Entries:           r.lastPage,
			ContinuationToken: r.nextToken,
		}
	}

	var (
		idx  = r.findMarker(marker)
		list = r.lastPage[idx:]
	)

	debug.Assert(uint(len(list)) >= cnt || r.walkDone)

	if uint(len(list)) >= cnt {
		entries := list[:cnt]
		return &cmn.ListObjects{
			UUID:              r.msg.UUID,
			Entries:           entries,
			ContinuationToken: entries[cnt-1].Name,
		}
	}
	return &cmn.ListObjects{Entries: list, UUID: r.msg.UUID}
}

// genNextPage calls DecPending either immediately on error or inside
// a goroutine if some work must be done.
func (r *ObjListXact) genNextPage(token string, cnt uint) error {
	if verbose {
		glog.Infof("[%s] token: %q", r, r.msg.ContinuationToken)
	}
	if token != "" && token == r.token {
		return nil
	}

	// Due to impossibility of getting object name from continuation token,
	// in case of remote bucket, a target keeps only the entire last sent page.
	// The page is replaced with a new one when a client asks for next page.
	if r.fromRemote {
		r.token = token
		return r.nextPageRemote()
	}

	if r.token > token {
		r._initTraverse() // Restart traversing as we cannot go back in time :(.
		r.lastPage = r.lastPage[:0]
	} else {
		if r.walkDone {
			return nil
		}
		r.discardObsolete(token)
	}
	r.token = token
	return r.nextPageAIS(cnt)
}

// Removes from local cache, the objects that have been already sent.
// Use only for AIS buckets(or Cached:true requests) - in other cases
// the marker, in general, is not an object name
func (r *ObjListXact) discardObsolete(token string) {
	if token == "" || len(r.lastPage) == 0 {
		return
	}
	j := r.findMarker(token)
	// Entire cache is "after" page marker, keep the whole cache
	if j == 0 {
		return
	}
	l := uint(len(r.lastPage))
	// All the cache data have been sent to clients, clean it up
	if j == l {
		r.lastPage = r.lastPage[:0]
		return
	}
	// To reuse local cache, copy items and fix the slice length
	copy(r.lastPage[0:], r.lastPage[j:])
	r.lastPage = r.lastPage[:l-j]
}

func (r *ObjListXact) traverseBucket(msg *apc.ListObjsMsg) {
	wi := walkinfo.NewWalkInfo(r.walkCtx(), r.t, msg)
	defer r.walkWg.Done()
	cb := func(fqn string, de fs.DirEntry) error {
		entry, err := wi.Callback(fqn, de)
		if err != nil || entry == nil {
			return err
		}
		if entry.Name <= msg.StartAfter {
			return nil
		}
		select {
		case r.objCache <- entry:
			/* do nothing */
		case <-r.walkStopCh.Listen():
			return errStopped
		}
		if !msg.IsFlagSet(apc.LsArchDir) {
			return nil
		}
		archList, err := listArchive(fqn)
		if archList == nil || err != nil {
			return err
		}
		for _, archEntry := range archList {
			e := &cmn.ObjEntry{
				Name:  path.Join(entry.Name, archEntry.name),
				Flags: entry.Flags | apc.EntryInArch,
				Size:  int64(archEntry.size),
			}
			select {
			case r.objCache <- e:
				/* do nothing */
			case <-r.walkStopCh.Listen():
				return errStopped
			}
		}
		return nil
	}
	opts := &fs.WalkBckOpts{
		WalkOpts: fs.WalkOpts{CTs: []string{fs.ObjectType}, Callback: cb, Sorted: true},
	}
	opts.WalkOpts.Bck.Copy(r.Bck().Bucket())
	opts.ValidateCallback = func(fqn string, de fs.DirEntry) error {
		if de.IsDir() {
			return wi.ProcessDir(fqn)
		}
		return nil
	}

	if err := fs.WalkBck(opts); err != nil {
		if err != filepath.SkipDir && err != errStopped {
			glog.Errorf("%s walk failed, err %v", r, err)
		}
	}
	close(r.objCache)
}

func listArchive(fqn string) ([]*archEntry, error) {
	var arch string
	for _, ext := range cos.ArchExtensions {
		if strings.HasSuffix(fqn, ext) {
			arch = ext
			break
		}
	}
	if arch == "" {
		return nil, nil
	}
	// list the archive content
	var (
		archList []*archEntry
		finfo    os.FileInfo
	)
	f, err := os.Open(fqn)
	if err == nil {
		switch arch {
		case cos.ExtTar:
			archList, err = listTar(f)
		case cos.ExtTgz, cos.ExtTarTgz:
			archList, err = listTgz(f)
		case cos.ExtZip:
			finfo, err = os.Stat(fqn)
			if err == nil {
				archList, err = listZip(f, finfo.Size())
			}
		case cos.ExtMsgpack:
			archList, err = listMsgpack(f)
		default:
			debug.Assert(false, arch)
		}
	}
	f.Close()
	if err != nil {
		return nil, err
	}
	// Files in archive can be in arbitrary order, but paging requires them sorted
	sort.Slice(archList, func(i, j int) bool { return archList[i].name < archList[j].name })
	return archList, nil
}

// list: tar, tgz, zip, msgpack
func listTar(reader io.Reader) ([]*archEntry, error) {
	fileList := make([]*archEntry, 0, 8)
	tr := tar.NewReader(reader)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				return fileList, nil
			}
			return nil, err
		}
		if hdr.FileInfo().IsDir() {
			continue
		}
		e := &archEntry{name: hdr.Name, size: uint64(hdr.Size)}
		fileList = append(fileList, e)
	}
}

func listTgz(reader io.Reader) ([]*archEntry, error) {
	gzr, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	return listTar(gzr)
}

func listZip(readerAt cos.ReadReaderAt, size int64) ([]*archEntry, error) {
	zr, err := zip.NewReader(readerAt, size)
	if err != nil {
		return nil, err
	}
	fileList := make([]*archEntry, 0, 8)
	for _, f := range zr.File {
		finfo := f.FileInfo()
		if finfo.IsDir() {
			continue
		}
		e := &archEntry{
			name: f.FileHeader.Name,
			size: f.FileHeader.UncompressedSize64,
		}
		fileList = append(fileList, e)
	}
	return fileList, nil
}

func listMsgpack(readerAt cos.ReadReaderAt) ([]*archEntry, error) {
	var (
		dst any
		dec = msgpack.NewDecoder(readerAt)
	)
	err := dec.Decode(&dst)
	if err != nil {
		return nil, err
	}
	out, ok := dst.(map[string]any)
	if !ok {
		debug.FailTypeCast(dst)
		return nil, fmt.Errorf("unexpected type (%T)", dst)
	}
	fileList := make([]*archEntry, 0, len(out))
	for fullname, v := range out {
		vout := v.([]byte)
		e := &archEntry{name: fullname, size: uint64(len(vout))}
		fileList = append(fileList, e)
	}
	return fileList, nil
}
