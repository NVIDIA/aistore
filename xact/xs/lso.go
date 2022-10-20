// Package xs contains most of the supported eXtended actions (xactions) with some
// exceptions that include certain storage services (mirror, EC) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
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
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/vmihailenco/msgpack"
)

// Xaction is on-demand one to avoid creating a new xaction per page even
// in passthrough mode. It just restarts `walk` if needed.
// Xaction is created once per bucket list request (per UUID)
type (
	lsoFactory struct {
		xreg.RenewBase
		xctn *LsoXact
		msg  *apc.LsoMsg
	}
	LsoXact struct {
		t         cluster.Target
		bck       *cluster.Bck
		msg       *apc.LsoMsg
		msgCh     chan *apc.LsoMsg  // incoming requests
		respCh    chan *ListObjsRsp // responses - next pages
		stopCh    cos.StopCh        // to stop xaction
		token     string            // continuation token -> last responded page
		nextToken string            // next continuation token -> next pages
		lastPage  cmn.LsoEntries    // last page contents
		walk      struct {
			pageCh chan *cmn.LsoEntry // channel to accumulate listed object entries
			stopCh *cos.StopCh        // to abort bucket walk
			wi     *walkInfo          // walking context
			wg     sync.WaitGroup     // to wait until the walk finishes
			done   bool               // done walking
		}
		xact.DemandBase
	}
	ListObjsRsp struct {
		Err    error
		Lst    *cmn.LsoResult
		Status int
	}
	// archived file
	archEntry struct {
		name string
		size uint64 // uncompressed size
	}
)

const pageChSize = 128

var (
	errStopped = errors.New("stopped")
	ErrGone    = errors.New("gone")
)

// interface guard
var (
	_ cluster.Xact   = (*LsoXact)(nil)
	_ xreg.Renewable = (*lsoFactory)(nil)
)

func (*lsoFactory) New(args xreg.Args, bck *cluster.Bck) xreg.Renewable {
	p := &lsoFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, msg: args.Custom.(*apc.LsoMsg)}
	return p
}

func (p *lsoFactory) Start() error {
	p.xctn = newXact(p.T, p.Bck, p.msg, p.UUID())
	return nil
}

func (*lsoFactory) Kind() string        { return apc.ActList }
func (p *lsoFactory) Get() cluster.Xact { return p.xctn }

func (p *lsoFactory) WhenPrevIsRunning(xprev xreg.Renewable) (xreg.WPR, error) {
	debug.Assertf(false, "%s vs %s", p.Str(p.Kind()), xprev) // xreg.usePrev() must've returned true
	return xreg.WprUse, nil
}

func newXact(t cluster.Target, bck *cluster.Bck, lsmsg *apc.LsoMsg, uuid string) *LsoXact {
	debug.Assert(bck.Props != nil)
	totallyIdle := cmn.GCO.Get().Timeout.MaxHostBusy.D()
	xctn := &LsoXact{
		t:      t,
		bck:    bck,
		msg:    lsmsg,
		msgCh:  make(chan *apc.LsoMsg),  // unbuffered
		respCh: make(chan *ListObjsRsp), // ditto
	}
	xctn.stopCh.Init()
	debug.Assert(lsmsg.PageSize > 0 && lsmsg.PageSize < 100000)
	xctn.lastPage = allocLsoEntries()
	xctn.DemandBase.Init(uuid, apc.ActList, bck, totallyIdle)
	return xctn
}

func (r *LsoXact) String() string { return fmt.Sprintf("%s: %s", r.t, &r.DemandBase) }

func (r *LsoXact) Run(*sync.WaitGroup) {
	if verbose {
		glog.Infoln(r.String())
	}
	if !r.listRemote() {
		r.initWalk()
	}
	for {
		select {
		case msg := <-r.msgCh:
			// Copy only the values that can change between calls
			debug.Assert(r.msg.UUID == msg.UUID && r.msg.Prefix == msg.Prefix && r.msg.Flags == msg.Flags)
			r.msg.ContinuationToken = msg.ContinuationToken
			r.msg.PageSize = msg.PageSize
			r.respCh <- r.doPage()
		case <-r.IdleTimer():
			r.stop(nil)
			return
		case errCause := <-r.ChanAbort():
			r.stop(errCause)
			return
		}
	}
}

func (r *LsoXact) stop(err error) {
	r.DemandBase.Stop()
	r.stopCh.Close()
	close(r.respCh) // not closing `r.msgCh` to avoid potential "closed channel" situation
	if r.walk.stopCh != nil {
		r.walk.stopCh.Close()
		r.walk.wg.Wait()
	}
	if r.lastPage != nil {
		freeLsoEntries(r.lastPage)
		r.lastPage = nil
	}
	r.Finish(err)
}

// skip on-demand idle-ness check
func (r *LsoXact) Abort(err error) (ok bool) {
	if ok = r.Base.Abort(err); ok {
		r.Finish(err)
	}
	return
}

func (r *LsoXact) listRemote() bool { return r.bck.IsRemote() && !r.msg.IsFlagSet(apc.LsObjCached) }

// Start fs.WalkBck, so that by the time we read the next page `r.pageCh` is already populated.
func (r *LsoXact) initWalk() {
	if r.walk.stopCh != nil {
		r.walk.stopCh.Close()
		r.walk.wg.Wait()
	}

	r.walk.pageCh = make(chan *cmn.LsoEntry, pageChSize)
	r.walk.done = false
	r.walk.stopCh = cos.NewStopCh()
	r.walk.wg.Add(1)

	go r.doWalk(r.msg.Clone())
	runtime.Gosched()
}

func (r *LsoXact) Do(msg *apc.LsoMsg) *ListObjsRsp {
	// The guarantee here is that we either put something on the channel and our
	// request will be processed (since the `msgCh` is unbuffered) or we receive
	// message that the xaction has been stopped.
	select {
	case r.msgCh <- msg:
		return <-r.respCh
	case <-r.stopCh.Listen():
		return &ListObjsRsp{Err: ErrGone}
	}
}

func (r *LsoXact) doPage() *ListObjsRsp {
	r.IncPending()
	defer r.DecPending()

	if r.listRemote() {
		if r.msg.ContinuationToken == "" || r.msg.ContinuationToken != r.token {
			// can't extract the next-to-list object name from the remotely generated
			// continuation token, keeping and returning the entire last page
			r.token = r.msg.ContinuationToken
			if err := r.nextPageR(); err != nil {
				return &ListObjsRsp{Status: http.StatusInternalServerError, Err: err}
			}
		}
		page := &cmn.LsoResult{UUID: r.msg.UUID, Entries: r.lastPage, ContinuationToken: r.nextToken}
		return &ListObjsRsp{Lst: page, Status: http.StatusOK}
	}

	if r.msg.ContinuationToken == "" || r.msg.ContinuationToken != r.token {
		r.nextPageA()
	}
	var (
		cnt  = r.msg.PageSize
		idx  = r.findToken(r.msg.ContinuationToken)
		lst  = r.lastPage[idx:]
		page *cmn.LsoResult
	)
	debug.Assert(uint(len(lst)) >= cnt || r.walk.done)
	if uint(len(lst)) >= cnt {
		entries := lst[:cnt]
		page = &cmn.LsoResult{UUID: r.msg.UUID, Entries: entries, ContinuationToken: entries[cnt-1].Name}
	} else {
		page = &cmn.LsoResult{UUID: r.msg.UUID, Entries: lst}
	}
	return &ListObjsRsp{Lst: page, Status: http.StatusOK}
}

func (r *LsoXact) objsAdd(*cluster.LOM) { r.ObjsAdd(1, 0) }

// Returns the index of the first object in the page that follows the continuation `token`
func (r *LsoXact) findToken(token string) uint {
	if r.listRemote() && r.token == token {
		return 0
	}
	return uint(sort.Search(len(r.lastPage), func(i int) bool { // TODO -- FIXME: revisit
		return !cmn.TokenGreaterEQ(token, r.lastPage[i].Name)
	}))
}

func (r *LsoXact) havePage(token string, cnt uint) bool {
	if r.walk.done {
		return true
	}
	idx := r.findToken(token)
	return idx+cnt < uint(len(r.lastPage))
}

func (r *LsoXact) nextPageR() error {
	npg := newNpgCtx(r.t, r.bck, r.msg, r.objsAdd)
	nentries := allocLsoEntries()
	lst, err := npg.nextPageR(nentries)
	if err != nil {
		r.nextToken = ""
		return err
	}
	if lst.ContinuationToken == "" {
		r.walk.done = true
	}
	freeLsoEntries(r.lastPage)
	r.lastPage = lst.Entries
	r.nextToken = lst.ContinuationToken
	return nil
}

func (r *LsoXact) gcLastPage(from, to int) {
	for i := from; i < to; i++ {
		r.lastPage[i] = nil
	}
}

func (r *LsoXact) nextPageA() {
	if r.token > r.msg.ContinuationToken {
		r.initWalk() // restart traversing the bucket (TODO: cache more and try to scroll back)
		r.gcLastPage(0, len(r.lastPage))
		r.lastPage = r.lastPage[:0]
	} else {
		if r.walk.done {
			return
		}
		r.shiftLastPage(r.msg.ContinuationToken)
	}

	r.token = r.msg.ContinuationToken
	if r.havePage(r.token, r.msg.PageSize) {
		return
	}
	for cnt := uint(0); cnt < r.msg.PageSize; {
		obj, ok := <-r.walk.pageCh
		if !ok {
			r.walk.done = true
			break
		}
		// Skip all objects until the requested continuation token.
		// TODO -- FIXME: revisit
		if cmn.TokenGreaterEQ(r.token, obj.Name) {
			continue
		}
		cnt++
		r.lastPage = append(r.lastPage, obj)
	}
}

// Removes objects that have been already sent. Is used only for AIS buckets
// and/or (cached == true) requests - in all other cases continuation token,
// in general, does not contain object name.
func (r *LsoXact) shiftLastPage(token string) {
	if token == "" || len(r.lastPage) == 0 {
		return
	}
	j := r.findToken(token)
	// the page is "after" the token - keep it
	if j == 0 {
		return
	}
	l := uint(len(r.lastPage))

	// (all sent)
	if j == l {
		r.gcLastPage(0, int(l))
		r.lastPage = r.lastPage[:0]
		return
	}

	// otherwise, shift the not-yet-transmitted entries and fix the slice
	copy(r.lastPage[0:], r.lastPage[j:])
	r.gcLastPage(int(l-j), int(l))
	r.lastPage = r.lastPage[:l-j]
}

func (r *LsoXact) doWalk(msg *apc.LsoMsg) {
	r.walk.wi = newWalkInfo(r.t, msg, r.objsAdd)
	opts := &fs.WalkBckOpts{
		WalkOpts: fs.WalkOpts{CTs: []string{fs.ObjectType}, Callback: r.cb, Sorted: true},
	}
	opts.WalkOpts.Bck.Copy(r.Bck().Bucket())
	opts.ValidateCallback = func(fqn string, de fs.DirEntry) error {
		if de.IsDir() {
			return r.walk.wi.processDir(fqn)
		}
		return nil
	}
	if err := fs.WalkBck(opts); err != nil {
		if err != filepath.SkipDir && err != errStopped {
			glog.Errorf("%s walk failed, err %v", r, err)
		}
	}
	close(r.walk.pageCh)
	r.walk.wg.Done()
}

func (r *LsoXact) cb(fqn string, de fs.DirEntry) error {
	entry, err := r.walk.wi.callback(fqn, de)
	if err != nil || entry == nil {
		return err
	}
	msg := r.walk.wi.lsmsg()
	if entry.Name <= msg.StartAfter {
		return nil
	}

	select {
	case r.walk.pageCh <- entry:
		/* do nothing */
	case <-r.walk.stopCh.Listen():
		return errStopped
	}

	if !msg.IsFlagSet(apc.LsArchDir) {
		return nil
	}

	// arch
	archList, err := listArchive(fqn)
	if archList == nil || err != nil {
		return err
	}
	for _, archEntry := range archList {
		e := &cmn.LsoEntry{
			Name:  path.Join(entry.Name, archEntry.name),
			Flags: entry.Flags | apc.EntryInArch,
			Size:  int64(archEntry.size),
		}
		select {
		case r.walk.pageCh <- e:
			/* do nothing */
		case <-r.walk.stopCh.Listen():
			return errStopped
		}
	}
	return nil
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
