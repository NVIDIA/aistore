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
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/tinylib/msgp/msgp"
	"github.com/vmihailenco/msgpack"
)

// `on-demand` per list-objects request
type (
	lsoFactory struct {
		streamingF
		msg *apc.LsoMsg
	}
	LsoXact struct {
		streamingX
		msg       *apc.LsoMsg
		msgCh     chan *apc.LsoMsg // incoming requests
		respCh    chan *LsoRsp     // responses - next pages
		remtCh    chan *LsoRsp     // remote paging by the responsible target
		stopCh    cos.StopCh       // to stop xaction
		token     string           // continuation token -> last responded page
		nextToken string           // next continuation token -> next pages
		lastPage  cmn.LsoEntries   // last page contents
		lensgl    int64            // sgl.Len()
		walk      struct {
			pageCh chan *cmn.LsoEntry // channel to accumulate listed object entries
			stopCh *cos.StopCh        // to abort bucket walk
			wi     *walkInfo          // walking context
			wg     sync.WaitGroup     // to wait until the walk finishes
			done   bool               // done walking
		}
	}
	LsoRsp struct {
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
	p := &lsoFactory{
		streamingF: streamingF{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, kind: apc.ActList},
		msg:        args.Custom.(*apc.LsoMsg),
	}
	debug.Assert(p.Bck.Props != nil && p.msg.PageSize > 0 && p.msg.PageSize < 100000)
	return p
}

func (p *lsoFactory) Start() error {
	r := &LsoXact{
		streamingX: streamingX{p: &p.streamingF},
		msg:        p.msg,
		msgCh:      make(chan *apc.LsoMsg), // unbuffered
		respCh:     make(chan *LsoRsp),     // ditto
		remtCh:     make(chan *LsoRsp),     // ditto
	}
	r.lastPage = allocLsoEntries()
	r.stopCh.Init()
	totallyIdle := cmn.GCO.Get().Timeout.MaxHostBusy.D()
	r.DemandBase.Init(p.UUID(), apc.ActList, p.Bck, totallyIdle)

	if r.listRemote() {
		trname := "lso-" + p.UUID()
		if err := p.newDM(trname, r.recv, 0 /*pdu*/); err != nil {
			return err
		}
		p.dm.SetXact(r)
		p.dm.Open()
	}
	p.xctn = r
	return nil
}

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
	if r.listRemote() {
		r.streamingX.fin(err, false /*postponeUnregRx below*/)
		r.stopCh.Close()
		r.lastmsg()
		r.postponeUnregRx()
		goto ex
	}

	r.DemandBase.Stop()
	r.stopCh.Close()

	r.walk.stopCh.Close()
	r.walk.wg.Wait()
	r.lastmsg()
	r.Finish(err)
ex:
	if r.lastPage != nil {
		freeLsoEntries(r.lastPage)
		r.lastPage = nil
	}
}

func (r *LsoXact) lastmsg() {
	select {
	case <-r.msgCh:
		r.respCh <- &LsoRsp{Err: ErrGone}
	default:
		break
	}
	close(r.respCh)
}

// compare w/ streaming (TODO: unify)
func (r *LsoXact) postponeUnregRx() { hk.Reg(r.ID()+hk.NameSuffix, r.fcleanup, time.Second/2) }

func (r *LsoXact) fcleanup() (d time.Duration) {
	d = hk.UnregInterval
	if r.wiCnt.Load() > 0 {
		d = time.Second
	} else {
		close(r.remtCh)
		close(r.msgCh)
	}
	return
}

// skip on-demand idleness check
func (r *LsoXact) Abort(err error) (ok bool) {
	if ok = r.Base.Abort(err); ok {
		r.Finish(err)
	}
	return
}

func (r *LsoXact) listRemote() bool { return r.p.Bck.IsRemote() && !r.msg.IsFlagSet(apc.LsObjCached) }

// Start `fs.WalkBck`, so that by the time we read the next page `r.pageCh` is already populated.
func (r *LsoXact) initWalk() {
	r.walk.pageCh = make(chan *cmn.LsoEntry, pageChSize)
	r.walk.done = false
	r.walk.stopCh = cos.NewStopCh()
	r.walk.wg.Add(1)

	go r.doWalk(r.msg.Clone())
	runtime.Gosched()
}

func (r *LsoXact) Do(msg *apc.LsoMsg) *LsoRsp {
	// The guarantee here is that we either put something on the channel and our
	// request will be processed (since the `msgCh` is unbuffered) or we receive
	// message that the xaction has been stopped.
	select {
	case r.msgCh <- msg:
		return <-r.respCh
	case <-r.stopCh.Listen():
		return &LsoRsp{Err: ErrGone}
	}
}

func (r *LsoXact) doPage() *LsoRsp {
	r.IncPending()
	defer r.DecPending()

	if r.listRemote() {
		if r.msg.ContinuationToken == "" || r.msg.ContinuationToken != r.token {
			// can't extract the next-to-list object name from the remotely generated
			// continuation token, keeping and returning the entire last page
			r.token = r.msg.ContinuationToken
			if err := r.nextPageR(); err != nil {
				return &LsoRsp{Status: http.StatusInternalServerError, Err: err}
			}
		}
		page := &cmn.LsoResult{UUID: r.msg.UUID, Entries: r.lastPage, ContinuationToken: r.nextToken}
		return &LsoRsp{Lst: page, Status: http.StatusOK}
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
	return &LsoRsp{Lst: page, Status: http.StatusOK}
}

// `ais show job` will report the sum of non-replicated obj numbers and
// sum of obj sizes - for all visited objects
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
	debug.Assert(r.msg.SID != "")
	var (
		page *cmn.LsoResult
		npg  = newNpgCtx(r.p.T, r.p.Bck, r.msg, r.LomAdd)
		smap = r.p.dm.Smap()
		tsi  = smap.GetTarget(r.msg.SID)
		err  error
	)
	if tsi == nil {
		err = fmt.Errorf("%s: lost or missing target ID=%q (%s)", r, r.msg.SID, smap)
		goto ex
	}

	r.wiCnt.Inc()
	if r.msg.SID == r.p.T.SID() {
		wantOnlyRemote := r.msg.WantOnlyRemoteProps()
		nentries := allocLsoEntries()
		page, err = npg.nextPageR(nentries)
		if !wantOnlyRemote && r.p.dm.Smap().CountActiveTargets() > 1 {
			if err == nil {
				err = r.bcast(page)
			} else {
				r.sendTerm(r.msg.UUID, nil, err)
			}
		}
	} else {
		debug.Assert(!r.msg.WantOnlyRemoteProps())
		select {
		case rsp := <-r.remtCh:
			if rsp == nil {
				err = ErrGone
			} else if rsp.Err != nil {
				err = rsp.Err
			} else {
				page = rsp.Lst
				err = npg.populate(page)
			}
		case <-r.stopCh.Listen():
			err = ErrGone
		}
	}
	r.wiCnt.Dec()
ex:
	if err != nil {
		r.nextToken = ""
		return err
	}
	if page.ContinuationToken == "" {
		r.walk.done = true
	}
	freeLsoEntries(r.lastPage)
	r.lastPage = page.Entries
	r.nextToken = page.ContinuationToken
	return nil
}

func (r *LsoXact) bcast(page *cmn.LsoResult) (err error) {
	var (
		mm        = r.p.T.PageMM()
		siz       = cos.MaxI64(r.lensgl, 32*cos.KiB)
		buf, slab = mm.AllocSize(siz)
		sgl       = mm.NewSGL(siz, slab.Size())
		mw        = msgp.NewWriterBuf(sgl, buf)
	)
	if err = page.EncodeMsg(mw); err == nil {
		err = mw.Flush()
	}
	r.lensgl = sgl.Len()
	if err != nil {
		return
	}
	o := transport.AllocSend()
	{
		o.Hdr.Bck = r.p.Bck.Clone()
		o.Hdr.ObjName = r.Name()
		o.Hdr.Opaque = []byte(r.p.UUID())
		o.Hdr.ObjAttrs.Size = sgl.Len()
	}
	o.Callback, o.CmplArg = r.sentCb, sgl // cleanup
	o.Reader = sgl
	roc := memsys.NewReader(sgl)
	r.p.dm.Bcast(o, roc)

	slab.Free(buf)
	return
}

func (r *LsoXact) sentCb(hdr transport.ObjHdr, _ io.ReadCloser, arg any, err error) {
	if err == nil {
		r.OutObjsAdd(1, hdr.ObjAttrs.Size)
	} else if bool(glog.FastV(4, glog.SmoduleXs)) || !cos.IsRetriableConnErr(err) {
		glog.Errorf("%s: failed to send [%+v]: %v", r.p.T, hdr, err)
	}
	sgl, ok := arg.(*memsys.SGL)
	debug.Assertf(ok, "%T", arg)
	sgl.Free()
}

func (r *LsoXact) gcLastPage(from, to int) {
	for i := from; i < to; i++ {
		r.lastPage[i] = nil
	}
}

func (r *LsoXact) nextPageA() {
	if r.token > r.msg.ContinuationToken {
		// restart traversing the bucket (TODO: cache more and try to scroll back)
		r.walk.stopCh.Close()
		r.walk.wg.Wait()
		r.initWalk()
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
		// Skip until the requested continuation token (TODO: revisit)
		if cmn.TokenGreaterEQ(r.token, obj.Name) {
			continue
		}
		cnt++
		r.lastPage = append(r.lastPage, obj)
	}
}

// Removes entries that were already sent to clients.
// Is used only for AIS buckets and (cached == true) requests.
func (r *LsoXact) shiftLastPage(token string) {
	if token == "" || len(r.lastPage) == 0 {
		return
	}
	j := r.findToken(token)
	// the page is "after" the token - keep it all
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
	r.walk.wi = newWalkInfo(r.p.T, msg, r.LomAdd)
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

//
// listing archives
//

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

//
// streaming receive: remote pages
//

func (r *LsoXact) recv(hdr transport.ObjHdr, objReader io.Reader, err error) error {
	debug.Assert(r.listRemote())
	r.IncPending()
	defer func() {
		r.DecPending()
		transport.DrainAndFreeReader(objReader)
	}()

	if hdr.Opcode == opcodeAbrt {
		err = errors.New(hdr.ObjName) // see `sendTerm`
	}
	if err != nil && !cos.IsEOF(err) {
		glog.Error(err)
		r.remtCh <- &LsoRsp{Status: http.StatusInternalServerError, Err: err}
		return err
	}

	debug.Assert(hdr.Opcode == 0)
	var (
		page      = &cmn.LsoResult{}
		buf, slab = r.p.T.PageMM().AllocSize(cmn.MsgpLsoBufSize)
		mr        = msgp.NewReaderBuf(objReader, buf)
	)
	err = page.DecodeMsg(mr)
	if err == nil {
		r.remtCh <- &LsoRsp{Lst: page, Status: http.StatusOK}
	} else {
		glog.Errorf("%s: failed to recv [%s: %s] num=%d from %s (%s, %s): %v",
			r.p.T, page.UUID, page.ContinuationToken, len(page.Entries),
			hdr.SID, hdr.Bck.DisplayName(), string(hdr.Opaque), err)
		r.remtCh <- &LsoRsp{Status: http.StatusInternalServerError, Err: err}
	}
	slab.Free(buf)
	return err
}
