// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xact"
)

const (
	getxBurstSize   = 128
	putxBurstSize   = 128
	encodeBurstSize = 128
)

type (
	xactECBase struct {
		xact.DemandBase
		config *cmn.Config // config
		stats  stats       // EC statistics
		bck    cmn.Bck     // which bucket xctn belongs to

		dOwner *dataOwner // data slice manager
		mgr    *Manager   // EC manager
	}

	xactReqBase struct {
		mpathReqCh chan mpathReq // notify about mountpath changes
		controlCh  chan RequestsControlMsg

		rejectReq atomic.Bool // marker if EC requests should be rejected
	}

	mpathReq struct {
		action string
		mpath  string
	}

	// Manages SGL objects that are waiting for a data from a remote target
	dataOwner struct {
		mtx    sync.Mutex
		slices map[string]*slice
	}
)

/////////////////
// xactReqBase //
/////////////////

func (r *xactReqBase) init() {
	r.mpathReqCh = make(chan mpathReq, 1)
	r.controlCh = make(chan RequestsControlMsg, 8)
}

// ClearRequests disables receiving new EC requests, they will be terminated with error
// Then it starts draining a channel from pending EC requests
// It does not enable receiving new EC requests, it has to be done explicitly, when EC is enabled again
func (r *xactReqBase) ClearRequests() {
	msg := RequestsControlMsg{
		Action: ActClearRequests,
	}

	r.controlCh <- msg
}

func (r *xactReqBase) EnableRequests() {
	msg := RequestsControlMsg{
		Action: ActEnableRequests,
	}

	r.controlCh <- msg
}

func (r *xactReqBase) setEcRequestsDisabled() {
	r.rejectReq.Store(true)
}

func (r *xactReqBase) setEcRequestsEnabled() {
	r.rejectReq.Store(false)
}

func (r *xactReqBase) ecRequestsEnabled() bool {
	return !r.rejectReq.Load()
}

////////////////
// xactECBase //
////////////////

func (r *xactECBase) init(config *cmn.Config, bck *cmn.Bck, mgr *Manager) {
	r.stats = stats{bck: *bck}
	r.config = config
	r.bck = *bck
	r.dOwner = &dataOwner{slices: make(map[string]*slice, 10)}
	r.mgr = mgr
}

func newSliceResponse(md *Metadata, attrs *cmn.ObjAttrs, fqn string) (reader cos.ReadOpenCloser, err error) {
	attrs.SetVersion(md.ObjVersion)
	attrs.Cksum = cos.NewCksum(md.CksumType, md.CksumValue)

	stat, err := os.Stat(fqn)
	if err != nil {
		return nil, err
	}
	attrs.Size = stat.Size()
	reader, err = cos.NewFileHandle(fqn)
	if err != nil {
		nlog.Warningln("failed to read file stats:", err)
		return nil, err
	}
	return reader, nil
}

// replica/full object request
func newReplicaResponse(attrs *cmn.ObjAttrs, bck *meta.Bck, objName string) (cos.ReadOpenCloser, error) {
	lom := core.AllocLOM(objName)
	defer core.FreeLOM(lom)
	if err := lom.InitBck(bck.Bucket()); err != nil {
		return nil, err
	}
	lom.Lock(false)
	reader, err := lom.NewDeferROC(false /*loaded*/)
	if err != nil {
		return nil, err
	}
	if lom.Lsize() == 0 {
		return nil, nil
	}
	attrs.Size = lom.Lsize()
	attrs.CopyVersion(lom.ObjAttrs())
	attrs.Atime = lom.AtimeUnix()
	attrs.Cksum = lom.Checksum()
	return reader, nil
}

// Sends the replica/meta/slice data: either to copy replicas/slices after
// encoding or to send requested "object" to a client. In the latter case
// if the local object does not exist, it sends an empty body and sets
// exists=false in response header
func (r *xactECBase) dataResponse(act intraReqType, hdr *transport.ObjHdr, fqn string, bck *meta.Bck, objName string,
	md *Metadata) (err error) {
	var (
		reader   cos.ReadOpenCloser
		objAttrs cmn.ObjAttrs
	)
	ireq := newIntraReq(act, nil, bck)
	if md != nil && md.SliceID != 0 {
		// slice request
		reader, err = newSliceResponse(md, &objAttrs, fqn)
		ireq.exists = err == nil
	} else {
		// replica/full object request
		reader, err = newReplicaResponse(&objAttrs, bck, objName)
		ireq.exists = err == nil
	}
	debug.Assert((objAttrs.Size == 0 && reader == nil) || (objAttrs.Size != 0 && reader != nil))

	rHdr := transport.ObjHdr{ObjName: objName, ObjAttrs: objAttrs, Opcode: act}
	rHdr.Bck.Copy(bck.Bucket())
	rHdr.Opaque = ireq.NewPack(g.smm)

	o := transport.AllocSend()
	o.Hdr, o.Callback = rHdr, r.sendCb

	r.ObjsAdd(1, objAttrs.Size)
	r.IncPending()
	return r.sendByDaemonID([]string{hdr.SID}, o, reader, false)
}

func (r *xactECBase) sendCb(hdr *transport.ObjHdr, _ io.ReadCloser, _ any, err error) {
	g.smm.Free(hdr.Opaque)
	if err != nil {
		err = cmn.NewErrFailedTo(core.T, "ec-send", hdr.Cname(), err)
		r.AddErr(err, 0)
	}
	r.DecPending()
}

// Send a data or request to one or few targets by their DaemonIDs. Most of the time
// only DaemonID is known - that is why the function gets DaemonID and internally
// transforms it into meta.Snode.
// * daemonIDs - a list of targets
// * hdr - transport header
// * reader - a data to send
// * cb - optional callback to be called when the transfer completes
// * isRequest - defines the type of request:
//   - true - send lightweight request to all targets (usually reader is nil
//     in this case)
//   - false - send a slice/replica/metadata to targets
func (r *xactECBase) sendByDaemonID(daemonIDs []string, o *transport.Obj, reader cos.ReadOpenCloser, isRequest bool) error {
	var (
		err   error
		nodes = meta.AllocNodes(len(daemonIDs))
		smap  = core.T.Sowner().Get()
	)
	for _, id := range daemonIDs {
		si, ok := smap.Tmap[id]
		if !ok {
			nlog.Errorln(meta.Tname(id), "not found in", smap.StringEx())
			continue
		}
		nodes = append(nodes, si)
	}
	if isRequest {
		err = r.mgr.req().Send(o, reader, nodes...)
	} else {
		err = r.mgr.resp().Send(o, reader, nodes...)
	}
	meta.FreeNodes(nodes)
	return err
}

// send request to a target, wait for its response, read the data into writer.
//   - daemonID - target to send a request
//   - bucket/objName - what to request
//   - uname - unique name for the operation: the name is built from daemonID,
//     bucket and object names. HTTP data receiving handler generates a name
//     when receiving data and if it finds a writer registered with the same
//     name, it puts the data to its writer and notifies when download is done
//   - request - request to send
//   - writer - an opened writer that will receive the replica/slice/meta
func (r *xactECBase) readRemote(lom *core.LOM, daemonID, uname string, request []byte, writer io.Writer) (int64, error) {
	hdr := transport.ObjHdr{ObjName: lom.ObjName, Opaque: request, Opcode: reqGet}
	hdr.Bck.Copy(lom.Bucket())

	o := transport.AllocSend()
	o.Hdr = hdr

	sw := &slice{writer: writer, twg: cos.NewTimeoutGroup(), lom: lom}
	sw.twg.Add(1)
	r.regWriter(uname, sw)

	if cmn.Rom.FastV(4, cos.SmoduleEC) {
		nlog.Infof("Requesting object %s from %s", lom, daemonID)
	}
	if err := r.sendByDaemonID([]string{daemonID}, o, nil, true); err != nil {
		r.unregWriter(uname)
		r.AddErr(err)
		return 0, err
	}
	if sw.twg.WaitTimeout(r.config.Timeout.SendFile.D()) {
		r.unregWriter(uname)
		err := fmt.Errorf("read-remote(%s): timeout %v", uname, r.config.Timeout.SendFile.D())
		r.AddErr(err)
		return 0, err
	}
	r.unregWriter(uname)

	if cmn.Rom.FastV(4, cos.SmoduleEC) {
		nlog.Infof("Received object %s from %s", lom, daemonID)
	}
	if sw.version != "" {
		lom.SetVersion(sw.version)
	}
	lom.SetCksum(sw.cksum)
	lom.Uncache()
	return sw.n, nil
}

// Registers a new slice that will wait for the data to come from
// a remote target
func (r *xactECBase) regWriter(uname string, writer *slice) bool {
	r.dOwner.mtx.Lock()
	_, ok := r.dOwner.slices[uname]
	if ok {
		nlog.Errorf("Writer for %s is already registered", uname)
	} else {
		r.dOwner.slices[uname] = writer
	}
	r.dOwner.mtx.Unlock()

	return !ok
}

// Unregisters a slice that has been waiting for the data to come from
// a remote target
func (r *xactECBase) unregWriter(uname string) {
	r.dOwner.mtx.Lock()
	delete(r.dOwner.slices, uname)
	r.dOwner.mtx.Unlock()
}

// Used to copy replicas/slices after the object is encoded after PUT/restored
// after GET, or to respond to meta/slice/replica request.
//   - daemonIDs - receivers of the data
//   - bucket/objName - object path
//   - reader - object/slice/meta data
//   - src - extra information about the data to send
//   - cb - a caller may set its own callback to execute when the transfer is done.
//     A special case:
//     if a caller does not define its own callback, and it sets the `obj` in
//     `src` it means that the caller wants to automatically free the memory
//     allocated for the `obj` SGL after the object is transferred. The caller
//     may set optional counter in `obj` - the default callback decreases the
//     counter each time the callback is called and when the value drops below 1,
//     `writeRemote` callback frees the SGL
//     The counter is used for sending slices of one big SGL to a few nodes. In
//     this case every slice must be sent to only one target, and transport bundle
//     cannot help to track automatically when SGL should be freed.
func (r *xactECBase) writeRemote(daemonIDs []string, lom *core.LOM, src *dataSource, cb transport.ObjSentCB) error {
	if src.metadata != nil && src.metadata.ObjVersion == "" {
		src.metadata.ObjVersion = lom.Version()
	}
	req := newIntraReq(src.reqType, src.metadata, lom.Bck())
	req.isSlice = src.isSlice

	putData := req.NewPack(g.smm)
	objAttrs := cmn.ObjAttrs{
		Size:  src.size,
		Atime: lom.AtimeUnix(),
	}
	objAttrs.CopyVersion(lom.ObjAttrs())
	if src.metadata != nil && src.metadata.SliceID != 0 {
		// for a slice read everything from slice's metadata
		if src.metadata.ObjVersion != "" {
			objAttrs.SetVersion(src.metadata.ObjVersion)
		}
		objAttrs.Cksum = cos.NewCksum(src.metadata.CksumType, src.metadata.CksumValue)
	} else {
		objAttrs.Cksum = lom.Checksum()
	}
	hdr := transport.ObjHdr{
		ObjName:  lom.ObjName,
		ObjAttrs: objAttrs,
		Opaque:   putData,
		Opcode:   src.reqType,
	}
	hdr.Bck.Copy(lom.Bucket())
	oldCallback := cb
	cb = func(hdr *transport.ObjHdr, reader io.ReadCloser, arg any, err error) {
		g.smm.Free(hdr.Opaque)
		if oldCallback != nil {
			oldCallback(hdr, reader, arg, err)
		}
		r.DecPending()
	}

	o := transport.AllocSend()
	o.Hdr, o.Callback = hdr, cb

	r.IncPending()
	return r.sendByDaemonID(daemonIDs, o, src.reader, false)
}

// Save data from a target response to SGL or file. When exists is false it
// just drains the response body and returns - because it does not contain
// any data. On completion the function must call writer.wg.Done to notify
// the caller that the data read is completed.
// * writer - where to save the slice/meta/replica data
// * exists - if the remote target had the requested object
// * reader - response body
func _writerReceive(writer *slice, exists bool, objAttrs cmn.ObjAttrs, reader io.Reader) (err error) {
	if !exists {
		writer.twg.Done()
		return ErrorNotFound
	}

	buf, slab := g.pmm.Alloc()
	writer.n, err = io.CopyBuffer(writer.writer, reader, buf)
	writer.cksum = objAttrs.Cksum
	if v := objAttrs.Version(); writer.version == "" && v != "" {
		writer.version = v
	}

	writer.twg.Done()
	slab.Free(buf)
	return err
}

func (r *xactECBase) ECStats() *Stats { return r.stats.stats() }

func (r *xactECBase) baseSnap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}
