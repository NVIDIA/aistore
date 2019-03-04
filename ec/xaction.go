// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
* Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
)

const (
	requestBufSizeGlobal = 800
	requestBufSizeFS     = 200
	maxBgJobsPerJogger   = 32
)

type (
	// Erasure coding runner: accepts requests and dispatches them to
	// a correct mountpath runner. Runner uses dedicated to EC memory manager
	// inherited by dependent mountpath runners
	XactEC struct {
		cmn.XactDemandBase
		cmn.Named
		t          cluster.Target
		mpathReqCh chan mpathReq // notify about mountpath changes
		ecCh       chan *Request // to request object encoding

		putJoggers map[string]*putJogger // mountpath joggers for PUT/DEL
		getJoggers map[string]*getJogger // mountpath joggers for GET
		bmd        cluster.Bowner        // bucket manager
		smap       cluster.Sowner        // cluster map
		si         *cluster.Snode        // target daemonInfo
		stats      stats                 // EC statistics

		dOwner *dataOwner // data slice manager

		reqBundle  *transport.StreamBundle // a stream bundle to send lightweight requests
		respBundle *transport.StreamBundle // a stream bungle to transfer data between targets
	}
)

type (
	bgProcess = func(req *Request, toDisk bool, buffer []byte, cb func(error))

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

func NewXact(netReq, netResp string, t cluster.Target, bmd cluster.Bowner, smap cluster.Sowner,
	si *cluster.Snode) *XactEC {
	availablePaths, disabledPaths := fs.Mountpaths.Get()
	totalPaths := len(availablePaths) + len(disabledPaths)
	runner := &XactEC{
		t:          t,
		mpathReqCh: make(chan mpathReq, 1),
		getJoggers: make(map[string]*getJogger, totalPaths),
		putJoggers: make(map[string]*putJogger, totalPaths),
		ecCh:       make(chan *Request, requestBufSizeGlobal),
		bmd:        bmd,
		smap:       smap,
		si:         si,

		dOwner: &dataOwner{
			mtx:    sync.Mutex{},
			slices: make(map[string]*slice, 10),
		},
	}

	cbReq := func(hdr transport.Header, reader io.ReadCloser, err error) {
		if err != nil {
			glog.Errorf("Failed to request %s/%s: %v", hdr.Bucket, hdr.Objname, err)
		}
	}

	client := transport.NewDefaultClient()
	extraReq := transport.Extra{Callback: cbReq}

	reqSbArgs := transport.SBArgs{
		Multiplier: transport.IntraBundleMultiplier,
		Extra:      &extraReq,
		Network:    netReq,
		Trname:     ReqStreamName,
	}

	respSbArgs := transport.SBArgs{
		Multiplier: transport.IntraBundleMultiplier,
		Trname:     RespStreamName,
		Network:    netResp,
	}

	runner.reqBundle = transport.NewStreamBundle(smap, si, client, reqSbArgs)
	runner.respBundle = transport.NewStreamBundle(smap, si, client, respSbArgs)

	// create all runners but do not start them until Run is called
	for mpath := range availablePaths {
		getJog := runner.newGetJogger(mpath)
		runner.getJoggers[mpath] = getJog
		putJog := runner.newPutJogger(mpath)
		runner.putJoggers[mpath] = putJog
	}
	for mpath := range disabledPaths {
		getJog := runner.newGetJogger(mpath)
		runner.getJoggers[mpath] = getJog
		putJog := runner.newPutJogger(mpath)
		runner.putJoggers[mpath] = putJog
	}

	return runner
}

func (r *XactEC) DispatchReq(w http.ResponseWriter, hdr transport.Header, object io.Reader) {
	iReq := intraReq{}
	if err := iReq.unmarshal(hdr.Opaque); err != nil {
		glog.Errorf("Failed to unmarshal request: %v", err)
		return
	}

	bckIsLocal := r.bmd.Get().IsLocal(hdr.Bucket)
	daemonID := iReq.Sender
	// command requests should not have a body, but if it has,
	// the body must be drained to avoid errors
	if hdr.ObjAttrs.Size != 0 {
		if _, err := ioutil.ReadAll(object); err != nil {
			glog.Errorf("Failed to read request body: %v", err)
			return
		}
	}

	switch iReq.Act {
	case reqDel:
		// object cleanup request: delete replicas, slices and metafiles
		if err := r.removeObjAndMeta(hdr.Bucket, hdr.Objname, bckIsLocal); err != nil {
			glog.Errorf("Failed to delete %s/%s: %v", hdr.Bucket, hdr.Objname, err)
		}
	case reqGet:
		// slice or replica request: send the object's data to the caller
		var fqn, errstr string
		if iReq.IsSlice {
			if glog.V(4) {
				glog.Infof("Received request for slice %d of %s", iReq.Meta.SliceID, hdr.Objname)
			}
			fqn, errstr = cluster.FQN(SliceType, hdr.Bucket, hdr.Objname, bckIsLocal)
		} else {
			if glog.V(4) {
				glog.Infof("Received request for replica %s", hdr.Objname)
			}
			fqn, errstr = cluster.FQN(fs.ObjectType, hdr.Bucket, hdr.Objname, bckIsLocal)
		}
		if errstr != "" {
			glog.Errorf(errstr)
			return
		}

		if err := r.dataResponse(reqPut, fqn, hdr.Bucket, hdr.Objname, daemonID); err != nil {
			glog.Errorf("Failed to send back [GET req] %q: %v", fqn, err)
		}
	case reqMeta:
		// metadata request: send the metadata to the caller
		fqn, errstr := cluster.FQN(MetaType, hdr.Bucket, hdr.Objname, bckIsLocal)
		if errstr != "" {
			glog.Errorf(errstr)
			return
		}
		if err := r.dataResponse(iReq.Act, fqn, hdr.Bucket, hdr.Objname, daemonID); err != nil {
			glog.Errorf("Failed to send back [META req] %q: %v", fqn, err)
		}
	default:
		// invalid request detected
		glog.Errorf("Invalid request: %s", string(hdr.Opaque))
	}
}

func (r *XactEC) DispatchResp(w http.ResponseWriter, hdr transport.Header, object io.Reader) {
	var err error
	iReq := intraReq{}
	if err = iReq.unmarshal(hdr.Opaque); err != nil {
		glog.Errorf("Failed to read request: %v", err)
		return
	}

	uname := unique(iReq.Sender, hdr.Bucket, hdr.Objname)
	if err != nil {
		r.dOwner.mtx.Lock()
		writer, ok := r.dOwner.slices[uname]
		r.dOwner.mtx.Unlock()
		if ok {
			r.unregWriter(uname)
			writer.wg.Done()
		}
		glog.Errorf("Response failed: %v", err)
		return
	}

	switch iReq.Act {
	// a remote target sent object's metadata. A slice should be waiting
	// for the information and be registered with `regWriter` beforehand
	case reqMeta:
		r.dOwner.mtx.Lock()
		writer, ok := r.dOwner.slices[uname]
		r.dOwner.mtx.Unlock()
		if !ok {
			glog.Errorf("No writer for %s", uname)
			return
		}
		if err := r.writerReceive(writer, iReq.Exists, object); err != nil && err != ErrorNotFound {
			glog.Errorf("Failed to receive data for %s: %v", iReq.Sender, err)
		}

		// object or slice received
	case reqPut:
		if glog.V(4) {
			glog.Infof("Response from %s, %s", iReq.Sender, uname)
		}
		r.dOwner.mtx.Lock()
		writer, ok := r.dOwner.slices[uname]
		r.dOwner.mtx.Unlock()
		// Case #1: it is response to slice/replica request by an object
		// restoration process. In this case there should exists
		// a slice waiting for the data to come(registered with `regWriter`.
		// Read the data into the slice writer and notify the slice when
		// the transfer is completed
		if ok {
			writer.lom.Version = hdr.ObjAttrs.Version
			writer.lom.Atime = time.Unix(0, hdr.ObjAttrs.Atime)
			if hdr.ObjAttrs.CksumType != "" {
				writer.lom.Cksum = cmn.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue)
			}
			if err := r.writerReceive(writer, iReq.Exists, object); err != nil {
				glog.Errorf("Failed to read replica: %v", err)
			}
			return
		}

		// Case #2: a remote target sent a replica/slice while it was
		// encoding or restoring an object. In this case it just saves
		// the sent replica or slice to a local file along with its metadata
		// look for metadata in request

		// First check if the request is valid: it must contain metadata
		meta := iReq.Meta
		if meta == nil {
			glog.Errorf("No metadata in request for %s/%s: %s", hdr.Bucket, hdr.Objname, string(hdr.Opaque))
			return
		}

		bckIsLocal := r.bmd.Get().IsLocal(hdr.Bucket)
		var (
			objFQN, errstr string
			err            error
		)
		if iReq.IsSlice {
			if glog.V(4) {
				glog.Infof("Got slice response from %s (#%d of %s/%s)",
					iReq.Sender, iReq.Meta.SliceID, hdr.Bucket, hdr.Objname)
			}
			objFQN, errstr = cluster.FQN(SliceType, hdr.Bucket, hdr.Objname, bckIsLocal)
			if errstr != "" {
				glog.Error(errstr)
				return
			}
		} else {
			if glog.V(4) {
				glog.Infof("Got replica response from %s (%s/%s)",
					iReq.Sender, hdr.Bucket, hdr.Objname)
			}
			objFQN, errstr = cluster.FQN(fs.ObjectType, hdr.Bucket, hdr.Objname, bckIsLocal)
			if errstr != "" {
				glog.Error(errstr)
				return
			}
		}
		// save slice/object
		tmpFQN := fs.CSM.GenContentFQN(objFQN, fs.WorkfileType, "ec")
		buf, slab := mem2.AllocFromSlab2(cmn.MiB)
		err = cmn.SaveReaderSafe(tmpFQN, objFQN, object, buf)
		if err == nil {
			lom := &cluster.LOM{FQN: objFQN, T: r.t}
			if errstr := lom.Fill("", 0); errstr != "" {
				glog.Errorf("Failed to read resolve FQN %s: %s", objFQN, errstr)
				slab.Free(buf)
				return
			}
			lom.Version = hdr.ObjAttrs.Version
			lom.Atime = time.Unix(0, hdr.ObjAttrs.Atime)
			if hdr.ObjAttrs.CksumType != "" {
				lom.Cksum = cmn.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue)
			}

			if errstr := lom.Persist(); errstr != "" {
				err = errors.New(errstr)
			}
		}
		if err != nil {
			glog.Errorf("Failed to save %s/%s data to %q: %v",
				hdr.Bucket, hdr.Objname, objFQN, err)
			slab.Free(buf)
			return
		}

		// save its metadata
		metaFQN := fs.CSM.GenContentFQN(objFQN, MetaType, "")
		metaBuf, err := meta.marshal()
		if err == nil {
			err = cmn.SaveReader(metaFQN, bytes.NewReader(metaBuf), buf)
		}
		slab.Free(buf)
		if err != nil {
			glog.Errorf("Failed to save metadata to %q: %v", metaFQN, err)
		}
	default:
		// should be unreachable
		glog.Errorf("Invalid request: %s", hdr.Opaque)
	}
}

func (r *XactEC) newGetJogger(mpath string) *getJogger {
	return &getJogger{
		parent: r,
		mpath:  mpath,
		workCh: make(chan *Request, requestBufSizeFS),
		stopCh: make(chan struct{}, 1),
		jobs:   make(map[uint64]bgProcess, 4),
		sema:   make(chan struct{}, maxBgJobsPerJogger),
		diskCh: make(chan struct{}, 1),
	}
}

func (r *XactEC) newPutJogger(mpath string) *putJogger {
	return &putJogger{
		parent: r,
		mpath:  mpath,
		workCh: make(chan *Request, requestBufSizeFS),
		stopCh: make(chan struct{}, 1),
	}
}

// Send a data or request to one or few targets by their DaemonIDs. Most of the time
// only DaemonID is known - that is why the function gets DaemonID and internally
// transforms it into cluster.Snode.
// * daemonIDs - a list of targets
// * hdr - transport header
// * reader - a data to send
// * cb - optional callback to be called when the transfer completes
// * isRequest - defines the type of request:
//		- true - send lightweight request to all targets (usually reader is nil
//			in this case)
//	    - false - send a slice/replica/metadata to targets
func (r *XactEC) sendByDaemonID(daemonIDs []string, hdr transport.Header,
	reader cmn.ReadOpenCloser, cb transport.SendCallback, isRequest bool) error {
	nodes := make([]*cluster.Snode, 0, len(daemonIDs))
	smap := r.smap.Get()
	for _, id := range daemonIDs {
		si, ok := smap.Tmap[id]
		if !ok {
			glog.Errorf("Target with ID %s not found", id)
			continue
		}
		nodes = append(nodes, si)
	}

	if len(nodes) == 0 {
		return errors.New("destination list is empty")
	}

	var err error
	if isRequest {
		err = r.reqBundle.Send(hdr, reader, cb, nodes)
	} else {
		err = r.respBundle.Send(hdr, reader, cb, nodes)
	}
	return err
}

// send request to a target, wait for its response, read the data into writer.
// * daemonID - target to send a request
// * bucket/objname - what to request
// * uname - unique name for the operation: the name is built from daemonID,
//		bucket and object names. HTTP data receiving handler generates a name
//		when receiving data and if it finds a writer registered with the same
//		name, it puts the data to its writer and notifies when download is done
// * request - request to send
// * writer - an opened writer that will receive the replica/slice/meta
func (r *XactEC) readRemote(lom *cluster.LOM, daemonID, uname string, request []byte, writer io.Writer) error {
	hdr := transport.Header{
		Bucket:  lom.Bucket,
		Objname: lom.Objname,
		Opaque:  request,
	}
	var reader cmn.ReadOpenCloser
	reader, hdr.ObjAttrs.Size = nil, 0

	sw := &slice{
		writer: writer,
		wg:     cmn.NewTimeoutGroup(),
		lom:    lom,
	}

	sw.wg.Add(1)
	r.regWriter(uname, sw)

	if glog.V(4) {
		glog.Infof("Requesting object %s/%s from %s", lom.Bucket, lom.Objname, daemonID)
	}
	if err := r.sendByDaemonID([]string{daemonID}, hdr, reader, nil, true); err != nil {
		r.unregWriter(uname)
		return err
	}
	c := cmn.GCO.Get()
	if sw.wg.WaitTimeout(c.Timeout.SendFile) {
		r.unregWriter(uname)
		return fmt.Errorf("timed out waiting for %s is read", uname)
	}
	r.unregWriter(uname)
	lom.Fill("", cluster.LomFstat)
	if glog.V(4) {
		glog.Infof("Received object %s/%s from %s", lom.Bucket, lom.Objname, daemonID)
	}

	return nil
}

// Used to copy replicas/slices after the object is encoded after PUT/restored
// after GET, or to respond to meta/slice/replica request.
// * daemonIDs - receivers of the data
// * bucket/objname - object path
// * reader - object/slice/meta data
// * src - extra information about the data to send
// * cb - a caller may set its own callback to execute when the transfer is done.
//		A special case:
//		if a caller does not define its own callback, and it sets the `obj` in
//		`src` it means that the caller wants to automatically free the memory
//		allocated for the `obj` SGL after the object is transferred. The caller
//		may set optional counter in `obj` - the default callback decreases the
//		counter each time the callback is called and when the value drops below 1,
//		`writeRemote` callback frees the SGL
//      The counter is used for sending slices of one big SGL to a few nodes. In
//		this case every slice must be sent to only one target, and transport bundle
//		cannot help to track automatically when SGL should be freed.
func (r *XactEC) writeRemote(daemonIDs []string, lom *cluster.LOM, src *dataSource, cb transport.SendCallback) error {
	req := r.newIntraReq(reqPut, src.metadata)
	req.IsSlice = src.isSlice

	putData, err := req.marshal()
	if err != nil {
		return err
	}
	objAttrs := transport.ObjectAttrs{
		Size:    src.size,
		Version: lom.Version,
		Atime:   lom.Atime.UnixNano(),
	}
	if lom.Cksum != nil {
		objAttrs.CksumType, objAttrs.CksumValue = lom.Cksum.Get()
	}
	hdr := transport.Header{
		Objname:  lom.Objname,
		Bucket:   lom.Bucket,
		Opaque:   putData,
		ObjAttrs: objAttrs,
	}
	if cb == nil && src.obj != nil {
		obj := src.obj
		cb = func(hdr transport.Header, reader io.ReadCloser, err error) {
			if obj != nil {
				obj.release()
			}
			if err != nil {
				glog.Errorf("Failed to send %s/%s to %v: %v", lom.Bucket, lom.Objname, daemonIDs, err)
			}
		}
	}
	return r.sendByDaemonID(daemonIDs, hdr, src.reader, cb, false)
}

func (r *XactEC) Run() (err error) {
	glog.Infof("Starting %s", r.Getname())

	for _, jog := range r.getJoggers {
		go jog.run()
	}
	for _, jog := range r.putJoggers {
		go jog.run()
	}
	conf := cmn.GCO.Get()
	tck := time.NewTicker(conf.Periodic.StatsTime)
	lastAction := time.Now()
	idleTimeout := conf.Timeout.SendFile * 3

	// as of now all requests are equal. Some may get throttling later
	for {
		select {
		case <-tck.C:
			if s := fmt.Sprintf("%v", r.Stats()); s != "" {
				glog.Info(s)
			}
		case req := <-r.ecCh:
			lastAction = time.Now()
			switch req.Action {
			case ActSplit:
				r.stats.updateEncode(req.LOM.Size)
			case ActDelete:
				r.stats.updateDelete()
			case ActRestore:
				r.stats.updateDecode()
			}
			r.dispatchRequest(req)
		case mpathRequest := <-r.mpathReqCh:
			switch mpathRequest.action {
			case cmn.ActMountpathAdd:
				r.addMpath(mpathRequest.mpath)
			case cmn.ActMountpathRemove:
				r.removeMpath(mpathRequest.mpath)
			}
		case <-r.ChanCheckTimeout():
			idleEnds := lastAction.Add(idleTimeout)
			if idleEnds.Before(time.Now()) && r.Timeout() {
				if glog.V(4) {
					glog.Infof("Idle time is over: %v. Last action at: %v",
						time.Now(), lastAction)
				}
				r.stop()
				return nil
			}
		case <-r.ChanAbort():
			r.stop()
			return fmt.Errorf("%s aborted, exiting", r)
		}
	}
}

// Utility function to cleanup both object/slice and its meta on the local node
// Used when processing object deletion request
func (r *XactEC) removeObjAndMeta(bucket, objname string, bckIsLocal bool) error {
	if glog.V(4) {
		glog.Infof("Delete request for %s/%s", bucket, objname)
	}

	// to be consistent with PUT, object's files are deleted in a reversed
	// order: first Metafile is removed, then Replica/Slice
	// Why: the main object is gone already, so we do not want any target
	// responds that it has the object because it has metafile. We delete
	// metafile that makes remained slices/replicas outdated and can be cleaned
	// up later by LRU or other runner
	for _, tp := range []string{MetaType, fs.ObjectType, SliceType} {
		fqnMeta, errstr := cluster.FQN(tp, bucket, objname, bckIsLocal)
		if errstr != "" {
			return errors.New(errstr)
		}
		if err := os.RemoveAll(fqnMeta); err != nil {
			return fmt.Errorf("error removing %s %q: %v", tp, fqnMeta, err)
		}
	}

	return nil
}

// Sends the replica/meta/slice data: either to copy replicas/slices after
// encoding or to send requested "object" to a client. In the latter case
// if the local object does not exist, it sends an empty body and sets
// exists=false in response header
func (r *XactEC) dataResponse(act intraReqType, fqn, bucket, objname, id string) error {
	var (
		reader cmn.ReadOpenCloser
		sz     int64
	)
	ireq := r.newIntraReq(act, nil)
	fh, err := cmn.NewFileHandle(fqn)
	lom := &cluster.LOM{FQN: fqn, T: r.t}
	if errstr := lom.Fill("", cluster.LomFstat|cluster.LomAtime|cluster.LomVersion|cluster.LomCksum); errstr != "" {
		// an error is OK. Log it and try to go on with what has been read
		glog.Warningf("Failed to read file stats: %s", errstr)
	}

	if err == nil && lom.Size != 0 {
		sz = lom.Size
		reader = fh
	} else {
		ireq.Exists = false
	}
	cmn.Assert((sz == 0 && reader == nil) || (sz != 0 && reader != nil))
	objAttrs := transport.ObjectAttrs{
		Size:    sz,
		Version: lom.Version,
		Atime:   lom.Atime.UnixNano(),
	}
	if lom.Cksum != nil {
		objAttrs.CksumType, objAttrs.CksumValue = lom.Cksum.Get()
	}
	rHdr := transport.Header{
		Bucket:   bucket,
		Objname:  objname,
		ObjAttrs: objAttrs,
	}
	if rHdr.Opaque, err = ireq.marshal(); err != nil {
		if fh != nil {
			fh.Close()
		}
		return err
	}

	cb := func(hdr transport.Header, c io.ReadCloser, err error) {
		if err != nil {
			glog.Errorf("Failed to send %s/%s: %v", hdr.Bucket, hdr.Objname, err)
		}
	}
	return r.sendByDaemonID([]string{id}, rHdr, reader, cb, false)
}

// save data from a target response to SGL. When exists is false it
// just drains the response body and returns - because it does not contain
// any data. On completion the function must call writer.wg.Done to notify
// the caller that the data read is completed.
// * writer - where to save the slice/meta/replica data
// * exists - if the remote target had the requested object
// * reader - response body
func (r *XactEC) writerReceive(writer *slice, exists bool, reader io.Reader) (err error) {
	buf, slab := mem2.AllocFromSlab2(cmn.MiB)

	if !exists {
		writer.wg.Done()
		// drain the body, to avoid panic:
		// http: panic serving: assertion failed: "expecting an error or EOF as the reason for failing to read
		_, _ = io.CopyBuffer(ioutil.Discard, reader, buf)
		slab.Free(buf)
		return ErrorNotFound
	}

	writer.n, err = io.CopyBuffer(writer.writer, reader, buf)
	if file, ok := writer.writer.(*os.File); ok {
		file.Close()
	}
	writer.wg.Done()
	slab.Free(buf)
	return err
}

func (r *XactEC) Stop(error) { r.Abort() }

func (r *XactEC) stop() {
	if r.Finished() {
		glog.Warningf("%s - not running, nothing to do", r)
		return
	}

	r.XactDemandBase.Stop()
	for _, jog := range r.getJoggers {
		jog.stop()
	}
	for _, jog := range r.putJoggers {
		jog.stop()
	}
	r.reqBundle.Close(true)
	r.respBundle.Close(true)
	r.EndTime(time.Now())
}

// Encode schedules FQN for erasure coding process
func (r *XactEC) Encode(req *Request) {
	req.putTime = time.Now()
	req.tm = time.Now()
	if glog.V(4) {
		glog.Infof("ECXAction (queue = %d): encode object %s", len(r.ecCh), req.LOM.Uname)
	}
	r.ecCh <- req
}

// Decode schedules an object to be restored from existing slices.
// A caller should wait for the main object restoration is completed. When
// ecrunner finishes main object restoration process it puts into request.ErrCh
// channel the error or nil. The caller may read the object after receiving
// a nil value from channel but ecrunner keeps working - it reuploads all missing
// slices or copies
func (r *XactEC) Decode(req *Request) {
	req.putTime = time.Now()
	req.tm = time.Now()
	r.ecCh <- req
}

// Cleanup deletes all object slices or copies after the main object is removed
func (r *XactEC) Cleanup(req *Request) {
	req.putTime = time.Now()
	req.tm = time.Now()
	r.ecCh <- req
}

func (r *XactEC) dispatchRequest(req *Request) {
	r.IncPending()
	switch req.Action {
	case ActRestore:
		jogger, ok := r.getJoggers[req.LOM.ParsedFQN.MpathInfo.Path]
		cmn.AssertMsg(ok, "Invalid mountpath given in EC request")
		r.stats.updateQueue(len(jogger.workCh))
		jogger.workCh <- req
	default:
		jogger, ok := r.putJoggers[req.LOM.ParsedFQN.MpathInfo.Path]
		cmn.AssertMsg(ok, "Invalid mountpath given in EC request")
		if glog.V(4) {
			glog.Infof("ECXAction (bg queue = %d): dispatching object %s....", len(jogger.workCh), req.LOM.Uname)
		}
		r.stats.updateQueue(len(jogger.workCh))
		jogger.workCh <- req
	}
}

func (r *XactEC) Stats() *ECStats {
	return r.stats.stats()
}

// Create a request header: initializes the `Sender` field with local target's
// daemon ID, and sets `Exists:true` that means "local object exists".
// Later `Exists` can be changed to `false` if local file is unreadable or does
// not exist
func (r *XactEC) newIntraReq(act intraReqType, meta *Metadata) *intraReq {
	return &intraReq{
		Act:    act,
		Sender: r.si.DaemonID,
		Meta:   meta,
		Exists: true,
	}
}

// Registers a new slice that will wait for the data to come from
// a remote target
func (r *XactEC) regWriter(uname string, writer *slice) bool {
	r.dOwner.mtx.Lock()
	_, ok := r.dOwner.slices[uname]
	if ok {
		glog.Errorf("Writer for %s is already registered", uname)
	} else {
		r.dOwner.slices[uname] = writer
	}
	r.dOwner.mtx.Unlock()

	return !ok
}

// Unregisters a slice that has been waiting for the data to come from
// a remote target
func (r *XactEC) unregWriter(uname string) (writer *slice, ok bool) {
	r.dOwner.mtx.Lock()
	wr, ok := r.dOwner.slices[uname]
	delete(r.dOwner.slices, uname)
	r.dOwner.mtx.Unlock()

	return wr, ok
}

//
// fsprunner methods
//
func (r *XactEC) ReqAddMountpath(mpath string) {
	r.mpathReqCh <- mpathReq{action: cmn.ActMountpathAdd, mpath: mpath}
}

func (r *XactEC) ReqRemoveMountpath(mpath string) {
	r.mpathReqCh <- mpathReq{action: cmn.ActMountpathRemove, mpath: mpath}
}

func (r *XactEC) ReqEnableMountpath(mpath string)  { /* do nothing */ }
func (r *XactEC) ReqDisableMountpath(mpath string) { /* do nothing */ }

func (r *XactEC) addMpath(mpath string) {
	jogger, ok := r.getJoggers[mpath]
	if ok && jogger != nil {
		glog.Warningf("Attempted to add already existing mountpath: %s", mpath)
		return
	}
	getJog := r.newGetJogger(mpath)
	r.getJoggers[mpath] = getJog
	go getJog.run()
	putJog := r.newPutJogger(mpath)
	r.putJoggers[mpath] = putJog
	go putJog.run()
}

func (r *XactEC) removeMpath(mpath string) {
	getJog, ok := r.getJoggers[mpath]
	cmn.AssertMsg(ok, "Mountpath unregister handler for EC called with invalid mountpath")
	getJog.stop()
	delete(r.getJoggers, mpath)

	putJog, ok := r.putJoggers[mpath]
	cmn.AssertMsg(ok, "Mountpath unregister handler for EC called with invalid mountpath")
	putJog.stop()
	delete(r.putJoggers, mpath)
}
