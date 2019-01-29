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
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
)

const (
	requestBufSizeGlobal = 800
	requestBufSizeFS     = 200
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

		joggers    map[string]*jogger // active mountpath runners
		bmd        cluster.Bowner     // bucket manager
		smap       cluster.Sowner     // cluster map
		si         *cluster.Snode     // target daemonInfo
		nameLocker cluster.NameLocker // to lock/unlock objects
		stats      stats              // EC statistics

		dOwner *dataOwner // data slice manager

		reqBundle  *transport.StreamBundle // a stream bundle to send lightweight requests
		respBundle *transport.StreamBundle // a stream bungle to transfer data between targets
	}
)

type (
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
	si *cluster.Snode, nameLocker cluster.NameLocker) *XactEC {
	runner := &XactEC{
		t:          t,
		mpathReqCh: make(chan mpathReq, 1),
		joggers:    make(map[string]*jogger),
		ecCh:       make(chan *Request, requestBufSizeGlobal),
		bmd:        bmd,
		smap:       smap,
		si:         si,
		nameLocker: nameLocker,

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

	ncpu := runtime.NumCPU()
	defaultTransport := http.DefaultTransport.(*http.Transport)
	client := &http.Client{Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          1000,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: defaultTransport.ExpectContinueTimeout,
		MaxIdleConnsPerHost:   ncpu,
	}}

	extraReq := transport.Extra{Callback: cbReq}
	runner.reqBundle = transport.NewStreamBundle(smap, si, client, netReq, ReqStreamName, &extraReq, cluster.Targets, 4)
	extraResp := transport.Extra{}
	runner.respBundle = transport.NewStreamBundle(smap, si, client, netResp, RespStreamName, &extraResp, cluster.Targets, 4)

	// create all runners but do not start them until Run is called
	availablePaths, disabledPaths := fs.Mountpaths.Get()
	for mpath := range availablePaths {
		mpr := runner.newMpathRunner(mpath)
		runner.joggers[mpath] = mpr
	}
	for mpath := range disabledPaths {
		mpr := runner.newMpathRunner(mpath)
		runner.joggers[mpath] = mpr
	}

	return runner
}

func (r *XactEC) DispatchReq(w http.ResponseWriter, hdr transport.Header, object io.Reader) {
	iReq := intraReq{}
	if err := iReq.unmarshal(hdr.Opaque); err != nil {
		glog.Errorf("Failed to unmarshal request: %v", err)
		return
	}

	islocal := r.bmd.Get().IsLocal(hdr.Bucket)
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
		if err := r.removeObjAndMeta(hdr.Bucket, hdr.Objname, islocal); err != nil {
			glog.Errorf("Failed to delete %s/%s: %v", hdr.Bucket, hdr.Objname, err)
		}
	case reqGet:
		// slice or replica request: send the object's data to the caller
		var fqn, errstr string
		if iReq.IsSlice {
			if glog.V(4) {
				glog.Infof("Received request for slice %d of %s", iReq.Meta.SliceID, hdr.Objname)
			}
			fqn, errstr = cluster.FQN(SliceType, hdr.Bucket, hdr.Objname, islocal)
		} else {
			if glog.V(4) {
				glog.Infof("Received request for replica %s", hdr.Objname)
			}
			fqn, errstr = cluster.FQN(fs.ObjectType, hdr.Bucket, hdr.Objname, islocal)
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
		fqn, errstr := cluster.FQN(MetaType, hdr.Bucket, hdr.Objname, islocal)
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

		// object or slice recieved
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
				writer.lom.Nhobj = cmn.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.Cksum)
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

		isLocal := r.bmd.Get().IsLocal(hdr.Bucket)
		var (
			objFQN, errstr string
			err            error
		)
		if iReq.IsSlice {
			if glog.V(4) {
				glog.Infof("Got slice responce from %s (#%d of %s/%s)",
					iReq.Sender, iReq.Meta.SliceID, hdr.Bucket, hdr.Objname)
			}
			objFQN, errstr = cluster.FQN(SliceType, hdr.Bucket, hdr.Objname, isLocal)
			if errstr != "" {
				glog.Error(errstr)
				return
			}
		} else {
			if glog.V(4) {
				glog.Infof("Got replica responce from %s (%s/%s)",
					iReq.Sender, hdr.Bucket, hdr.Objname)
			}
			objFQN, errstr = cluster.FQN(fs.ObjectType, hdr.Bucket, hdr.Objname, isLocal)
			if errstr != "" {
				glog.Error(errstr)
				return
			}
		}
		// save slice/object
		r.nameLocker.Lock(uname, true)
		tmpFQN := fs.CSM.GenContentFQN(objFQN, fs.WorkfileType, "ec")
		buf, slab := mem2.AllocFromSlab2(cmn.MiB)
		err = cmn.SaveReaderSafe(tmpFQN, objFQN, object, buf)
		if err == nil {
			lom := &cluster.LOM{Fqn: objFQN, T: r.t}
			if errstr := lom.Fill(0); errstr != "" {
				glog.Errorf("Failed to read resolve FQN %s: %s",
					objFQN, errstr)
				slab.Free(buf)
				return
			}
			lom.Version = hdr.ObjAttrs.Version
			lom.Atime = time.Unix(0, hdr.ObjAttrs.Atime)
			if hdr.ObjAttrs.CksumType != "" {
				lom.Nhobj = cmn.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.Cksum)
			}

			if errstr := lom.Persist(); errstr != "" {
				err = errors.New(errstr)
			}
		}
		r.nameLocker.Unlock(uname, true)
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

func (r *XactEC) newMpathRunner(mpath string) *jogger {
	return &jogger{
		parent: r,
		mpath:  mpath,
		topCh:  make(chan *Request, requestBufSizeFS),
		bgCh:   make(chan *Request, requestBufSizeFS),
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
		return errors.New("Destination list is empty")
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
func (r *XactEC) readRemote(lom *cluster.LOM, daemonID, uname string, request []byte, writer *memsys.SGL) error {
	hdr := transport.Header{
		Bucket:  lom.Bucket,
		Objname: lom.Objname,
		Opaque:  request,
	}
	var reader cmn.ReadOpenCloser
	reader, hdr.ObjAttrs.Size = nil, 0

	sw := &slice{
		sgl: writer,
		wg:  cmn.NewTimeoutGroup(),
		lom: lom,
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
		return fmt.Errorf("Timed out waiting for %s is read", uname)
	}
	r.unregWriter(uname)
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
	if lom.Nhobj != nil {
		objAttrs.CksumType, objAttrs.Cksum = lom.Nhobj.Get()
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
			if obj != nil && obj.sgl != nil {
				cnt := atomic.AddInt32(&obj.cnt, -1)
				if cnt < 1 {
					obj.sgl.Free()
					obj.sgl = nil
				}
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

	for _, mpr := range r.joggers {
		go mpr.run()
	}
	conf := cmn.GCO.Get()
	tck := time.NewTicker(conf.Periodic.StatsTime)
	lastAction := time.Now()

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
			idleEnds := lastAction.Add(IdleTimeout)
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
func (r *XactEC) removeObjAndMeta(bucket, objname string, islocal bool) error {
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
		fqnMeta, errstr := cluster.FQN(tp, bucket, objname, islocal)
		if errstr != "" {
			return errors.New(errstr)
		}
		if err := os.RemoveAll(fqnMeta); err != nil {
			return fmt.Errorf("Error removing %s %q: %v", tp, fqnMeta, err)
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
	sgl, err := readFile(fqn)
	lom := &cluster.LOM{Fqn: fqn, T: r.t}
	if errstr := lom.Fill(cluster.LomAtime | cluster.LomVersion | cluster.LomCksum); errstr != "" {
		// an error is OK. Log it and try to go on with what has been read
		glog.Warningf("Failed to read file stats: %s", errstr)
	}

	if sgl != nil && err == nil {
		sz = sgl.Size()
		reader = memsys.NewReader(sgl)
	} else {
		ireq.Exists = false
	}
	objAttrs := transport.ObjectAttrs{
		Size:    sz,
		Version: lom.Version,
		Atime:   lom.Atime.UnixNano(),
	}
	if lom.Nhobj != nil {
		objAttrs.CksumType, objAttrs.Cksum = lom.Nhobj.Get()
	}
	rHdr := transport.Header{
		Bucket:   bucket,
		Objname:  objname,
		ObjAttrs: objAttrs,
	}
	if rHdr.Opaque, err = ireq.marshal(); err != nil {
		if sgl != nil {
			sgl.Free()
		}
		return err
	}

	cb := func(hdr transport.Header, c io.ReadCloser, err error) {
		if err != nil {
			glog.Errorf("Failed to send %s/%s: %v", hdr.Bucket, hdr.Objname, err)
		}
		if sgl != nil {
			sgl.Free()
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
func (r *XactEC) writerReceive(writer *slice, exists bool, reader io.Reader) error {
	buf, slab := mem2.AllocFromSlab2(cmn.MiB)

	if !exists {
		writer.wg.Done()
		// drain the body, to avoid panic:
		// http: panic serving: assertion failed: "expecting an error or EOF as the reason for failing to read
		_, _ = io.CopyBuffer(ioutil.Discard, reader, buf)
		slab.Free(buf)
		return ErrorNotFound
	}

	writer.n, writer.err = io.CopyBuffer(writer.sgl, reader, buf)
	writer.wg.Done()
	slab.Free(buf)
	return nil
}

func (r *XactEC) Stop(error) { r.Abort() }

func (r *XactEC) stop() {
	if r.Finished() {
		glog.Warningf("%s - not running, nothing to do", r)
		return
	}

	r.XactDemandBase.Stop()
	for _, mpr := range r.joggers {
		mpr.stop()
	}
	r.reqBundle.Close(true)
	r.respBundle.Close(true)
	r.EndTime(time.Now())
}

// Encode schedules FQN for erasure coding process
func (r *XactEC) Encode(req *Request) {
	req.putTime = time.Now()
	req.tm = time.Now()
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
	jogger, ok := r.joggers[req.LOM.ParsedFQN.MpathInfo.Path]
	cmn.Assert(ok, "Invalid mountpath given in EC request")
	switch req.Action {
	case ActRestore:
		r.stats.updateQueue(len(jogger.topCh))
		jogger.topCh <- req
	default:
		r.stats.updateQueue(len(jogger.bgCh))
		jogger.bgCh <- req
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
	jogger, ok := r.joggers[mpath]
	if ok && jogger != nil {
		glog.Warningf("Attempted to add already existing mountpath: %s", mpath)
		return
	}
	runner := r.newMpathRunner(mpath)
	r.joggers[mpath] = runner
	go runner.run()
}

func (r *XactEC) removeMpath(mpath string) {
	jogger, ok := r.joggers[mpath]
	cmn.Assert(ok, "Mountpath unregister handler for EC called with invalid mountpath")
	jogger.stop()
	delete(r.joggers, mpath)
}
