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
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/reedsolomon"
)

// a mountpath jogger: processes requests to one mountpath
type jogger struct {
	parent *XactEC
	slab   *memsys.Slab2
	buffer []byte
	mpath  string // mountpath that the jogger manages

	topCh  chan *Request // channel to request TOP priority operation (restore)
	bgCh   chan *Request // channel to request operation with BACKground priority (encode, cleanup)
	stopCh chan struct{} // jogger management channel: to stop it
}

func (c *jogger) run() {
	glog.Infof("Started EC for mountpath: %s", c.mpath)
	c.buffer, c.slab = mem2.AllocFromSlab2(cmn.MiB)

loop:
	for {
		// first, process top priority requests (Restore)
		select {
		case req := <-c.topCh:
			c.parent.stats.updateWaitTime(time.Since(req.tm))
			req.tm = time.Now()
			c.ec(req)
			c.parent.DecPending()
			// restart loop to check if there are more top priority requests
			continue loop
		case <-c.stopCh:
			c.slab.Free(c.buffer)
			c.buffer = nil
			c.slab = nil
			return
		default:
			break
		}

		// last, process background requests (cleanup, encode)
		select {
		case req := <-c.topCh:
			// check top priority request in this select, too, otherwise
			// the loop hangs on this select if the first request is a top one
			c.parent.stats.updateWaitTime(time.Since(req.tm))
			req.tm = time.Now()
			c.ec(req)
			c.parent.DecPending()
		case req := <-c.bgCh:
			c.parent.stats.updateWaitTime(time.Since(req.tm))
			req.tm = time.Now()
			c.ec(req)
			c.parent.DecPending()
		case <-c.stopCh:
			c.slab.Free(c.buffer)
			c.buffer = nil
			c.slab = nil
			return
		}
	}
}

func (c *jogger) stop() {
	glog.Infof("Stopping EC for mountpath: %s", c.mpath)
	c.stopCh <- struct{}{}
	close(c.stopCh)
}

// starts EC process
func (c *jogger) ec(req *Request) {
	var (
		err error
		act = "encoding"
	)

	switch req.Action {
	case ActSplit:
		err = c.encode(req)
		c.parent.stats.updateEncodeTime(time.Since(req.tm), err != nil)
	case ActRestore:
		err = c.restore(req)
		act = "restoring"
		c.parent.stats.updateDecodeTime(time.Since(req.tm), err != nil)
	case ActDelete:
		err = c.cleanup(req)
		act = "cleaning up"
		c.parent.stats.updateDeleteTime(time.Since(req.tm), err != nil)
	default:
		err = fmt.Errorf("Invalid EC action: %v", req.Action)
	}

	if err != nil {
		glog.Errorf("Error occurred during %s object [%s/%s], fqn: %q, err: %v",
			act, req.LOM.Bucket, req.LOM.Objname, req.LOM.Fqn, err)
	}

	if req.ErrCh != nil {
		req.ErrCh <- err
		close(req.ErrCh)
	}
	if err == nil {
		c.parent.stats.updateObjTime(time.Since(req.putTime))
	}
}

// removes all temporary slices in case of erasure coding fails in the middle
func (c *jogger) freeSGL(slices []*slice) {
	for _, s := range slices {
		if s != nil && s.sgl != nil {
			s.sgl.Free()
			s.sgl = nil
		}
	}
}

// calculates and stores data and parity slices
func (c *jogger) encode(req *Request) error {
	if glog.V(4) {
		glog.Infof("Encoding %q...", req.LOM.Fqn)
	}
	_, chk := req.LOM.Nhobj.Get()
	meta := &Metadata{
		Size:     req.LOM.Size,
		Data:     req.LOM.Bprops.DataSlices,
		Parity:   req.LOM.Bprops.ParitySlices,
		IsCopy:   req.IsCopy,
		Checksum: chk,
	}

	// calculate the number of targets required to encode the object
	// For replicated: ParitySlices + original object
	// For encoded: ParitySlices + DataSlices + original object
	reqTargets := req.LOM.Bprops.ParitySlices + 1
	if !req.IsCopy {
		reqTargets += req.LOM.Bprops.DataSlices
	}
	targetCnt := len(c.parent.smap.Get().Tmap)
	if targetCnt < reqTargets {
		return fmt.Errorf("Object %s/%s requires %d targets to encode, only %d found",
			req.LOM.Bucket, req.LOM.Objname, reqTargets, targetCnt)
	}

	metabuf, err := meta.marshal()
	if err != nil {
		return err
	}

	// Save metadata before encoding the object
	metaFQN, errstr := cluster.FQN(MetaType, req.LOM.Bucket, req.LOM.Objname, req.LOM.Bislocal)
	if errstr != "" {
		return errors.New(errstr)
	}
	if err := cmn.SaveReader(metaFQN, bytes.NewReader(metabuf), c.buffer); err != nil {
		return err
	}

	// if an object is small just make `parity` copies
	if meta.IsCopy {
		if copySlices, err := c.createCopies(req, meta); err != nil {
			c.freeSGL(copySlices)
			c.cleanup(req)
		}
		return err
	}

	// big object is erasure encoded
	slices, err := c.sendSlices(req, meta)
	if err != nil {
		c.freeSGL(slices)
		c.cleanup(req)
	}

	return err
}

// the final step of replica restoration process: the main target detects which
// nodes do not have replicas and copy it to them
// * bucket/objname - object path
// * sgl - replica content to sent to remote targets
// * metadata - object's EC metadata
// * nodes - targets that have metadata and replica - filled by requestMeta
// * replicaCnt - total number of replicas including main one
func (c *jogger) copyMissingReplicas(lom *cluster.LOM, sgl *memsys.SGL, metadata *Metadata, nodes map[string]*Metadata, replicaCnt int) {
	targets, errstr := cluster.HrwTargetList(lom.Bucket, lom.Objname, c.parent.smap.Get(), replicaCnt)
	if errstr != "" {
		sgl.Free()
		glog.Errorf("Failed to get list of %d targets: %s", replicaCnt, errstr)
		return
	}

	// fill the list of daemonIDs that do not have replica
	daemons := make([]string, 0, len(targets))
	for _, target := range targets {
		if target.DaemonID == c.parent.si.DaemonID {
			continue
		}

		if _, ok := nodes[target.DaemonID]; !ok {
			daemons = append(daemons, target.DaemonID)
		}
	}

	// if any target lost its replica send the replica to it, and free allocated
	// memory on complition
	// Otherwise just free allocated memory and return immediately
	if len(daemons) == 0 {
		sgl.Free()
		return
	}
	cb := func(hdr transport.Header, reader io.ReadCloser, err error) {
		if err != nil {
			glog.Errorf("Failed to send %s/%s to %v: %v", lom.Bucket, lom.Objname, daemons, err)
		}
		sgl.Free()
	}
	src := &dataSource{
		reader:   memsys.NewReader(sgl),
		size:     sgl.Size(),
		metadata: metadata,
	}
	if err := c.parent.writeRemote(daemons, lom, src, cb); err != nil {
		glog.Errorf("Failed to copy replica %s/%s to %v: %v", lom.Bucket, lom.Objname, daemons, err)
	}
}

// starting point of restoration of the object that was replicated
// * req - original request from a target
// * meta - rebuilt object's metadata
// * nodes - filled by requestMeta the list of targets what responsed to GET
//      metadata request with valid metafile
func (c *jogger) restoreReplicated(req *Request, meta *Metadata, nodes map[string]*Metadata) error {
	var writer *memsys.SGL
	// try read a replica from targets one by one until the replica is got
	for node, _ := range nodes {
		uname := unique(node, req.LOM.Bucket, req.LOM.Objname)
		iReqBuf, err := c.parent.newIntraReq(reqGet, nil).marshal()
		if err != nil {
			glog.Errorf("Failed to marshal %v", err)
			continue
		}

		w := mem2.NewSGL(cmn.KiB)
		if err := c.parent.readRemote(req.LOM, node, uname, iReqBuf, w); err != nil {
			glog.Errorf("Failed to read from %s", node)
			w.Free()
			w = nil
			continue
		}
		if w.Size() != 0 {
			// a valid replica is found - break and do not free SGL
			writer = w
			break
		}
		w.Free()
	}
	if glog.V(4) {
		glog.Infof("Found meta -> obj get %s/%s, writer found: %v", req.LOM.Bucket, req.LOM.Objname, writer != nil)
	}

	if writer == nil {
		return errors.New("Failed to read a replica from any target")
	}

	// Save received replica and its metadata locally - it is main replica
	objFQN, errstr := cluster.FQN(fs.ObjectType, req.LOM.Bucket, req.LOM.Objname, req.LOM.Bislocal)
	if errstr != "" {
		writer.Free()
		return errors.New(errstr)
	}
	req.LOM.Fqn = objFQN
	tmpFQN := fs.CSM.GenContentFQN(objFQN, fs.WorkfileType, "ec")
	if err := cmn.SaveReaderSafe(tmpFQN, objFQN, memsys.NewReader(writer), c.buffer); err != nil {
		writer.Free()
		return err
	}

	if errstr := req.LOM.Persist(); errstr != "" {
		writer.Free()
		return errors.New(errstr)
	}

	b, err := jsoniter.Marshal(meta)
	if err != nil {
		writer.Free()
		return err
	}
	metaFQN := fs.CSM.GenContentFQN(objFQN, MetaType, "")
	if err := cmn.SaveReader(metaFQN, bytes.NewReader(b), c.buffer); err != nil {
		writer.Free()
		return err
	}

	// now a client can read the object, but EC needs to restore missing
	// replicas. So, execute copying replicas in background and return
	go c.copyMissingReplicas(req.LOM, writer, meta, nodes, meta.Parity+1)

	return nil
}

// Main object is not found and it is clear that it was encoded. Request
// all data and parity slices from targets in a cluster:
// * req - original request
// * meta - reconstructed metadata
// * nodes - targets that responded with valid metadata, it does not make sense
//    to request slice from the entire cluster
// Returns:
// * []slice - a list of received slices in correct order (missing slices = nil)
// * map[int]string - a map of slice locations: SliceID <-> DaemonID
func (c *jogger) requestSlices(req *Request, meta *Metadata, nodes map[string]*Metadata) ([]*slice, map[int]string, error) {
	wgSlices := cmn.NewTimeoutGroup()
	sliceCnt := meta.Data + meta.Parity
	slices := make([]*slice, sliceCnt, sliceCnt)
	daemons := make([]string, 0, len(nodes)) // target to be requested for a slice
	idToNode := make(map[int]string)         // which target what slice returned

	for k, v := range nodes {
		if v.SliceID < 1 || v.SliceID > sliceCnt {
			glog.Errorf("Node %s has invalid slice ID %d", k, v.SliceID)
		}

		if glog.V(4) {
			glog.Infof("Slice %s/%s ID %d requesting from %s", req.LOM.Bucket, req.LOM.Objname, v.SliceID, k)
		}
		// create SGL to receive the slice data and save it to correct
		// position in the slice list
		writer := &slice{
			sgl: mem2.NewSGL(cmn.KiB * 512),
			wg:  wgSlices,
			lom: req.LOM,
		}
		slices[v.SliceID-1] = writer
		idToNode[v.SliceID] = k
		wgSlices.Add(1)
		uname := unique(k, req.LOM.Bucket, req.LOM.Objname)
		if c.parent.regWriter(uname, writer) {
			daemons = append(daemons, k)
		}
	}

	iReq := c.parent.newIntraReq(reqGet, meta)
	iReq.IsSlice = true
	request, err := iReq.marshal()
	if err != nil {
		c.freeSGL(slices)
		return nil, nil, err
	}
	hdr := transport.Header{
		Bucket:  req.LOM.Bucket,
		Objname: req.LOM.Objname,
		Opaque:  request,
	}

	// broadcast slice request and wait for all targets respond
	if glog.V(4) {
		glog.Infof("Requesting daemons %v for slices of %s/%s", daemons, req.LOM.Bucket, req.LOM.Objname)
	}
	if err := c.parent.sendByDaemonID(daemons, hdr, nil, nil, true); err != nil {
		c.freeSGL(slices)
		return nil, nil, err
	}
	conf := cmn.GCO.Get()
	if wgSlices.WaitTimeout(conf.Timeout.SendFile) {
		glog.Errorf("Timed out waiting for %s/%s slices", req.LOM.Bucket, req.LOM.Objname)
	}
	return slices, idToNode, nil
}

// reconstruct the main object from slices, save it locally
// * req - original request
// * meta - rebuild metadata
// * slices - all slices received from targets
// * idToNode - remote location of the slices (SliceID <-> DaemonID)
// Returns:
// * list of created SGLs to be freed later
func (c *jogger) restoreMainObj(req *Request, meta *Metadata, slices []*slice, idToNode map[int]string) ([]*memsys.SGL, error) {
	sliceCnt := meta.Data + meta.Parity
	sliceSize := SliceSize(meta.Size, meta.Data)
	readers := make([]io.Reader, sliceCnt, sliceCnt)
	writers := make([]io.Writer, sliceCnt, sliceCnt)
	sgls := make([]*memsys.SGL, sliceCnt, sliceCnt)

	// allocate memory for reconstructed(missing) slices - EC requirement,
	// and open existing slices for reading
	for i, slice := range slices {
		if slice != nil && slice.sgl != nil && glog.V(4) {
			glog.Infof("Got slice %d size %d (want %d) of %s/%s",
				i+1, slice.sgl.Size(), sliceSize, req.LOM.Bucket, req.LOM.Objname)
		}
		if slice == nil || slice.sgl == nil {
			sgl := mem2.NewSGL(cmn.KiB * 512)
			sgls[i] = sgl
			writers[i] = sgl
			delete(idToNode, i+1)
		} else {
			readers[i] = memsys.NewReader(slice.sgl)
		}
	}

	// reconstruct the main object from slices
	if glog.V(4) {
		glog.Infof("Reconstructing %s/%s", req.LOM.Bucket, req.LOM.Objname)
	}
	stream, err := reedsolomon.NewStreamC(meta.Data, meta.Parity, true, true)
	if err != nil {
		return sgls, err
	}
	if err = stream.Reconstruct(readers, writers); err != nil {
		return sgls, err
	}

	srcReaders := make([]io.Reader, meta.Data, meta.Data)
	for i := 0; i < meta.Data; i++ {
		if slices[i] != nil && slices[i].sgl != nil && slices[i].sgl.Size() == sliceSize {
			srcReaders[i] = memsys.NewReader(slices[i].sgl)
		} else {
			srcReaders[i] = memsys.NewReader(sgls[i])
		}
	}

	src := io.MultiReader(srcReaders...)
	isLocal := req.LOM.Bislocal
	mainFQN, errstr := cluster.FQN(fs.ObjectType, req.LOM.Bucket, req.LOM.Objname, isLocal)
	if glog.V(4) {
		glog.Infof("Saving main object %s/%s to %q", req.LOM.Bucket, req.LOM.Objname, mainFQN)
	}
	if errstr != "" {
		return sgls, errors.New(errstr)
	}

	req.LOM.Fqn = mainFQN
	tmpFQN := fs.CSM.GenContentFQN(mainFQN, fs.WorkfileType, "ec")
	if err := cmn.SaveReaderSafe(tmpFQN, mainFQN, src, c.buffer, meta.Size); err != nil {
		return sgls, err
	}
	if errstr := req.LOM.Persist(); errstr != "" {
		return sgls, errors.New(errstr)
	}

	// save object's metadata locally
	mainMeta := *meta
	mainMeta.SliceID = 0
	metaBuf, err := mainMeta.marshal()
	if err != nil {
		return sgls, err
	}
	metaFQN := fs.CSM.GenContentFQN(mainFQN, MetaType, "")
	if glog.V(4) {
		glog.Infof("Saving main meta %s/%s to %q", req.LOM.Bucket, req.LOM.Objname, metaFQN)
	}
	if err := cmn.SaveReader(metaFQN, bytes.NewReader(metaBuf), c.buffer); err != nil {
		return sgls, err
	}

	return sgls, nil
}

// upload missing slices to targets that do not have any slice at the moment
// of reconstruction:
// * req - original request
// * meta - rebuilt object's metadata
// * sgls - object slices reconstructed by `restoreMainObj`
// * idToNode - a map of targets that already contain a slice (SliceID <-> target)
func (c *jogger) uploadRestoredSlices(req *Request, meta *Metadata, sgls []*memsys.SGL, idToNode map[int]string) error {
	sliceCnt := meta.Data + meta.Parity
	nodeToId := make(map[string]int, len(idToNode))
	emptyNodes := make([]string, 0)
	// transpose SliceID <-> DaemonID map for faster lookup
	for k, v := range idToNode {
		nodeToId[v] = k
	}

	// generate the list of targets that should have a slice and find out
	// the targets without any one
	targets, errstr := cluster.HrwTargetList(req.LOM.Bucket, req.LOM.Objname, c.parent.smap.Get(), sliceCnt+1)
	if errstr != "" {
		return errors.New(errstr)
	}
	for _, t := range targets {
		if t.DaemonID == c.parent.si.DaemonID {
			continue
		}
		if _, ok := nodeToId[t.DaemonID]; ok {
			continue
		}
		emptyNodes = append(emptyNodes, t.DaemonID)
	}
	if glog.V(4) {
		glog.Infof("Empty nodes for %s/%s are %#v", req.LOM.Bucket, req.LOM.Objname, emptyNodes)
	}

	// send reconstructed slices one by one to targets that are "empty".
	// Do not wait until the data transfer is completed
	idx := 0
	for _, tgt := range emptyNodes {
		for idx < len(sgls) && sgls[idx] == nil {
			idx++
		}
		if glog.V(4) {
			glog.Infof("For %s found %s/%s slice %d (%d)",
				tgt, req.LOM.Bucket, req.LOM.Objname, idx, len(sgls))
		}

		if idx >= len(sgls) {
			// unlikely but we need to free allocated memory for rest of slices
			glog.Errorf("Numbers of restored slices and empty targets mismatch")
			sgls[idx].Free()
			continue
		}

		// every slice's SGL must be freed on transfer completion
		cb := func(daemonID string, sgl *memsys.SGL, id int) transport.SendCallback {
			return func(hdr transport.Header, reader io.ReadCloser, err error) {
				if err != nil {
					glog.Errorf("Failed to send %s/%s to %v: %v", req.LOM.Bucket, req.LOM.Objname, daemonID, err)
				}
				if sgl != nil {
					sgl.Free()
				}
			}
		}(tgt, sgls[idx], idx+1)

		// clone the object's metadata and set the correct SliceID before sending
		sliceMeta := *meta
		sliceMeta.SliceID = idx + 1
		dataSrc := &dataSource{
			reader:   memsys.NewReader(sgls[idx]),
			size:     sgls[idx].Size(),
			metadata: &sliceMeta,
			isSlice:  true,
		}

		if glog.V(4) {
			glog.Infof("Sending slice %d %s/%s to %s", sliceMeta.SliceID+1, req.LOM.Bucket, req.LOM.Objname, tgt)
		}
		if err := c.parent.writeRemote([]string{tgt}, req.LOM, dataSrc, cb); err != nil {
			glog.Errorf("Failed to send slice %d of %s/%s to %s", idx+1, req.LOM.Bucket, req.LOM.Objname, tgt)
		}
	}

	return nil
}

// main function that starts restoring an object that was encoded
// * req - original request
// * meta - rebuild object's metadata
// * nodes - the list of targets that responded with valid metadata
func (c *jogger) restoreEncoded(req *Request, meta *Metadata, nodes map[string]*Metadata) error {
	if glog.V(4) {
		glog.Infof("Starting EC restore %s/%s", req.LOM.Bucket, req.LOM.Objname)
	}

	// unregister all SGLs from a list of waiting slices for the data to come
	freeWriters := func() {
		for k, _ := range nodes {
			uname := unique(k, req.LOM.Bucket, req.LOM.Objname)
			c.parent.unregWriter(uname)
		}
	}

	// download all slices from the targets that have sent metadata
	slices, idToNode, err := c.requestSlices(req, meta, nodes)
	if err != nil {
		freeWriters()
		return err
	}

	// restore and save locally the main replica
	sgls, err := c.restoreMainObj(req, meta, slices, idToNode)
	freeMem := func() {
		for _, sgl := range sgls {
			if sgl != nil {
				sgl.Free()
			}
		}
	}
	if err != nil {
		glog.Errorf("Failed to restore main object: %v", err)
		freeWriters()
		freeMem()
		c.freeSGL(slices)
		return err
	}

	// main replica is ready to download by a client.
	// Start a background process that uploads reconstructed data to
	// remote targets and then return from the function
	go func() {
		if err := c.uploadRestoredSlices(req, meta, sgls, idToNode); err != nil {
			glog.Errorf("Failed to restore slices of %s/%s: %v", req.LOM.Bucket, req.LOM.Objname, err)
			freeMem()
			c.freeSGL(slices)
			return
		}
		if glog.V(4) {
			glog.Infof("Slices %s/%s restored successfully", req.LOM.Bucket, req.LOM.Objname)
		}
	}()

	if glog.V(4) {
		glog.Infof("Main object %s/%s restored successfully", req.LOM.Bucket, req.LOM.Objname)
	}
	freeWriters()
	return nil
}

// Entry point: restores main objects and slices if possible
func (c *jogger) restore(req *Request) error {
	if req.LOM.Bprops == nil || !req.LOM.Bprops.ECEnabled {
		return ErrorECDisabled
	}

	if glog.V(4) {
		glog.Infof("Restoring %s/%s", req.LOM.Bucket, req.LOM.Objname)
	}
	meta, nodes, err := c.requestMeta(req)
	if glog.V(4) {
		glog.Infof("Find meta for %s/%s: %v, err: %v", req.LOM.Bucket, req.LOM.Objname, meta != nil, err)
	}
	if err != nil {
		return err
	}

	if meta.IsCopy {
		return c.restoreReplicated(req, meta, nodes)
	}

	if len(nodes) < meta.Data {
		return fmt.Errorf("Cannot restore: too many slices missing. Found %d slices, need %d or more", meta.Data, len(nodes))
	}
	return c.restoreEncoded(req, meta, nodes)
}

// a client has deleted the main object and requested to cleanup all its
// replicas and slices
// Just remove local metafile if it exists and broadcast the request to all
func (c *jogger) cleanup(req *Request) error {
	fqnMeta, errstr := cluster.FQN(MetaType, req.LOM.Bucket, req.LOM.Objname, req.LOM.Bislocal)
	if errstr != "" {
		glog.Errorf("Failed to get path for metadata of %s/%s: %v", req.LOM.Bucket, req.LOM.Objname, errstr)
		return nil
	}

	if err := os.RemoveAll(fqnMeta); err != nil {
		// logs the error but move on - notify all other target to do cleanup
		glog.Errorf("Error removing metafile %q", fqnMeta)
	}

	request, err := c.parent.newIntraReq(reqDel, nil).marshal()
	if err != nil {
		return err
	}
	hdr := transport.Header{
		Bucket:  req.LOM.Bucket,
		Objname: req.LOM.Objname,
		Opaque:  request,
		ObjAttrs: transport.ObjectAttrs{
			Size: 0,
		},
	}
	return c.parent.reqBundle.SendV(hdr, nil, nil)
}

// Sends object replicas to targets that must have replicas after the client
// uploads the main replica
func (c *jogger) createCopies(req *Request, metadata *Metadata) ([]*slice, error) {
	var (
		copies   = req.LOM.Bprops.ParitySlices
		replicas = make([]*slice, copies, copies)
	)

	// generate a list of target to send the replica (all excluding this one)
	targets, errstr := cluster.HrwTargetList(req.LOM.Bucket, req.LOM.Objname, c.parent.smap.Get(), copies+1)
	if errstr != "" {
		return replicas, errors.New(errstr)
	}
	targets = targets[1:]

	// Because object encoding is called after the main replica is saved to
	// disk it needs to read it from the local storage
	sgl, err := readFile(req.LOM.Fqn)
	if err != nil {
		return replicas, err
	}

	nodes := make([]string, len(targets), len(targets))
	for i, tgt := range targets {
		replicas[i] = &slice{}
		nodes[i] = tgt.DaemonID
	}

	// broadcast the replica to the targets
	cb := func(hdr transport.Header, reader io.ReadCloser, err error) {
		if err != nil {
			glog.Errorf("Failed to to %v: %v", nodes, err)
		}
		sgl.Free()
	}
	src := &dataSource{
		reader:   memsys.NewReader(sgl),
		size:     sgl.Size(),
		metadata: metadata,
	}
	err = c.parent.writeRemote(nodes, req.LOM, src, cb)

	return replicas, err
}

// broadcast request for object's metadata
// After all target respond, the metadata filtered: the number of different
// object hashes is calculated. The most frequent has wins, and all metadatas
// with different hashes are considered obsolete and will be discarded
func (c *jogger) requestMeta(req *Request) (meta *Metadata, nodes map[string]*Metadata, err error) {
	type nodeMeta struct {
		writer *slice
		id     string
	}
	metaWG := cmn.NewTimeoutGroup()
	request, _ := c.parent.newIntraReq(reqMeta, nil).marshal()
	hdr := transport.Header{
		Bucket:  req.LOM.Bucket,
		Objname: req.LOM.Objname,
		Opaque:  request,
		ObjAttrs: transport.ObjectAttrs{
			Size: 0,
		},
	}

	writers := make([]*slice, 0)

	tmap := c.parent.smap.Get().Tmap
	for _, node := range tmap {
		if node.DaemonID == c.parent.si.DaemonID {
			continue
		}

		writer := &slice{
			sgl: mem2.NewSGL(cmn.KiB),
			wg:  metaWG,
		}
		metaWG.Add(1)
		uname := unique(node.DaemonID, req.LOM.Bucket, req.LOM.Objname)
		if c.parent.regWriter(uname, writer) {
			writers = append(writers, writer)
		}
	}

	// broadcase the request to every target and wait for them to respond
	if err := c.parent.reqBundle.SendV(hdr, nil, nil); err != nil {
		glog.Errorf("Failed to request metafile for %s/%s: %v", req.LOM.Bucket, req.LOM.Objname, err)
		for _, wr := range writers {
			if wr != nil && wr.sgl != nil {
				wr.sgl.Free()
				wr.sgl = nil
			}
		}
		return nil, nil, err
	}

	conf := cmn.GCO.Get()
	if metaWG.WaitTimeout(conf.Timeout.SendFile) {
		glog.Errorf("Timed out waiting for %s/%s metafiles", req.LOM.Bucket, req.LOM.Objname)
	}

	// build the map of existing replicas: metas map (DaemonID <-> metadata)
	// and detect the metadata with the most frequent hash in it
	chk := make(map[string]int, len(tmap))
	metas := make(map[string]*Metadata, len(tmap))
	chkMax := 0
	chkVal := ""
	for _, node := range tmap {
		if node.DaemonID == c.parent.si.DaemonID {
			continue
		}

		uname := unique(node.DaemonID, req.LOM.Bucket, req.LOM.Objname)
		wr, ok := c.parent.unregWriter(uname)
		if !ok || wr == nil || wr.sgl == nil {
			continue
		}
		if wr.sgl.Size() == 0 {
			wr.sgl.Free()
			continue
		}

		b, err := ioutil.ReadAll(memsys.NewReader(wr.sgl))
		wr.sgl.Free()
		if err != nil {
			glog.Errorf("Error reading metadata from %s: %v", node.DaemonID, err)
			continue
		}

		var md Metadata
		if err := jsoniter.Unmarshal(b, &md); err != nil {
			glog.Errorf("Failed to unmarshal %s metadata: %v", node.DaemonID, err)
			continue
		}

		metas[node.DaemonID] = &md
		cnt, _ := chk[md.Checksum]
		cnt++
		chk[md.Checksum] = cnt

		if cnt > chkMax {
			chkMax = cnt
			chkVal = md.Checksum
		}
	}

	// no target returned its metadata
	if chkMax == 0 {
		return meta, nodes, ErrorNoMetafile
	}

	// cleanup: delete all metadatas that have "obsolete" information
	nodes = make(map[string]*Metadata)
	for k, v := range metas {
		if v.Checksum == chkVal {
			meta = v
			nodes[k] = v
		} else {
			glog.Warningf("Hashes of target %s[slice id %d] mismatch: %s == %s", k, v.SliceID, chkVal, v.Checksum)
		}
	}

	return meta, nodes, nil
}

// generateSlices gets FQN to the original file and encodes it into EC slices
// * fqn - the path to original object
// * dataSlices - the number of data slices
// * paritySlices - the number of parity slices
// Returns:
// * SGL that hold all the objects data
// * constructed from the main object slices
func generateSlices(fqn string, dataSlices, paritySlices int) (*memsys.SGL, []*slice, error) {
	var (
		totalCnt = paritySlices + dataSlices
		slices   = make([]*slice, totalCnt, totalCnt)
		sgl      *memsys.SGL
	)

	// read the object into memory
	fSrc, err := readFile(fqn)
	if err != nil {
		return sgl, slices, err
	}
	fileSize := fSrc.Size()

	sliceSize := SliceSize(fileSize, dataSlices)
	padSize := sliceSize*int64(dataSlices) - fileSize
	initSize := cmn.MinI64(sliceSize, cmn.MiB)

	// TODO: what if file is bigger than available memory?
	sgl = mem2.NewSGL(initSize)
	buf, slab := mem2.AllocFromSlab2(32 * cmn.KiB)
	if _, err = io.CopyBuffer(sgl, fSrc, buf); err != nil {
		fSrc.Close()
		slab.Free(buf)
		return sgl, slices, err
	}

	fSrc.Close()
	slab.Free(buf)
	// make the last slice the same size as the others by padding with 0's
	for padSize > 0 {
		byteCnt := cmn.Min(int(padSize), len(slicePadding))
		padding := slicePadding[:byteCnt]
		if _, err = sgl.Write(padding); err != nil {
			return sgl, slices, err
		}
		padSize -= int64(byteCnt)
	}

	// readers are slices of original object(no memory allocated),
	// writers are slices created by EC encoding process(memory is allocated)
	readers := make([]io.Reader, dataSlices, dataSlices)
	writers := make([]io.Writer, paritySlices, paritySlices)
	for i := 0; i < dataSlices; i++ {
		reader := memsys.NewSliceReader(sgl, int64(i)*sliceSize, sliceSize)
		slices[i] = &slice{reader: reader}
		readers[i] = reader
	}
	for i := 0; i < paritySlices; i++ {
		writer := mem2.NewSGL(initSize)
		slices[i+dataSlices] = &slice{sgl: writer}
		writers[i] = writer
	}

	stream, err := reedsolomon.NewStreamC(dataSlices, paritySlices, true, true)
	if err != nil {
		return sgl, slices, err
	}

	err = stream.Encode(readers, writers)
	return sgl, slices, err
}

// copies the constructed EC slices to remote targets
// * req - original request
// * meta - EC metadata
// Returns:
// * list of all slices, sent to targets
func (c *jogger) sendSlices(req *Request, meta *Metadata) ([]*slice, error) {
	totalCnt := req.LOM.Bprops.ParitySlices + req.LOM.Bprops.DataSlices

	// totalCnt+1: first node gets the full object, other totalCnt nodes
	// gets a slice each
	targets, errstr := cluster.HrwTargetList(req.LOM.Bucket, req.LOM.Objname, c.parent.smap.Get(), totalCnt+1)
	if errstr != "" {
		return nil, errors.New(errstr)
	}

	// load the data slices from original object and construct parity ones
	sgl, slices, err := generateSlices(req.LOM.Fqn, req.LOM.Bprops.DataSlices, req.LOM.Bprops.ParitySlices)
	if err != nil {
		if sgl != nil {
			sgl.Free()
		}
		c.freeSGL(slices)
		return nil, err
	}

	wg := sync.WaitGroup{}
	ch := make(chan error, totalCnt)
	objSGL := &slice{cnt: int32(req.LOM.Bprops.DataSlices), sgl: sgl}
	sliceSize := SliceSize(req.LOM.Size, req.LOM.Bprops.DataSlices)

	// transfer a slice to remote target
	// If the slice is data one - no immediate cleanup is required because this
	// slice is just a reader of global SGL for the entire file (that is why a
	// counter is used here)
	copySlice := func(i int) {
		defer wg.Done()

		var sglSlice *slice
		if i < req.LOM.Bprops.DataSlices {
			// the slice is just a reader that does not allocate new memory
			sglSlice = objSGL
		} else {
			// the slice uses its own SGL, so the counter is 1
			sglSlice = &slice{cnt: 1, sgl: slices[i].sgl}
		}

		// In case of data slice, reopen its reader, because it was read
		// to the end by erasure encoding while calculating parity slices
		var reader cmn.ReadOpenCloser
		if slices[i].reader != nil {
			slices[i].reader.Seek(0, io.SeekStart)
			reader = slices[i].reader
		} else {
			reader = memsys.NewReader(slices[i].sgl)
		}

		mcopy := *meta
		mcopy.SliceID = i + 1
		src := &dataSource{
			reader:   reader,
			size:     sliceSize,
			obj:      sglSlice,
			metadata: &mcopy,
			isSlice:  true,
		}
		err := c.parent.writeRemote([]string{targets[i+1].DaemonID}, req.LOM, src, nil)
		if err != nil {
			ch <- err
			return
		}
	}

	for i := 0; i < totalCnt; i++ {
		wg.Add(1)
		go copySlice(i)
	}

	wg.Wait()
	close(ch)

	if err, ok := <-ch; ok {
		glog.Errorf("Error while copying %d slices (with %d parity) for %q: %v",
			req.LOM.Bprops.DataSlices, req.LOM.Bprops.ParitySlices, req.LOM.Fqn, err)
	} else if glog.V(4) {
		glog.Infof("EC created %d slices (with %d parity) for %q: %v",
			req.LOM.Bprops.DataSlices, req.LOM.Bprops.ParitySlices, req.LOM.Fqn, err)
	}

	return slices, nil
}
