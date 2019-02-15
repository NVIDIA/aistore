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

// a mountpath getJogger: processes GET requests to one mountpath
type getJogger struct {
	parent *XactEC
	mpath  string // mountpath that the jogger manages

	workCh chan *Request // channel to request TOP priority operation (restore)
	stopCh chan struct{} // jogger management channel: to stop it

	jobID  uint64
	jobs   map[uint64]bgProcess
	jobMtx sync.Mutex
	sema   chan struct{}
	diskCh chan struct{}
}

func (c *getJogger) run() {
	glog.Infof("Started EC for mountpath: %s", c.mpath)

	for {
		select {
		case req := <-c.workCh:
			c.parent.stats.updateWaitTime(time.Since(req.tm))
			req.tm = time.Now()
			c.ec(req)
			c.parent.DecPending()
		case <-c.stopCh:
			return
		}
	}
}

func (c *getJogger) stop() {
	glog.Infof("Stopping EC for mountpath: %s", c.mpath)
	c.stopCh <- struct{}{}
	close(c.stopCh)
}

// starts EC process
func (c *getJogger) ec(req *Request) {
	switch req.Action {
	case ActRestore:
		c.sema <- struct{}{}
		toDisk := useDisk(0 /*size of the original object is unknown*/)
		c.jobID++
		jobID := c.jobID
		ch := req.ErrCh
		cb := func(err error) {
			c.jobMtx.Lock()
			delete(c.jobs, jobID)
			c.jobMtx.Unlock()
			if ch != nil {
				ch <- err
				close(ch)
			}
		}
		restore := func(req *Request, toDisk bool, buffer []byte, cb func(error)) {
			err := c.restore(req, toDisk, buffer)
			c.parent.stats.updateDecodeTime(time.Since(req.tm), err != nil)
			if cb != nil {
				cb(err)
			}
			if err == nil {
				c.parent.stats.updateObjTime(time.Since(req.putTime))
			}
			<-c.sema
		}
		c.jobMtx.Lock()
		c.jobs[jobID] = restore
		c.jobMtx.Unlock()
		buffer, slab := mem2.AllocFromSlab2(cmn.MiB)
		go func() {
			restore(req, toDisk, buffer, cb)
			slab.Free(buffer)
		}()
	default:
		err := fmt.Errorf("invalid EC action for getJogger: %v", req.Action)
		glog.Errorf("Error occurred during restoring object [%s/%s], fqn: %q, err: %v",
			req.LOM.Bucket, req.LOM.Objname, req.LOM.FQN, err)
		if req.ErrCh != nil {
			req.ErrCh <- err
			close(req.ErrCh)
		}
	}
}

// the final step of replica restoration process: the main target detects which
// nodes do not have replicas and copy it to them
// * bucket/objname - object path
// * reader - replica content to sent to remote targets
// * metadata - object's EC metadata
// * nodes - targets that have metadata and replica - filled by requestMeta
// * replicaCnt - total number of replicas including main one
func (c *getJogger) copyMissingReplicas(lom *cluster.LOM, reader cmn.ReadOpenCloser, metadata *Metadata, nodes map[string]*Metadata, replicaCnt int) {
	targets, errstr := cluster.HrwTargetList(lom.Bucket, lom.Objname, c.parent.smap.Get(), replicaCnt)
	if errstr != "" {
		freeObject(reader)
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
	// memory on completion
	// Otherwise just free allocated memory and return immediately
	if len(daemons) == 0 {
		freeObject(reader)
		return
	}
	var (
		srcReader cmn.ReadOpenCloser
		err       error
	)

	if sgl, ok := reader.(*memsys.SGL); ok {
		srcReader = memsys.NewReader(sgl)
	} else if _, ok := reader.(*cmn.FileHandle); ok {
		srcReader, err = cmn.NewFileHandle(lom.FQN)
	} else {
		cmn.AssertFmt(false, "unsupported reader type", reader)
	}

	if err != nil {
		glog.Error(err)
		freeObject(reader)
		return
	}
	cb := func(hdr transport.Header, reader io.ReadCloser, err error) {
		if err != nil {
			glog.Errorf("Failed to send %s/%s to %v: %v", lom.Bucket, lom.Objname, daemons, err)
		}
		freeObject(reader)
	}

	src := &dataSource{
		reader:   srcReader,
		size:     lom.Size,
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
func (c *getJogger) restoreReplicatedFromMemory(req *Request, meta *Metadata, nodes map[string]*Metadata, buffer []byte) error {
	var writer *memsys.SGL
	// try read a replica from targets one by one until the replica is got
	for node := range nodes {
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
		return errors.New("failed to read a replica from any target")
	}

	// Save received replica and its metadata locally - it is main replica
	objFQN, errstr := cluster.FQN(fs.ObjectType, req.LOM.Bucket, req.LOM.Objname, req.LOM.BckIsLocal)
	if errstr != "" {
		writer.Free()
		return errors.New(errstr)
	}
	req.LOM.FQN = objFQN
	tmpFQN := fs.CSM.GenContentFQN(objFQN, fs.WorkfileType, "ec")
	if err := cmn.SaveReaderSafe(tmpFQN, objFQN, memsys.NewReader(writer), buffer); err != nil {
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
	if err := cmn.SaveReader(metaFQN, bytes.NewReader(b), buffer); err != nil {
		writer.Free()
		<-c.diskCh
		return err
	}

	// now a client can read the object, but EC needs to restore missing
	// replicas. So, execute copying replicas in background and return
	go c.copyMissingReplicas(req.LOM, writer, meta, nodes, meta.Parity+1)

	return nil
}

func (c *getJogger) restoreReplicatedFromDisk(req *Request, meta *Metadata, nodes map[string]*Metadata, buffer []byte) error {
	var writer *os.File
	// try read a replica from targets one by one until the replica is got
	objFQN, errstr := cluster.FQN(fs.ObjectType, req.LOM.Bucket, req.LOM.Objname, req.LOM.BckIsLocal)
	if errstr != "" {
		return errors.New(errstr)
	}
	tmpFQN := fs.CSM.GenContentFQN(objFQN, fs.WorkfileType, "ec-restore-repl")

	for node := range nodes {
		uname := unique(node, req.LOM.Bucket, req.LOM.Objname)
		iReqBuf, err := c.parent.newIntraReq(reqGet, nil).marshal()
		if err != nil {
			glog.Errorf("Failed to marshal %v", err)
			continue
		}

		w, err := cmn.CreateFile(tmpFQN)
		if err != nil {
			glog.Errorf("Failed to create file: %v", err)
			break
		}
		req.LOM.FQN = tmpFQN
		err = c.parent.readRemote(req.LOM, node, uname, iReqBuf, w)
		w.Close()

		if err == nil && req.LOM.Size != 0 {
			// a valid replica is found - break and do not free SGL
			writer = w
			break
		}

		os.RemoveAll(tmpFQN)
	}
	if glog.V(4) {
		glog.Infof("Found meta -> obj get %s/%s, writer found: %v", req.LOM.Bucket, req.LOM.Objname, writer != nil)
	}

	if writer == nil {
		return errors.New("failed to read a replica from any target")
	}
	req.LOM.FQN = objFQN
	if err := cmn.MvFile(tmpFQN, objFQN); err != nil {
		return err
	}

	if errstr := req.LOM.Persist(); errstr != "" {
		return errors.New(errstr)
	}

	b, err := jsoniter.Marshal(meta)
	if err != nil {
		return err
	}
	metaFQN := fs.CSM.GenContentFQN(objFQN, MetaType, "")
	if err := cmn.SaveReader(metaFQN, bytes.NewReader(b), buffer); err != nil {
		<-c.diskCh
		return err
	}

	// now a client can read the object, but EC needs to restore missing
	// replicas. So, execute copying replicas in background and return
	reader, err := cmn.NewFileHandle(objFQN)
	if err != nil {
		return err
	}
	go c.copyMissingReplicas(req.LOM, reader, meta, nodes, meta.Parity+1)

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
func (c *getJogger) requestSlices(req *Request, meta *Metadata, nodes map[string]*Metadata, toDisk bool) ([]*slice, map[int]string, error) {
	wgSlices := cmn.NewTimeoutGroup()
	sliceCnt := meta.Data + meta.Parity
	slices := make([]*slice, sliceCnt)
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
		var writer *slice
		if toDisk {
			prefix := fmt.Sprintf("ec-restore-%d", v.SliceID)
			fqn := fs.CSM.GenContentFQN(req.LOM.FQN, fs.WorkfileType, prefix)
			fh, err := cmn.CreateFile(fqn)
			if err != nil {
				return slices, nil, err
			}
			writer = &slice{
				writer:  fh,
				wg:      wgSlices,
				lom:     req.LOM,
				workFQN: fqn,
			}
		} else {
			writer = &slice{
				writer: mem2.NewSGL(cmn.KiB * 512),
				wg:     wgSlices,
				lom:    req.LOM,
			}
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
		freeSlices(slices)
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
		freeSlices(slices)
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
func (c *getJogger) restoreMainObj(req *Request, meta *Metadata, slices []*slice, idToNode map[int]string, toDisk bool, buffer []byte) ([]*slice, error) {
	var err error
	sliceCnt := meta.Data + meta.Parity
	sliceSize := SliceSize(meta.Size, meta.Data)
	readers := make([]io.Reader, sliceCnt)
	writers := make([]io.Writer, sliceCnt)
	restored := make([]*slice, sliceCnt)

	// allocate memory for reconstructed(missing) slices - EC requirement,
	// and open existing slices for reading
	for i, sl := range slices {
		if sl != nil && sl.writer != nil {
			sz := sl.n
			if glog.V(4) {
				glog.Infof("Got slice %d size %d (want %d) of %s/%s",
					i+1, sz, sliceSize, req.LOM.Bucket, req.LOM.Objname)
			}
			if sz == 0 {
				freeObject(sl.obj)
				sl.obj = nil
				freeObject(sl.writer)
				sl.writer = nil
			}
		}
		if sl == nil || sl.writer == nil {
			if toDisk {
				prefix := fmt.Sprintf("ec-rebuild-%d", i)
				fqn := fs.CSM.GenContentFQN(req.LOM.FQN, fs.WorkfileType, prefix)
				file, err := cmn.CreateFile(fqn)
				if err != nil {
					break
				}
				writers[i] = file
				restored[i] = &slice{workFQN: fqn, n: sliceSize}
			} else {
				sgl := mem2.NewSGL(cmn.KiB * 512)
				restored[i] = &slice{obj: sgl, n: sliceSize}
				writers[i] = sgl
			}
			delete(idToNode, i+1)
		} else {
			if sgl, ok := sl.writer.(*memsys.SGL); ok {
				readers[i] = memsys.NewReader(sgl)
			} else if sl.workFQN != "" {
				readers[i], err = cmn.NewFileHandle(sl.workFQN)
				if err != nil {
					break
				}
			} else {
				err = fmt.Errorf("Unsupported slice source: %T", sl.writer)
				break
			}
		}
	}

	if err != nil {
		return restored, err
	}

	// reconstruct the main object from slices
	if glog.V(4) {
		glog.Infof("Reconstructing %s/%s", req.LOM.Bucket, req.LOM.Objname)
	}
	stream, err := reedsolomon.NewStreamC(meta.Data, meta.Parity, true, true)
	if err != nil {
		return restored, err
	}
	if err = stream.Reconstruct(readers, writers); err != nil {
		return restored, err
	}

	srcReaders := make([]io.Reader, meta.Data)
	for i := 0; i < meta.Data; i++ {
		if slices[i] != nil && slices[i].writer != nil {
			if sgl, ok := slices[i].writer.(*memsys.SGL); ok {
				srcReaders[i] = memsys.NewReader(sgl)
			} else if slices[i].workFQN != "" {
				srcReaders[i], err = cmn.NewFileHandle(slices[i].workFQN)
				if err != nil {
					return restored, err
				}
			} else {
				return restored, fmt.Errorf("invalid writer: %T", slices[i].writer)
			}
		} else {
			if restored[i].workFQN != "" {
				srcReaders[i], err = cmn.NewFileHandle(restored[i].workFQN)
				if err != nil {
					return restored, err
				}
			} else if sgl, ok := restored[i].obj.(*memsys.SGL); ok {
				srcReaders[i] = memsys.NewReader(sgl)
			} else {
				return restored, fmt.Errorf("empty slice %d of %s/%s", i, req.LOM.Bucket, req.LOM.Objname)
			}
		}
	}

	src := io.MultiReader(srcReaders...)
	mainFQN, errstr := cluster.FQN(fs.ObjectType, req.LOM.Bucket, req.LOM.Objname, req.LOM.BckIsLocal)
	if glog.V(4) {
		glog.Infof("Saving main object %s/%s to %q", req.LOM.Bucket, req.LOM.Objname, mainFQN)
	}
	if errstr != "" {
		return restored, errors.New(errstr)
	}

	c.diskCh <- struct{}{}
	req.LOM.FQN = mainFQN
	tmpFQN := fs.CSM.GenContentFQN(mainFQN, fs.WorkfileType, "ec")
	if err := cmn.SaveReaderSafe(tmpFQN, mainFQN, src, buffer, meta.Size); err != nil {
		<-c.diskCh
		return restored, err
	}
	<-c.diskCh
	if errstr := req.LOM.Persist(); errstr != "" {
		return restored, errors.New(errstr)
	}

	// save object's metadata locally
	mainMeta := *meta
	mainMeta.SliceID = 0
	metaBuf, err := mainMeta.marshal()
	if err != nil {
		return restored, err
	}
	metaFQN := fs.CSM.GenContentFQN(mainFQN, MetaType, "")
	if glog.V(4) {
		glog.Infof("Saving main meta %s/%s to %q", req.LOM.Bucket, req.LOM.Objname, metaFQN)
	}
	if err := cmn.SaveReader(metaFQN, bytes.NewReader(metaBuf), buffer); err != nil {
		return restored, err
	}

	return restored, nil
}

// upload missing slices to targets that do not have any slice at the moment
// of reconstruction:
// * req - original request
// * meta - rebuilt object's metadata
// * slices - object slices reconstructed by `restoreMainObj`
// * idToNode - a map of targets that already contain a slice (SliceID <-> target)
func (c *getJogger) uploadRestoredSlices(req *Request, meta *Metadata, slices []*slice, idToNode map[int]string) error {
	sliceCnt := meta.Data + meta.Parity
	nodeToID := make(map[string]int, len(idToNode))
	// transpose SliceID <-> DaemonID map for faster lookup
	for k, v := range idToNode {
		nodeToID[v] = k
	}

	// generate the list of targets that should have a slice and find out
	// the targets without any one
	targets, errstr := cluster.HrwTargetList(req.LOM.Bucket, req.LOM.Objname, c.parent.smap.Get(), sliceCnt+1)
	if errstr != "" {
		return errors.New(errstr)
	}
	emptyNodes := make([]string, 0, len(targets))
	for _, t := range targets {
		if t.DaemonID == c.parent.si.DaemonID {
			continue
		}
		if _, ok := nodeToID[t.DaemonID]; ok {
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
		for idx < len(slices) && slices[idx] == nil {
			idx++
		}
		if glog.V(4) {
			glog.Infof("For %s found %s/%s slice %d (%d)",
				tgt, req.LOM.Bucket, req.LOM.Objname, idx, len(slices))
		}

		if idx >= len(slices) {
			// unlikely but we need to free allocated memory for rest of slices
			glog.Errorf("Numbers of restored slices and empty targets mismatch")
			slices[idx].free()
			continue
		}

		// every slice's SGL must be freed on transfer completion
		cb := func(daemonID string, s *slice, id int) transport.SendCallback {
			return func(hdr transport.Header, reader io.ReadCloser, err error) {
				if err != nil {
					glog.Errorf("Failed to send %s/%s to %v: %v", req.LOM.Bucket, req.LOM.Objname, daemonID, err)
				}
				if s != nil {
					s.free()
				}
			}
		}(tgt, slices[idx], idx+1)

		// clone the object's metadata and set the correct SliceID before sending
		sliceMeta := *meta
		sliceMeta.SliceID = idx + 1
		var reader cmn.ReadOpenCloser
		if slices[idx].workFQN != "" {
			reader, _ = cmn.NewFileHandle(slices[idx].workFQN)
		} else {
			if s, ok := slices[idx].obj.(*memsys.SGL); ok {
				reader = memsys.NewReader(s)
			} else {
				glog.Errorf("Invalid reader type of %s: %T", req.LOM.Objname, slices[idx].obj)
				continue
			}
		}
		dataSrc := &dataSource{
			reader:   reader,
			size:     slices[idx].n,
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
func (c *getJogger) restoreEncoded(req *Request, meta *Metadata, nodes map[string]*Metadata, toDisk bool, buffer []byte) error {
	if glog.V(4) {
		glog.Infof("Starting EC restore %s/%s", req.LOM.Bucket, req.LOM.Objname)
	}

	// unregister all SGLs from a list of waiting slices for the data to come
	freeWriters := func() {
		for k := range nodes {
			uname := unique(k, req.LOM.Bucket, req.LOM.Objname)
			c.parent.unregWriter(uname)
		}
	}

	// download all slices from the targets that have sent metadata
	slices, idToNode, err := c.requestSlices(req, meta, nodes, toDisk)
	if err != nil {
		freeWriters()
		return err
	}

	// restore and save locally the main replica
	restored, err := c.restoreMainObj(req, meta, slices, idToNode, toDisk, buffer)
	if err != nil {
		glog.Errorf("Failed to restore main object %s/%s: %v",
			req.LOM.Bucket, req.LOM.Objname, err)
		freeWriters()
		freeSlices(restored)
		freeSlices(slices)
		return err
	}

	// main replica is ready to download by a client.
	// Start a background process that uploads reconstructed data to
	// remote targets and then return from the function
	go func() {
		if err := c.uploadRestoredSlices(req, meta, restored, idToNode); err != nil {
			glog.Errorf("Failed to restore slices of %s/%s: %v", req.LOM.Bucket, req.LOM.Objname, err)
			freeSlices(restored)
			freeSlices(slices)
			return
		}
		// do not free `restored` here - it is done in transport callback when
		// transport completes sending restored slices to correct target
		freeSlices(slices)
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
func (c *getJogger) restore(req *Request, toDisk bool, buffer []byte) error {
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
		if toDisk {
			return c.restoreReplicatedFromDisk(req, meta, nodes, buffer)
		}
		return c.restoreReplicatedFromMemory(req, meta, nodes, buffer)
	}

	if len(nodes) < meta.Data {
		return fmt.Errorf("cannot restore: too many slices missing (found %d slices, need %d or more)", meta.Data, len(nodes))
	}
	return c.restoreEncoded(req, meta, nodes, toDisk, buffer)
}

// broadcast request for object's metadata
// After all target respond, the metadata filtered: the number of different
// object hashes is calculated. The most frequent has wins, and all metadatas
// with different hashes are considered obsolete and will be discarded
//
// NOTE: metafiles are tiny things - less than 100 bytes, that is why they
// always use SGL. No `toDisk` check required
func (c *getJogger) requestMeta(req *Request) (meta *Metadata, nodes map[string]*Metadata, err error) {
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
			writer: mem2.NewSGL(cmn.KiB),
			wg:     metaWG,
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
			if wr != nil && wr.writer != nil {
				freeObject(wr.writer)
				wr.writer = nil
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
		if !ok || wr == nil || wr.writer == nil {
			continue
		}
		sgl, ok := wr.writer.(*memsys.SGL)
		cmn.Assert(ok)
		if sgl.Size() == 0 {
			freeObject(wr.writer)
			continue
		}

		b, err := ioutil.ReadAll(memsys.NewReader(sgl))
		freeObject(wr.writer)
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
		cnt := chk[md.Checksum]
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
