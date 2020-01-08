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
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/OneOfOne/xxhash"
	"github.com/klauspost/reedsolomon"
)

// a mountpath getJogger: processes GET requests to one mountpath
type getJogger struct {
	parent *XactGet
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
	glog.Infof("Started EC for mountpath: %s, bucket %s", c.mpath, c.parent.bckName)

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
	glog.Infof("Stopping EC for mountpath: %s, bucket: %s", c.mpath, c.parent.bckName)
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
		buffer, slab := mm.AllocDefault()
		go func() {
			restore(req, toDisk, buffer, cb)
			slab.Free(buffer)
		}()
	default:
		err := fmt.Errorf("invalid EC action for getJogger: %v", req.Action)
		glog.Errorf("Error restoring object [%s/%s], fqn: %q, err: %v",
			req.LOM.Bucket(), req.LOM.Objname, req.LOM.FQN, err)
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
	targets, err := cluster.HrwTargetList(lom.Uname(), c.parent.smap.Get(), replicaCnt)
	if err != nil {
		freeObject(reader)
		glog.Errorf("failed to get list of %d targets: %s", replicaCnt, err)
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
	)

	switch r := reader.(type) {
	case *memsys.SGL:
		srcReader = memsys.NewReader(r)
	case *cmn.FileHandle:
		srcReader, err = cmn.NewFileHandle(lom.FQN)
	default:
		cmn.AssertFmt(false, "unsupported reader type", reader)
	}

	if err != nil {
		glog.Error(err)
		freeObject(reader)
		return
	}

	// _ io.ReadCloser: pass copyMisssingReplicas reader argument(memsys.SGL type)
	// instead of callback's reader argument(memsys.Reader type) to freeObject
	// Reason: memsys.Reader does not provide access to internal memsys.SGL that must be freed
	cb := func(hdr transport.Header, _ io.ReadCloser, _ unsafe.Pointer, err error) {
		if err != nil {
			glog.Errorf("Failed to send %s/%s to %v: %v", lom.Bucket(), lom.Objname, daemons, err)
		}
		freeObject(reader)
	}

	src := &dataSource{
		reader:   srcReader,
		size:     lom.Size(),
		metadata: metadata,
		reqType:  ReqPut,
	}
	if err := c.parent.writeRemote(daemons, lom, src, cb); err != nil {
		glog.Errorf("Failed to copy replica %s/%s to %v: %v", lom.Bucket(), lom.Objname, daemons, err)
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
		uname := unique(node, req.LOM.Bck(), req.LOM.Objname)
		iReqBuf := c.parent.newIntraReq(reqGet, nil).Marshal()

		w := mm.NewSGL(cmn.KiB)
		if _, err := c.parent.readRemote(req.LOM, node, uname, iReqBuf, w); err != nil {
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
		glog.Infof("Found meta -> obj get %s/%s, writer found: %v", req.LOM.Bucket(), req.LOM.Objname, writer != nil)
	}

	if writer == nil {
		return errors.New("failed to read a replica from any target")
	}

	// Save received replica and its metadata locally - it is main replica
	objFQN := req.LOM.FQN
	req.LOM.FQN = objFQN
	req.LOM.SetSize(writer.Size())
	tmpFQN := fs.CSM.GenContentFQN(objFQN, fs.WorkfileType, "ec")
	if _, err := cmn.SaveReaderSafe(tmpFQN, objFQN, memsys.NewReader(writer), buffer, false); err != nil {
		writer.Free()
		return err
	}

	if err := req.LOM.Persist(); err != nil {
		writer.Free()
		return err
	}

	b := cmn.MustMarshal(meta)
	metaFQN := fs.CSM.GenContentFQN(objFQN, MetaType, "")
	if _, err := cmn.SaveReader(metaFQN, bytes.NewReader(b), buffer, false); err != nil {
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
	var (
		writer *os.File
		n      int64
	)
	// try read a replica from targets one by one until the replica is got
	objFQN := req.LOM.FQN
	tmpFQN := fs.CSM.GenContentFQN(objFQN, fs.WorkfileType, "ec-restore-repl")

	for node := range nodes {
		uname := unique(node, req.LOM.Bck(), req.LOM.Objname)
		iReqBuf := c.parent.newIntraReq(reqGet, nil).Marshal()

		w, err := cmn.CreateFile(tmpFQN)
		if err != nil {
			glog.Errorf("Failed to create file: %v", err)
			break
		}
		req.LOM.FQN = tmpFQN
		n, err = c.parent.readRemote(req.LOM, node, uname, iReqBuf, w)
		w.Close()

		if err == nil && n != 0 {
			// a valid replica is found - break and do not free SGL
			req.LOM.SetSize(n)
			writer = w
			break
		}

		os.RemoveAll(tmpFQN)
	}
	if glog.V(4) {
		glog.Infof("Found meta -> obj get %s/%s, writer found: %v", req.LOM.Bucket(), req.LOM.Objname, writer != nil)
	}

	if writer == nil {
		return errors.New("failed to read a replica from any target")
	}
	req.LOM.FQN = objFQN
	if err := cmn.Rename(tmpFQN, objFQN); err != nil {
		return err
	}

	if err := req.LOM.Persist(); err != nil {
		return err
	}

	b := cmn.MustMarshal(meta)
	metaFQN := fs.CSM.GenContentFQN(objFQN, MetaType, "")
	if _, err := cmn.SaveReader(metaFQN, bytes.NewReader(b), buffer, false); err != nil {
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
			glog.Infof("Slice %s/%s ID %d requesting from %s", req.LOM.Bucket(), req.LOM.Objname, v.SliceID, k)
		}
		// create SGL to receive the slice data and save it to correct
		// position in the slice list
		var writer *slice
		var lom = *(req.LOM)
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
				lom:     &lom,
				workFQN: fqn,
			}
		} else {
			writer = &slice{
				writer: mm.NewSGL(cmn.KiB * 512),
				wg:     wgSlices,
				lom:    &lom,
			}
		}
		slices[v.SliceID-1] = writer
		idToNode[v.SliceID] = k
		wgSlices.Add(1)
		uname := unique(k, req.LOM.Bck(), req.LOM.Objname)
		if c.parent.regWriter(uname, writer) {
			daemons = append(daemons, k)
		}
	}

	iReq := c.parent.newIntraReq(reqGet, meta)
	iReq.IsSlice = true
	request := iReq.Marshal()
	hdr := transport.Header{
		Bucket:   req.LOM.Bucket(),
		BckIsAIS: req.LOM.IsAIS(),
		Objname:  req.LOM.Objname,
		Opaque:   request,
	}

	// broadcast slice request and wait for all targets respond
	if glog.V(4) {
		glog.Infof("Requesting daemons %v for slices of %s/%s", daemons, req.LOM.Bucket(), req.LOM.Objname)
	}
	if err := c.parent.sendByDaemonID(daemons, hdr, nil, nil, true); err != nil {
		freeSlices(slices)
		return nil, nil, err
	}
	conf := cmn.GCO.Get()
	if wgSlices.WaitTimeout(conf.Timeout.SendFile) {
		glog.Errorf("Timed out waiting for %s/%s slices", req.LOM.Bucket(), req.LOM.Objname)
	}
	return slices, idToNode, nil
}

func noSliceWriter(req *Request, writers []io.Writer, restored []*slice,
	hashes []*xxhash.XXHash64, idToNode map[int]string, toDisk bool,
	id int, sliceSize int64) error {
	if toDisk {
		prefix := fmt.Sprintf("ec-rebuild-%d", id)
		fqn := fs.CSM.GenContentFQN(req.LOM.FQN, fs.WorkfileType, prefix)
		file, err := cmn.CreateFile(fqn)
		if err != nil {
			return err
		}
		hashes[id] = xxhash.New64()
		writers[id] = io.MultiWriter(file, hashes[id])
		restored[id] = &slice{workFQN: fqn, n: sliceSize}
	} else {
		sgl := mm.NewSGL(sliceSize)
		restored[id] = &slice{obj: sgl, n: sliceSize}
		hashes[id] = xxhash.New64()
		writers[id] = io.MultiWriter(sgl, hashes[id])
	}

	// id from slices object differs from id of idToNode object
	delete(idToNode, id+1)

	return nil
}

func checkSliceChecksum(reader io.Reader, recvCksm *cmn.Cksum, wg *sync.WaitGroup, errCh chan int, i int, sliceSize int64) {
	defer wg.Done()

	if kind, _ := recvCksm.Get(); kind == cmn.ChecksumNone {
		glog.Errorf("Checksum of a slice is of type %s", cmn.ChecksumNone)
		return
	}

	buf, slab := mm.AllocForSize(sliceSize)
	actualCksm, err := cmn.ComputeXXHash(reader, buf)
	slab.Free(buf)

	if err != nil {
		glog.Errorf("Couldn't compute hash of a slice: %s", err)
		errCh <- i
		return
	}

	if !cmn.EqCksum(cmn.NewCksum(cmn.ChecksumXXHash, actualCksm), recvCksm) {
		glog.Errorf("Checksum of slice does not match. Got: %s, expected: %s", recvCksm.String(), cmn.NewCksum(cmn.ChecksumXXHash, actualCksm).String())
		errCh <- i
	}
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
	hashes := make([]*xxhash.XXHash64, sliceCnt)

	cksmWg := &sync.WaitGroup{}
	cksmErrCh := make(chan int, sliceCnt)

	// allocate memory for reconstructed(missing) slices - EC requirement,
	// and open existing slices for reading
	for i, sl := range slices {
		if sl != nil && sl.writer != nil {
			sz := sl.n
			if glog.V(4) {
				glog.Infof("Got slice %d size %d (want %d) of %s/%s",
					i+1, sz, sliceSize, req.LOM.Bucket(), req.LOM.Objname)
			}
			if sz == 0 {
				freeObject(sl.obj)
				sl.obj = nil
				freeObject(sl.writer)
				sl.writer = nil
			}
		}
		if sl == nil || sl.writer == nil {
			if err = noSliceWriter(req, writers, restored, hashes, idToNode, toDisk, i, sliceSize); err != nil {
				break
			}
		} else {
			var cksmReader io.Reader
			if sgl, ok := sl.writer.(*memsys.SGL); ok {
				readers[i] = memsys.NewReader(sgl)
				cksmReader = memsys.NewReader(sgl)
			} else if sl.workFQN != "" {
				readers[i], err = cmn.NewFileHandle(sl.workFQN)
				cksmReader, _ = cmn.NewFileHandle(sl.workFQN)
				if err != nil {
					break
				}
			} else {
				err = fmt.Errorf("unsupported slice source: %T", sl.writer)
				break
			}

			cksmWg.Add(1)
			go checkSliceChecksum(cksmReader, sl.cksum, cksmWg, cksmErrCh, i, sliceSize)
		}
	}

	if err != nil {
		return restored, err
	}

	// reconstruct the main object from slices
	if glog.V(4) {
		glog.Infof("Reconstructing %s/%s", req.LOM.Bucket(), req.LOM.Objname)
	}
	stream, err := reedsolomon.NewStreamC(meta.Data, meta.Parity, true, true)
	if err != nil {
		return restored, err
	}

	// Wait for checksum checks to complete
	cksmWg.Wait()
	close(cksmErrCh)

	for i := range cksmErrCh {
		// slice's checksum did not match, however we might be able to restore object anyway
		glog.Warningf("Slice checksum mismatch for %s", req.LOM.Objname)
		if err := noSliceWriter(req, writers, restored, hashes, idToNode, toDisk, i, sliceSize); err != nil {
			return restored, err
		}
		readers[i] = nil
	}

	if err := stream.Reconstruct(readers, writers); err != nil {
		return restored, err
	}

	version := ""
	for idx, rst := range restored {
		if rst == nil {
			continue
		}
		if hashes[idx] != nil {
			rst.cksum = cmn.NewCksum(cmn.ChecksumXXHash, cmn.HashToStr(hashes[idx]))
		}
		if version == "" && rst.version != "" {
			version = rst.version
		}
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
				return restored, fmt.Errorf("empty slice %d of %s/%s", i, req.LOM.Bucket(), req.LOM.Objname)
			}
		}
	}

	src := io.MultiReader(srcReaders...)
	mainFQN := req.LOM.FQN
	if glog.V(4) {
		glog.Infof("Saving main object %s/%s to %q", req.LOM.Bucket(), req.LOM.Objname, mainFQN)
	}

	c.diskCh <- struct{}{}
	req.LOM.FQN = mainFQN
	req.LOM.SetSize(meta.Size)
	if version != "" {
		req.LOM.SetVersion(version)
	}
	tmpFQN := fs.CSM.GenContentFQN(mainFQN, fs.WorkfileType, "ec")
	// recalculate hash for the main object before saving the object's xattrs
	// otherwise the main object gets hash from one of slices
	cksum, err := cmn.SaveReaderSafe(tmpFQN, mainFQN, src, buffer, true, meta.Size)
	if err != nil {
		<-c.diskCh
		return restored, err
	}
	<-c.diskCh
	req.LOM.SetCksum(cksum)
	// Persist called without a lock. It's not a problem, as the LOM should not be used as the object is missing
	if err := req.LOM.Persist(); err != nil {
		return restored, err
	}

	// save object's metadata locally
	mainMeta := *meta
	mainMeta.SliceID = 0
	metaBuf := mainMeta.marshal()
	metaSize := len(metaBuf)

	metaFQN := fs.CSM.GenContentFQN(mainFQN, MetaType, "")
	if glog.V(4) {
		glog.Infof("Saving main meta %s/%s to %q", req.LOM.Bucket(), req.LOM.Objname, metaFQN)
	}

	if _, err := cmn.SaveReader(metaFQN, bytes.NewReader(metaBuf), buffer, false); err != nil {
		return restored, err
	}

	// FIXME: slice meta file should be a different kind of LOM
	metaLom := &cluster.LOM{T: c.parent.t, FQN: metaFQN}
	if err := metaLom.Init(req.LOM.Bucket(), cmn.ProviderFromBool(req.LOM.IsAIS())); err != nil {
		return restored, err
	}

	metaLom.SetSize(int64(metaSize))
	if err := metaLom.Persist(); err != nil {
		return restored, err
	}

	req.LOM.ReCache()
	return restored, nil
}

// *slices - slices to search through
// *start - id which search should start from
// Returns:
// slice or nil if not found
// first index after found slice
func getNextNonEmptySlice(slices []*slice, start int) (*slice, int) {
	i := cmn.Max(0, start)

	for i < len(slices) && slices[i] == nil {
		i++
	}

	if i == len(slices) {
		return nil, i
	}

	return slices[i], i + 1
}

// upload missing slices to targets that do not have any slice at the moment
// of reconstruction:
// * req - original request
// * meta - rebuilt object's metadata
// * slices - object slices reconstructed by `restoreMainObj`
// * idToNode - a map of targets that already contain a slice (SliceID <-> target)
func (c *getJogger) uploadRestoredSlices(req *Request, meta *Metadata, slices []*slice, idToNode map[int]string) {
	sliceCnt := meta.Data + meta.Parity
	nodeToID := make(map[string]int, len(idToNode))
	// transpose SliceID <-> DaemonID map for faster lookup
	for k, v := range idToNode {
		nodeToID[v] = k
	}

	// generate the list of targets that should have a slice and find out
	// the targets without any one
	// FIXME: when fewer targets than sliceCnt+1, send slices to those available anyway
	targets, err := cluster.HrwTargetList(req.LOM.Uname(), c.parent.smap.Get(), sliceCnt+1)
	if err != nil {
		glog.Warning(err)
		return
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
		glog.Infof("Empty nodes for %s/%s are %#v", req.LOM.Bucket(), req.LOM.Objname, emptyNodes)
	}

	// send reconstructed slices one by one to targets that are "empty".
	// Do not wait until the data transfer is completed
	idx := 0
	for _, tgt := range emptyNodes {
		// get next non-empty slice
		sl, nextIdx := getNextNonEmptySlice(slices, idx)

		if glog.V(4) {
			glog.Infof("For %s found %s/%s slice %d (%d)",
				tgt, req.LOM.Bucket(), req.LOM.Objname, idx, len(slices))
		}

		if sl == nil {
			// The number of empty nodes is larger than non-empty slices
			// There's nothing to be done for next nodes, safe to break a loop
			glog.Errorf("Numbers of restored slices is smaller than numer of empty targets")
			break
		}

		// every slice's SGL must be freed on transfer completion
		cb := func(daemonID string, s *slice) transport.SendCallback {
			return func(hdr transport.Header, reader io.ReadCloser, _ unsafe.Pointer, err error) {
				if err != nil {
					glog.Errorf("Failed to send %s/%s to %v: %v", req.LOM.Bucket(), req.LOM.Objname, daemonID, err)
				}
				if s != nil {
					s.free()
				}
			}
		}(tgt, sl)

		// clone the object's metadata and set the correct SliceID before sending
		sliceMeta := *meta
		sliceMeta.SliceID = idx + 1
		var reader cmn.ReadOpenCloser
		if sl.workFQN != "" {
			reader, _ = cmn.NewFileHandle(sl.workFQN)
		} else {
			if s, ok := sl.obj.(*memsys.SGL); ok {
				reader = memsys.NewReader(s)
			} else {
				glog.Errorf("Invalid reader type of %s: %T", req.LOM.Objname, sl.obj)
				continue
			}
		}
		dataSrc := &dataSource{
			reader:   reader,
			size:     sl.n,
			metadata: &sliceMeta,
			isSlice:  true,
			reqType:  ReqPut,
		}

		if glog.V(4) {
			glog.Infof("Sending slice %d %s/%s to %s", sliceMeta.SliceID+1, req.LOM.Bucket(), req.LOM.Objname, tgt)
		}
		if sl.cksum != nil {
			sliceMeta.CksumType, sliceMeta.CksumValue = sl.cksum.Get()
		}
		if err := c.parent.writeRemote([]string{tgt}, req.LOM, dataSrc, cb); err != nil {
			glog.Errorf("Failed to send slice %d of %s/%s to %s", idx+1, req.LOM.Bucket(), req.LOM.Objname, tgt)
		}

		idx = nextIdx
	}

	sl, idx := getNextNonEmptySlice(slices, idx)
	if sl != nil {
		glog.Errorf("Number of restored slices is greater than number of empty targets")
	}
	for sl != nil {
		// Free allocated memory of additional slices
		sl.free()
		sl, idx = getNextNonEmptySlice(slices, idx)
	}
}

// main function that starts restoring an object that was encoded
// * req - original request
// * meta - rebuild object's metadata
// * nodes - the list of targets that responded with valid metadata
func (c *getJogger) restoreEncoded(req *Request, meta *Metadata, nodes map[string]*Metadata, toDisk bool, buffer []byte) error {
	if glog.V(4) {
		glog.Infof("Starting EC restore %s/%s", req.LOM.Bucket(), req.LOM.Objname)
	}

	// unregister all SGLs from a list of waiting slices for the data to come
	freeWriters := func() {
		for k := range nodes {
			uname := unique(k, req.LOM.Bck(), req.LOM.Objname)
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
			req.LOM.Bucket(), req.LOM.Objname, err)
		freeWriters()
		freeSlices(restored)
		freeSlices(slices)
		return err
	}

	c.parent.ObjectsInc()
	c.parent.BytesAdd(req.LOM.Size())

	// main replica is ready to download by a client.
	// Start a background process that uploads reconstructed data to
	// remote targets and then return from the function
	go func() {
		c.uploadRestoredSlices(req, meta, restored, idToNode)

		// do not free `restored` here - it is done in transport callback when
		// transport completes sending restored slices to correct target
		freeSlices(slices)
		if glog.V(4) {
			glog.Infof("Slices %s/%s restored successfully", req.LOM.Bucket(), req.LOM.Objname)
		}
	}()

	if glog.V(4) {
		glog.Infof("Main object %s/%s restored successfully", req.LOM.Bucket(), req.LOM.Objname)
	}
	freeWriters()
	return nil
}

// Entry point: restores main objects and slices if possible
func (c *getJogger) restore(req *Request, toDisk bool, buffer []byte) error {
	if req.LOM.Bprops() == nil || !req.LOM.Bprops().EC.Enabled {
		return ErrorECDisabled
	}

	if glog.V(4) {
		glog.Infof("Restoring %s/%s", req.LOM.Bucket(), req.LOM.Objname)
	}
	meta, nodes, err := c.requestMeta(req)
	if glog.V(4) {
		glog.Infof("Find meta for %s/%s: %v, err: %v", req.LOM.Bucket(), req.LOM.Objname, meta != nil, err)
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

// broadcast request for object's metadata. The function returns the list of
// nodes(with their EC metadata) that have the lastest object version
func (c *getJogger) requestMeta(req *Request) (meta *Metadata, nodes map[string]*Metadata, err error) {
	tmap := c.parent.smap.Get().Tmap
	wg := &sync.WaitGroup{}
	mtx := &sync.Mutex{}
	metas := make(map[string]*Metadata, len(tmap))
	chk := make(map[string]int, len(tmap))
	chkMax := 0
	chkVal := ""
	for _, node := range tmap {
		if node.DaemonID == c.parent.si.DaemonID {
			continue
		}
		wg.Add(1)
		go func(si *cluster.Snode) {
			defer wg.Done()
			md, err := RequestECMeta(req.LOM.Bucket(), req.LOM.Objname, req.LOM.Bck().Provider, si)
			if err != nil {
				if glog.FastV(4, glog.SmoduleAIS) {
					glog.Infof("No EC meta %s from %s: %v", req.LOM.Objname, si.Name(), err)
				}
				return
			}

			mtx.Lock()
			metas[si.DaemonID] = md
			// detect the metadata with the latest version on the fly.
			// At this moment it is the most frequent hash in the list.
			// TODO: fix when an EC Metadata versioning is introduced
			cnt := chk[md.ObjCksum]
			cnt++
			chk[md.ObjCksum] = cnt
			if cnt > chkMax {
				chkMax = cnt
				chkVal = md.ObjCksum
			}
			mtx.Unlock()
		}(node)
	}
	wg.Wait()

	// no target has object's metadata
	if len(metas) == 0 {
		return meta, nodes, ErrorNoMetafile
	}

	// cleanup: delete all metadatas that have "obsolete" information
	nodes = make(map[string]*Metadata)
	for k, v := range metas {
		if v.ObjCksum == chkVal {
			meta = v
			nodes[k] = v
		} else {
			glog.Warningf("Hashes of target %s[slice id %d] mismatch: %s == %s", k, v.SliceID, chkVal, v.ObjCksum)
		}
	}

	return meta, nodes, nil
}
