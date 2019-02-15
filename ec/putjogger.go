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

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/klauspost/reedsolomon"
)

// a mountpath putJogger: processes PUT/DEL requests to one mountpath
type putJogger struct {
	parent *XactEC
	slab   *memsys.Slab2
	buffer []byte
	mpath  string // mountpath that the jogger manages

	workCh chan *Request // channel to request TOP priority operation (restore)
	stopCh chan struct{} // jogger management channel: to stop it

	toDisk bool // use files or SGL
}

func (c *putJogger) run() {
	glog.Infof("Started EC for mountpath: %s", c.mpath)
	c.buffer, c.slab = mem2.AllocFromSlab2(cmn.MiB)

	for {
		select {
		case req := <-c.workCh:
			c.parent.stats.updateWaitTime(time.Since(req.tm))
			memRequired := req.LOM.Size *
				int64(req.LOM.Bprops.DataSlices+req.LOM.Bprops.ParitySlices) /
				int64(req.LOM.Bprops.ParitySlices)
			c.toDisk = useDisk(memRequired)
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

func (c *putJogger) stop() {
	glog.Infof("Stopping EC for mountpath: %s", c.mpath)
	c.stopCh <- struct{}{}
	close(c.stopCh)
}

// starts EC process
func (c *putJogger) ec(req *Request) {
	var (
		err error
		act = "encoding"
	)

	switch req.Action {
	case ActSplit:
		err = c.encode(req)
		c.parent.stats.updateEncodeTime(time.Since(req.tm), err != nil)
	case ActDelete:
		err = c.cleanup(req)
		act = "cleaning up"
		c.parent.stats.updateDeleteTime(time.Since(req.tm), err != nil)
	default:
		err = fmt.Errorf("invalid EC action for putJogger: %v", req.Action)
	}

	if err != nil {
		glog.Errorf("Error occurred during %s object [%s/%s], fqn: %q, err: %v",
			act, req.LOM.Bucket, req.LOM.Objname, req.LOM.FQN, err)
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
func (c *putJogger) freeSGL(slices []*slice) {
	for _, s := range slices {
		if s != nil {
			s.free()
		}
	}
}

// calculates and stores data and parity slices
func (c *putJogger) encode(req *Request) error {
	if glog.V(4) {
		glog.Infof("Encoding %q...", req.LOM.FQN)
	}
	_, cksumValue := req.LOM.Cksum.Get()
	meta := &Metadata{
		Size:     req.LOM.Size,
		Data:     req.LOM.Bprops.DataSlices,
		Parity:   req.LOM.Bprops.ParitySlices,
		IsCopy:   req.IsCopy,
		Checksum: cksumValue,
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
		return fmt.Errorf("object %s/%s requires %d targets to encode, only %d found",
			req.LOM.Bucket, req.LOM.Objname, reqTargets, targetCnt)
	}

	metabuf, err := meta.marshal()
	if err != nil {
		return err
	}

	// Save metadata before encoding the object
	metaFQN, errstr := cluster.FQN(MetaType, req.LOM.Bucket, req.LOM.Objname, req.LOM.BckIsLocal)
	if errstr != "" {
		return errors.New(errstr)
	}
	if err := cmn.SaveReader(metaFQN, bytes.NewReader(metabuf), c.buffer); err != nil {
		return err
	}

	// if an object is small just make `parity` copies
	if meta.IsCopy {
		if err := c.createCopies(req, meta); err != nil {
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

// a client has deleted the main object and requested to cleanup all its
// replicas and slices
// Just remove local metafile if it exists and broadcast the request to all
func (c *putJogger) cleanup(req *Request) error {
	fqnMeta, errstr := cluster.FQN(MetaType, req.LOM.Bucket, req.LOM.Objname, req.LOM.BckIsLocal)
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
func (c *putJogger) createCopies(req *Request, metadata *Metadata) error {
	var (
		copies = req.LOM.Bprops.ParitySlices
	)

	// generate a list of target to send the replica (all excluding this one)
	targets, errstr := cluster.HrwTargetList(req.LOM.Bucket, req.LOM.Objname, c.parent.smap.Get(), copies+1)
	if errstr != "" {
		return errors.New(errstr)
	}
	targets = targets[1:]

	// Because object encoding is called after the main replica is saved to
	// disk it needs to read it from the local storage
	fh, err := cmn.NewFileHandle(req.LOM.FQN)
	if err != nil {
		return err
	}

	nodes := make([]string, 0, len(targets))
	for _, tgt := range targets {
		nodes = append(nodes, tgt.DaemonID)
	}

	// broadcast the replica to the targets
	cb := func(hdr transport.Header, reader io.ReadCloser, err error) {
		if err != nil {
			glog.Errorf("Failed to to %v: %v", nodes, err)
		}
	}
	src := &dataSource{
		reader:   fh,
		size:     req.LOM.Size,
		metadata: metadata,
	}
	err = c.parent.writeRemote(nodes, req.LOM, src, cb)

	return err
}

// generateSlicesToMemory gets FQN to the original file and encodes it into EC slices
// * fqn - the path to original object
// * dataSlices - the number of data slices
// * paritySlices - the number of parity slices
// Returns:
// * SGL that hold all the objects data
// * constructed from the main object slices
func generateSlicesToMemory(fqn string, dataSlices, paritySlices int) (cmn.ReadOpenCloser, []*slice, error) {
	var (
		totalCnt = paritySlices + dataSlices
		slices   = make([]*slice, totalCnt)
		sgl      *memsys.SGL
	)

	// read the object into memory
	sgl, err := readFile(fqn)
	if err != nil {
		return sgl, slices, err
	}
	fileSize := sgl.Size()

	sliceSize := SliceSize(fileSize, dataSlices)
	padSize := sliceSize*int64(dataSlices) - fileSize
	initSize := cmn.MinI64(sliceSize, cmn.MiB)

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
	readers := make([]io.Reader, dataSlices)
	writers := make([]io.Writer, paritySlices)
	for i := 0; i < dataSlices; i++ {
		reader := memsys.NewSliceReader(sgl, int64(i)*sliceSize, sliceSize)
		slices[i] = &slice{reader: reader}
		readers[i] = reader
	}
	for i := 0; i < paritySlices; i++ {
		writer := mem2.NewSGL(initSize)
		slices[i+dataSlices] = &slice{obj: writer}
		writers[i] = writer
	}

	stream, err := reedsolomon.NewStreamC(dataSlices, paritySlices, true, true)
	if err != nil {
		return sgl, slices, err
	}

	err = stream.Encode(readers, writers)
	return sgl, slices, err
}

// generateSlicesToDisk gets FQN to the original file and encodes it into EC slices
// * fqn - the path to original object
// * dataSlices - the number of data slices
// * paritySlices - the number of parity slices
// Returns:
// * Main object file handle
// * constructed from the main object slices
func generateSlicesToDisk(fqn string, dataSlices, paritySlices int) (cmn.ReadOpenCloser, []*slice, error) {
	var (
		totalCnt = paritySlices + dataSlices
		slices   = make([]*slice, totalCnt)
		fh       *cmn.FileHandle
	)

	stat, err := os.Stat(fqn)
	if err != nil {
		return fh, slices, err
	}
	fileSize := stat.Size()

	fh, err = cmn.NewFileHandle(fqn)
	if err != nil {
		return fh, slices, err
	}

	sliceSize := SliceSize(fileSize, dataSlices)
	padSize := sliceSize*int64(dataSlices) - fileSize

	// readers are slices of original object(no memory allocated),
	// writers are slices created by EC encoding process
	readers := make([]io.Reader, dataSlices)
	writers := make([]io.Writer, paritySlices)
	sizeLeft := fileSize
	for i := 0; i < dataSlices; i++ {
		var (
			reader cmn.ReadOpenCloser
			err    error
		)
		if sizeLeft < sliceSize {
			reader, err = cmn.NewFileSectionHandle(fh, int64(i)*sliceSize, sizeLeft, padSize)
		} else {
			reader, err = cmn.NewFileSectionHandle(fh, int64(i)*sliceSize, sliceSize, 0)
		}
		if err != nil {
			return fh, slices, err
		}
		slices[i] = &slice{obj: fh, reader: reader}
		readers[i] = reader
		sizeLeft -= sliceSize
	}
	for i := 0; i < paritySlices; i++ {
		workFQN := fs.CSM.GenContentFQN(fqn, fs.WorkfileType, fmt.Sprintf("ec-write-%d", i))
		writer, err := cmn.CreateFile(workFQN)
		if err != nil {
			return fh, slices, err
		}
		slices[i+dataSlices] = &slice{writer: writer, workFQN: workFQN}
		writers[i] = writer
	}

	stream, err := reedsolomon.NewStreamC(dataSlices, paritySlices, true, true)
	if err != nil {
		return fh, slices, err
	}

	err = stream.Encode(readers, writers)
	for _, wr := range writers {
		// writer can be only *os.File within this function
		f, ok := wr.(*os.File)
		cmn.Assert(ok)
		f.Close()
	}
	return fh, slices, err
}

// copies the constructed EC slices to remote targets
// * req - original request
// * meta - EC metadata
// Returns:
// * list of all slices, sent to targets
func (c *putJogger) sendSlices(req *Request, meta *Metadata) ([]*slice, error) {
	totalCnt := req.LOM.Bprops.ParitySlices + req.LOM.Bprops.DataSlices

	// totalCnt+1: first node gets the full object, other totalCnt nodes
	// gets a slice each
	targets, errstr := cluster.HrwTargetList(req.LOM.Bucket, req.LOM.Objname, c.parent.smap.Get(), totalCnt+1)
	if errstr != "" {
		return nil, errors.New(errstr)
	}

	// load the data slices from original object and construct parity ones
	var (
		objReader cmn.ReadOpenCloser
		slices    []*slice
		err       error
	)
	if c.toDisk {
		objReader, slices, err = generateSlicesToDisk(req.LOM.FQN, req.LOM.Bprops.DataSlices, req.LOM.Bprops.ParitySlices)
	} else {
		objReader, slices, err = generateSlicesToMemory(req.LOM.FQN, req.LOM.Bprops.DataSlices, req.LOM.Bprops.ParitySlices)
	}
	if err != nil {
		freeObject(objReader)
		c.freeSGL(slices)
		return nil, err
	}

	wg := sync.WaitGroup{}
	ch := make(chan error, totalCnt)
	mainObj := &slice{refCnt: int32(req.LOM.Bprops.DataSlices), obj: objReader}
	sliceSize := SliceSize(req.LOM.Size, req.LOM.Bprops.DataSlices)

	// transfer a slice to remote target
	// If the slice is data one - no immediate cleanup is required because this
	// slice is just a reader of global SGL for the entire file (that is why a
	// counter is used here)
	copySlice := func(i int) {
		defer wg.Done()

		var data *slice
		if i < req.LOM.Bprops.DataSlices {
			// the slice is just a reader that does not allocate new memory
			data = mainObj
		} else {
			// the slice uses its own SGL, so the counter is 1
			data = &slice{refCnt: 1, obj: slices[i].obj, workFQN: slices[i].workFQN}
		}

		// In case of data slice, reopen its reader, because it was read
		// to the end by erasure encoding while calculating parity slices
		var (
			reader cmn.ReadOpenCloser
			err    error
		)
		if slices[i].reader != nil {
			reader = slices[i].reader
			if sgl, ok := reader.(*memsys.SliceReader); ok {
				_, err = sgl.Seek(0, io.SeekStart)
			} else if sgl, ok := reader.(*memsys.Reader); ok {
				_, err = sgl.Seek(0, io.SeekStart)
			} else if f, ok := reader.(*cmn.FileSectionHandle); ok {
				_, err = f.Open()
			} else {
				cmn.AssertFmt(false, "unsupported reader type", reader)
			}
		} else {
			if sgl, ok := slices[i].obj.(*memsys.SGL); ok {
				reader = memsys.NewReader(sgl)
			} else if slices[i].workFQN != "" {
				reader, err = cmn.NewFileHandle(slices[i].workFQN)
			} else {
				cmn.AssertFmt(false, "unsupported reader type", slices[i].obj)
			}
		}
		if err != nil {
			ch <- fmt.Errorf("failed to reset reader: %v", err)
			return
		}

		mcopy := *meta
		mcopy.SliceID = i + 1
		src := &dataSource{
			reader:   reader,
			size:     sliceSize,
			obj:      data,
			metadata: &mcopy,
			isSlice:  true,
		}
		err = c.parent.writeRemote([]string{targets[i+1].DaemonID}, req.LOM, src, nil)
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
			req.LOM.Bprops.DataSlices, req.LOM.Bprops.ParitySlices, req.LOM.FQN, err)
	} else if glog.V(4) {
		glog.Infof("EC created %d slices (with %d parity) for %q: %v",
			req.LOM.Bprops.DataSlices, req.LOM.Bprops.ParitySlices, req.LOM.FQN, err)
	}

	return slices, nil
}
