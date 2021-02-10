// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/klauspost/reedsolomon"
)

// to avoid starving ecencode xaction, allow to run ecencode after every put batch
const putBatchSize = 8

type (
	encodeCtx struct {
		lom          *cluster.LOM     // replica
		meta         *Metadata        //
		fh           *cmn.FileHandle  // file handle for the replica
		slices       []*slice         // all EC slices
		sliceSize    int64            // calculated slice size
		padSize      int64            // zero tail of the last object's data slice
		dataSlices   int              // the number of data slices
		paritySlices int              // the number of parity slices
		cksums       []*cmn.CksumHash // checksums of parity slices (filled by reed-solomon)
	}

	// a mountpath putJogger: processes PUT/DEL requests to one mountpath
	putJogger struct {
		parent *XactPut
		slab   *memsys.Slab
		buffer []byte
		mpath  string

		putCh  chan *Request // top priority operation (object PUT)
		xactCh chan *Request // low priority operation (ec-encode)
		stopCh chan struct{} // jogger management channel: to stop it

		toDisk bool // use files or SGL
	}
)

var (
	encCtxPool         sync.Pool
	emptyCtx           encodeCtx
	errSliceSendFailed = errors.New("failed to send slice")
)

func newCtx(lom *cluster.LOM, meta *Metadata) (ctx *encodeCtx, err error) {
	ctx = allocCtx()
	ctx.lom = lom
	ctx.dataSlices = lom.Bprops().EC.DataSlices
	ctx.paritySlices = lom.Bprops().EC.ParitySlices
	ctx.meta = meta

	totalCnt := ctx.paritySlices + ctx.dataSlices
	ctx.sliceSize = SliceSize(ctx.lom.Size(), ctx.dataSlices)
	ctx.slices = make([]*slice, totalCnt)
	ctx.padSize = ctx.sliceSize*int64(ctx.dataSlices) - ctx.lom.Size()

	ctx.fh, err = cmn.NewFileHandle(lom.FQN)
	return ctx, err
}

func allocCtx() (ctx *encodeCtx) {
	if v := encCtxPool.Get(); v != nil {
		ctx = v.(*encodeCtx)
	} else {
		ctx = &encodeCtx{}
	}
	return
}

func freeCtx(ctx *encodeCtx) {
	*ctx = emptyCtx
	encCtxPool.Put(ctx)
}

func (ctx *encodeCtx) freeReplica() {
	freeObject(ctx.fh)
}

func (c *putJogger) freeResources() {
	c.slab.Free(c.buffer)
	c.buffer = nil
	c.slab = nil
}

func (c *putJogger) processRequest(req *Request) {
	var memRequired int64
	lom, err := req.LIF.LOM(c.parent.t.Bowner().Get())
	defer cluster.FreeLOM(lom)
	if err != nil {
		return
	}
	if req.Action == ActSplit {
		if err = lom.Load(); err != nil {
			return
		}
		ecConf := lom.Bprops().EC
		memRequired = lom.Size() * int64(ecConf.DataSlices+ecConf.ParitySlices) / int64(ecConf.ParitySlices)
		c.toDisk = useDisk(memRequired)
	}

	c.parent.stats.updateWaitTime(time.Since(req.tm))
	req.tm = time.Now()
	if err = c.ec(req, lom); err != nil {
		glog.Errorf("Failed to %s object %s (fqn: %q, err: %v)", req.Action, lom, lom.FQN, err)
	}
	// In case of everything is OK, a transport bundle calls `DecPending`
	// on finishing transferring all the data
	if err != nil || req.Action == ActDelete {
		c.parent.DecPending()
	}
	if req.Callback != nil {
		req.Callback(lom, err)
	}
}

func (c *putJogger) run() {
	glog.Infof("Started EC for mountpath: %s, bucket %s", c.mpath, c.parent.bck)
	c.buffer, c.slab = mm.Alloc()
	putsDone := 0

	for {
		// first, process requests with high priority
		select {
		case req := <-c.putCh:
			c.processRequest(req)
			// repeat in case of more objects in the HIGH-priority queue
			putsDone++
			if putsDone < putBatchSize {
				continue
			}
		case <-c.stopCh:
			c.freeResources()
			return
		default:
		}

		putsDone = 0
		// process all other requests
		select {
		case req := <-c.putCh:
			c.processRequest(req)
		case req := <-c.xactCh:
			c.processRequest(req)
		case <-c.stopCh:
			c.freeResources()
			return
		}
	}
}

func (c *putJogger) stop() {
	glog.Infof("Stopping EC for mountpath: %s, bucket %s", c.mpath, c.parent.bck)
	c.stopCh <- struct{}{}
	close(c.stopCh)
}

func (c *putJogger) ec(req *Request, lom *cluster.LOM) (err error) {
	switch req.Action {
	case ActSplit:
		err = c.encode(req, lom)
		c.parent.stats.updateEncodeTime(time.Since(req.tm), err != nil)
	case ActDelete:
		err = c.cleanup(lom)
		c.parent.stats.updateDeleteTime(time.Since(req.tm), err != nil)
	default:
		err = fmt.Errorf("invalid EC action for putJogger: %v", req.Action)
	}

	if req.ErrCh != nil {
		req.ErrCh <- err
		close(req.ErrCh)
	}
	if err == nil {
		c.parent.stats.updateObjTime(time.Since(req.putTime))
	}
	return err
}

func (c *putJogger) replicate(ctx *encodeCtx) error {
	err := c.createCopies(ctx)
	if err != nil {
		ctx.freeReplica()
		c.cleanup(ctx.lom)
	}
	return err
}

func (c *putJogger) splitAndDistribute(ctx *encodeCtx) error {
	err := initializeSlices(ctx)
	if err == nil {
		err = c.sendSlices(ctx)
	}
	if err != nil {
		ctx.freeReplica()
		if err != errSliceSendFailed {
			freeSlices(ctx.slices)
		}
		c.cleanup(ctx.lom)
	}
	return err
}

// calculates and stores data and parity slices
func (c *putJogger) encode(req *Request, lom *cluster.LOM) error {
	var (
		cksumValue, cksumType string
		ecConf                = lom.Bprops().EC
	)
	if glog.FastV(4, glog.SmoduleEC) {
		glog.Infof("Encoding %q...", lom.FQN)
	}
	if lom.Cksum() != nil {
		cksumType, cksumValue = lom.Cksum().Get()
	}
	meta := &Metadata{
		Size:      lom.Size(),
		Data:      ecConf.DataSlices,
		Parity:    ecConf.ParitySlices,
		IsCopy:    req.IsCopy,
		ObjCksum:  cksumValue,
		CksumType: cksumType,
	}

	// calculate the number of targets required to encode the object
	// For replicated: ParitySlices + original object
	// For encoded: ParitySlices + DataSlices + original object
	reqTargets := ecConf.ParitySlices + 1
	if !req.IsCopy {
		reqTargets += ecConf.DataSlices
	}
	targetCnt := len(c.parent.smap.Get().Tmap)
	if targetCnt < reqTargets {
		return fmt.Errorf("object %s requires %d targets to encode, only %d found",
			lom, reqTargets, targetCnt)
	}

	// Save metadata before encoding the object
	ctMeta := cluster.NewCTFromLOM(lom, MetaType)
	metaBuf := bytes.NewReader(meta.Marshal())
	if err := ctMeta.Write(c.parent.t, metaBuf, -1); err != nil {
		return err
	}

	c.parent.ObjectsInc()
	c.parent.BytesAdd(lom.Size())

	ctx, err := newCtx(lom, meta)
	defer freeCtx(ctx)
	if err != nil {
		return err
	}

	// if an object is small just make `parity` copies
	if meta.IsCopy {
		return c.replicate(ctx)
	}

	return c.splitAndDistribute(ctx)
}

func (c *putJogger) ctSendCallback(hdr transport.ObjHdr, _ io.ReadCloser, _ unsafe.Pointer, err error) {
	c.parent.t.SmallMMSA().Free(hdr.Opaque)
	if err != nil {
		glog.Errorf("failed to send o[%s/%s], err: %v", hdr.Bck, hdr.ObjName, err)
	}
}

func (c *putJogger) replicaSendCallback(hdr transport.ObjHdr, reader io.ReadCloser, _ unsafe.Pointer, err error) {
	if err != nil {
		glog.Errorf("Failed to send %s/%s replica: %v", hdr.Bck, hdr.ObjName, err)
	}
	c.parent.DecPending()
}

// Remove slices and replicas across the cluster: remove local metafile
// if exists and broadcast the request to other targets
func (c *putJogger) cleanup(lom *cluster.LOM) error {
	ctMeta := cluster.NewCTFromLOM(lom, MetaType)
	if err := cmn.RemoveFile(ctMeta.FQN()); err != nil {
		// logs the error but move on - notify all other target to do cleanup
		glog.Errorf("Error removing metafile %q", ctMeta.FQN())
	}

	mm := c.parent.t.SmallMMSA()
	request := c.parent.newIntraReq(reqDel, nil, lom.Bck()).NewPack(mm)
	o := transport.AllocSend()
	o.Hdr = transport.ObjHdr{Bck: lom.Bucket(), ObjName: lom.ObjName, Opaque: request}
	o.Callback = c.ctSendCallback
	return c.parent.mgr.req().Send(o, nil)
}

// Sends object replicas to targets that must have replicas after the client
// uploads the main replica
func (c *putJogger) createCopies(ctx *encodeCtx) error {
	// generate a list of target to send the replica (all excluding this one)
	targets, err := cluster.HrwTargetList(ctx.lom.Uname(), c.parent.smap.Get(), ctx.paritySlices+1)
	if err != nil {
		return err
	}
	targets = targets[1:]

	nodes := make([]string, 0, len(targets))
	for _, tgt := range targets {
		nodes = append(nodes, tgt.ID())
	}

	// broadcast the replica to the targets
	src := &dataSource{
		reader:   ctx.fh,
		size:     ctx.lom.Size(),
		metadata: ctx.meta,
		reqType:  reqPut,
	}
	return c.parent.writeRemote(nodes, ctx.lom, src, c.replicaSendCallback)
}

// Fills slices with calculated checksums, reports errors to error channel
func checksumDataSlices(ctx *encodeCtx, cksmReaders []io.Reader, cksumType string) error {
	buf, slab := mm.Alloc(ctx.sliceSize)
	defer slab.Free(buf)
	for i, reader := range cksmReaders {
		_, cksum, err := cmn.CopyAndChecksum(ioutil.Discard, reader, buf, cksumType)
		if err != nil {
			return err
		}
		ctx.slices[i].cksum = cksum.Clone()
	}
	return nil
}

// generateSlicesToMemory gets FQN to the original file and encodes it into EC slices
func generateSlicesToMemory(ctx *encodeCtx) error {
	// writers are slices created by EC encoding process(memory is allocated)
	conf := ctx.lom.CksumConf()
	initSize := cmn.MinI64(ctx.sliceSize, cmn.MiB)
	sliceWriters := make([]io.Writer, ctx.paritySlices)
	for i := 0; i < ctx.paritySlices; i++ {
		writer := mm.NewSGL(initSize)
		ctx.slices[i+ctx.dataSlices] = &slice{obj: writer}
		if conf.Type == cmn.ChecksumNone {
			sliceWriters[i] = writer
		} else {
			ctx.cksums[i] = cmn.NewCksumHash(conf.Type)
			sliceWriters[i] = cmn.NewWriterMulti(writer, ctx.cksums[i].H)
		}
	}

	return finalizeSlices(ctx, sliceWriters)
}

func initializeSlices(ctx *encodeCtx) (err error) {
	// readers are slices of original object(no memory allocated)
	cksmReaders := make([]io.Reader, ctx.dataSlices)
	sizeLeft := ctx.lom.Size()
	for i := 0; i < ctx.dataSlices; i++ {
		var (
			reader     cmn.ReadOpenCloser
			cksmReader cmn.ReadOpenCloser
			offset     = int64(i) * ctx.sliceSize
		)
		if sizeLeft < ctx.sliceSize {
			reader = cmn.NewSectionHandle(ctx.fh, offset, sizeLeft, ctx.padSize)
			cksmReader = cmn.NewSectionHandle(ctx.fh, offset, sizeLeft, ctx.padSize)
		} else {
			reader = cmn.NewSectionHandle(ctx.fh, offset, ctx.sliceSize, 0)
			cksmReader = cmn.NewSectionHandle(ctx.fh, offset, ctx.sliceSize, 0)
		}
		ctx.slices[i] = &slice{obj: ctx.fh, reader: reader}
		cksmReaders[i] = cksmReader
		sizeLeft -= ctx.sliceSize
	}

	// We have established readers of data slices, we can already start calculating hashes for them
	// during calculating parity slices and their hashes
	if cksumType := ctx.lom.CksumConf().Type; cksumType != cmn.ChecksumNone {
		ctx.cksums = make([]*cmn.CksumHash, ctx.paritySlices)
		err = checksumDataSlices(ctx, cksmReaders, cksumType)
	}
	return
}

func finalizeSlices(ctx *encodeCtx, writers []io.Writer) error {
	stream, err := reedsolomon.NewStreamC(ctx.dataSlices, ctx.paritySlices, true, true)
	if err != nil {
		return err
	}

	// Calculate parity slices and their checksums
	readers := make([]io.Reader, ctx.dataSlices)
	for i := 0; i < ctx.dataSlices; i++ {
		readers[i] = ctx.slices[i].reader
	}
	if err := stream.Encode(readers, writers); err != nil {
		return err
	}

	if cksumType := ctx.lom.CksumConf().Type; cksumType != cmn.ChecksumNone {
		for i := range ctx.cksums {
			ctx.cksums[i].Finalize()
			ctx.slices[i+ctx.dataSlices].cksum = ctx.cksums[i].Clone()
		}
	}
	return nil
}

// generateSlicesToDisk gets FQN to the original file and encodes it into EC slices
func generateSlicesToDisk(ctx *encodeCtx) error {
	writers := make([]io.Writer, ctx.paritySlices)
	sliceWriters := make([]io.Writer, ctx.paritySlices)

	defer func() {
		for _, wr := range writers {
			if wr == nil {
				continue
			}
			// writer can be only *os.File within this function
			f := wr.(*os.File)
			cmn.Close(f)
		}
	}()

	conf := ctx.lom.CksumConf()
	for i := 0; i < ctx.paritySlices; i++ {
		workFQN := fs.CSM.GenContentFQN(ctx.lom, fs.WorkfileType, fmt.Sprintf("ec-write-%d", i))
		writer, err := ctx.lom.CreateFile(workFQN)
		if err != nil {
			return err
		}
		ctx.slices[i+ctx.dataSlices] = &slice{writer: writer, workFQN: workFQN}
		writers[i] = writer
		if conf.Type == cmn.ChecksumNone {
			sliceWriters[i] = writer
		} else {
			ctx.cksums[i] = cmn.NewCksumHash(conf.Type)
			sliceWriters[i] = cmn.NewWriterMulti(writer, ctx.cksums[i].H)
		}
	}

	return finalizeSlices(ctx, sliceWriters)
}

func (c *putJogger) sendSlice(ctx *encodeCtx, data *slice, node *cluster.Snode, idx int, counter *atomic.Int32) error {
	// Reopen the slice's reader, because it was read to the end by erasure
	// encoding while calculating parity slices.
	reader, err := ctx.slices[idx].reopenReader()
	if err != nil {
		return err
	}

	mcopy := &Metadata{}
	cmn.CopyStruct(mcopy, ctx.meta)
	mcopy.SliceID = idx + 1
	mcopy.ObjVersion = ctx.lom.Version()
	if ctx.slices[idx].cksum != nil {
		mcopy.CksumType, mcopy.CksumValue = ctx.slices[idx].cksum.Get()
	}

	src := &dataSource{
		reader:   reader,
		size:     ctx.sliceSize,
		obj:      data,
		metadata: mcopy,
		isSlice:  true,
		reqType:  reqPut,
	}
	sentCB := func(hdr transport.ObjHdr, _ io.ReadCloser, _ unsafe.Pointer, err error) {
		if data != nil {
			data.release()
		}
		if err != nil {
			glog.Errorf("Failed to send %s/%s: %v", hdr.Bck, hdr.ObjName, err)
		}
		if cnt := counter.Dec(); cnt == 0 {
			c.parent.DecPending()
		}
	}

	return c.parent.writeRemote([]string{node.ID()}, ctx.lom, src, sentCB)
}

// Copies the constructed EC slices to remote targets.
func (c *putJogger) sendSlices(ctx *encodeCtx) error {
	totalCnt := ctx.paritySlices + ctx.dataSlices
	// totalCnt+1 because the first node gets the replica
	targets, err := cluster.HrwTargetList(ctx.lom.Uname(), c.parent.smap.Get(), totalCnt+1)
	if err != nil {
		return err
	}

	// load the data slices from original object and construct parity ones
	if c.toDisk {
		err = generateSlicesToDisk(ctx)
	} else {
		err = generateSlicesToMemory(ctx)
	}

	if err != nil {
		return err
	}

	dataSlice := &slice{refCnt: *atomic.NewInt32(int32(ctx.dataSlices)), obj: ctx.fh}
	toSend := cmn.Min(totalCnt, len(targets)-1)
	counter := atomic.NewInt32(int32(toSend))

	// If the slice is data one - no immediate cleanup is required because this
	// slice is just a section reader of the entire file.
	var copyErr error
	for i := 0; i < toSend; i++ {
		var sl *slice
		// Each data slice is a section reader of the replica, so the memory is
		// freed only after the last data slice is sent. Parity slices allocate memory,
		// so the counter is set to 1, to free immediately after send.
		if i < ctx.dataSlices {
			sl = dataSlice
		} else {
			sl = &slice{refCnt: *atomic.NewInt32(1), obj: ctx.slices[i].obj, workFQN: ctx.slices[i].workFQN}
		}
		if err := c.sendSlice(ctx, sl, targets[i+1], i, counter); err != nil {
			sl.release()
			copyErr = err
		}
	}

	if copyErr != nil {
		glog.Errorf("Error while copying (data=%d, parity=%d) for %q: %v",
			ctx.dataSlices, ctx.paritySlices, ctx.lom.ObjName, copyErr)
		err = errSliceSendFailed
	} else if glog.FastV(4, glog.SmoduleEC) {
		glog.Infof("EC created (data=%d, parity=%d) for %q",
			ctx.dataSlices, ctx.paritySlices, ctx.lom.ObjName)
	}

	return err
}
