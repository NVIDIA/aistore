// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/klauspost/reedsolomon"
)

type (
	encodeCtx struct {
		lom          *cluster.LOM     // replica
		meta         *Metadata        //
		fh           *cos.FileHandle  // file handle for the replica
		sliceSize    int64            // calculated slice size
		padSize      int64            // zero tail of the last object's data slice
		dataSlices   int              // the number of data slices
		paritySlices int              // the number of parity slices
		cksums       []*cos.CksumHash // checksums of parity slices (filled by reed-solomon)
		slices       []*slice         // all EC slices (in the order of slice IDs)
		targets      []*meta.Snode    // target list (in the order of slice IDs: targets[i] receives slices[i])
	}

	// a mountpath putJogger: processes PUT/DEL requests to one mountpath
	putJogger struct {
		parent *XactPut
		slab   *memsys.Slab
		buffer []byte
		mpath  string

		putCh  chan *request // top priority operation (object PUT)
		xactCh chan *request // low priority operation (ec-encode)
		stopCh cos.StopCh    // jogger management channel: to stop it

		toDisk bool // use files or SGL
	}
)

var (
	encCtxPool         sync.Pool
	emptyCtx           encodeCtx
	errSliceSendFailed = errors.New("failed to send slice")
)

func allocCtx() (ctx *encodeCtx) {
	if v := encCtxPool.Get(); v != nil {
		ctx = v.(*encodeCtx)
	} else {
		ctx = &encodeCtx{}
	}
	return
}

func (ctx *encodeCtx) freeReplica() {
	freeObject(ctx.fh)
}

///////////////
// putJogger //
///////////////

func (*putJogger) newCtx(lom *cluster.LOM, meta *Metadata) (ctx *encodeCtx, err error) {
	ctx = allocCtx()
	ctx.lom = lom
	ctx.dataSlices = lom.Bprops().EC.DataSlices
	ctx.paritySlices = lom.Bprops().EC.ParitySlices
	ctx.meta = meta

	totalCnt := ctx.paritySlices + ctx.dataSlices
	ctx.sliceSize = SliceSize(ctx.lom.SizeBytes(), ctx.dataSlices)
	ctx.slices = make([]*slice, totalCnt)
	ctx.padSize = ctx.sliceSize*int64(ctx.dataSlices) - ctx.lom.SizeBytes()

	ctx.fh, err = cos.NewFileHandle(lom.FQN)
	return ctx, err
}

func (*putJogger) freeCtx(ctx *encodeCtx) {
	*ctx = emptyCtx
	encCtxPool.Put(ctx)
}

func (c *putJogger) freeResources() {
	c.slab.Free(c.buffer)
	c.buffer = nil
	c.slab = nil
}

func (c *putJogger) processRequest(req *request) {
	lom, err := req.LIF.LOM()
	if err != nil {
		return
	}

	c.parent.IncPending()
	defer func() {
		if req.Callback != nil {
			req.Callback(lom, err)
		}
		cluster.FreeLOM(lom)
		c.parent.DecPending()
	}()

	if req.Action == ActSplit {
		if err = lom.Load(false /*cache it*/, false /*locked*/); err != nil {
			return
		}
		ecConf := lom.Bprops().EC
		memRequired := lom.SizeBytes() * int64(ecConf.DataSlices+ecConf.ParitySlices) / int64(ecConf.ParitySlices)
		c.toDisk = useDisk(memRequired, c.parent.config)
	}

	c.parent.stats.updateWaitTime(time.Since(req.tm))
	req.tm = time.Now()
	if err = c.ec(req, lom); err != nil {
		err = fmt.Errorf("%s: failed to %s %s: %w", c.parent.t, req.Action, lom.StringEx(), err)
		nlog.Errorln(err)
		c.parent.AddErr(err)
	}
}

func (c *putJogger) run(wg *sync.WaitGroup) {
	nlog.Infof("Started EC for mountpath: %s, bucket %s", c.mpath, c.parent.bck)
	defer wg.Done()
	c.buffer, c.slab = mm.Alloc()
	for {
		select {
		case req := <-c.putCh:
			c.processRequest(req)
			freeReq(req)
		case req := <-c.xactCh:
			c.processRequest(req)
			freeReq(req)
		case <-c.stopCh.Listen():
			c.freeResources()
			return
		}
	}
}

func (c *putJogger) stop() {
	nlog.Infof("Stopping EC for mountpath: %s, bucket %s", c.mpath, c.parent.bck)
	c.stopCh.Close()
}

func (c *putJogger) ec(req *request, lom *cluster.LOM) (err error) {
	switch req.Action {
	case ActSplit:
		if err = c.encode(req, lom); err != nil {
			ctMeta := cluster.NewCTFromLOM(lom, fs.ECMetaType)
			errRm := cos.RemoveFile(ctMeta.FQN())
			debug.AssertNoErr(errRm)
		}
		c.parent.stats.updateEncodeTime(time.Since(req.tm), err != nil)
	case ActDelete:
		err = c.cleanup(lom)
		c.parent.stats.updateDeleteTime(time.Since(req.tm), err != nil)
	default:
		err = fmt.Errorf("invalid EC action for putJogger: %v", req.Action)
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
func (c *putJogger) encode(req *request, lom *cluster.LOM) error {
	var (
		cksumValue, cksumType string
		ecConf                = lom.Bprops().EC
	)
	if c.parent.config.FastV(4, cos.SmoduleEC) {
		nlog.Infof("Encoding %q...", lom.FQN)
	}
	if lom.Checksum() != nil {
		cksumType, cksumValue = lom.Checksum().Get()
	}
	reqTargets := ecConf.ParitySlices + 1
	if !req.IsCopy {
		reqTargets += ecConf.DataSlices
	}
	smap := c.parent.smap.Get()
	targetCnt := smap.CountActiveTs()
	if targetCnt < reqTargets {
		return fmt.Errorf("%v: given EC config (d=%d, p=%d), %d targets required to encode %s (have %d, %s)",
			cmn.ErrNotEnoughTargets, ecConf.DataSlices, ecConf.ParitySlices, reqTargets, lom, targetCnt, smap.StringEx())
	}

	ctMeta := cluster.NewCTFromLOM(lom, fs.ECMetaType)
	generation := mono.NanoTime()
	meta := &Metadata{
		MDVersion:   MDVersionLast,
		Generation:  generation,
		Size:        lom.SizeBytes(),
		Data:        ecConf.DataSlices,
		Parity:      ecConf.ParitySlices,
		IsCopy:      req.IsCopy,
		ObjCksum:    cksumValue,
		CksumType:   cksumType,
		FullReplica: c.parent.t.SID(),
		Daemons:     make(cos.MapStrUint16, reqTargets),
	}

	c.parent.LomAdd(lom)

	ctx, err := c.newCtx(lom, meta)
	defer c.freeCtx(ctx)
	if err != nil {
		return err
	}
	targets, err := smap.HrwTargetList(ctx.lom.Uname(), reqTargets)
	if err != nil {
		return err
	}
	ctx.targets = targets[1:]
	meta.Daemons[targets[0].ID()] = 0 // main or full replica always on the first target
	for i, tgt := range ctx.targets {
		sliceID := uint16(i + 1)
		if meta.IsCopy {
			sliceID = 0
		}
		meta.Daemons[tgt.ID()] = sliceID
	}

	if meta.IsCopy {
		err = c.replicate(ctx)
	} else {
		err = c.splitAndDistribute(ctx)
	}
	if err != nil {
		return err
	}
	metaBuf := bytes.NewReader(meta.NewPack())
	if err := ctMeta.Write(c.parent.t, metaBuf, -1); err != nil {
		return err
	}
	if _, exists := c.parent.t.Bowner().Get().Get(ctMeta.Bck()); !exists {
		if errRm := cos.RemoveFile(ctMeta.FQN()); errRm != nil {
			nlog.Errorf("nested error: encode -> remove metafile: %v", errRm)
		}
		return fmt.Errorf("%s metafile saved while bucket %s was being destroyed", ctMeta.ObjectName(), ctMeta.Bucket())
	}
	return nil
}

func (c *putJogger) ctSendCallback(hdr *transport.ObjHdr, _ io.ReadCloser, _ any, err error) {
	c.parent.t.ByteMM().Free(hdr.Opaque)
	if err != nil {
		nlog.Errorf("failed to send o[%s]: %v", hdr.Cname(), err)
	}
	c.parent.DecPending()
}

// Remove slices and replicas across the cluster: remove local metafile
// if exists and broadcast the request to other targets
func (c *putJogger) cleanup(lom *cluster.LOM) error {
	ctMeta := cluster.NewCTFromLOM(lom, fs.ECMetaType)
	md, err := LoadMetadata(ctMeta.FQN())
	if err != nil {
		if os.IsNotExist(err) {
			// Metafile does not exist = nothing to clean up
			err = nil
		}
		return err
	}
	nodes := md.RemoteTargets(c.parent.t)
	if err := cos.RemoveFile(ctMeta.FQN()); err != nil {
		return err
	}

	mm := c.parent.t.ByteMM()
	request := newIntraReq(reqDel, nil, lom.Bck()).NewPack(mm)
	o := transport.AllocSend()
	o.Hdr = transport.ObjHdr{ObjName: lom.ObjName, Opaque: request, Opcode: reqDel}
	o.Hdr.Bck.Copy(lom.Bucket())
	o.Callback = c.ctSendCallback
	c.parent.IncPending()
	return c.parent.mgr.req().Send(o, nil, nodes...)
}

// Sends object replicas to targets that must have replicas after the client
// uploads the main replica
func (c *putJogger) createCopies(ctx *encodeCtx) error {
	// generate a list of target to send the replica (all excluding this one)
	nodes := make([]string, 0, len(ctx.targets))
	for _, tgt := range ctx.targets {
		nodes = append(nodes, tgt.ID())
	}

	// broadcast the replica to the targets
	src := &dataSource{
		reader:   ctx.fh,
		size:     ctx.lom.SizeBytes(),
		metadata: ctx.meta,
		reqType:  reqPut,
	}
	return c.parent.writeRemote(nodes, ctx.lom, src, nil)
}

func checksumDataSlices(ctx *encodeCtx, cksmReaders []io.Reader, cksumType string) error {
	debug.Assert(cksumType != "") // caller checks for 'none'
	for i, reader := range cksmReaders {
		_, cksum, err := cos.CopyAndChecksum(io.Discard, reader, nil, cksumType)
		if err != nil {
			return err
		}
		ctx.slices[i].cksum = cksum.Clone()
	}
	return nil
}

// generateSlicesToMemory gets FQN to the original file and encodes it into EC slices
// writers are slices created by EC encoding process(memory is allocated)
func generateSlicesToMemory(ctx *encodeCtx) error {
	var (
		cksumType    = ctx.lom.CksumType()
		initSize     = min(ctx.sliceSize, cos.MiB)
		sliceWriters = make([]io.Writer, ctx.paritySlices)
	)
	for i := 0; i < ctx.paritySlices; i++ {
		writer := mm.NewSGL(initSize)
		ctx.slices[i+ctx.dataSlices] = &slice{obj: writer}
		if cksumType == cos.ChecksumNone {
			sliceWriters[i] = writer
		} else {
			ctx.cksums[i] = cos.NewCksumHash(cksumType)
			sliceWriters[i] = cos.NewWriterMulti(writer, ctx.cksums[i].H)
		}
	}

	return finalizeSlices(ctx, sliceWriters)
}

func initializeSlices(ctx *encodeCtx) (err error) {
	// readers are slices of original object(no memory allocated)
	cksmReaders := make([]io.Reader, ctx.dataSlices)
	sizeLeft := ctx.lom.SizeBytes()
	for i := 0; i < ctx.dataSlices; i++ {
		var (
			reader     cos.ReadOpenCloser
			cksmReader cos.ReadOpenCloser
			offset     = int64(i) * ctx.sliceSize
		)
		if sizeLeft < ctx.sliceSize {
			reader = cos.NewSectionHandle(ctx.fh, offset, sizeLeft, ctx.padSize)
			cksmReader = cos.NewSectionHandle(ctx.fh, offset, sizeLeft, ctx.padSize)
		} else {
			reader = cos.NewSectionHandle(ctx.fh, offset, ctx.sliceSize, 0)
			cksmReader = cos.NewSectionHandle(ctx.fh, offset, ctx.sliceSize, 0)
		}
		ctx.slices[i] = &slice{obj: ctx.fh, reader: reader}
		cksmReaders[i] = cksmReader
		sizeLeft -= ctx.sliceSize
	}

	// We have established readers of data slices, we can already start calculating hashes for them
	// during calculating parity slices and their hashes
	if cksumType := ctx.lom.CksumType(); cksumType != cos.ChecksumNone {
		ctx.cksums = make([]*cos.CksumHash, ctx.paritySlices)
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

	if cksumType := ctx.lom.CksumType(); cksumType != cos.ChecksumNone {
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
			cos.Close(f)
		}
	}()

	cksumType := ctx.lom.CksumType()
	for i := 0; i < ctx.paritySlices; i++ {
		workFQN := fs.CSM.Gen(ctx.lom, fs.WorkfileType, fmt.Sprintf("ec-write-%d", i))
		writer, err := ctx.lom.CreateFile(workFQN)
		if err != nil {
			return err
		}
		ctx.slices[i+ctx.dataSlices] = &slice{writer: writer, workFQN: workFQN}
		writers[i] = writer
		if cksumType == cos.ChecksumNone {
			sliceWriters[i] = writer
		} else {
			ctx.cksums[i] = cos.NewCksumHash(cksumType)
			sliceWriters[i] = cos.NewWriterMulti(writer, ctx.cksums[i].H)
		}
	}

	return finalizeSlices(ctx, sliceWriters)
}

func (c *putJogger) sendSlice(ctx *encodeCtx, data *slice, node *meta.Snode, idx int) error {
	// Reopen the slice's reader, because it was read to the end by erasure
	// encoding while calculating parity slices.
	reader, err := ctx.slices[idx].reopenReader()
	if err != nil {
		data.release()
		return err
	}

	mcopy := &Metadata{}
	cos.CopyStruct(mcopy, ctx.meta)
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
	sentCB := func(hdr *transport.ObjHdr, _ io.ReadCloser, _ any, err error) {
		if data != nil {
			data.release()
		}
		if err != nil {
			nlog.Errorln("Failed to send", hdr.Cname()+": ", err)
		}
	}

	return c.parent.writeRemote([]string{node.ID()}, ctx.lom, src, sentCB)
}

// Copies the constructed EC slices to remote targets.
func (c *putJogger) sendSlices(ctx *encodeCtx) (err error) {
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
	// If the slice is data one - no immediate cleanup is required because this
	// slice is just a section reader of the entire file.
	var copyErr error
	for i, tgt := range ctx.targets {
		var sl *slice
		// Each data slice is a section reader of the replica, so the memory is
		// freed only after the last data slice is sent. Parity slices allocate memory,
		// so the counter is set to 1, to free immediately after send.
		if i < ctx.dataSlices {
			sl = dataSlice
		} else {
			sl = &slice{refCnt: *atomic.NewInt32(1), obj: ctx.slices[i].obj, workFQN: ctx.slices[i].workFQN}
		}
		if err := c.sendSlice(ctx, sl, tgt, i); err != nil {
			copyErr = err
		}
	}

	if copyErr != nil {
		nlog.Errorf("Error while copying (data=%d, parity=%d) for %q: %v",
			ctx.dataSlices, ctx.paritySlices, ctx.lom.ObjName, copyErr)
		err = errSliceSendFailed
	} else if c.parent.config.FastV(4, cos.SmoduleEC) {
		nlog.Infof("EC created (data=%d, parity=%d) for %q",
			ctx.dataSlices, ctx.paritySlices, ctx.lom.ObjName)
	}

	return err
}
