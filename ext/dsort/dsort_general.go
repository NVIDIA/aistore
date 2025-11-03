// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/dsort/shard"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// This is general implementation of dsort, for all types of workloads.
// Historically, it is the first implementation of dsorter ever created.
// It tries to use memory in extraction phase to pull as most of the data
// into memory as possible. This way, later, dsorter will use it later in
// creation phase. It means that if the data takes less space than the available
// memory in the cluster, we can extract whole data into memory and make
// the creation phase super fast.

const (
	GeneralType = "dsort_general"
)

type (
	dsorterGeneral struct {
		m       *Manager
		mw      *memoryWatcher
		streams dsortStreams

		creationPhase struct {
			adjuster *concAdjuster

			streamWriters struct {
				mu      sync.Mutex
				writers map[string]*streamWriter
			}
		}
	}

	streamWriter struct {
		w   io.Writer
		n   int64
		err error
		wg  *cos.TimeoutGroup
	}

	remoteRequest struct {
		Record    *shard.Record    `json:"r"`
		RecordObj *shard.RecordObj `json:"o"`
	}
)

// interface guard
var _ dsorter = (*dsorterGeneral)(nil)

func newDsorterGeneral(m *Manager) (*dsorterGeneral, error) {
	var mem sys.MemStat
	if err := mem.Get(); err != nil {
		return nil, err
	}
	maxMemoryToUse := calcMaxMemoryUsage(m.Pars.MaxMemUsage, &mem)
	ds := &dsorterGeneral{
		m:  m,
		mw: newMemoryWatcher(m, maxMemoryToUse),
	}
	ds.creationPhase.streamWriters.writers = make(map[string]*streamWriter, 10000)
	return ds, nil
}

func (ds *dsorterGeneral) newStreamWriter(pathToContents string, w io.Writer) *streamWriter {
	writer := &streamWriter{
		w:  w,
		wg: cos.NewTimeoutGroup(),
	}
	writer.wg.Add(1)
	ds.creationPhase.streamWriters.mu.Lock()
	ds.creationPhase.streamWriters.writers[pathToContents] = writer
	ds.creationPhase.streamWriters.mu.Unlock()
	return writer
}

func (ds *dsorterGeneral) pullStreamWriter(objName string) *streamWriter {
	ds.creationPhase.streamWriters.mu.Lock()
	writer := ds.creationPhase.streamWriters.writers[objName]
	delete(ds.creationPhase.streamWriters.writers, objName)
	ds.creationPhase.streamWriters.mu.Unlock()
	return writer
}

func (*dsorterGeneral) name() string { return GeneralType }

func (ds *dsorterGeneral) init(*cmn.Config) error {
	ds.creationPhase.adjuster = newConcAdjuster(
		ds.m.Pars.CreateConcMaxLimit,
		1, /*goroutineLimitCoef*/
	)
	return nil
}

func (ds *dsorterGeneral) start() error {
	// Requests are usually small packets, no more 1KB that is why we want to
	// utilize intraControl network.
	config := cmn.GCO.Get()
	reqNetwork := cmn.NetIntraControl
	// Responses to the other targets are objects that is why we want to use
	// intraData network.
	respNetwork := cmn.NetIntraData

	client := transport.NewIntraDataClient()

	trname := fmt.Sprintf(recvReqStreamNameFmt, ds.m.ManagerUUID)
	reqSbArgs := bundle.Args{
		Multiplier: ds.m.Pars.SbundleMult,
		Net:        reqNetwork,
		Trname:     trname,
		Ntype:      core.Targets,
		Extra: &transport.Extra{
			Config: config,
		},
	}
	if err := transport.Handle(trname, ds.recvReq); err != nil {
		return errors.WithStack(err)
	}

	trname = fmt.Sprintf(recvRespStreamNameFmt, ds.m.ManagerUUID)
	respSbArgs := bundle.Args{
		Multiplier: ds.m.Pars.SbundleMult,
		Net:        respNetwork,
		Trname:     trname,
		Ntype:      core.Targets,
		Extra: &transport.Extra{
			Compression: config.Dsort.Compression,
			Config:      config,
		},
	}
	if err := transport.Handle(trname, ds.recvResp); err != nil {
		return errors.WithStack(err)
	}

	ds.streams.request = bundle.New(client, reqSbArgs)
	ds.streams.response = bundle.New(client, respSbArgs)

	// start watching memory
	return ds.mw.watch()
}

func (ds *dsorterGeneral) cleanupStreams() error {
	return ds.m.cleanupDsortStreams(&ds.streams)
}

func (ds *dsorterGeneral) cleanup() {
	ds.mw.stop()
}

func (ds *dsorterGeneral) finalCleanup() error {
	return ds.cleanupStreams()
}

func (ds *dsorterGeneral) postRecordDistribution() {
	// In shard creation we should not expect memory increase (at least
	// not from dsort). Also it would be really hard to have concurrent
	// sends and memory cleanup. We must stop before sending records
	// because it affects content of the records.
	ds.mw.stopWatchingExcess()
}

// createShardsLocally waits until it's given the signal to start creating
// shards, then creates shards in parallel.
func (ds *dsorterGeneral) createShardsLocally() (err error) {
	phaseInfo := &ds.m.creationPhase

	ds.creationPhase.adjuster.start()
	defer ds.creationPhase.adjuster.stop()

	metrics := ds.m.Metrics.Creation
	metrics.begin()
	defer metrics.finish()
	metrics.mu.Lock()
	metrics.ToCreate = int64(len(phaseInfo.metadata.Shards))
	metrics.mu.Unlock()

	group, ctx := errgroup.WithContext(context.Background())

outer:
	for _, s := range phaseInfo.metadata.Shards {
		select {
		case <-ds.m.listenAborted():
			_ = group.Wait()
			return ds.m.newErrAborted()
		case <-ctx.Done():
			break outer // context was canceled, therefore we have an error
		default:
		}

		ds.creationPhase.adjuster.acquireGoroutineSema()
		cs := &dsgCreateShard{ds, s}
		group.Go(cs.do)
	}

	return group.Wait()
}

func (ds *dsorterGeneral) preShardCreation(_ string, mi *fs.Mountpath) error {
	ds.creationPhase.adjuster.acquireSema(mi)
	return nil
}

func (ds *dsorterGeneral) postShardCreation(mi *fs.Mountpath) {
	ds.creationPhase.adjuster.releaseSema(mi)
}

// loads content from disk or memory, local or remote
func (ds *dsorterGeneral) Load(w io.Writer, rec *shard.Record, obj *shard.RecordObj) (int64, error) {
	if ds.m.aborted() {
		return 0, ds.m.newErrAborted()
	}
	if rec.DaemonID != core.T.SID() {
		return ds.loadRemote(w, rec, obj)
	}
	return ds.loadLocal(w, obj)
}

func (ds *dsorterGeneral) loadLocal(w io.Writer, obj *shard.RecordObj) (written int64, _ error) {
	var (
		slab      *memsys.Slab
		buf       []byte
		storeType = obj.StoreType
	)
	if storeType != shard.SGLStoreType { // SGL does not need buffer as it is buffer itself
		buf, slab = g.mem.AllocSize(obj.Size)
	}
	defer func() {
		if storeType != shard.SGLStoreType {
			slab.Free(buf)
		}
		ds.m.decrementRef(1)
	}()

	fullContentPath := ds.m.recm.FullContentPath(obj)

	if ds.m.Pars.DryRun {
		r := cos.NopReader(obj.MetadataSize + obj.Size)
		return io.CopyBuffer(w, r, buf)
	}

	switch storeType {
	case shard.OffsetStoreType:
		f, errO := os.Open(fullContentPath)
		if errO != nil {
			return written, errors.WithMessage(errO, "(offset) open local content failed")
		}
		defer cos.Close(f)
		if _, err := f.Seek(obj.Offset-obj.MetadataSize, io.SeekStart); err != nil {
			return written, errors.WithMessage(err, "(offset) seek local content failed")
		}
		n, err := io.CopyBuffer(w, io.LimitReader(f, obj.MetadataSize+obj.Size), buf)
		written += n
		if err != nil {
			return written, errors.WithMessage(err, "(offset) copy local content failed")
		}
	case shard.SGLStoreType:
		debug.Assert(buf == nil)
		v, ok := ds.m.recm.RecordContents().Load(fullContentPath)
		debug.Assert(ok, fullContentPath)
		ds.m.recm.RecordContents().Delete(fullContentPath)
		sgl := v.(*memsys.SGL)
		defer sgl.Free()

		// No need for `io.CopyBuffer` since SGL implements `io.WriterTo`.
		n, err := io.Copy(w, sgl)
		written += n
		if err != nil {
			return written, errors.WithMessage(err, "(sgl) copy local content failed")
		}
	case shard.DiskStoreType:
		f, errO := os.Open(fullContentPath)
		if errO != nil {
			return written, errors.WithMessage(errO, "(disk) open local content failed")
		}
		defer cos.Close(f)
		n, err := io.CopyBuffer(w, f, buf)
		written += n
		if err != nil {
			return written, errors.WithMessage(err, "(disk) copy local content failed")
		}
	default:
		debug.Assert(false, storeType)
	}

	return written, nil
}

func (ds *dsorterGeneral) loadRemote(w io.Writer, rec *shard.Record, obj *shard.RecordObj) (int64, error) {
	var (
		tid    = rec.DaemonID
		tsi    = ds.m.smap.GetTarget(tid)
		writer = ds.newStreamWriter(rec.MakeUniqueName(obj), w)
	)
	if tsi == nil {
		return 0, errors.Errorf("cannot send request to node %q - not present in %s", tid, ds.m.smap)
	}
	req := remoteRequest{
		Record:    rec,
		RecordObj: obj,
	}
	opaque := cos.MustMarshal(req)
	o := transport.AllocSend()
	o.Hdr = transport.ObjHdr{Opaque: opaque}
	o.SentCB, o.CmplArg = ds.sentCallback, &req

	if err := ds.streams.request.Send(o, nil, tsi); err != nil {
		return 0, errors.WithStack(err)
	}

	// May happen that the target we are trying to contact was
	// aborted or for some reason is not responding. Thus we need to do
	// some precaution and wait for the content only for limited time or
	// until we receive abort signal.
	var (
		beforeRecv = mono.NanoTime()
		pulled     bool
	)
	timed, stopped := writer.wg.WaitTimeoutWithStop(ds.m.callTimeout, ds.m.listenAborted())
	if timed || stopped {
		// In case of timeout or abort we need to pull the writer to
		// avoid concurrent Close and Write on `writer.w`.
		pulled = ds.pullStreamWriter(rec.MakeUniqueName(obj)) != nil
	} else {
		// stats
		delta := mono.Since(beforeRecv)
		core.T.StatsUpdater().Inc(stats.DsortCreationRespCount)
		core.T.StatsUpdater().Add(stats.DsortCreationRespLatency, int64(delta))
	}

	// If we timed out or were stopped but failed to pull the
	// writer then someone else should've done it and we barely
	// missed. In this case we should wait for the job to finish
	// (when stopped, we should receive an error anyway).

	if pulled { // managed to pull the writer, can safely return error
		var err error
		switch {
		case stopped:
			err = cmn.NewErrAborted("wait for remote content", "", nil)
		case timed:
			err = errors.Errorf("wait for remote content timed out (%q was waiting for %q)", core.T.SID(), tid)
		default:
			debug.Assert(false, "pulled but not stopped or timed?")
		}
		return 0, err
	}

	if timed || stopped {
		writer.wg.Wait()
	}

	return writer.n, writer.err
}

func (ds *dsorterGeneral) sentCallback(_ *transport.ObjHdr, _ io.ReadCloser, arg any, err error) {
	if err == nil {
		core.T.StatsUpdater().Add(stats.DsortCreationReqCount, 1)
		return
	}
	req := arg.(*remoteRequest)
	nlog.Errorf("%s: [dsort] %s failed to send remore-req %s: %v",
		core.T, ds.m.ManagerUUID, req.Record.MakeUniqueName(req.RecordObj), err)
}

func (ds *dsorterGeneral) errHandler(err error, node *meta.Snode, o *transport.Obj) {
	*o = transport.Obj{Hdr: o.Hdr}
	o.Hdr.Opaque = []byte(err.Error())
	o.Hdr.ObjAttrs.Size = 0
	if err = ds.streams.response.Send(o, nil, node); err != nil {
		ds.m.abort(err)
	}
}

// implements receiver i/f
// (note: ObjHdr and its fields must be consumed synchronously)
func (ds *dsorterGeneral) recvReq(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
	ds.m.inFlightInc()
	defer func() {
		ds.m.inFlightDec()
		transport.FreeRecv(objReader)
	}()
	req := remoteRequest{}
	if errM := jsoniter.Unmarshal(hdr.Opaque, &req); errM != nil {
		if err == nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, apc.ActDsort, "recv request", cos.BHead(hdr.Opaque), errM)
		}
		ds.m.abort(err)
		return err
	}

	fromNode := ds.m.smap.GetTarget(hdr.SID)
	if fromNode == nil {
		return fmt.Errorf("received request (%v) from %q not present in the %s", req.Record, hdr.SID, ds.m.smap)
	}

	if err != nil {
		ds.errHandler(err, fromNode, &transport.Obj{Hdr: *hdr})
		return err
	}

	if ds.m.aborted() {
		return ds.m.newErrAborted()
	}

	o := transport.AllocSend()
	o.Hdr = transport.ObjHdr{ObjName: req.Record.MakeUniqueName(req.RecordObj)}
	o.SentCB = ds.responseCallback

	fullContentPath := ds.m.recm.FullContentPath(req.RecordObj)

	if ds.m.Pars.DryRun {
		lr := cos.NopReader(req.RecordObj.MetadataSize + req.RecordObj.Size)
		r := cos.NopOpener(io.NopCloser(lr))
		o.Hdr.ObjAttrs.Size = req.RecordObj.MetadataSize + req.RecordObj.Size
		ds.streams.response.Send(o, r, fromNode)
		return nil
	}

	switch req.RecordObj.StoreType {
	case shard.OffsetStoreType:
		o.Hdr.ObjAttrs.Size = req.RecordObj.MetadataSize + req.RecordObj.Size
		offset := req.RecordObj.Offset - req.RecordObj.MetadataSize
		r, err := cos.NewFileSectionHandle(fullContentPath, offset, o.Hdr.ObjAttrs.Size)
		if err != nil {
			ds.errHandler(err, fromNode, o)
			return err
		}
		ds.streams.response.Send(o, r, fromNode)
	case shard.SGLStoreType:
		v, ok := ds.m.recm.RecordContents().Load(fullContentPath)
		debug.Assert(ok, fullContentPath)
		ds.m.recm.RecordContents().Delete(fullContentPath)
		sgl := v.(*memsys.SGL)
		o.Hdr.ObjAttrs.Size = sgl.Size()
		ds.streams.response.Send(o, sgl, fromNode)
	case shard.DiskStoreType:
		f, err := cos.NewFileHandle(fullContentPath)
		if err != nil {
			ds.errHandler(err, fromNode, o)
			return err
		}
		fi, err := f.Stat()
		if err != nil {
			cos.Close(f)
			ds.errHandler(err, fromNode, o)
			return err
		}
		o.Hdr.ObjAttrs.Size = fi.Size()
		ds.streams.response.Send(o, f, fromNode)
	default:
		debug.Assert(false)
	}
	return nil
}

func (ds *dsorterGeneral) responseCallback(hdr *transport.ObjHdr, rc io.ReadCloser, _ any, err error) {
	if sgl, ok := rc.(*memsys.SGL); ok {
		sgl.Free()
	}
	ds.m.decrementRef(1)
	if err != nil {
		nlog.Errorf("%s: [dsort] %s failed to send rsp %s (size %d): %v - aborting...",
			core.T, ds.m.ManagerUUID, hdr.ObjName, hdr.ObjAttrs.Size, err)
		ds.m.abort(err)
	}
}

func (ds *dsorterGeneral) postExtraction() {
	ds.mw.stopWatchingReserved()
}

// (note: ObjHdr and its fields must be consumed synchronously)
func (ds *dsorterGeneral) recvResp(hdr *transport.ObjHdr, object io.Reader, err error) error {
	ds.m.inFlightInc()
	defer func() {
		transport.DrainAndFreeReader(object)
		ds.m.inFlightDec()
	}()

	if err != nil {
		ds.m.abort(err)
		return err
	}

	if ds.m.aborted() {
		return ds.m.newErrAborted()
	}

	writer := ds.pullStreamWriter(hdr.ObjName)
	if writer == nil { // was removed after timing out
		return nil
	}

	if len(hdr.Opaque) > 0 {
		writer.n, writer.err = 0, errors.New(string(hdr.Opaque))
		writer.wg.Done()
		return nil
	}

	buf, slab := g.mem.AllocSize(hdr.ObjAttrs.Size)
	writer.n, writer.err = io.CopyBuffer(writer.w, object, buf)
	writer.wg.Done()
	slab.Free(buf)

	return nil
}

func (ds *dsorterGeneral) preShardExtraction(expectedUncompressedSize uint64) bool {
	return ds.mw.reserveMem(expectedUncompressedSize)
}

func (ds *dsorterGeneral) postShardExtraction(expectedUncompressedSize uint64) {
	ds.mw.unreserveMem(expectedUncompressedSize)
}

func (ds *dsorterGeneral) onAbort() {
	_ = ds.cleanupStreams()
}

////////////////////
// dsgCreateShard //
////////////////////

type dsgCreateShard struct {
	ds    *dsorterGeneral
	shard *shard.Shard
}

func (cs *dsgCreateShard) do() (err error) {
	lom := core.AllocLOM(cs.shard.Name)
	err = cs.ds.m.createShard(cs.shard, lom)
	core.FreeLOM(lom)
	cs.ds.creationPhase.adjuster.releaseGoroutineSema()
	return
}
