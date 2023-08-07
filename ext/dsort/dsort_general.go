// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/ext/dsort/extract"
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
	DSorterGeneralType = "dsort_general"
)

type (
	dsorterGeneral struct {
		m  *Manager
		mw *memoryWatcher

		streams struct {
			cleanupDone atomic.Bool
			request     *bundle.Streams
			response    *bundle.Streams
		}

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
		Record    *extract.Record    `json:"r"`
		RecordObj *extract.RecordObj `json:"o"`
	}
)

// interface guard
var _ dsorter = (*dsorterGeneral)(nil)

func newDSorterGeneral(m *Manager) (*dsorterGeneral, error) {
	var mem sys.MemStat
	if err := mem.Get(); err != nil {
		return nil, err
	}
	maxMemoryToUse := calcMaxMemoryUsage(m.pars.MaxMemUsage, &mem)
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

func (*dsorterGeneral) name() string { return DSorterGeneralType }

func (ds *dsorterGeneral) init() error {
	ds.creationPhase.adjuster = newConcAdjuster(
		ds.m.pars.CreateConcMaxLimit,
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
		Multiplier: 20,
		Net:        reqNetwork,
		Trname:     trname,
		Ntype:      cluster.Targets,
	}
	if err := transport.HandleObjStream(trname, ds.recvReq); err != nil {
		return errors.WithStack(err)
	}

	trname = fmt.Sprintf(recvRespStreamNameFmt, ds.m.ManagerUUID)
	respSbArgs := bundle.Args{
		Multiplier: ds.m.pars.SbundleMult,
		Net:        respNetwork,
		Trname:     trname,
		Ntype:      cluster.Targets,
		Extra: &transport.Extra{
			Compression: config.DSort.Compression,
			Config:      config,
			MMSA:        mm,
		},
	}
	if err := transport.HandleObjStream(trname, ds.recvResp); err != nil {
		return errors.WithStack(err)
	}

	ds.streams.request = bundle.New(ds.m.ctx.smapOwner, ds.m.ctx.node, client, reqSbArgs)
	ds.streams.response = bundle.New(ds.m.ctx.smapOwner, ds.m.ctx.node, client, respSbArgs)

	// start watching memory
	return ds.mw.watch()
}

func (ds *dsorterGeneral) cleanupStreams() (err error) {
	if !ds.streams.cleanupDone.CAS(false, true) {
		return nil
	}

	if ds.streams.request != nil {
		trname := fmt.Sprintf(recvReqStreamNameFmt, ds.m.ManagerUUID)
		if unhandleErr := transport.Unhandle(trname); unhandleErr != nil {
			err = errors.WithStack(unhandleErr)
		}
	}

	if ds.streams.response != nil {
		trname := fmt.Sprintf(recvRespStreamNameFmt, ds.m.ManagerUUID)
		if unhandleErr := transport.Unhandle(trname); unhandleErr != nil {
			err = errors.WithStack(unhandleErr)
		}
	}

	for _, streamBundle := range []*bundle.Streams{ds.streams.request, ds.streams.response} {
		if streamBundle != nil {
			// NOTE: We don't want stream to send a message at this point as the
			//  receiver might have closed its corresponding stream.
			streamBundle.Close(false /*gracefully*/)
		}
	}

	return err
}

func (ds *dsorterGeneral) cleanup() {
	ds.mw.stop()
}

func (ds *dsorterGeneral) finalCleanup() error {
	return ds.cleanupStreams()
}

func (ds *dsorterGeneral) postRecordDistribution() {
	// In shard creation we should not expect memory increase (at least
	// not from dSort). Also it would be really hard to have concurrent
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

CreateAllShards:
	for _, s := range phaseInfo.metadata.Shards {
		select {
		case <-ds.m.listenAborted():
			_ = group.Wait()
			return newDSortAbortedError(ds.m.ManagerUUID)
		case <-ctx.Done():
			break CreateAllShards // context was canceled, therefore we have an error
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
func (ds *dsorterGeneral) Load(w io.Writer, rec *extract.Record, obj *extract.RecordObj) (int64, error) {
	if ds.m.aborted() {
		return 0, newDSortAbortedError(ds.m.ManagerUUID)
	}
	if rec.DaemonID != ds.m.ctx.node.ID() {
		return ds.loadRemote(w, rec, obj)
	}
	return ds.loadLocal(w, obj)
}

func (ds *dsorterGeneral) loadLocal(w io.Writer, obj *extract.RecordObj) (written int64, err error) {
	var (
		slab      *memsys.Slab
		buf       []byte
		storeType = obj.StoreType
	)

	if storeType != extract.SGLStoreType { // SGL does not need buffer as it is buffer itself
		buf, slab = mm.AllocSize(obj.Size)
	}

	defer func() {
		if storeType != extract.SGLStoreType {
			slab.Free(buf)
		}
		ds.m.decrementRef(1)
	}()

	fullContentPath := ds.m.recm.FullContentPath(obj)

	if ds.m.pars.DryRun {
		r := cos.NopReader(obj.MetadataSize + obj.Size)
		written, err = io.CopyBuffer(w, r, buf)
		return
	}

	var n int64
	switch storeType {
	case extract.OffsetStoreType:
		f, err := os.Open(fullContentPath) // TODO: it should be open always
		if err != nil {
			return written, errors.WithMessage(err, "(offset) open local content failed")
		}
		defer cos.Close(f)
		_, err = f.Seek(obj.Offset-obj.MetadataSize, io.SeekStart)
		if err != nil {
			return written, errors.WithMessage(err, "(offset) seek local content failed")
		}
		if n, err = io.CopyBuffer(w, io.LimitReader(f, obj.MetadataSize+obj.Size), buf); err != nil {
			return written, errors.WithMessage(err, "(offset) copy local content failed")
		}
	case extract.SGLStoreType:
		debug.Assert(buf == nil)
		v, ok := ds.m.recm.RecordContents().Load(fullContentPath)
		debug.Assert(ok, fullContentPath)
		ds.m.recm.RecordContents().Delete(fullContentPath)
		sgl := v.(*memsys.SGL)
		defer sgl.Free()

		// No need for `io.CopyBuffer` since SGL implements `io.WriterTo`.
		if n, err = io.Copy(w, sgl); err != nil {
			return written, errors.WithMessage(err, "(sgl) copy local content failed")
		}
	case extract.DiskStoreType:
		f, err := os.Open(fullContentPath)
		if err != nil {
			return written, errors.WithMessage(err, "(disk) open local content failed")
		}
		defer cos.Close(f)
		if n, err = io.CopyBuffer(w, f, buf); err != nil {
			return written, errors.WithMessage(err, "(disk) copy local content failed")
		}
	default:
		debug.Assert(false, storeType)
	}

	debug.Assert(n > 0)
	written += n
	return
}

func (ds *dsorterGeneral) loadRemote(w io.Writer, rec *extract.Record, obj *extract.RecordObj) (int64, error) {
	var (
		cbErr      error
		beforeRecv int64
		beforeSend int64

		daemonID = rec.DaemonID
		twg      = cos.NewTimeoutGroup()
		writer   = ds.newStreamWriter(rec.MakeUniqueName(obj), w)
		metrics  = ds.m.Metrics.Creation

		toNode = ds.m.smap.GetTarget(daemonID)
	)

	if toNode == nil {
		return 0, errors.Errorf("tried to send request to node %q which is not present in the smap", daemonID)
	}

	req := remoteRequest{
		Record:    rec,
		RecordObj: obj,
	}
	opaque := cos.MustMarshal(req)
	o := transport.AllocSend()
	o.Hdr = transport.ObjHdr{Opaque: opaque}
	if ds.m.Metrics.extended {
		beforeSend = mono.NanoTime()
	}
	o.Callback = func(hdr transport.ObjHdr, r io.ReadCloser, _ any, err error) {
		if err != nil {
			cbErr = err
		}
		if ds.m.Metrics.extended {
			delta := mono.Since(beforeSend)
			metrics.mu.Lock()
			metrics.RequestStats.updateTime(delta)
			metrics.mu.Unlock()

			ds.m.ctx.stats.AddMany(
				cos.NamedVal64{Name: stats.DSortCreationReqCount, Value: 1},
				cos.NamedVal64{Name: stats.DSortCreationReqLatency, Value: int64(delta)},
			)
		}
		twg.Done()
	}

	twg.Add(1)
	if err := ds.streams.request.Send(o, nil, toNode); err != nil {
		return 0, errors.WithStack(err)
	}

	// Send should be synchronous to make sure that 'wait timeout' is
	// calculated only for the receive side.
	twg.Wait()

	if cbErr != nil {
		return 0, errors.WithStack(cbErr)
	}

	if ds.m.Metrics.extended {
		beforeRecv = mono.NanoTime()
	}

	// It may happen that the target we are trying to contact was
	// aborted or for some reason is not responding. Thus we need to do
	// some precaution and wait for the content only for limited time or
	// until we receive abort signal.
	var pulled bool
	timed, stopped := writer.wg.WaitTimeoutWithStop(ds.m.callTimeout, ds.m.listenAborted())
	if timed || stopped {
		// In case of time out or abort we need to pull the writer to
		// avoid concurrent Close and Write on `writer.w`.
		pulled = ds.pullStreamWriter(rec.MakeUniqueName(obj)) != nil
	}

	if ds.m.Metrics.extended {
		delta := mono.Since(beforeRecv)
		metrics.mu.Lock()
		metrics.ResponseStats.updateTime(delta)
		metrics.mu.Unlock()

		ds.m.ctx.stats.AddMany(
			cos.NamedVal64{Name: stats.DSortCreationRespCount, Value: 1},
			cos.NamedVal64{Name: stats.DSortCreationRespLatency, Value: int64(delta)},
		)
	}

	// If we timed out or were stopped but failed to pull the
	// writer then someone else should've done it and we barely
	// missed. In this case we should wait for the job to finish
	// (when stopped we should receive error anyway).

	if pulled { // managed to pull the writer, can safely return error
		var err error
		switch {
		case stopped:
			err = cmn.NewErrAborted("wait for remote content", "", nil)
		case timed:
			err = errors.Errorf("wait for remote content has timed out (%q was waiting for %q)",
				ds.m.ctx.node.ID(), daemonID)
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

func (ds *dsorterGeneral) errHandler(err error, node *meta.Snode, o *transport.Obj) {
	*o = transport.Obj{Hdr: o.Hdr}
	o.Hdr.Opaque = []byte(err.Error())
	o.Hdr.ObjAttrs.Size = 0
	if err = ds.streams.response.Send(o, nil, node); err != nil {
		ds.m.abort(err)
	}
}

// implements receiver i/f
func (ds *dsorterGeneral) recvReq(hdr transport.ObjHdr, objReader io.Reader, err error) error {
	ds.m.inFlightInc()
	defer ds.m.inFlightDec()

	transport.FreeRecv(objReader)
	req := remoteRequest{}
	if err := jsoniter.Unmarshal(hdr.Opaque, &req); err != nil {
		err := fmt.Errorf(cmn.FmtErrUnmarshal, DSortName, "recv request", cos.BHead(hdr.Opaque), err)
		ds.m.abort(err)
		return err
	}

	fromNode := ds.m.smap.GetTarget(hdr.SID)
	if fromNode == nil {
		err := fmt.Errorf("received request (%v) from %q not present in the %s", req.Record, hdr.SID, ds.m.smap)
		return err
	}

	if err != nil {
		ds.errHandler(err, fromNode, &transport.Obj{Hdr: hdr})
		return err
	}

	if ds.m.aborted() {
		return newDSortAbortedError(ds.m.ManagerUUID)
	}

	var (
		beforeSend int64
		o          = transport.AllocSend() // NOTE: Send(o, ...) must be called
	)
	if ds.m.Metrics.extended {
		beforeSend = mono.NanoTime()
	}
	o.Hdr = transport.ObjHdr{ObjName: req.Record.MakeUniqueName(req.RecordObj)}
	o.Callback, o.CmplArg = ds.responseCallback, beforeSend

	fullContentPath := ds.m.recm.FullContentPath(req.RecordObj)

	if ds.m.pars.DryRun {
		lr := cos.NopReader(req.RecordObj.MetadataSize + req.RecordObj.Size)
		r := cos.NopOpener(io.NopCloser(lr))
		o.Hdr.ObjAttrs.Size = req.RecordObj.MetadataSize + req.RecordObj.Size
		ds.streams.response.Send(o, r, fromNode)
		return nil
	}

	switch req.RecordObj.StoreType {
	case extract.OffsetStoreType:
		o.Hdr.ObjAttrs.Size = req.RecordObj.MetadataSize + req.RecordObj.Size
		offset := req.RecordObj.Offset - req.RecordObj.MetadataSize
		r, err := cos.NewFileSectionHandle(fullContentPath, offset, o.Hdr.ObjAttrs.Size)
		if err != nil {
			ds.errHandler(err, fromNode, o)
			return err
		}
		ds.streams.response.Send(o, r, fromNode)
	case extract.SGLStoreType:
		v, ok := ds.m.recm.RecordContents().Load(fullContentPath)
		debug.Assert(ok, fullContentPath)
		ds.m.recm.RecordContents().Delete(fullContentPath)
		sgl := v.(*memsys.SGL)
		o.Hdr.ObjAttrs.Size = sgl.Size()
		ds.streams.response.Send(o, sgl, fromNode)
	case extract.DiskStoreType:
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

func (ds *dsorterGeneral) responseCallback(hdr transport.ObjHdr, rc io.ReadCloser, x any, err error) {
	if ds.m.Metrics.extended {
		dur := mono.Since(x.(int64))
		ds.m.Metrics.Creation.mu.Lock()
		ds.m.Metrics.Creation.LocalSendStats.updateTime(dur)
		ds.m.Metrics.Creation.LocalSendStats.updateThroughput(hdr.ObjAttrs.Size, dur)
		ds.m.Metrics.Creation.mu.Unlock()
	}

	if sgl, ok := rc.(*memsys.SGL); ok {
		sgl.Free()
	}
	ds.m.decrementRef(1)
	if err != nil {
		ds.m.abort(err)
	}
}

func (ds *dsorterGeneral) postExtraction() {
	ds.mw.stopWatchingReserved()
}

func (ds *dsorterGeneral) recvResp(hdr transport.ObjHdr, object io.Reader, err error) error {
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
		return newDSortAbortedError(ds.m.ManagerUUID)
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

	var beforeSend int64
	if ds.m.Metrics.extended {
		beforeSend = mono.NanoTime()
	}

	buf, slab := mm.AllocSize(hdr.ObjAttrs.Size)
	writer.n, writer.err = io.CopyBuffer(writer.w, object, buf)
	writer.wg.Done()
	slab.Free(buf)

	if ds.m.Metrics.extended {
		metrics := ds.m.Metrics.Creation
		dur := mono.Since(beforeSend)
		metrics.mu.Lock()
		metrics.LocalRecvStats.updateTime(dur)
		metrics.LocalRecvStats.updateThroughput(writer.n, dur)
		metrics.mu.Unlock()
	}
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
	shard *extract.Shard
}

func (cs *dsgCreateShard) do() (err error) {
	lom := cluster.AllocLOM(cs.shard.Name)
	err = cs.ds.m.createShard(cs.shard, lom)
	cluster.FreeLOM(lom)
	cs.ds.creationPhase.adjuster.releaseGoroutineSema()
	return
}
