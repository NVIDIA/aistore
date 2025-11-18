// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/dsort/shard"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// This implementation of dsorter focuses on creation phase and maximizing
// memory usage in that phase. It has an active push mechanism which instead
// of waiting for requests, it sends all the record objects it has for the
// shard other target is building. The requirement for using this dsorter
// implementation is a lot of memory available. In creation phase target
// needs to have enough memory to build a given shard all in memory otherwise
// it would be easy to deadlock when targets would send the record objects in
// incorrect order.

const (
	MemType = "dsort_mem"
)

type (
	rwConnection struct {
		r   io.Reader
		wgr *cos.TimeoutGroup
		// In case the reader is first to connect, the data is copied into SGL
		// so that the reader will not block on the connection.
		sgl *memsys.SGL

		w   io.Writer
		wgw *sync.WaitGroup

		n int64
	}

	rwConnector struct {
		mu          sync.Mutex
		m           *Manager
		connections map[string]*rwConnection
	}

	dsorterMem struct {
		m             *Manager
		streams       dsortStreams
		creationPhase struct {
			connector   *rwConnector // used to connect readers (streams, local data) with writers (shards)
			reqShardsCh chan string

			adjuster struct {
				read  *concAdjuster
				write *concAdjuster
			}
		}
	}
)

// interface guard
var _ dsorter = (*dsorterMem)(nil)

func newRWConnection(r io.Reader, w io.Writer) *rwConnection {
	debug.Assert(r != nil || w != nil)
	wgr := cos.NewTimeoutGroup()
	wgr.Add(1)
	wgw := &sync.WaitGroup{}
	wgw.Add(1)
	return &rwConnection{
		r:   r,
		w:   w,
		wgr: wgr,
		wgw: wgw,
	}
}

func newRWConnector(m *Manager) *rwConnector {
	return &rwConnector{
		m:           m,
		connections: make(map[string]*rwConnection, 1000),
	}
}

func (c *rwConnector) free() {
	c.mu.Lock()
	for _, v := range c.connections {
		if v.sgl != nil {
			v.sgl.Free()
		}
	}
	c.mu.Unlock()
}

func (c *rwConnector) connect(key string, r io.Reader, w io.Writer) (rwc *rwConnection, all bool) {
	var ok bool

	if rwc, ok = c.connections[key]; !ok {
		rwc = newRWConnection(r, w)
		c.connections[key] = rwc
	} else {
		if rwc.r == nil {
			rwc.r = r
		}
		if rwc.w == nil {
			rwc.w = w
		}
	}
	all = rwc.r != nil && rwc.w != nil
	return
}

func (c *rwConnector) connectReader(key string, r io.Reader, size int64) (err error) {
	c.mu.Lock()
	rw, all := c.connect(key, r, nil)
	c.mu.Unlock()

	if !all {
		rw.sgl = g.mem.NewSGL(size)
		_, err = io.Copy(rw.sgl, r)
		rw.wgr.Done()
		return
	}

	rw.wgr.Done()
	rw.wgw.Wait()
	return
}

func (c *rwConnector) connectWriter(key string, w io.Writer) (int64, error) {
	c.mu.Lock()
	rw, all := c.connect(key, nil, w)
	c.mu.Unlock()
	defer rw.wgw.Done() // inform the reader that the copying has finished

	timed, stopped := rw.wgr.WaitTimeoutWithStop(c.m.callTimeout, c.m.listenAborted()) // wait for reader
	if timed {
		return 0, errors.Errorf("%s: timed out waiting for remote content", core.T)
	}
	if stopped {
		return 0, errors.Errorf("%s: aborted waiting for remote content", core.T)
	}

	if all { // reader connected and left SGL with the content
		n, err := io.Copy(rw.w, rw.sgl)
		rw.sgl.Free()
		rw.sgl = nil
		return n, err
	}

	n, err := io.CopyBuffer(rw.w, rw.r, nil)
	rw.n = n
	return n, err
}

func newDsorterMem(m *Manager) *dsorterMem {
	return &dsorterMem{
		m: m,
	}
}

func (*dsorterMem) name() string { return MemType }

func (ds *dsorterMem) init(config *cmn.Config) error {
	ds.creationPhase.connector = newRWConnector(ds.m)
	ds.creationPhase.reqShardsCh = make(chan string, max(1024, config.Dsort.Burst))

	ds.creationPhase.adjuster.read = newConcAdjuster(
		ds.m.Pars.CreateConcMaxLimit,
		1, /*goroutineLimitCoef*/
	)
	ds.creationPhase.adjuster.write = newConcAdjuster(
		ds.m.Pars.CreateConcMaxLimit,
		1, /*goroutineLimitCoef*/
	)
	return nil
}

func (ds *dsorterMem) start() error {
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
	return nil
}

func (ds *dsorterMem) cleanupStreams() (err error) {
	return ds.m.cleanupDsortStreams(&ds.streams)
}

func (*dsorterMem) cleanup() {}

func (ds *dsorterMem) finalCleanup() error {
	err := ds.cleanupStreams()
	close(ds.creationPhase.reqShardsCh)
	ds.creationPhase.connector.free()
	ds.creationPhase.connector = nil
	return err
}

func (*dsorterMem) postRecordDistribution() {}

func (ds *dsorterMem) preShardCreation(shardName string, mi *fs.Mountpath) error {
	bsi := &buildingShardInfo{
		shardName: shardName,
	}
	o := transport.AllocSend()
	o.Hdr.Opaque = bsi.NewPack(core.T.ByteMM())
	if ds.m.smap.HasActiveTs(core.T.SID() /*except*/) {
		if err := ds.streams.request.Send(o, nil); err != nil {
			return err
		}
	}
	ds.creationPhase.reqShardsCh <- shardName // we also need to inform ourselves
	ds.creationPhase.adjuster.write.acquireSema(mi)
	return nil
}

func (ds *dsorterMem) postShardCreation(mi *fs.Mountpath) {
	ds.creationPhase.adjuster.write.releaseSema(mi)
}

func (ds *dsorterMem) Load(w io.Writer, rec *shard.Record, obj *shard.RecordObj) (int64, error) {
	if ds.m.aborted() {
		return 0, ds.m.newErrAborted()
	}
	return ds.creationPhase.connector.connectWriter(rec.MakeUniqueName(obj), w)
}

// createShardsLocally waits until it's given the signal to start creating
// shards, then creates shards in parallel.
func (ds *dsorterMem) createShardsLocally() error {
	phaseInfo := &ds.m.creationPhase

	ds.creationPhase.adjuster.read.start()
	ds.creationPhase.adjuster.write.start()

	metrics := ds.m.Metrics.Creation
	metrics.begin()
	metrics.mu.Lock()
	metrics.ToCreate = int64(len(phaseInfo.metadata.Shards))
	metrics.mu.Unlock()

	var (
		mem    sys.MemStat
		wg     = &sync.WaitGroup{}
		errCh  = make(chan error, 2)
		stopCh = &cos.StopCh{}
	)
	stopCh.Init()

	// cleanup
	defer func(metrics *ShardCreation, stopCh *cos.StopCh) {
		stopCh.Close()
		metrics.finish()
		ds.creationPhase.adjuster.write.stop()
		ds.creationPhase.adjuster.read.stop()
	}(metrics, stopCh)

	if err := mem.Get(); err != nil {
		return err
	}
	maxMemoryToUse := calcMaxMemoryUsage(ds.m.Pars.MaxMemUsage, &mem)
	sa := newInmemShardAllocator(maxMemoryToUse - mem.ActualUsed)

	// read
	wg.Go(func() {
		ds.localRead(stopCh, errCh)
	})

	// write
	wg.Go(func() {
		ds.localWrite(sa, stopCh, errCh)
	})

	wg.Wait()

	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (ds *dsorterMem) localRead(stopCh *cos.StopCh, errCh chan error) {
	var (
		phaseInfo  = &ds.m.creationPhase
		group, ctx = errgroup.WithContext(context.Background())
	)
outer:
	for {
		// If that was the last shard to send we need to break and we will
		// be waiting for the result.
		if len(phaseInfo.metadata.SendOrder) == 0 {
			break outer
		}

		select {
		case shardName := <-ds.creationPhase.reqShardsCh:
			shard, ok := phaseInfo.metadata.SendOrder[shardName]
			if !ok {
				break
			}

			ds.creationPhase.adjuster.read.acquireGoroutineSema()
			es := &dsmExtractShard{ds, shard}
			group.Go(es.do)

			delete(phaseInfo.metadata.SendOrder, shardName)
		case <-ds.m.listenAborted():
			stopCh.Close()
			group.Wait()
			errCh <- ds.m.newErrAborted()
			return
		case <-ctx.Done(): // context was canceled, therefore we have an error
			stopCh.Close()
			break outer
		case <-stopCh.Listen(): // writing side stopped we need to do the same
			break outer
		}
	}

	errCh <- group.Wait()
}

func (ds *dsorterMem) localWrite(sa *inmemShardAllocator, stopCh *cos.StopCh, errCh chan error) {
	var (
		phaseInfo  = &ds.m.creationPhase
		group, ctx = errgroup.WithContext(context.Background())
	)
outer:
	for _, s := range phaseInfo.metadata.Shards {
		select {
		case <-ds.m.listenAborted():
			stopCh.Close()
			group.Wait()
			errCh <- ds.m.newErrAborted()
			return
		case <-ctx.Done(): // context was canceled, therefore we have an error
			stopCh.Close()
			break outer
		case <-stopCh.Listen():
			break outer // reading side stopped we need to do the same
		default:
		}

		sa.alloc(uint64(s.Size))

		ds.creationPhase.adjuster.write.acquireGoroutineSema()
		cs := &dsmCreateShard{ds, s, sa}
		group.Go(cs.do)
	}

	errCh <- group.Wait()
}

func (ds *dsorterMem) connectOrSend(rec *shard.Record, obj *shard.RecordObj, tsi *meta.Snode) error {
	debug.Assert(core.T.SID() == rec.DaemonID, core.T.SID()+" vs "+rec.DaemonID)
	var (
		resp = &dsmCS{
			ds:  ds,
			tsi: tsi,
			rsp: RemoteResponse{Record: rec, RecordObj: obj},
		}
		fullContentPath = ds.m.recm.FullContentPath(obj)
	)

	ct, err := core.NewDsortCT(&ds.m.Pars.OutputBck, fullContentPath)
	if err != nil {
		return err
	}

	ds.creationPhase.adjuster.read.acquireSema(ct.Mountpath())
	defer func() {
		if !resp.decRef {
			ds.m.decrementRef(1)
		}
		ds.creationPhase.adjuster.read.releaseSema(ct.Mountpath())
	}()

	if ds.m.aborted() {
		return ds.m.newErrAborted()
	}

	resp.hdr.Opaque = cos.MustMarshal(resp.rsp)
	if ds.m.Pars.DryRun {
		lr := cos.NopReader(obj.MetadataSize + obj.Size)
		r := cos.NopOpener(io.NopCloser(lr))
		resp.hdr.ObjAttrs.Size = obj.MetadataSize + obj.Size
		return resp.connectOrSend(r)
	}

	switch obj.StoreType {
	case shard.OffsetStoreType:
		resp.hdr.ObjAttrs.Size = obj.MetadataSize + obj.Size
		r, err := cos.NewFileSectionHandle(fullContentPath, obj.Offset-obj.MetadataSize, resp.hdr.ObjAttrs.Size)
		if err != nil {
			return err
		}
		return resp.connectOrSend(r)
	case shard.DiskStoreType:
		f, err := cos.NewFileHandle(fullContentPath)
		if err != nil {
			return err
		}
		fi, err := f.Stat()
		if err != nil {
			cos.Close(f)
			return err
		}
		resp.hdr.ObjAttrs.Size = fi.Size()
		return resp.connectOrSend(f)
	default:
		debug.Assert(false, obj.StoreType)
		return nil
	}
}

func (ds *dsorterMem) sentCallback(_ *transport.ObjHdr, rc io.ReadCloser, x any, err error) {
	if sgl, ok := rc.(*memsys.SGL); ok {
		sgl.Free()
	}
	ds.m.decrementRef(1)
	if err != nil {
		req := x.(*RemoteResponse)
		nlog.Errorf("%s: [dsort] %s failed to send remore-rsp %s: %v - aborting...",
			core.T, ds.m.ManagerUUID, req.Record.MakeUniqueName(req.RecordObj), err)
		ds.m.abort(err)
	}
}

func (*dsorterMem) postExtraction() {}

// implements receiver i/f
// (note: ObjHdr and its fields must be consumed synchronously)
func (ds *dsorterMem) recvReq(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
	ds.m.inFlightInc()
	defer func() {
		transport.DrainAndFreeReader(objReader)
		ds.m.inFlightDec()
	}()

	if err != nil {
		ds.m.abort(err)
		return err
	}

	unpacker := cos.NewUnpacker(hdr.Opaque)
	req := buildingShardInfo{}
	if err := unpacker.ReadAny(&req); err != nil {
		ds.m.abort(err)
		return err
	}

	if ds.m.aborted() {
		return ds.m.newErrAborted()
	}

	ds.creationPhase.reqShardsCh <- req.shardName
	return nil
}

// (note: ObjHdr and its fields must be consumed synchronously)
func (ds *dsorterMem) recvResp(hdr *transport.ObjHdr, object io.Reader, err error) error {
	ds.m.inFlightInc()
	defer func() {
		transport.DrainAndFreeReader(object)
		ds.m.inFlightDec()
	}()

	if err != nil {
		ds.m.abort(err)
		return err
	}

	req := RemoteResponse{}
	if err := jsoniter.Unmarshal(hdr.Opaque, &req); err != nil {
		ds.m.abort(err)
		return err
	}

	if ds.m.aborted() {
		return ds.m.newErrAborted()
	}

	uname := req.Record.MakeUniqueName(req.RecordObj)
	if err := ds.creationPhase.connector.connectReader(uname, object, hdr.ObjAttrs.Size); err != nil {
		ds.m.abort(err)
		return err
	}

	return nil
}

func (*dsorterMem) preShardExtraction(uint64) bool { return true }
func (*dsorterMem) postShardExtraction(uint64)     {}
func (ds *dsorterMem) onAbort()                    { _ = ds.cleanupStreams() }

////////////////////
// dsmCreateShard //
////////////////////

type dsmCreateShard struct {
	ds    *dsorterMem
	shard *shard.Shard
	sa    *inmemShardAllocator
}

func (cs *dsmCreateShard) do() (err error) {
	lom := core.AllocLOM(cs.shard.Name)
	err = cs.ds.m.createShard(cs.shard, lom)
	core.FreeLOM(lom)
	cs.ds.creationPhase.adjuster.write.releaseGoroutineSema()
	cs.sa.free(uint64(cs.shard.Size))
	return
}

////////////////////
// dsmExtractShard //
////////////////////

type dsmExtractShard struct {
	ds    *dsorterMem
	shard *shard.Shard
}

func (es *dsmExtractShard) do() error {
	ds, shard := es.ds, es.shard
	defer ds.creationPhase.adjuster.read.releaseGoroutineSema()

	bck := meta.NewBck(ds.m.Pars.OutputBck.Name, ds.m.Pars.OutputBck.Provider, cmn.NsGlobal)
	if err := bck.Init(core.T.Bowner()); err != nil {
		return err
	}
	smap := core.T.Sowner().Get()
	tsi, err := smap.HrwName2T(bck.MakeUname(shard.Name))
	if err != nil {
		return err
	}
	for _, rec := range shard.Records.All() {
		for _, obj := range rec.Objects {
			if err := ds.connectOrSend(rec, obj, tsi); err != nil {
				return err
			}
		}
	}
	return nil
}

///////////
// dsmCS //
///////////

type dsmCS struct {
	ds     *dsorterMem
	tsi    *meta.Snode
	rsp    RemoteResponse
	hdr    transport.ObjHdr
	decRef bool
}

func (resp *dsmCS) connectOrSend(r cos.ReadOpenCloser) (err error) {
	if resp.tsi.ID() == core.T.SID() {
		uname := resp.rsp.Record.MakeUniqueName(resp.rsp.RecordObj)
		err = resp.ds.creationPhase.connector.connectReader(uname, r, resp.hdr.ObjAttrs.Size)
		cos.Close(r)
	} else {
		o := transport.AllocSend()
		o.Hdr = resp.hdr
		o.SentCB, o.CmplArg = resp.ds.sentCallback, &resp.rsp
		err = resp.ds.streams.response.Send(o, r, resp.tsi)
		resp.decRef = true // sentCallback will call decrementRef
	}
	return
}
