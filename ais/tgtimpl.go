// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/NVIDIA/aistore/ais/backend"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact/xreg"
)

func (*target) DataClient() *http.Client { return g.client.data }

func (*target) GetAllRunning(inout *core.AllRunningInOut, periodic bool) {
	xreg.GetAllRunning(inout, periodic)
}

func (t *target) Health(si *meta.Snode, timeout time.Duration, query url.Values) ([]byte, int, error) {
	return t.reqHealth(si, timeout, query, t.owner.smap.get())
}

func (t *target) Backend(bck *meta.Bck) core.Backend {
	if bck.IsRemoteAIS() {
		return t.backend[apc.AIS]
	}
	if bck.IsHTTP() {
		return t.backend[apc.HTTP]
	}
	provider := bck.Provider
	if bck.Props != nil {
		provider = bck.RemoteBck().Provider
	}
	config := cmn.GCO.Get()
	if _, ok := config.Backend.Providers[provider]; ok {
		bp, k := t.backend[provider]
		debug.Assert(k, provider)
		if bp != nil {
			return bp
		}
		// nil when configured & not-built
	}
	c, _ := backend.NewDummyBackend(t, nil)
	return c
}

func (t *target) PutObject(lom *core.LOM, params *core.PutParams) error {
	debug.Assert(params.WorkTag != "" && !params.Atime.IsZero())
	workFQN := fs.CSM.Gen(lom, fs.WorkfileType, params.WorkTag)
	poi := allocPOI()
	{
		poi.t = t
		poi.lom = lom
		poi.config = cmn.GCO.Get()
		poi.r = params.Reader
		poi.workFQN = workFQN
		poi.atime = params.Atime.UnixNano()
		poi.xctn = params.Xact
		poi.size = params.Size
		poi.owt = params.OWT
		poi.skipEC = params.SkipEC
		poi.coldGET = params.ColdGET
	}
	if poi.owt != cmn.OwtPut {
		poi.cksumToUse = params.Cksum
	}
	_, err := poi.putObject()
	freePOI(poi)
	debug.Assert(err != nil || params.Size <= 0 || params.Size == lom.Lsize(true), lom.String(), params.Size, lom.Lsize(true))
	return err
}

func (t *target) FinalizeObj(lom *core.LOM, workFQN string, xctn core.Xact, owt cmn.OWT) (ecode int, err error) {
	if err = cos.Stat(workFQN); err != nil {
		return
	}
	poi := allocPOI()
	{
		poi.t = t
		poi.atime = time.Now().UnixNano()
		poi.lom = lom
		poi.workFQN = workFQN
		poi.owt = owt
		poi.xctn = xctn
	}
	ecode, err = poi.finalize()
	freePOI(poi)
	return
}

func (t *target) EvictObject(lom *core.LOM) (ecode int, err error) {
	ecode, err = t.DeleteObject(lom, true /*evict*/)
	return
}

func (t *target) HeadObjT2T(lom *core.LOM, si *meta.Snode) bool {
	return t.headt2t(lom, si, t.owner.smap.get())
}

// CopyObject:
// - either creates a full replica of the source object (the `lom` argument)
// - or transforms the object
//
// In both cases, the result is placed at the `params`-defined destination
// in accordance with the configured destination bucket policies.
//
// Destination object _may_ have a different name and _may_ be located in a different bucket.
//
// Scenarios include (but are not limited to):
//   - if both src and dst LOMs are from local buckets the copying then takes place between AIS targets
//     (of this same cluster);
//   - if the src is located in a remote bucket, we always first make sure it is also present in
//     the AIS cluster (by performing a cold GET if need be).
//   - if the dst is cloud, we perform a regular PUT logic thus also making sure that the new
//     replica gets created in the cloud bucket of _this_ AIS cluster.
func (t *target) CopyObject(lom *core.LOM, dm core.DM, params *core.CopyParams) (size int64, err error) {
	coi := (*copyOI)(params)
	// defaults
	coi.OWT = cmn.OwtCopy
	coi.Finalize = false
	if coi.ObjnameTo == "" {
		coi.ObjnameTo = lom.ObjName
	}
	realDM, ok := dm.(*bundle.DataMover) // TODO -- FIXME: eliminate typecast
	debug.Assert(ok)

	size, err = coi.do(t, realDM, lom)

	coi.stats(size, err)
	return size, err
}

// use `backend.GetObj` (compare w/ other instances calling `backend.GetObjReader`)
func (t *target) GetCold(ctx context.Context, lom *core.LOM, owt cmn.OWT) (ecode int, err error) {
	// 1. lock
	switch owt {
	case cmn.OwtGetPrefetchLock:
		// do nothing
	case cmn.OwtGetTryLock, cmn.OwtGetLock:
		if owt == cmn.OwtGetTryLock {
			if !lom.TryLock(true) {
				if cmn.Rom.FastV(4, cos.SmoduleAIS) {
					nlog.Warningf("%s: %s(%s) is busy", t, lom, owt)
				}
				return 0, cmn.ErrSkip // e.g. prefetch can skip it and keep on going
			}
		} else {
			lom.Lock(true)
		}
	default:
		// for cmn.OwtGet, see goi.getCold
		debug.Assert(false, owt.String())
		return http.StatusInternalServerError, errors.New("invalid " + owt.String())
	}

	// 2. GET remote object and store it
	var (
		now     = mono.NanoTime()
		backend = t.Backend(lom.Bck())
	)
	if ecode, err = backend.GetObj(ctx, lom, owt, nil /*origReq*/); err != nil {
		if owt != cmn.OwtGetPrefetchLock {
			lom.Unlock(true)
		}
		nlog.Infoln(t.String()+":", "failed to GET remote", lom.Cname()+":", err, ecode)
		return ecode, err
	}

	// 3. unlock
	switch owt {
	case cmn.OwtGetPrefetchLock:
		// do nothing
	case cmn.OwtGetTryLock, cmn.OwtGetLock:
		lom.Unlock(true)
	}

	// 4. stats
	t.coldstats(backend, lom.Lsize(), now)
	return 0, nil
}

func (t *target) coldstats(backend core.Backend, size, started int64) {
	t.statsT.AddMany(
		cos.NamedVal64{Name: backend.MetricName(stats.GetCount), Value: 1},
		cos.NamedVal64{Name: backend.MetricName(stats.GetLatencyTotal), Value: mono.SinceNano(started)},
		cos.NamedVal64{Name: backend.MetricName(stats.GetSize), Value: size},
	)
}

func (t *target) GetColdBlob(params *core.BlobParams, oa *cmn.ObjAttrs) (xctn core.Xact, err error) {
	debug.Assert(params.Lom != nil)
	debug.Assert(params.Msg != nil)
	_, xctn, err = t.blobdl(params, oa)
	return xctn, err
}

func (t *target) Promote(params *core.PromoteParams) (ecode int, err error) {
	lom := core.AllocLOM(params.ObjName)
	if err = lom.InitBck(params.Bck.Bucket()); err == nil {
		ecode, err = t._promote(params, lom)
	}
	core.FreeLOM(lom)
	return
}

func (t *target) _promote(params *core.PromoteParams, lom *core.LOM) (ecode int, err error) {
	smap := t.owner.smap.get()
	tsi, local, erh := lom.HrwTarget(&smap.Smap)
	if erh != nil {
		return 0, erh
	}
	var size int64
	if local {
		size, ecode, err = t._promLocal(params, lom)
	} else {
		size, err = t._promRemote(params, lom, tsi, smap)
		if err == nil && size >= 0 && params.Xact != nil {
			params.Xact.OutObjsAdd(1, size)
		}
	}
	if err != nil {
		return
	}
	if size >= 0 && params.Xact != nil {
		params.Xact.ObjsAdd(1, size) // (as initiator)
	}
	if params.DeleteSrc {
		if errRm := cos.RemoveFile(params.SrcFQN); errRm != nil {
			nlog.Errorf("%s: failed to remove promoted source %q: %v", t, params.SrcFQN, errRm)
		}
	}
	return
}

func (t *target) _promLocal(params *core.PromoteParams, lom *core.LOM) (fileSize int64, ecode int, err error) {
	var (
		cksum     *cos.CksumHash
		workFQN   string
		extraCopy = true
	)
	fileSize = -1

	if err = lom.Load(true /*cache it*/, false /*locked*/); err == nil && !params.OverwriteDst {
		return
	}
	if params.DeleteSrc {
		// To use `params.SrcFQN` as `workFQN`, make sure both are
		// located on the same filesystem. About "filesystem sharing" see also:
		// * https://github.com/NVIDIA/aistore/blob/main/docs/overview.md#terminology
		mi, _, err := fs.FQN2Mpath(params.SrcFQN)
		extraCopy = err != nil || !mi.FS.Equal(lom.Mountpath().FS)
	}
	if extraCopy {
		workFQN = fs.CSM.Gen(lom, fs.WorkfileType, fs.WorkfilePut)
		buf, slab := t.gmm.Alloc()
		fileSize, cksum, err = cos.CopyFile(params.SrcFQN, workFQN, buf, lom.CksumType())
		slab.Free(buf)
		if err != nil {
			return
		}
		lom.SetCksum(cksum.Clone())
	} else {
		// avoid extra copy: use the source as `workFQN`
		var fi os.FileInfo
		fi, err = os.Stat(params.SrcFQN)
		if err != nil {
			if os.IsNotExist(err) {
				err = nil
			}
			return
		}

		fileSize = fi.Size()
		workFQN = params.SrcFQN
		if params.Cksum != nil {
			lom.SetCksum(params.Cksum) // already computed somewhere else, use it
		} else {
			clone := lom.CloneMD(params.SrcFQN)
			if cksum, err = clone.ComputeCksum(lom.CksumType()); err != nil {
				core.FreeLOM(clone)
				return
			}
			lom.SetCksum(cksum.Clone())
			core.FreeLOM(clone)
		}
	}
	if params.Cksum != nil && cksum != nil {
		if !cksum.Equal(params.Cksum) {
			err = cos.NewErrDataCksum(
				cksum.Clone(),
				params.Cksum,
				params.SrcFQN+" => "+lom.String() /*detail*/)
			return
		}
	}
	poi := allocPOI()
	{
		poi.atime = time.Now().UnixNano()
		poi.t = t
		poi.config = params.Config
		poi.lom = lom
		poi.workFQN = workFQN
		poi.owt = cmn.OwtPromote
		poi.xctn = params.Xact
	}
	lom.SetSize(fileSize)
	ecode, err = poi.finalize()
	freePOI(poi)
	return
}

// TODO: use DM streams
// TODO: Xact.InObjsAdd on the receive side
func (t *target) _promRemote(params *core.PromoteParams, lom *core.LOM, tsi *meta.Snode, smap *smapX) (int64, error) {
	lom.FQN = params.SrcFQN

	// when not overwriting check w/ remote target first (and separately)
	if !params.OverwriteDst && t.headt2t(lom, tsi, smap) {
		return -1, nil
	}

	coiParams := core.AllocCOI()
	{
		coiParams.BckTo = lom.Bck()
		coiParams.OWT = cmn.OwtPromote
		coiParams.Xact = params.Xact
		coiParams.Config = params.Config
	}
	coi := (*copyOI)(coiParams)
	size, err := coi.send(t, nil /*DM*/, lom, lom.ObjName, tsi)
	core.FreeCOI(coiParams)

	return size, err
}
