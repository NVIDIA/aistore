// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/backend"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
)

// interface guard
var _ cluster.Target = (*target)(nil)

func (t *target) FSHC(err error, path string) { t.fsErr(err, path) }
func (t *target) PageMM() *memsys.MMSA        { return t.gmm }
func (t *target) ByteMM() *memsys.MMSA        { return t.smm }
func (t *target) DB() dbdriver.Driver         { return t.db }

func (t *target) Backend(bck *cluster.Bck) cluster.BackendProvider {
	if bck.IsRemoteAIS() {
		return t.backend[apc.ProviderAIS]
	}
	if bck.IsHTTP() {
		return t.backend[apc.ProviderHTTP]
	}
	provider := bck.Provider
	if bck.Props != nil {
		provider = bck.RemoteBck().Provider
	}
	if ext, ok := t.backend[provider]; ok {
		return ext
	}
	c, _ := backend.NewDummyBackend(t)
	return c
}

// essentially, t.doPut() for external use
func (t *target) PutObject(lom *cluster.LOM, params *cluster.PutObjectParams) error {
	debug.Assert(params.WorkTag != "" && !params.Atime.IsZero())
	workFQN := fs.CSM.Gen(lom, fs.WorkfileType, params.WorkTag)
	poi := allocPutObjInfo()
	{
		poi.t = t
		poi.lom = lom
		poi.r = params.Reader
		poi.workFQN = workFQN
		poi.atime = params.Atime
		poi.xctn = params.Xact
		poi.owt = params.OWT
		poi.skipEC = params.SkipEncode
	}
	if poi.owt != cmn.OwtPut {
		poi.cksumToUse = params.Cksum
	}
	_, err := poi.putObject()
	freePutObjInfo(poi)
	return err
}

func (t *target) FinalizeObj(lom *cluster.LOM, workFQN string, xctn cluster.Xact) (errCode int, err error) {
	poi := allocPutObjInfo()
	{
		poi.t = t
		poi.lom = lom
		poi.workFQN = workFQN
		poi.owt = cmn.OwtFinalize
		poi.xctn = xctn
	}
	errCode, err = poi.finalize()
	freePutObjInfo(poi)
	return
}

func (t *target) EvictObject(lom *cluster.LOM) (errCode int, err error) {
	errCode, err = t.DeleteObject(lom, true /*evict*/)
	return
}

// CopyObject creates either a full replica of an object (the `lom` argument)
//     - or -
// transforms the object and places it at the destination, in accordance with the `params`.
//
// The destination _may_ have a different name and _may_ be located in a different bucket.
// Scenarios include (but are not limited to):
// - if both src and dst LOMs are from local buckets the copying then takes place between AIS targets
//   (of this same cluster);
// - if the src is located in a remote bucket, we always first make sure it is also present in
//   the AIS cluster (by performing a cold GET if need be).
// - if the dst is cloud, we perform a regular PUT logic thus also making sure that the new
//   replica gets created in the cloud bucket of _this_ AIS cluster.
func (t *target) CopyObject(lom *cluster.LOM, params *cluster.CopyObjectParams, dryRun bool) (size int64, err error) {
	objNameTo := lom.ObjName
	coi := allocCopyObjInfo()
	{
		coi.CopyObjectParams = *params
		coi.t = t
		coi.owt = cmn.OwtMigrate
		coi.finalize = false
		coi.dryRun = dryRun
	}
	if params.ObjNameTo != "" {
		objNameTo = params.ObjNameTo
	}
	if params.DP != nil { // NOTE: w/ transformation
		return coi.copyReader(lom, objNameTo)
	}
	size, err = coi.copyObject(lom, objNameTo)
	freeCopyObjInfo(coi)
	return
}

func (t *target) GetCold(ctx context.Context, lom *cluster.LOM, owt cmn.OWT) (errCode int, err error) {
	// 1. lock
	switch owt {
	case cmn.OwtGetPrefetchLock:
		// do nothing
	case cmn.OwtGetTryLock, cmn.OwtGetLock:
		if owt == cmn.OwtGetTryLock {
			if !lom.TryLock(true) {
				if glog.FastV(4, glog.SmoduleAIS) {
					glog.Warningf("%s: %s(%s) is busy", t, lom, owt)
				}
				return 0, cmn.ErrSkip // e.g. prefetch can skip it and keep on going
			}
		} else {
			lom.Lock(true)
		}
	case cmn.OwtGet:
		for lom.UpgradeLock() {
			// The action was performed by some other goroutine and we don't need
			// to do it again. But we need to check on the object.
			if err := lom.Load(true /*cache it*/, true /*locked*/); err != nil {
				glog.Errorf("%s: %s load err: %v - retrying...", t, lom, err)
				continue
			}
			return 0, nil
		}
	default:
		debug.Assert(false)
		return
	}

	// 2. get from remote
	if errCode, err = t.Backend(lom.Bck()).GetObj(ctx, lom, owt); err != nil {
		if owt != cmn.OwtGetPrefetchLock {
			lom.Unlock(true)
		}
		glog.Errorf("%s: failed to GET remote %s (%s): %v(%d)", t, lom.FullName(), owt, err, errCode)
		return
	}

	// 3. unlock or downgrade
	switch owt {
	case cmn.OwtGetPrefetchLock:
		// do nothing
	case cmn.OwtGetTryLock, cmn.OwtGetLock:
		lom.Unlock(true)
	case cmn.OwtGet:
		if err = lom.Load(true /*cache it*/, true /*locked*/); err == nil {
			t.statsT.AddMany(
				cos.NamedVal64{Name: stats.GetColdCount, Value: 1},
				cos.NamedVal64{Name: stats.GetColdSize, Value: lom.SizeBytes()},
			)
			lom.DowngradeLock()
		} else {
			errCode = http.StatusInternalServerError
			lom.Unlock(true)
			glog.Errorf("%s: unexpected failure to load %s (%s): %v", t, lom.FullName(), owt, err)
		}
	}
	return
}

func (t *target) Promote(params cluster.PromoteParams) (errCode int, err error) {
	lom := cluster.AllocLOM(params.ObjName)
	defer cluster.FreeLOM(lom)
	if eri := lom.InitBck(params.Bck.Bucket()); eri != nil {
		return 0, eri
	}
	smap := t.owner.smap.get()
	tsi, local, erh := lom.HrwTarget(&smap.Smap)
	if erh != nil {
		return 0, erh
	}
	if local {
		errCode, err = t.promoteLocal(&params, lom)
	} else {
		err = t.promoteRemote(&params, lom, tsi)
	}
	if err != nil {
		return
	}
	if params.DeleteSrc {
		if errRm := cos.RemoveFile(params.SrcFQN); errRm != nil {
			glog.Errorf("%s: failed to remove promoted source %q: %v", t, params.SrcFQN, errRm)
		}
	}
	return
}

func (t *target) promoteLocal(params *cluster.PromoteParams, lom *cluster.LOM) (int, error) {
	var (
		cksum     *cos.CksumHash
		fileSize  int64
		workFQN   string
		extraCopy = true
	)
	if err := lom.Load(true /*cache it*/, false /*locked*/); err == nil && !params.OverwriteDst {
		return 0, nil
	}
	if params.DeleteSrc {
		// To use `params.SrcFQN` as `workFQN`, make sure both are
		// located on the same filesystem. About "filesystem sharing" see also:
		// * https://github.com/NVIDIA/aistore/blob/master/docs/overview.md#terminology
		mi, _, err := fs.FQN2Mpath(params.SrcFQN)
		extraCopy = err != nil || !mi.FilesystemInfo.Equal(lom.MpathInfo().FilesystemInfo)
	}
	if extraCopy {
		var err error
		workFQN = fs.CSM.Gen(lom, fs.WorkfileType, fs.WorkfilePut)
		buf, slab := t.gmm.Alloc()
		fileSize, cksum, err = cos.CopyFile(params.SrcFQN, workFQN, buf, lom.CksumConf().Type)
		slab.Free(buf)
		if err != nil {
			return 0, err
		}
		lom.SetCksum(cksum.Clone())
	} else {
		// avoid extra copy: use the source as `workFQN`
		fi, err := os.Stat(params.SrcFQN)
		if err != nil {
			if os.IsNotExist(err) {
				err = nil
			}
			return 0, err
		}
		fileSize = fi.Size()
		workFQN = params.SrcFQN
		if params.Cksum != nil {
			lom.SetCksum(params.Cksum) // already computed somewhere else, use it
		} else {
			clone := lom.CloneMD(params.SrcFQN)
			if cksum, err = clone.ComputeCksum(); err != nil {
				return 0, err
			}
			lom.SetCksum(cksum.Clone())
			cluster.FreeLOM(clone)
		}
	}
	if params.Cksum != nil && cksum != nil {
		if !cksum.Equal(params.Cksum) {
			detail := params.SrcFQN + " => " + lom.String()
			return 0, cos.NewBadDataCksumError(cksum.Clone(), params.Cksum, detail)
		}
	}
	poi := allocPutObjInfo()
	{
		poi.atime = time.Now()
		poi.t = t
		poi.lom = lom
		poi.workFQN = workFQN
		poi.owt = cmn.OwtPromote
		poi.xctn = params.Xact
	}
	lom.SetSize(fileSize)
	errCode, err := poi.finalize()
	freePutObjInfo(poi)
	if err != nil {
		return errCode, err
	}
	if params.Xact != nil {
		params.Xact.ObjsAdd(1, fileSize)
	}
	return 0, nil
}

// TODO: use DM streams
// TODO: Xact.InObjsAdd on the receive side
func (t *target) promoteRemote(params *cluster.PromoteParams, lom *cluster.LOM, tsi *cluster.Snode) error {
	lom.FQN = params.SrcFQN
	// when not overwriting check w/ remote target first (and separately)
	if !params.OverwriteDst && t.HeadObjT2T(lom, tsi) {
		return nil
	}
	coi := allocCopyObjInfo()
	{
		coi.t = t
		coi.BckTo = lom.Bck()
		coi.owt = cmn.OwtPromote
		coi.Xact = params.Xact
	}
	size, err := coi.sendRemote(lom, lom.ObjName, tsi)
	freeCopyObjInfo(coi)
	if err != nil {
		return err
	}
	if params.Xact != nil {
		params.Xact.OutObjsAdd(1, size)
	}
	return nil
}

//
// implements health.fspathDispatcher interface
//

func (t *target) DisableMpath(mpath, reason string) (err error) {
	glog.Warningf("Disabling mountpath %s: %s", mpath, reason)
	_, err = t.fsprg.disableMpath(mpath, true /*dont-resilver*/) // NOTE: not resilvering upon FSCH calling
	return
}

func (t *target) RebalanceNamespace(si *cluster.Snode) (b []byte, status int, err error) {
	// pull the data
	query := url.Values{}
	query.Set(apc.QparamRebData, "true")
	cargs := allocCargs()
	{
		cargs.si = si
		cargs.req = cmn.HreqArgs{
			Method: http.MethodGet,
			Base:   si.URL(cmn.NetIntraData),
			Path:   apc.URLPathRebalance.S,
			Query:  query,
		}
		cargs.timeout = apc.DefaultTimeout
	}
	res := t.call(cargs)
	b, status, err = res.bytes, res.status, res.err
	freeCargs(cargs)
	freeCR(res)
	return
}
