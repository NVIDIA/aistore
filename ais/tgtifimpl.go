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

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/backend"
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
var _ cluster.Target = (*targetrunner)(nil)

func (t *targetrunner) Sname() string               { return t.si.String() }
func (t *targetrunner) SID() string                 { return t.si.ID() }
func (t *targetrunner) FSHC(err error, path string) { t.fsErr(err, path) }
func (t *targetrunner) PageMM() *memsys.MMSA        { return t.gmm }
func (t *targetrunner) ByteMM() *memsys.MMSA        { return t.smm }
func (t *targetrunner) DB() dbdriver.Driver         { return t.db }

func (t *targetrunner) Backend(bck *cluster.Bck) cluster.BackendProvider {
	if bck.Bck.IsRemoteAIS() {
		return t.backend[cmn.ProviderAIS]
	}
	if bck.Bck.IsHTTP() {
		return t.backend[cmn.ProviderHTTP]
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
func (t *targetrunner) PutObject(lom *cluster.LOM, params cluster.PutObjectParams) error {
	debug.Assert(params.Tag != "")
	workFQN := fs.CSM.GenContentFQN(lom, fs.WorkfileType, params.Tag)
	poi := allocPutObjInfo()
	{
		poi.t = t
		poi.lom = lom
		poi.r = params.Reader
		poi.workFQN = workFQN
		poi.started = params.Started
		poi.recvType = params.RecvType
		poi.skipEC = params.SkipEncode
	}
	if poi.recvType != cluster.RegularPut {
		poi.cksumToUse = params.Cksum
	}
	_, err := poi.putObject()
	freePutObjInfo(poi)
	return err
}

func (t *targetrunner) FinalizeObj(lom *cluster.LOM, workFQN string) (errCode int, err error) {
	poi := allocPutObjInfo()
	{
		poi.t = t
		poi.lom = lom
		poi.workFQN = workFQN
		poi.recvType = cluster.Finalize
	}
	errCode, err = poi.finalize()
	freePutObjInfo(poi)
	return
}

func (t *targetrunner) EvictObject(lom *cluster.LOM) (errCode int, err error) {
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
func (t *targetrunner) CopyObject(lom *cluster.LOM, params *cluster.CopyObjectParams, localOnly bool) (size int64, err error) {
	objNameTo := lom.ObjName
	coi := allocCopyObjInfo()
	{
		coi.CopyObjectParams = *params
		coi.t = t
		coi.finalize = false
		coi.localOnly = localOnly
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

// recomputes checksum if called with a bad one. TODO: optimize
func (t *targetrunner) GetCold(ctx context.Context, lom *cluster.LOM, ty cluster.GetColdType) (errCode int, err error) {
	switch ty {
	case cluster.Prefetch:
		if !lom.TryLock(true) {
			glog.Warningf("%s: skipping prefetch: %s is busy", t.si, lom)
			return 0, cmn.ErrSkip
		}
	case cluster.PrefetchWait:
		lom.Lock(true /*exclusive*/)
	case cluster.GetCold:
		for lom.UpgradeLock() {
			// The action was performed by some other goroutine and we don't need
			// to do it again. But we need to check on the object.
			if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
				glog.Errorf("%s: %s load err: %v - retrying...", t.si, lom, err)
				continue
			}
			return 0, nil
		}
	default:
		debug.Assertf(false, "%v", ty)
		return
	}

	if errCode, err = t.Backend(lom.Bck()).GetObj(ctx, lom); err != nil {
		lom.Unlock(true)
		glog.Errorf("%s: failed to GET remote %s: %v(%d)", t.si, lom.FullName(), err, errCode)
		return
	}

	switch ty {
	case cluster.Prefetch, cluster.PrefetchWait:
		lom.Unlock(true)
	case cluster.GetCold:
		t.statsT.AddMany(
			cos.NamedVal64{Name: stats.GetColdCount, Value: 1},
			cos.NamedVal64{Name: stats.GetColdSize, Value: lom.SizeBytes()},
		)
		lom.DowngradeLock()
	default:
		debug.Assertf(false, "get-cold-type=%d", ty)
	}
	return
}

func (t *targetrunner) PromoteFile(params cluster.PromoteFileParams) (nlom *cluster.LOM, err error) {
	var (
		tsi   *cluster.Snode
		smap  = t.owner.smap.get()
		lom   = cluster.AllocLOM(params.ObjName)
		local bool
	)
	if err = lom.Init(params.Bck.Bck); err != nil {
		cluster.FreeLOM(lom)
		return
	}
	if tsi, local, err = lom.HrwTarget(&smap.Smap); err != nil {
		cluster.FreeLOM(lom)
		return
	}
	// TODO: handle overwrite (lookup first)
	// TODO: FreeLOM
	if !local {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Attempt to promote %q => (lom: %q, target_id: %s)", params.SrcFQN, lom, tsi.ID())
		}
		lom.FQN = params.SrcFQN
		coi := allocCopyObjInfo()
		{
			coi.t = t
			coi.promoteFile = true
			coi.BckTo = lom.Bck()
		}
		sendParams := allocSendParams()
		{
			sendParams.ObjNameTo = lom.ObjName
			sendParams.Tsi = tsi
		}
		_, err = coi.putRemote(lom, sendParams)
		freeSendParams(sendParams)
		freeCopyObjInfo(coi)
		if err == nil && !params.KeepOrig {
			if err := cos.RemoveFile(params.SrcFQN); err != nil {
				glog.Errorf("[promote] Failed to remove source file %q, err: %v", params.SrcFQN, err)
			}
		}
		return
	}

	// local; NOTE: cluster.FreeLOM(lom) by the caller
	err = lom.Load(false /*cache it*/, false /*locked*/)
	if err == nil && !params.Overwrite {
		glog.Errorf("[promote] %s already exists", lom)
		return
	}
	var (
		cksum    *cos.CksumHash
		fileSize int64
		workFQN  string
		poi      = &putObjInfo{t: t, lom: lom}
	)
	copyFile := params.KeepOrig
	if !params.KeepOrig {
		// To use `params.SrcFQN` as `workFQN` we must be sure that they are on
		// the same device. Right now, we do it by ensuring that they are on the
		// same mountpath but this is stronger assumption.
		//
		// TODO: Try to determine if `params.SrcFQN` and `dstFQN` are on the device
		//  without requiring it to be on the same mountpath.
		info, _, err := fs.FQN2Mpath(params.SrcFQN)
		copyFile = err != nil || info.Path != lom.MpathInfo().Path
	}
	if copyFile {
		workFQN = fs.CSM.GenContentFQN(lom, fs.WorkfileType, fs.WorkfilePut)

		buf, slab := t.gmm.Alloc()
		fileSize, cksum, err = cos.CopyFile(params.SrcFQN, workFQN, buf, lom.CksumConf().Type)
		slab.Free(buf)
		if err != nil {
			return
		}
		lom.SetCksum(cksum.Clone())
	} else {
		workFQN = params.SrcFQN // Use the file as it would be intermediate (work) file.

		var fi os.FileInfo
		if fi, err = os.Stat(params.SrcFQN); err != nil {
			return
		}
		fileSize = fi.Size()

		if params.Cksum != nil {
			// Checksum already computed somewhere else.
			lom.SetCksum(params.Cksum)
		} else {
			clone := lom.Clone(params.SrcFQN)
			if cksum, err = clone.ComputeCksum(); err != nil {
				return
			}
			lom.SetCksum(cksum.Clone())
			cluster.FreeLOM(clone)
		}
	}
	if params.Cksum != nil && cksum != nil {
		if !cksum.Equal(params.Cksum) {
			err = cos.NewBadDataCksumError(cksum.Clone(), params.Cksum, params.SrcFQN+" => "+lom.String())
			return
		}
	}

	poi.workFQN = workFQN
	lom.SetSize(fileSize)
	if _, err = poi.finalize(); err == nil {
		nlom = lom
		if !params.KeepOrig {
			if err := cos.RemoveFile(params.SrcFQN); err != nil {
				glog.Errorf("[promote] Failed to remove source file %q, err: %v", params.SrcFQN, err)
			}
		}
	}
	return
}

//
// implements health.fspathDispatcher interface
//

func (t *targetrunner) DisableMpath(mpath, reason string) (err error) {
	glog.Warningf("Disabling mountpath %s: %s", mpath, reason)
	_, err = t.fsprg.disableMpath(mpath, false /*dont-resilver*/)
	return
}

func (t *targetrunner) RebalanceNamespace(si *cluster.Snode) (b []byte, status int, err error) {
	// pull the data
	query := url.Values{}
	query.Set(cmn.URLParamRebData, "true")
	args := callArgs{
		si: si,
		req: cmn.ReqArgs{
			Method: http.MethodGet,
			Base:   si.URL(cmn.NetworkIntraData),
			Path:   cmn.URLPathRebalance.S,
			Query:  query,
		},
		timeout: cmn.DefaultTimeout,
	}
	res := t.call(args)
	b, status, err = res.bytes, res.status, res.err
	_freeCallRes(res)
	return
}
