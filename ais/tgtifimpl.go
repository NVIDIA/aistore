// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/backend"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/lru"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

//
// implements cluster.Target interfaces
//

// interface guard
var _ cluster.Target = (*targetrunner)(nil)

func (t *targetrunner) Sname() string               { return t.si.String() }
func (t *targetrunner) SID() string                 { return t.si.ID() }
func (t *targetrunner) FSHC(err error, path string) { t.fsErr(err, path) }
func (t *targetrunner) MMSA() *memsys.MMSA          { return t.gmm }
func (t *targetrunner) SmallMMSA() *memsys.MMSA     { return t.smm }
func (t *targetrunner) DB() dbdriver.Driver         { return t.dbDriver }

func (t *targetrunner) Backend(bck *cluster.Bck) cluster.BackendProvider {
	if bck.Bck.IsRemoteAIS() {
		return t.cloud[cmn.ProviderAIS]
	}
	if bck.Bck.IsHTTP() {
		return t.cloud[cmn.ProviderHTTP]
	}
	providerName := bck.Provider
	if bck.Props != nil {
		// TODO: simplify logic
		providerName = bck.RemoteBck().Provider
	}
	if ext, ok := t.cloud[providerName]; ok {
		return ext
	}
	c, _ := backend.NewDummyBackend(t)
	return c
}

func (t *targetrunner) GFN(gfnType cluster.GFNType) cluster.GFN {
	switch gfnType {
	case cluster.GFNLocal:
		return &t.gfn.local
	case cluster.GFNGlobal:
		return &t.gfn.global
	}
	cmn.AssertMsg(false, "Invalid GFN type")
	return nil
}

// RunLRU is triggered by the stats evaluation of a remaining capacity, see `target_stats.go`.
func (t *targetrunner) RunLRU(id string, force bool, bcks ...cmn.Bck) {
	regToIC := id == ""
	if regToIC {
		id = cmn.GenUUID()
	}

	xlru := xreg.RenewLRU(id)
	if xlru == nil {
		return
	}

	if regToIC && xlru.ID().String() == id {
		regMsg := xactRegMsg{UUID: id, Kind: cmn.ActLRU, Srcs: []string{t.si.ID()}}
		msg := t.newAisMsg(&cmn.ActionMsg{Action: cmn.ActRegGlobalXaction, Value: regMsg}, nil, nil)
		t.bcastAsyncIC(msg)
	}

	ini := lru.InitLRU{
		T:                   t,
		Xaction:             xlru.(*lru.Xaction),
		StatsT:              t.statsT,
		Force:               force,
		Buckets:             bcks,
		GetFSUsedPercentage: ios.GetFSUsedPercentage,
		GetFSStats:          ios.GetFSStats,
	}

	xlru.AddNotif(&xaction.NotifXact{
		NotifBase: nl.NotifBase{When: cluster.UponTerm, Dsts: []string{equalIC}, F: t.callerNotifyFin},
		Xact:      xlru,
	})
	lru.Run(&ini) // Blocking call.
}

// slight variation vs t.doPut() above
func (t *targetrunner) PutObject(lom *cluster.LOM, params cluster.PutObjectParams) error {
	debug.Assert(params.Tag != "")
	workFQN := fs.CSM.GenContentFQN(lom, fs.WorkfileType, params.Tag)
	poi := allocPutObjInfo()
	{
		poi.t = t
		poi.lom = lom
		poi.r = params.Reader
		poi.workFQN = workFQN
		poi.ctx = context.Background()
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

// Send params.Reader to a given target directly.
// `params.HdrMeta` should be populated with content length, checksum, version, atime.
// `params.Reader` is closed always, even on errors.
func (t *targetrunner) sendTo(lom *cluster.LOM, params cluster.SendToParams) error {
	debug.Assert(!t.Snode().Equals(params.Tsi))
	debug.Assert(params.HdrMeta != nil)
	// TODO -- FIXME: lom-in-flight sizing here and elsewhere
	debug.Func(func() {
		if params.HdrMeta != lom {
			size := params.HdrMeta.Size()
			debug.Assertf(size > 0 || size == -1, "size=%d", size)
		}
	})
	if params.DM != nil {
		return _sendObjDM(lom, params)
	}
	err := t._sendPUT(lom, params)
	if params.Locked {
		lom.Unlock(false)
	}
	return err
}

//
// streaming send/receive via bundle.DataMover
//

// _sendObjDM requires params.HdrMeta not to be nil. Even though it uses
// transport the whole function is synchronous as callers expect that the reader
// will be sent when function finishes.
func _sendObjDM(lom *cluster.LOM, params cluster.SendToParams) error {
	cmn.Assert(params.HdrMeta != nil)
	var (
		wg = &sync.WaitGroup{}
		o  = transport.AllocSend()
		cb = func(_ transport.ObjHdr, _ io.ReadCloser, lomptr unsafe.Pointer, err error) {
			lom = (*cluster.LOM)(lomptr)
			if params.Locked {
				lom.Unlock(false)
			}
			if err != nil {
				glog.Errorf("failed to send %s => %s @ %s, err: %v", lom, params.BckTo, params.Tsi, err)
			}
			wg.Done()
		}
	)

	wg.Add(1)

	o.Hdr.FromHdrProvider(params.HdrMeta, params.ObjNameTo, params.BckTo.Bck, nil)
	o.Callback = cb
	o.CmplPtr = unsafe.Pointer(lom)
	if err := params.DM.Send(o, params.Reader, params.Tsi); err != nil {
		if params.Locked {
			lom.Unlock(false)
		}
		glog.Error(err)
		return err
	}
	wg.Wait()
	return nil
}

// _sendPUT requires params.HdrMeta not to be nil.
func (t *targetrunner) _sendPUT(lom *cluster.LOM, params cluster.SendToParams) error {
	var (
		hdr   http.Header
		query = cmn.AddBckToQuery(nil, params.BckTo.Bck)
	)
	debug.Assert(params.HdrMeta != nil)
	if params.HdrMeta == lom {
		hdr = make(http.Header, 4)
		lom.ToHTTPHdr(hdr)
	} else {
		hdr = cmn.ToHTTPHdr(params.HdrMeta)
		if size := params.HdrMeta.Size(); size > 0 {
			hdr.Set(cmn.HeaderContentLength, strconv.FormatInt(size, 10))
		}
	}
	hdr.Set(cmn.HeaderPutterID, t.si.ID())
	query.Set(cmn.URLParamRecvType, strconv.Itoa(int(cluster.Migrated)))
	reqArgs := cmn.ReqArgs{
		Method: http.MethodPut,
		Base:   params.Tsi.URL(cmn.NetworkIntraData),
		Path:   cmn.URLPathObjects.Join(params.BckTo.Name, params.ObjNameTo),
		Query:  query,
		Header: hdr,
		BodyR:  params.Reader,
	}
	req, _, cancel, err := reqArgs.ReqWithTimeout(cmn.GCO.Get().Timeout.SendFile)
	if err != nil {
		cmn.Close(params.Reader)
		return fmt.Errorf("unexpected failure to create request, err: %w", err)
	}
	defer cancel()
	resp, err := t.client.data.Do(req)
	if err != nil {
		return fmt.Errorf("failed to PUT to %s, err: %w", reqArgs.URL(), err)
	}
	cmn.DrainReader(resp.Body)
	resp.Body.Close()
	return nil
}

func (t *targetrunner) EvictObject(lom *cluster.LOM) (errCode int, err error) {
	ctx := context.Background()
	errCode, err = t.DeleteObject(ctx, lom, true /*evict*/)
	return
}

// CopyObject creates a replica of an object (the `lom` argument) in accordance with the
// `params` specification.  The destination _may_ have a different name and be located
// in a different bucket.
// There are a few possible scenarios:
// - if both src and dst LOMs are from local buckets the copying then takes place between AIS targets
//   (of this same cluster);
// - if the src is located in a cloud bucket, we always first make sure it is also present in
//   the AIS cluster (by performing a cold GET if need be).
// - if the dst is cloud, we perform a regular PUT logic thus also making sure that the new
//   replica gets created in the cloud bucket of _this_ AIS cluster.
func (t *targetrunner) CopyObject(lom *cluster.LOM, params cluster.CopyObjectParams,
	localOnly bool) (size int64, err error) {
	objNameTo := lom.ObjName
	coi := allocCopyObjInfo()
	{
		coi.CopyObjectParams = params
		coi.t = t
		coi.uncache = false
		coi.finalize = false
		coi.localOnly = localOnly
	}
	if params.ObjNameTo != "" {
		objNameTo = params.ObjNameTo
	}
	if params.DP != nil {
		return coi.copyReader(lom, objNameTo)
	}
	defer freeCopyObjInfo(coi)
	return coi.copyObject(lom, objNameTo)
}

// FIXME: recomputes checksum if called with a bad one (optimize).
func (t *targetrunner) GetCold(ctx context.Context, lom *cluster.LOM, ty cluster.GetColdType) (errCode int, err error) {
	switch ty {
	case cluster.Prefetch:
		if !lom.TryLock(true) {
			glog.Warningf("[prefetch] cold GET race, skipping (lom: %s)", lom)
			return 0, cmn.ErrSkip
		}
	case cluster.PrefetchWait:
		lom.Lock(true /*exclusive*/)
	case cluster.GetCold:
		for lom.UpgradeLock() {
			// The action was performed by some other goroutine and we don't need
			// to do it again. But we need to ensure that the operation was successful.
			if err := lom.Load(false); err != nil {
				if cmn.IsErrObjNought(err) {
					// Try to get `UpgradeLock` again and retry getting object.
					glog.Errorf("[get_cold] Object was supposed to be downloaded but doesn't exists, retrying (fqn: %q, err: %v)", lom.FQN, err)
					continue
				}
				return http.StatusBadRequest, err
			}
			if vchanged, errCode, err := t.CheckRemoteVersion(ctx, lom); err != nil {
				return errCode, err
			} else if vchanged {
				// Need to re-download the object as the version has changed.
				glog.Errorf("[get_cold] Backend provider has newer version of the object (fqn: %q)", lom.FQN)
				continue
			}
			// Object exists and version is up-to-date.
			return 0, nil
		}
	default:
		cmn.Assertf(false, "%v", ty)
	}

	if errCode, err = t.Backend(lom.Bck()).GetObj(ctx, lom); err != nil {
		lom.Unlock(true)
		glog.Errorf("[get_cold] Remote object get failed (lom: %s, err_code: %d, err: %v)", lom, errCode, err)
		return
	}

	switch ty {
	case cluster.Prefetch, cluster.PrefetchWait:
		lom.Unlock(true)
	case cluster.GetCold:
		t.statsT.AddMany(
			stats.NamedVal64{Name: stats.GetColdCount, Value: 1},
			stats.NamedVal64{Name: stats.GetColdSize, Value: lom.Size()},
		)
		lom.DowngradeLock()
	default:
		cmn.Assertf(false, "%v", ty)
	}
	return
}

// TODO: unify with ActRenameObject (refactor)
func (t *targetrunner) PromoteFile(params cluster.PromoteFileParams) (nlom *cluster.LOM, err error) {
	lom := cluster.AllocLOM(params.ObjName)
	if err = lom.Init(params.Bck.Bck); err != nil {
		return
	}
	// local or remote?
	var (
		si   *cluster.Snode
		smap = t.owner.smap.get()
	)
	if si, err = cluster.HrwTarget(lom.Uname(), &smap.Smap); err != nil {
		return
	}
	// remote; TODO: handle overwrite (lookup first)
	if si.ID() != t.si.ID() {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Attempt to promote %q => (lom: %q, target_id: %s)", params.SrcFQN, lom, si.ID())
		}
		lom.FQN = params.SrcFQN
		coi := allocCopyObjInfo()
		coi.t = t
		coi.BckTo = lom.Bck()
		sendParams := cluster.SendToParams{ObjNameTo: lom.ObjName, Tsi: si}
		_, err = coi.putRemote(lom, sendParams)
		freeCopyObjInfo(coi)
		if err == nil && !params.KeepOrig {
			if err := cmn.RemoveFile(params.SrcFQN); err != nil {
				glog.Errorf("[promote] Failed to remove source file %q, err: %v", params.SrcFQN, err)
			}
		}
		return
	}

	// local
	err = lom.Load(false)
	if err == nil && !params.Overwrite {
		// TODO: Handle the case where the object does not exist but there are two
		//  or more targets racing to override the same object with their local promotions.
		glog.Errorf("[promote] %s already exists", lom)
		return
	}
	if glog.V(4) {
		s := ""
		if params.Overwrite {
			s = " with overwrite"
		}
		glog.Infof("Attempt to promote%s %q => (lom: %q)", s, params.SrcFQN, lom)
	}

	var (
		cksum    *cmn.CksumHash
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
		info, _, err := fs.ParseMpathInfo(params.SrcFQN)
		copyFile = err != nil || info.Path != lom.MpathInfo().Path
	}
	if copyFile {
		workFQN = fs.CSM.GenContentFQN(lom, fs.WorkfileType, fs.WorkfilePut)

		buf, slab := t.gmm.Alloc()
		fileSize, cksum, err = cmn.CopyFile(params.SrcFQN, workFQN, buf, lom.CksumConf().Type)
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
			err = cmn.NewBadDataCksumError(cksum.Clone(), params.Cksum, params.SrcFQN+" => "+lom.String())
			return
		}
	}

	cmn.Assert(workFQN != "")
	poi.workFQN = workFQN
	lom.SetSize(fileSize)
	if _, err = poi.finalize(); err == nil {
		nlom = lom
		if !params.KeepOrig {
			if err := cmn.RemoveFile(params.SrcFQN); err != nil {
				glog.Errorf("[promote] Failed to remove source file %q, err: %v", params.SrcFQN, err)
			}
		}
	}
	return
}

//
// implements health.fspathDispatcher interface
//

func (t *targetrunner) DisableMountpath(mountpath, reason string) (disabled bool, err error) {
	glog.Warningf("Disabling mountpath %s: %s", mountpath, reason)
	return t.fsprg.disableMountpath(mountpath)
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
