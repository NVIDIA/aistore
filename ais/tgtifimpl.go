// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/cloud"
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

func (t *targetrunner) FSHC(err error, path string) { t.fsErr(err, path) }
func (t *targetrunner) MMSA() *memsys.MMSA          { return t.gmm }
func (t *targetrunner) SmallMMSA() *memsys.MMSA     { return t.smm }
func (t *targetrunner) DB() dbdriver.Driver         { return t.dbDriver }

func (t *targetrunner) Cloud(bck *cluster.Bck) cluster.CloudProvider {
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
	c, _ := cloud.NewDummyCloud(t)
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

// gets triggered by the stats evaluation of a remaining capacity
// and then runs in a goroutine - see stats package, target_stats.go
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
		t.bcastToIC(msg, false /*wait*/)
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
	lru.Run(&ini) // blocking

	xlru.Finish()
}

// slight variation vs t.httpobjget()
func (t *targetrunner) GetObject(w io.Writer, lom *cluster.LOM, started time.Time) error {
	goi := &getObjInfo{
		started: started,
		t:       t,
		lom:     lom,
		w:       w,
		ctx:     context.Background(),
	}
	_, err := goi.getObject()
	return err
}

// slight variation vs t.doPut() above
func (t *targetrunner) PutObject(lom *cluster.LOM, params cluster.PutObjectParams) (string, error) {
	debug.Assert(params.Tag != "")
	workFQN := fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, params.Tag)
	poi := &putObjInfo{
		t:       t,
		lom:     lom,
		r:       params.Reader,
		workFQN: workFQN,
		ctx:     context.Background(),
		started: params.Started,
		skipEC:  params.SkipEncode,
	}
	if params.RecvType == cluster.Migrated {
		poi.migrated = true
		poi.cksumToUse = params.Cksum
	} else if params.RecvType == cluster.ColdGet {
		poi.cold = true
		poi.cksumToUse = params.Cksum
	}
	var err error
	if params.WithFinalize {
		_, err = poi.putObject()
	} else {
		err = poi.writeToFile()
	}
	return workFQN, err
}

// Send params.Reader to a given target directly.
// `params.HdrMeta` should be populated with content length, checksum, version, atime.
// `params.Reader` is closed always, even on errors.
func (t *targetrunner) sendTo(lom *cluster.LOM, params cluster.SendToParams) error {
	debug.Assert(!t.Snode().Equals(params.Tsi))
	cmn.Assert(params.HdrMeta != nil)

	if params.HdrMeta.Size() < -1 {
		return fmt.Errorf("[%s/%s] content length of %d not supported, -1 or greater required", lom.BckName(), lom.ObjName, params.HdrMeta.Size())
	}

	if params.DM != nil {
		return _sendObjDM(lom, params)
	}
	err := t._sendPUT(params)
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

func (t *targetrunner) _recvObjDM(w http.ResponseWriter, hdr transport.ObjHdr, objReader io.Reader, err error) {
	if err != nil {
		glog.Error(err)
		return
	}
	defer cmn.DrainReader(objReader)
	lom := &cluster.LOM{T: t, ObjName: hdr.ObjName}
	if err := lom.Init(hdr.Bck); err != nil {
		glog.Error(err)
		return
	}
	lom.SetAtimeUnix(hdr.ObjAttrs.Atime)
	lom.SetVersion(hdr.ObjAttrs.Version)

	params := cluster.PutObjectParams{
		Tag:          fs.WorkfilePut,
		Reader:       ioutil.NopCloser(objReader),
		RecvType:     cluster.Migrated,
		Cksum:        cmn.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue),
		Started:      time.Now(),
		WithFinalize: true,
	}
	if _, err := t.PutObject(lom, params); err != nil {
		glog.Error(err)
	}
}

// _sendPUT requires params.HdrMeta not to be nil.
func (t *targetrunner) _sendPUT(params cluster.SendToParams) error {
	cmn.Assert(params.HdrMeta != nil)
	var (
		query = cmn.AddBckToQuery(nil, params.BckTo.Bck)
		hdr   = cmn.ToHTTPHdr(params.HdrMeta)
	)

	hdr.Set(cmn.HeaderPutterID, t.si.ID())
	query.Add(cmn.URLParamRecvType, strconv.Itoa(int(cluster.Migrated)))
	reqArgs := cmn.ReqArgs{
		Method: http.MethodPut,
		Base:   params.Tsi.URL(cmn.NetworkIntraData),
		Path:   cmn.JoinWords(cmn.Version, cmn.Objects, params.BckTo.Name, params.ObjNameTo),
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
	resp, err := t.httpclientGetPut.Do(req)
	if err != nil {
		return fmt.Errorf("failed to PUT to %s, err: %w", reqArgs.URL(), err)
	}
	cmn.DrainReader(resp.Body)
	resp.Body.Close()
	return nil
}

func (t *targetrunner) EvictObject(lom *cluster.LOM) error {
	ctx := context.Background()
	_, err := t.DeleteObject(ctx, lom, true /*evict*/)
	return err
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
func (t *targetrunner) CopyObject(lom *cluster.LOM, params cluster.CopyObjectParams, localOnly bool) (copied bool, size int64, err error) {
	var (
		coi = &copyObjInfo{
			CopyObjectParams: params,
			t:                t,
			uncache:          false,
			finalize:         false,
			localOnly:        localOnly,
		}

		objNameTo = lom.ObjName
	)
	if params.ObjNameTo != "" {
		objNameTo = params.ObjNameTo
	}
	if params.DP != nil {
		return coi.copyReader(lom, objNameTo)
	}

	copied, err = coi.copyObject(lom, objNameTo)
	return copied, lom.Size(), err
}

// FIXME: recomputes checksum if called with a bad one (optimize)
func (t *targetrunner) GetCold(ctx context.Context, lom *cluster.LOM, ty cluster.GetColdType) (errCode int, err error) {
	if ty == cluster.Prefetch {
		if !lom.TryLock(true) {
			glog.Infof("prefetch: cold GET race: %s - skipping", lom)
			return 0, cmn.ErrSkip
		}
	} else {
		lom.Lock(true) // one cold-GET at a time
	}
	var workFQN string
	if workFQN, errCode, err = t.Cloud(lom.Bck()).GetObj(ctx, lom); err != nil {
		lom.Unlock(true)
		glog.Errorf("%s: GET failed %d, err: %v", lom, errCode, err)
		return
	}
	defer func() {
		if err != nil {
			lom.Unlock(true)
			if errRemove := cmn.RemoveFile(workFQN); errRemove != nil {
				glog.Errorf("Nested error %s => (remove %s => err: %v)", err, workFQN, errRemove)
				t.fsErr(errRemove, workFQN)
			}
		}
	}()
	if err = cmn.Rename(workFQN, lom.FQN); err != nil {
		err = fmt.Errorf("unexpected failure to rename %s => %s, err: %v", workFQN, lom.FQN, err)
		t.fsErr(err, lom.FQN)
		return
	}
	if err = lom.Persist(); err != nil {
		return
	}
	lom.ReCache()

	// NOTE: GET - downgrade and keep the lock, PREFETCH - unlock
	if ty == cluster.Prefetch || ty == cluster.PrefetchWait {
		lom.Unlock(true)
	} else {
		t.statsT.AddMany(
			stats.NamedVal64{Name: stats.GetColdCount, Value: 1},
			stats.NamedVal64{Name: stats.GetColdSize, Value: lom.Size()},
		)
		lom.DowngradeLock()
	}
	return
}

// TODO: unify with ActRenameObject (refactor)
func (t *targetrunner) PromoteFile(params cluster.PromoteFileParams) (nlom *cluster.LOM, err error) {
	lom := &cluster.LOM{T: t, ObjName: params.ObjName}
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
		if params.Verbose {
			glog.Infof("promote/PUT %s => %s @ %s", params.SrcFQN, lom, si.ID())
		}
		lom.FQN = params.SrcFQN
		var (
			coi        = &copyObjInfo{t: t}
			sendParams = cluster.SendToParams{ObjNameTo: lom.ObjName, Tsi: si}
		)
		coi.BckTo = lom.Bck()
		_, _, err = coi.putRemote(lom, sendParams)
		if err == nil && !params.KeepOrig {
			os.Remove(params.SrcFQN)
		}
		return
	}

	// local
	err = lom.Load(false)
	if err == nil && !params.Overwrite {
		// TODO: handle the case where the object does not exist but there are
		// two or more targets racing to override the same object with their local promotions
		glog.Errorf("%s already exists", lom)
		return
	}
	if params.Verbose {
		s := ""
		if params.Overwrite {
			s = "+"
		}
		glog.Infof("promote%s %s => %s", s, params.SrcFQN, lom)
	}
	var (
		cksum   *cmn.CksumHash
		fi      os.FileInfo
		written int64
		workFQN string
		poi     = &putObjInfo{t: t, lom: lom}
		conf    = lom.CksumConf()
	)
	if params.KeepOrig {
		workFQN = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut)

		buf, slab := t.gmm.Alloc()
		written, cksum, err = cmn.CopyFile(params.SrcFQN, workFQN, buf, conf.Type)
		slab.Free(buf)
		if err != nil {
			return
		}
		lom.SetCksum(cksum.Clone())
	} else {
		workFQN = params.SrcFQN // use the file as it would be intermediate (work) file
		fi, err = os.Stat(params.SrcFQN)
		if err != nil {
			return
		}
		written = fi.Size()

		if params.Cksum != nil {
			// Checksum already computed somewhere else.
			lom.SetCksum(params.Cksum)
		} else {
			clone := lom.Clone(params.SrcFQN)
			if cksum, err = clone.ComputeCksum(); err != nil {
				return
			}
			lom.SetCksum(cksum.Clone())
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
	lom.SetSize(written)
	_, err = poi.finalize()
	if err == nil {
		nlom = lom
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

func (t *targetrunner) RebalanceNamespace(si *cluster.Snode) ([]byte, int, error) {
	// pull the data
	query := url.Values{}
	query.Add(cmn.URLParamRebData, "true")
	args := callArgs{
		si: si,
		req: cmn.ReqArgs{
			Method: http.MethodGet,
			Base:   si.URL(cmn.NetworkIntraData),
			Path:   cmn.JoinWords(cmn.Version, cmn.Rebalance),
			Query:  query,
		},
		timeout: cmn.DefaultTimeout,
	}
	res := t.call(args)
	return res.bytes, res.status, res.err
}
