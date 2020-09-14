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
	"time"

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
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
)

//
// implements cluster.Target interfaces
//

var _ cluster.Target = &targetrunner{}

func (t *targetrunner) FSHC(err error, path string) { t.fshc(err, path) }
func (t *targetrunner) GetMMSA() *memsys.MMSA       { return t.gmm }
func (t *targetrunner) GetSmallMMSA() *memsys.MMSA  { return t.smm }
func (t *targetrunner) GetFSPRG() fs.PathRunGroup   { return &t.fsprg }
func (t *targetrunner) GetDB() dbdriver.Driver      { return t.dbDriver }

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
		providerName = bck.BackendBck().Provider
	}
	if ext, ok := t.cloud[providerName]; ok {
		return ext
	}
	c, _ := cloud.NewDummyCloud(t)
	return c
}

func (t *targetrunner) GetGFN(gfnType cluster.GFNType) cluster.GFN {
	switch gfnType {
	case cluster.GFNLocal:
		return &t.gfn.local
	case cluster.GFNGlobal:
		return &t.gfn.global
	}
	cmn.AssertMsg(false, "Invalid GFN type")
	return nil
}

func (t *targetrunner) GetXactRegistry() cluster.XactRegistry {
	return xaction.Registry
}

// gets triggered by the stats evaluation of a remaining capacity
// and then runs in a goroutine - see stats package, target_stats.go
func (t *targetrunner) RunLRU(id string, force bool, bcks ...cmn.Bck) {
	regToIC := id == ""
	if regToIC {
		id = cmn.GenUUID()
	}

	xlru := xaction.Registry.RenewLRU(id)
	if xlru == nil {
		return
	}

	if regToIC && xlru.ID().String() == id {
		t.registerIC(xactRegMsg{UUID: id, Kind: cmn.ActLRU, Scrs: []string{t.si.ID()}})
	}

	ini := lru.InitLRU{
		T:                   t,
		Xaction:             xlru,
		StatsT:              t.statsT,
		Force:               force,
		Buckets:             bcks,
		GetFSUsedPercentage: ios.GetFSUsedPercentage,
		GetFSStats:          ios.GetFSStats,
	}

	xlru.AddNotif(&cmn.NotifXact{
		NotifBase: cmn.NotifBase{
			When: cmn.UponTerm,
			Ty:   notifXact,
			Dsts: []string{equalIC},
			F:    t.xactCallerNotify,
		},
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
	err, _ := goi.getObject()
	return err
}

// slight variation vs t.doPut() above
func (t *targetrunner) PutObject(lom *cluster.LOM, params cluster.PutObjectParams) error {
	poi := &putObjInfo{
		t:       t,
		lom:     lom,
		r:       params.Reader,
		workFQN: params.WorkFQN,
		ctx:     context.Background(),
		started: params.Started,
		skipEC:  params.SkipEncode,
	}
	if params.RecvType == cluster.Migrated {
		poi.cksumToCheck = params.Cksum
		poi.migrated = true
	} else if params.RecvType == cluster.ColdGet {
		poi.cold = true
		poi.cksumToCheck = params.Cksum
	}
	var err error
	if params.WithFinalize {
		err, _ = poi.putObject()
	} else {
		err = poi.writeToFile()
	}
	return err
}

// Send params.Reader to a given target directly.
// Header should be populated with content length, checksum, version, atime.
// Reader is closed always, even on errors.
func (t *targetrunner) SendTo(params cluster.SendToParams) error {
	debug.Assert(!t.Snode().Equals(params.Tsi))

	query := url.Values{}
	query = cmn.AddBckToQuery(query, params.BckTo.Bck)
	query.Add(cmn.URLParamRecvType, strconv.Itoa(int(cluster.Migrated)))

	params.Header.Set(cmn.HeaderPutterID, t.si.ID())

	reqArgs := cmn.ReqArgs{
		Method: http.MethodPut,
		Base:   params.Tsi.URL(cmn.NetworkIntraData),
		Path:   cmn.URLPath(cmn.Version, cmn.Objects, params.BckTo.Name, params.ObjNameTo),
		Query:  query,
		Header: params.Header,
		BodyR:  params.Reader,
	}
	req, _, cancel, err := reqArgs.ReqWithTimeout(cmn.GCO.Get().Timeout.SendFile)
	if err != nil {
		errc := params.Reader.Close()
		debug.AssertNoErr(errc)
		err = fmt.Errorf("unexpected failure to create request, err: %w", err)
		return err
	}
	defer cancel()
	resp, err := t.httpclientGetPut.Do(req)
	if err != nil {
		return fmt.Errorf("failed to PUT to %s, err: %w", reqArgs.URL(), err)
	}
	if resp != nil && resp.Body != nil {
		errc := resp.Body.Close()
		debug.AssertNoErr(errc)
	}
	return nil
}

func (t *targetrunner) EvictObject(lom *cluster.LOM) error {
	ctx := context.Background()
	err, _ := t.objDelete(ctx, lom, true /*evict*/)
	return err
}

func (t *targetrunner) CopyObject(lom *cluster.LOM, params cluster.CopyObjectParams, localOnly ...bool) (copied bool, err error) {
	coi := &copyObjInfo{
		CopyObjectParams: params,
		t:                t,
		uncache:          false,
		finalize:         false,
	}
	if len(localOnly) > 0 {
		coi.localOnly = localOnly[0]
	}
	copied, err = coi.copyObject(lom, lom.ObjName)
	return
}

// FIXME: recomputes checksum if called with a bad one (optimize)
func (t *targetrunner) GetCold(ctx context.Context, lom *cluster.LOM, prefetch bool) (err error, errCode int) {
	if prefetch {
		if !lom.TryLock(true) {
			glog.Infof("prefetch: cold GET race: %s - skipping", lom)
			return cmn.ErrSkip, 0
		}
	} else {
		lom.Lock(true) // one cold-GET at a time
	}
	var (
		workFQN = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfileColdget)
	)
	if err, errCode = t.Cloud(lom.Bck()).GetObj(ctx, workFQN, lom); err != nil {
		lom.Unlock(true)
		glog.Errorf("%s: GET failed %d, err: %v", lom, errCode, err)
		return
	}
	defer func() {
		if err != nil {
			lom.Unlock(true)
			if errRemove := cmn.RemoveFile(workFQN); errRemove != nil {
				glog.Errorf("Nested error %s => (remove %s => err: %v)", err, workFQN, errRemove)
				t.fshc(errRemove, workFQN)
			}
		}
	}()
	if err = cmn.Rename(workFQN, lom.FQN); err != nil {
		err = fmt.Errorf("unexpected failure to rename %s => %s, err: %v", workFQN, lom.FQN, err)
		t.fshc(err, lom.FQN)
		return
	}
	if err = lom.Persist(); err != nil {
		return
	}
	lom.ReCache()

	// NOTE: GET - downgrade and keep the lock, PREFETCH - unlock
	if prefetch {
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
		_, err = coi.putRemote(lom, sendParams)
		return
	}

	// local
	err = lom.Load(false)
	if err == nil && !params.Overwrite {
		err = fmt.Errorf("%s already exists", lom)
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
	err, _ = poi.finalize()
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
			Path:   cmn.URLPath(cmn.Version, cmn.Rebalance),
			Query:  query,
		},
		timeout: cmn.DefaultTimeout,
	}
	res := t.call(args)
	return res.bytes, res.status, res.err
}
