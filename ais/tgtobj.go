// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"encoding"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
)

type (
	putObjInfo struct {
		started time.Time // started time of receiving - used to calculate the recv duration
		t       *targetrunner
		lom     *cluster.LOM
		// Reader with the content of the object.
		r io.ReadCloser
		// Checksum which needs to be checked on receive. It is only checked
		// on specific occasions: see `writeToFile` method.
		cksumToCheck *cmn.Cksum
		// object size aka Content-Length
		size int64
		// Context used when putting the object which should be contained in
		// cloud bucket. It usually contains credentials to access the cloud.
		ctx context.Context
		// FQN which is used only temporarily for receiving file. After
		// successful receive is renamed to actual FQN.
		workFQN string
		// Determines if the object was already in cluster and is being PUT due to
		// migration or replication of some sort.
		migrated bool
		// Determines if the recv is cold recv: either from another cluster or cloud.
		cold bool
		// if true, poi won't erasure-encode an object when finalizing
		skipEC bool
	}

	getObjInfo struct {
		started time.Time // started time of receiving - used to calculate the recv duration
		t       *targetrunner
		lom     *cluster.LOM
		// Writer where the object will be written.
		w io.Writer
		// Context used when receiving the object which is contained in cloud
		// bucket. It usually contains credentials to access the cloud.
		ctx context.Context
		// Contains object range query
		ranges cmn.RangesQuery
		// Determines if it is GFN request
		isGFN bool
		// true: chunked transfer (en)coding as per https://tools.ietf.org/html/rfc7230#page-36
		chunked bool
	}

	// Contains information packed in append handle.
	handleInfo struct {
		nodeID       string
		filePath     string
		partialCksum *cmn.CksumHash
	}

	appendObjInfo struct {
		started time.Time // started time of receiving - used to calculate the recv duration
		t       *targetrunner
		lom     *cluster.LOM

		// Reader with the content of the object.
		r io.ReadCloser
		// Object size aka Content-Length.
		size int64
		// Append/Flush operation.
		op string
		hi handleInfo // Information contained in handle.

		cksum *cmn.Cksum // Expected checksum of the final object.
	}

	writerOnly struct{ io.Writer }
)

////////////////
// PUT OBJECT //
////////////////

func (poi *putObjInfo) putObject() (err error, errCode int) {
	lom := poi.lom
	// optimize out if the checksums do match
	if poi.cksumToCheck != nil {
		if lom.Cksum().Equal(poi.cksumToCheck) {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s is valid %s: PUT is a no-op", lom, poi.cksumToCheck)
			}
			cmn.DrainReader(poi.r)
			return nil, 0
		}
	}

	if !daemon.dryRun.disk {
		if err := poi.writeToFile(); err != nil {
			return err, http.StatusInternalServerError
		}
		if err, errCode := poi.finalize(); err != nil {
			return err, errCode
		}
	}
	if !poi.migrated && !poi.cold {
		delta := time.Since(poi.started)
		poi.t.statsT.AddMany(
			stats.NamedVal64{Name: stats.PutCount, Value: 1},
			stats.NamedVal64{Name: stats.PutLatency, Value: int64(delta)},
		)
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("PUT %s: %d µs", lom, int64(delta/time.Microsecond))
		}
	}
	return nil, 0
}

func (poi *putObjInfo) finalize() (err error, errCode int) {
	if err, errCode = poi.tryFinalize(); err != nil {
		if err1 := fs.Access(poi.workFQN); err1 == nil || !os.IsNotExist(err1) {
			if err1 == nil {
				err1 = err
			}
			poi.t.fshc(err1, poi.workFQN)
			if err2 := cmn.RemoveFile(poi.workFQN); err2 != nil {
				glog.Errorf("Nested error: %s => (remove %s => err: %v)", err1, poi.workFQN, err2)
			}
		}
		poi.lom.Uncache()
		return
	}
	if !poi.skipEC {
		if ecErr := ec.ECM.EncodeObject(poi.lom); ecErr != nil && ecErr != ec.ErrorECDisabled {
			err = ecErr
			return
		}
	}

	poi.t.putMirror(poi.lom)
	return
}

// poi.workFQN => LOM
func (poi *putObjInfo) tryFinalize() (err error, errCode int) {
	var (
		lom = poi.lom
		bck = lom.Bck()
	)
	if bck.IsRemote() && !poi.migrated {
		var version string
		if bck.IsCloud() || bck.IsHTTP() { // TODO: should `HTTP` be part of `IsCloud`?!
			version, err, errCode = poi.putCloud()
		} else {
			version, err, errCode = poi.putRemoteAIS()
		}
		if err != nil {
			glog.Errorf("%s: PUT failed, err: %v", lom, err)
			return
		}
		if lom.VerConf().Enabled {
			lom.SetVersion(version)
		}
	}

	// check if bucket was destroyed while PUT was in the progress.
	var (
		bmd        = poi.t.owner.bmd.Get()
		_, present = bmd.Get(bck)
	)
	if !present {
		err = fmt.Errorf("%s: PUT failed, bucket %s does not exist", lom, bck)
		errCode = http.StatusBadRequest
		return
	}

	lom.Lock(true)
	defer lom.Unlock(true)

	if bck.IsAIS() && lom.VerConf().Enabled && !poi.migrated {
		if err = lom.IncVersion(); err != nil {
			return
		}
	}
	if err := cmn.Rename(poi.workFQN, lom.FQN); err != nil {
		return fmt.Errorf("rename failed => %s: %w", lom, err), 0
	}
	if lom.HasCopies() {
		if err = lom.DelAllCopies(); err != nil {
			return
		}
	}
	if err = lom.Persist(); err != nil {
		return
	}
	lom.ReCache()
	return
}

func (poi *putObjInfo) putCloud() (ver string, err error, errCode int) {
	var (
		lom = poi.lom
		bck = lom.Bck()
	)
	file, errOpen := os.Open(poi.workFQN)
	if errOpen != nil {
		err = fmt.Errorf("failed to open %s err: %w", poi.workFQN, errOpen)
		return
	}

	cloud := poi.t.Cloud(bck)
	customMD := cmn.SimpleKVs{
		cluster.SourceObjMD: cloud.Provider(),
	}

	ver, err, errCode = cloud.PutObj(poi.ctx, file, lom)
	if ver != "" {
		customMD[cluster.VersionObjMD] = ver
	}
	lom.SetCustomMD(customMD)
	debug.AssertNoErr(file.Close())
	return
}

func (poi *putObjInfo) putRemoteAIS() (ver string, err error, errCode int) {
	var (
		lom = poi.lom
		bck = lom.Bck()
	)
	cmn.Assert(bck.IsRemoteAIS())
	fh, errOpen := cmn.NewFileHandle(poi.workFQN) // Closed by `PutObj`.
	if errOpen != nil {
		err = fmt.Errorf("failed to open %s err: %w", poi.workFQN, errOpen)
		return
	}
	ver, err, errCode = poi.t.Cloud(bck).PutObj(poi.ctx, fh, lom)
	return
}

// NOTE: LOM is updated on the end of the call with proper size and checksum.
// NOTE: `roi.r` is closed on the end of the call.
func (poi *putObjInfo) writeToFile() (err error) {
	var (
		written int64
		file    *os.File
		buf     []byte
		slab    *memsys.Slab
		reader  = poi.r
		writer  io.Writer
		writers = make([]io.Writer, 0, 4)
		cksums  = struct {
			store *cmn.CksumHash // store with LOM
			given *cmn.CksumHash // compute additionally
			expct *cmn.Cksum     // and validate against `expct` if required/available
		}{}
		conf = poi.lom.CksumConf()
	)
	if daemon.dryRun.disk {
		return
	}
	if file, err = poi.lom.CreateFile(poi.workFQN); err != nil {
		return
	}
	writer = file
	if poi.size == 0 {
		buf, slab = poi.t.gmm.Alloc()
	} else {
		buf, slab = poi.t.gmm.Alloc(poi.size)
	}
	// cleanup
	defer func() { // free & cleanup on err
		slab.Free(buf)
		debug.AssertNoErr(reader.Close())

		if err != nil {
			if nestedErr := file.Close(); nestedErr != nil {
				glog.Errorf("Nested (%v): failed to close received object %s, err: %v", err, poi.workFQN, nestedErr)
			}
			if nestedErr := cmn.RemoveFile(poi.workFQN); nestedErr != nil {
				glog.Errorf("Nested (%v): failed to remove %s, err: %v", err, poi.workFQN, nestedErr)
			}
		}
	}()
	// checksums
	if conf.Type == cmn.ChecksumNone {
		goto write
	}
	if poi.cold {
		// compute checksum and save it as part of the object metadata
		cksums.store = cmn.NewCksumHash(conf.Type)
		writers = append(writers, cksums.store.H)
		// if validate-cold-get and the cksum is provided we should also check md5 hash (aws, gcp)
		if conf.ValidateColdGet && poi.cksumToCheck != nil && poi.cksumToCheck.Type() != cmn.ChecksumNone {
			cksums.expct = poi.cksumToCheck
			cksums.given = cmn.NewCksumHash(poi.cksumToCheck.Type())
			writers = append(writers, cksums.given.H)
		}
	} else {
		if !poi.migrated || conf.ValidateObjMove {
			cksums.store = cmn.NewCksumHash(conf.Type)
			writers = append(writers, cksums.store.H)
			if poi.cksumToCheck != nil && poi.cksumToCheck.Type() != cmn.ChecksumNone {
				cksums.expct = poi.cksumToCheck
				cksums.given = cmn.NewCksumHash(poi.cksumToCheck.Type())
				writers = append(writers, cksums.given.H)
			}
		} else {
			// if migration validation is not configured we can just take
			// the checksum that has arrived with the object (and compute it if not present)
			poi.lom.SetCksum(poi.cksumToCheck)
			if poi.cksumToCheck == nil || poi.cksumToCheck.Type() == cmn.ChecksumNone {
				cksums.store = cmn.NewCksumHash(conf.Type)
				writers = append(writers, cksums.store.H)
			}
		}
	}
write:
	if len(writers) == 0 {
		written, err = io.CopyBuffer(writer, reader, buf)
	} else {
		writers = append(writers, writer)
		written, err = io.CopyBuffer(cmn.NewWriterMulti(writers...), reader, buf)
	}
	if err != nil {
		return
	}
	// validate
	if cksums.given != nil {
		cksums.given.Finalize()
		if !cksums.given.Equal(cksums.expct) {
			err = cmn.NewBadDataCksumError(cksums.expct, &cksums.given.Cksum, poi.lom.String())
			poi.t.statsT.AddMany(
				stats.NamedVal64{Name: stats.ErrCksumCount, Value: 1},
				stats.NamedVal64{Name: stats.ErrCksumSize, Value: written},
			)
			return
		}
	}
	// ok
	poi.lom.SetSize(written)
	if cksums.store != nil {
		cksums.store.Finalize()
		poi.lom.SetCksum(&cksums.store.Cksum)
	} else {
		poi.lom.SetCksum(cmn.NewCksum(cmn.ChecksumNone, ""))
	}
	if err = file.Close(); err != nil {
		return fmt.Errorf("failed to close received file %s, err: %w", poi.workFQN, err)
	}
	return nil
}

////////////////
// GET OBJECT //
////////////////

func (goi *getObjInfo) getObject() (err error, errCode int) {
	var (
		cs                                            fs.CapStatus
		doubleCheck, retry, retried, coldGet, capRead bool
	)
	// under lock: lom init, restore from cluster
	goi.lom.Lock(false)
do:
	// all subsequent checks work with disks - skip all if dryRun.disk=true
	if daemon.dryRun.disk {
		goto get
	}
	err = goi.lom.Load()
	if err != nil {
		coldGet = cmn.IsObjNotExist(err)
		if !coldGet {
			goi.lom.Unlock(false)
			return err, http.StatusInternalServerError
		}
		capRead = true // set flag to avoid calling later
		cs = fs.GetCapStatus()
		if cs.OOS {
			// immediate return for no space left to restore object
			goi.lom.Unlock(false)
			return cs.Err, http.StatusInternalServerError
		}
	}

	if coldGet && goi.lom.Bck().IsAIS() {
		// try lookup and restore
		goi.lom.Unlock(false)
		doubleCheck, err, errCode = goi.tryRestoreObject()
		if doubleCheck && err != nil {
			lom2 := &cluster.LOM{T: goi.t, ObjName: goi.lom.ObjName}
			er2 := lom2.Init(goi.lom.Bck().Bck)
			if er2 == nil {
				er2 = lom2.Load()
				if er2 == nil {
					goi.lom = lom2
					err = nil
				}
			}
		}
		if err != nil {
			return
		}
		goi.lom.Lock(false)
		goto get
	}
	// exists && remote|cloud: check ver if requested
	if !coldGet && goi.lom.Bck().IsRemote() {
		if goi.lom.Version() != "" && goi.lom.VerConf().ValidateWarmGet {
			goi.lom.Unlock(false)
			if coldGet, err, errCode = goi.t.CheckCloudVersion(goi.ctx, goi.lom); err != nil {
				goi.lom.Uncache()
				return
			}
			goi.lom.Lock(false)
		}
	}

	// checksum validation, if requested
	if !coldGet && goi.lom.CksumConf().ValidateWarmGet {
		err, errCode, coldGet = goi.tryRecoverObject()
		if err != nil {
			if !coldGet {
				goi.lom.Unlock(false)
				glog.Error(err)
				return
			}
			glog.Errorf("%v - proceeding to execute cold GET from %s", err, goi.lom.Bck())
		}
	}

	// 3. coldget
	if coldGet {
		goi.lom.Unlock(false) // `GetCold` will lock again and return with object locked
		if !capRead {
			capRead = true
			cs = fs.GetCapStatus()
		}
		if cs.OOS {
			// no space left to prefetch object
			return cs.Err, http.StatusBadRequest
		}
		goi.lom.SetAtimeUnix(goi.started.UnixNano())
		if err, errCode := goi.t.GetCold(goi.ctx, goi.lom, false /*prefetch*/); err != nil {
			return err, errCode
		}
		goi.t.putMirror(goi.lom)
	}

	// 4. get locally and stream back
get:
	retry, err, errCode = goi.finalize(coldGet)
	if retry && !retried {
		glog.Warningf("GET %s: uncaching and retrying...", goi.lom)
		retried = true
		goi.lom.Uncache()
		goto do
	}

	goi.lom.Unlock(false)
	return
}

// validate checksum; if corrupted try to recover from other replicas or EC slices
func (goi *getObjInfo) tryRecoverObject() (err error, code int, coldGet bool) {
	var (
		lom     = goi.lom
		retried bool
	)
retry:
	err = lom.ValidateMetaChecksum()
	if err == nil {
		err = lom.ValidateContentChecksum()
	}
	if err == nil {
		return
	}
	code = http.StatusInternalServerError
	if _, ok := err.(*cmn.BadCksumError); !ok {
		return
	}
	if !lom.Bck().IsAIS() {
		coldGet = true
		return
	}

	glog.Warning(err)
	redundant := lom.HasCopies() || lom.Bprops().EC.Enabled
	//
	// return err if there's no redundancy OR already recovered once (and failed)
	//
	if retried || !redundant {
		//
		// TODO: mark `deleted` and postpone actual deletion
		//
		if erl := lom.Remove(); erl != nil {
			glog.Warningf("%s: failed to remove corrupted %s, err: %v", goi.t.si, lom, erl)
		}
		return
	}
	//
	// try to recover from BAD CHECKSUM
	//
	cmn.RemoveFile(lom.FQN) // TODO: ditto

	if lom.HasCopies() {
		retried = true
		goi.lom.Unlock(false)
		// lookup and restore the object from local replicas
		restored := lom.RestoreObjectFromAny()
		goi.lom.Lock(false)
		if restored {
			glog.Warningf("%s: recovered corrupted %s from local replica", goi.t.si, lom)
			code = 0
			goto retry
		}
	}
	if lom.Bprops().EC.Enabled {
		retried = true
		goi.lom.Unlock(false)
		cmn.RemoveFile(lom.FQN)
		_, err, code = goi.tryRestoreObject()
		goi.lom.Lock(false)
		if err == nil {
			glog.Warningf("%s: recovered corrupted %s from EC slices", goi.t.si, lom)
			code = 0
			goto retry
		}
	}

	// TODO: ditto
	if erl := lom.Remove(); erl != nil {
		glog.Warningf("%s: failed to remove corrupted %s, err: %v", goi.t.si, lom, erl)
	}
	return
}

// an attempt to restore an object that is missing in the ais bucket - from:
// 1) local FS
// 2) other FSes or targets when resilvering (rebalancing) is running (aka GFN)
// 3) other targets if the bucket erasure coded
// 4) Cloud
func (goi *getObjInfo) tryRestoreObject() (doubleCheck bool, err error, errCode int) {
	var (
		tsi, gfnNode         *cluster.Snode
		smap                 = goi.t.owner.smap.get()
		tname                = goi.t.si.String()
		marked               = xaction.GetResilverMarked()
		interrupted, running = marked.Interrupted, marked.Xact != nil
		gfnActive            = goi.t.gfn.local.active()
		ecEnabled            = goi.lom.Bprops().EC.Enabled
	)
	tsi, err = cluster.HrwTarget(goi.lom.Uname(), &smap.Smap)
	if err != nil {
		return
	}
	if interrupted || running || gfnActive {
		if goi.lom.RestoreObjectFromAny() { // get-from-neighbor local (mountpaths) variety
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s restored", goi.lom)
			}
			return
		}
		doubleCheck = running
	}

	// FIXME: if there're not enough EC targets to restore a "sliced" object,
	// we might be able to restore it if it was replicated. In this case even
	// just one additional target might be sufficient. This won't succeed if
	// an object was sliced, neither will ecmanager.RestoreObject(lom)
	enoughECRestoreTargets := goi.lom.Bprops().EC.RequiredRestoreTargets() <= goi.t.owner.smap.Get().CountTargets()

	// cluster-wide lookup ("get from neighbor")
	marked = xaction.GetRebMarked()
	interrupted, running = marked.Interrupted, marked.Xact != nil
	if running {
		doubleCheck = true
	}
	gfnActive = goi.t.gfn.global.active()
	if running && tsi.ID() != goi.t.si.ID() {
		if goi.t.LookupRemoteSingle(goi.lom, tsi) {
			gfnNode = tsi
			goto gfn
		}
	}
	if running || !enoughECRestoreTargets || ((interrupted || gfnActive) && !ecEnabled) {
		gfnNode = goi.t.lookupRemoteAll(goi.lom, smap)
	}

gfn:
	if gfnNode != nil {
		if goi.getFromNeighbor(goi.lom, gfnNode) {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s: GFN %s <= %s", tname, goi.lom, gfnNode)
			}
			return
		}
	}

	// restore from existing EC slices if possible
	if ecErr := ec.ECM.RestoreObject(goi.lom); ecErr == nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: EC-recovered %s", tname, goi.lom)
		}
		return
	} else if ecErr != ec.ErrorECDisabled {
		err = fmt.Errorf("%s: failed to EC-recover %s: %v", tname, goi.lom, ecErr)
	}

	s := fmt.Sprintf("GET local: %s(%s) %s", goi.lom, goi.lom.FQN, cmn.DoesNotExist)
	if err != nil {
		err = fmt.Errorf("%s => [%v]", s, err)
	} else {
		err = errors.New(s)
	}
	errCode = http.StatusNotFound
	return
}

func (goi *getObjInfo) getFromNeighbor(lom *cluster.LOM, tsi *cluster.Snode) (ok bool) {
	query := url.Values{}
	query.Add(cmn.URLParamIsGFNRequest, "true")
	query = cmn.AddBckToQuery(query, lom.Bck().Bck)
	reqArgs := cmn.ReqArgs{
		Method: http.MethodGet,
		Base:   tsi.URL(cmn.NetworkIntraData),
		Path:   cmn.URLPath(cmn.Version, cmn.Objects, lom.BckName(), lom.ObjName),
		Query:  query,
	}
	req, _, cancel, err := reqArgs.ReqWithTimeout(lom.Config().Timeout.SendFile)
	if err != nil {
		glog.Errorf("failed to create request, err: %v", err)
		return
	}
	defer cancel()

	resp, err := goi.t.httpclientGetPut.Do(req) // nolint:bodyclose // closed by `poi.putObject`
	if err != nil {
		glog.Errorf("GFN failure, URL %q, err: %v", reqArgs.URL(), err)
		return
	}
	var (
		workFQN = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfileRemote)
	)
	lom.ParseHdr(resp.Header)
	poi := &putObjInfo{
		t:        goi.t,
		lom:      lom,
		r:        resp.Body,
		migrated: true,
		workFQN:  workFQN,
	}
	if err, _ := poi.putObject(); err != nil {
		glog.Error(err)
		return
	}
	ok = true
	return
}

func (goi *getObjInfo) finalize(coldGet bool) (retry bool, err error, errCode int) {
	var (
		file    *os.File
		sgl     *memsys.SGL
		slab    *memsys.Slab
		buf     []byte
		reader  io.Reader
		hdr     http.Header // if it is http request we will write also header
		written int64
	)
	defer func() {
		if file != nil {
			debug.AssertNoErr(file.Close())
		}
		if buf != nil {
			slab.Free(buf)
		}
		if sgl != nil {
			sgl.Free()
		}
	}()

	// loopback if disk IO is disabled
	if daemon.dryRun.disk {
		rd := newDryReader(daemon.dryRun.size)
		if _, err = io.Copy(goi.w, rd); err != nil {
			err = fmt.Errorf("dry-run: failed to send random response, err: %v", err)
			errCode = http.StatusInternalServerError
			goi.t.statsT.Add(stats.ErrGetCount, 1)
			return
		}
		delta := time.Since(goi.started)
		goi.t.statsT.AddMany(
			stats.NamedVal64{Name: stats.GetCount, Value: 1},
			stats.NamedVal64{Name: stats.GetLatency, Value: int64(delta)},
		)
		return
	}

	if goi.lom.Size() == 0 {
		// TODO -- FIXME
		return
	}

	if rw, ok := goi.w.(http.ResponseWriter); ok {
		hdr = rw.Header()
	}

	fqn := goi.lom.FQN
	if !coldGet && !goi.isGFN {
		// best-effort GET load balancing (see also mirror.findLeastUtilized())
		fqn = goi.lom.LoadBalanceGET()
	}
	file, err = os.Open(fqn)
	if err != nil {
		if os.IsNotExist(err) {
			errCode = http.StatusNotFound
			retry = true // (!lom.IsAIS() || lom.ECEnabled() || GFN...)
		} else {
			goi.t.fshc(err, fqn)
			err = fmt.Errorf("%s: err: %w", goi.lom, err)
			errCode = http.StatusInternalServerError
		}
		return
	}

	var (
		r    *cmn.HTTPRange
		size = goi.lom.Size()
	)
	if goi.ranges.Size > 0 {
		size = goi.ranges.Size
	}

	if hdr != nil {
		ranges, err := cmn.ParseMultiRange(goi.ranges.Range, size)
		if err != nil {
			if err == cmn.ErrNoOverlap {
				hdr.Set(cmn.HeaderContentRange, fmt.Sprintf("%s*/%d", cmn.HeaderContentRangeValPrefix, size))
			}
			return false, err, http.StatusRequestedRangeNotSatisfiable
		}

		if len(ranges) > 0 {
			if len(ranges) > 1 {
				err = fmt.Errorf("multi-range is not supported")
				errCode = http.StatusRequestedRangeNotSatisfiable
				return false, err, errCode
			}
			r = &ranges[0]

			hdr.Set(cmn.HeaderAcceptRanges, "bytes")
			hdr.Set(cmn.HeaderContentRange, r.ContentRange(size))
		}
	}

	cksumConf := goi.lom.CksumConf()
	cksumRange := cksumConf.Type != cmn.ChecksumNone && r != nil && cksumConf.EnableReadRange

	if hdr != nil {
		if goi.lom.Cksum() != nil && !cksumRange {
			cksumType, cksumValue := goi.lom.Cksum().Get()
			if cksumType != cmn.ChecksumNone {
				hdr.Set(cmn.HeaderObjCksumType, cksumType)
				hdr.Set(cmn.HeaderObjCksumVal, cksumValue)
			}
		}
		if goi.lom.Version() != "" {
			hdr.Set(cmn.HeaderObjVersion, goi.lom.Version())
		}
		hdr.Set(cmn.HeaderObjSize, strconv.FormatInt(goi.lom.Size(), 10))
		hdr.Set(cmn.HeaderObjAtime, cmn.UnixNano2S(goi.lom.AtimeUnix()))
		if r != nil {
			hdr.Set(cmn.HeaderContentLength, strconv.FormatInt(r.Length, 10))
		} else {
			hdr.Set(cmn.HeaderContentLength, strconv.FormatInt(size, 10))
		}
	}

	w := goi.w
	if r == nil {
		reader = file
		if goi.chunked {
			w = writerOnly{goi.w} // hide ReadFrom; CopyBuffer will use the buffer instead
			buf, slab = goi.t.gmm.Alloc(goi.lom.Size())
		}
	} else {
		buf, slab = goi.t.gmm.Alloc(r.Length)
		reader = io.NewSectionReader(file, r.Start, r.Length)
		if cksumRange {
			var cksum *cmn.CksumHash
			sgl = slab.MMSA().NewSGL(r.Length, slab.Size())
			if _, cksum, err = cmn.CopyAndChecksum(sgl, reader, buf, cksumConf.Type); err != nil {
				return
			}
			hdr.Set(cmn.HeaderObjCksumVal, cksum.Value())
			hdr.Set(cmn.HeaderObjCksumType, cksumConf.Type)
			reader = io.NewSectionReader(file, r.Start, r.Length)
		}
	}
	written, err = io.CopyBuffer(w, reader, buf)

	if err != nil {
		if cmn.IsErrConnectionReset(err) {
			return
		}
		goi.t.fshc(err, fqn)
		err = fmt.Errorf("failed to GET %s, err: %w", fqn, err)
		errCode = http.StatusInternalServerError
		goi.t.statsT.Add(stats.ErrGetCount, 1)
		return
	}

	// GFN: atime must be already set
	if !coldGet && !goi.isGFN {
		goi.lom.SetAtimeUnix(goi.started.UnixNano())
		goi.lom.ReCache() // GFN and cold GETs already did this
	}

	// Update objects which were sent during GFN. Thanks to this we will not
	// have to resend them in rebalance. In case of race between rebalance
	// and GFN, the former wins and it will result in double send.
	if goi.isGFN {
		goi.t.rebManager.FilterAdd([]byte(goi.lom.Uname()))
	}

	delta := time.Since(goi.started)
	if glog.FastV(4, glog.SmoduleAIS) {
		s := fmt.Sprintf("GET: %s(%s), %d µs", goi.lom, cmn.B2S(written, 1), int64(delta/time.Microsecond))
		if coldGet {
			s += " (cold)"
		}
		glog.Infoln(s)
	}
	goi.t.statsT.AddMany(
		stats.NamedVal64{Name: stats.GetThroughput, Value: written},
		stats.NamedVal64{Name: stats.GetLatency, Value: int64(delta)},
		stats.NamedVal64{Name: stats.GetCount, Value: 1},
	)
	return
}

///////////////////
// APPEND OBJECT //
///////////////////

func (aoi *appendObjInfo) appendObject() (newHandle string, err error, errCode int) {
	filePath := aoi.hi.filePath
	switch aoi.op {
	case cmn.AppendOp:
		var f *os.File
		if filePath == "" {
			filePath = fs.CSM.GenContentParsedFQN(aoi.lom.ParsedFQN, fs.WorkfileType, fs.WorkfileAppend)
			f, err = aoi.lom.CreateFile(filePath)
			if err != nil {
				errCode = http.StatusInternalServerError
				return
			}
			aoi.hi.partialCksum = cmn.NewCksumHash(aoi.lom.CksumConf().Type)
		} else {
			f, err = os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				errCode = http.StatusInternalServerError
				return
			}
			cmn.Assert(aoi.hi.partialCksum != nil)
		}

		var (
			buf  []byte
			slab *memsys.Slab
		)
		if aoi.size == 0 {
			buf, slab = aoi.t.gmm.Alloc()
		} else {
			buf, slab = aoi.t.gmm.Alloc(aoi.size)
		}

		w := cmn.NewWriterMulti(f, aoi.hi.partialCksum.H)
		_, err = io.CopyBuffer(w, aoi.r, buf)

		slab.Free(buf)
		debug.AssertNoErr(f.Close())
		if err != nil {
			errCode = http.StatusInternalServerError
			return
		}

		newHandle = combineAppendHandle(aoi.t.si.ID(), filePath, aoi.hi.partialCksum)
	case cmn.FlushOp:
		if filePath == "" {
			err = errors.New("handle not provided")
			errCode = http.StatusBadRequest
			return
		}
		cmn.Assert(aoi.hi.partialCksum != nil)
		aoi.hi.partialCksum.Finalize()
		partialCksum := aoi.hi.partialCksum.Clone()
		if aoi.cksum != nil && !partialCksum.Equal(aoi.cksum) {
			err = cmn.NewBadDataCksumError(partialCksum, aoi.cksum)
			errCode = http.StatusInternalServerError
			return
		}
		if _, err := aoi.t.PromoteFile(filePath, aoi.lom.Bck(), aoi.lom.ObjName, partialCksum,
			true /*overwrite*/, false /*safe*/, false /*verbose*/); err != nil {
			return "", err, 0
		}
	default:
		cmn.AssertMsg(false, aoi.op)
	}

	delta := time.Since(aoi.started)
	aoi.t.statsT.AddMany(
		stats.NamedVal64{Name: stats.AppendCount, Value: 1},
		stats.NamedVal64{Name: stats.AppendLatency, Value: int64(delta)},
	)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("PUT %s: %d µs", aoi.lom, int64(delta/time.Microsecond))
	}
	return
}

func parseAppendHandle(handle string) (hi handleInfo, err error) {
	if handle == "" {
		return
	}
	p := strings.SplitN(handle, "|", 4)
	if len(p) != 4 {
		return hi, fmt.Errorf("invalid handle provided: %q", handle)
	}
	hi.partialCksum = cmn.NewCksumHash(p[2])
	buf, err := base64.StdEncoding.DecodeString(p[3])
	if err != nil {
		return hi, err
	}
	err = hi.partialCksum.H.(encoding.BinaryUnmarshaler).UnmarshalBinary(buf)
	if err != nil {
		return hi, err
	}
	hi.nodeID = p[0]
	hi.filePath = p[1]
	return
}

func combineAppendHandle(nodeID, filePath string, partialCksum *cmn.CksumHash) string {
	buf, err := partialCksum.H.(encoding.BinaryMarshaler).MarshalBinary()
	cmn.AssertNoErr(err)
	cksumTy := partialCksum.Type()
	cksumBinary := base64.StdEncoding.EncodeToString(buf)
	return nodeID + "|" + filePath + "|" + cksumTy + "|" + cksumBinary
}
