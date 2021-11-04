// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"archive/tar"
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
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xreg"
)

//
// PUT, GET, APPEND (to file | to archive), and COPY object
//

type (
	putObjInfo struct {
		started    time.Time
		t          *targetrunner
		lom        *cluster.LOM
		r          io.ReadCloser    // reader that has the content
		cksumToUse *cos.Cksum       // if available (not `none`), can be validated and will be stored
		size       int64            // object size aka Content-Length
		workFQN    string           // temp fqn to be renamed
		recvType   cluster.RecvType // enum { RegularPut, Cold, Migrated, ... }
		skipEC     bool             // true: do not erasure-encode when finalizing
	}

	getObjInfo struct {
		started time.Time
		nanotim int64
		t       *targetrunner
		lom     *cluster.LOM
		w       io.Writer       // not necessarily http.ResponseWriter
		ctx     context.Context // context used when getting object from remote backend (access creds)
		ranges  rangesQuery     // range read query
		archive archiveQuery    // archive query
		isGFN   bool            // is GFN request
		chunked bool            // chunked transfer (en)coding: https://tools.ietf.org/html/rfc7230#page-36
	}

	// Contains information packed in append handle.
	handleInfo struct {
		nodeID       string
		filePath     string
		partialCksum *cos.CksumHash
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

		cksum *cos.Cksum // Expected checksum of the final object.
	}

	copyObjInfo struct {
		cluster.CopyObjectParams
		t           *targetrunner
		localOnly   bool // copy locally with no HRW=>target
		finalize    bool // copies and EC (as in poi.finalize())
		promoteFile bool // Determines if we are promoting a file.
	}

	appendArchObjInfo struct {
		started time.Time // started time of receiving - used to calculate the recv duration
		t       *targetrunner
		lom     *cluster.LOM

		// Reader with the content of the object.
		r io.ReadCloser
		// Object size aka Content-Length.
		size     int64
		filename string // path inside an archive
		mime     string // archive type
	}
)

////////////////
// PUT OBJECT //
////////////////

func (poi *putObjInfo) putObject() (int, error) {
	debug.Assert(cluster.RegularPut <= poi.recvType && poi.recvType <= cluster.Migrated)
	lom := poi.lom
	// optimize out if the checksums do match
	if !poi.cksumToUse.IsEmpty() {
		if lom.EqCksum(poi.cksumToUse) {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s is valid %s: PUT is a no-op", lom, poi.cksumToUse)
			}
			cos.DrainReader(poi.r)
			return 0, nil
		}
	}
	if err := poi.writeToFile(); err != nil {
		return http.StatusInternalServerError, err
	}
	if errCode, err := poi.finalize(); err != nil {
		return errCode, err
	}
	if poi.recvType == cluster.RegularPut {
		delta := time.Since(poi.started)
		poi.t.statsT.AddMany(
			stats.NamedVal64{Name: stats.PutCount, Value: 1},
			stats.NamedVal64{Name: stats.PutLatency, Value: int64(delta)},
		)
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("PUT %s: %s", lom, delta)
		}
	}
	return 0, nil
}

func (poi *putObjInfo) finalize() (errCode int, err error) {
	if errCode, err = poi.tryFinalize(); err != nil {
		if err1 := fs.Access(poi.workFQN); err1 == nil || !os.IsNotExist(err1) {
			if err1 == nil {
				err1 = err
			}
			poi.t.fsErr(err1, poi.workFQN)
			if err2 := cos.RemoveFile(poi.workFQN); err2 != nil {
				glog.Errorf("Nested error: %s => (remove %s => err: %v)", err1, poi.workFQN, err2)
			}
		}
		poi.lom.Uncache(true /*delDirty*/)
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
func (poi *putObjInfo) tryFinalize() (errCode int, err error) {
	var (
		lom = poi.lom
		bck = lom.Bck()
		bmd = poi.t.owner.bmd.Get()
	)
	// remote versioning
	if bck.IsRemote() && (poi.recvType == cluster.RegularPut || poi.recvType == cluster.Finalize) {
		errCode, err = poi.putRemote()
		if err != nil {
			glog.Errorf("PUT %s: %v", lom, err)
			return
		}
	}
	if _, present := bmd.Get(bck); !present {
		err = fmt.Errorf("PUT %s: bucket %s does not exist", lom, bck)
		errCode = http.StatusBadRequest
		return
	}
	if poi.recvType == cluster.ColdGet {
		debug.Assert(!lom.TryLock(true)) // cold GET: caller must take a lock
	} else {
		lom.Lock(true)
		defer lom.Unlock(true)
	}
	// ais versioning
	if bck.IsAIS() && lom.VersionConf().Enabled {
		if poi.recvType == cluster.RegularPut {
			if remSrc, ok := lom.GetCustomKey(cmn.SourceObjMD); !ok || remSrc == "" {
				if err = lom.IncVersion(); err != nil {
					glog.Error(err)
				}
			}
		}
	}
	if err = cos.Rename(poi.workFQN, lom.FQN); err != nil {
		err = fmt.Errorf(cmn.FmtErrFailed, poi.t.si, "rename", lom, err)
		return
	}
	if lom.HasCopies() {
		// TODO: recover
		if errdc := lom.DelAllCopies(); errdc != nil {
			glog.Errorf("PUT %s: failed to delete old copies [%v], proceeding to PUT anyway...", lom, errdc)
		}
	}
	err = lom.Persist(true)
	if err != nil {
		lom.Uncache(true /*delDirty*/)
	}
	return
}

func (poi *putObjInfo) putRemote() (errCode int, err error) {
	var (
		lom     = poi.lom
		backend = poi.t.Backend(lom.Bck())
	)
	lmfh, err := cos.NewFileHandle(poi.workFQN)
	if err != nil {
		err = fmt.Errorf(cmn.FmtErrFailed, poi.t.Snode(), "open", poi.workFQN, err)
		return
	}
	if poi.recvType == cluster.RegularPut && !lom.Bck().IsRemoteAIS() {
		// some/all of those are set by the backend.PutObj()
		lom.ObjAttrs().DelCustomKeys(cmn.SourceObjMD, cmn.CRC32CObjMD, cmn.ETag, cmn.MD5ObjMD, cmn.VersionObjMD)
	}
	errCode, err = backend.PutObj(lmfh, lom)
	if err == nil && !lom.Bck().IsRemoteAIS() {
		lom.SetCustomKey(cmn.SourceObjMD, backend.Provider())
	}
	return
}

// NOTE: LOM is updated at the end of this call with proper size and checksum.
//       `poi.r` (reader) is closed upon exit.
func (poi *putObjInfo) writeToFile() (err error) {
	var (
		written int64
		buf     []byte
		slab    *memsys.Slab
		reader  = poi.r
		lmfh    *os.File
		writer  io.Writer
		writers = make([]io.Writer, 0, 4)
		cksums  = struct {
			store *cos.CksumHash // store with LOM
			given *cos.CksumHash // compute additionally
			expct *cos.Cksum     // and validate against `expct` if required/available
		}{}
		conf = poi.lom.CksumConf()
	)
	if lmfh, err = poi.lom.CreateFile(poi.workFQN); err != nil {
		return
	}
	writer = cos.WriterOnly{Writer: lmfh} // Hiding `ReadFrom` for `*os.File` introduced in Go1.15.
	if poi.size == 0 {
		buf, slab = poi.t.gmm.Alloc()
	} else {
		buf, slab = poi.t.gmm.AllocSize(poi.size)
	}
	// cleanup
	defer func() {
		slab.Free(buf)
		if err == nil {
			cos.Close(reader)
			return // ok
		}
		// not ok
		reader.Close()
		if lmfh != nil {
			if nestedErr := lmfh.Close(); nestedErr != nil {
				glog.Errorf("Nested (%v): failed to close received object %s, err: %v",
					err, poi.workFQN, nestedErr)
			}
		}
		if nestedErr := cos.RemoveFile(poi.workFQN); nestedErr != nil {
			glog.Errorf("Nested (%v): failed to remove %s, err: %v", err, poi.workFQN, nestedErr)
		}
	}()
	// checksums
	if conf.Type == cos.ChecksumNone {
		poi.lom.SetCksum(cos.NoneCksum)
		goto write
	}
	if poi.recvType == cluster.Migrated && !conf.ShouldValidate() && !poi.cksumToUse.IsEmpty() {
		// if migration validation is not configured we can just take
		// the checksum that has arrived with the object (and compute it if not present)
		poi.lom.SetCksum(poi.cksumToUse)
		goto write
	}

	// compute checksum and save it as part of the object metadata
	cksums.store = cos.NewCksumHash(conf.Type)
	writers = append(writers, cksums.store.H)
	if conf.ShouldValidate() && !poi.cksumToUse.IsEmpty() {
		// if validate-cold-get and the cksum is provided we should also check md5 hash (aws, gcp)
		// or if the object is migrated, and `conf.ValidateObjMove` we should check with existing checksum
		cksums.expct = poi.cksumToUse
		cksums.given = cos.NewCksumHash(poi.cksumToUse.Type())
		writers = append(writers, cksums.given.H)
	}
write:
	if len(writers) == 0 {
		written, err = io.CopyBuffer(writer, reader, buf)
	} else {
		writers = append(writers, writer)
		written, err = io.CopyBuffer(cos.NewWriterMulti(writers...), reader, buf)
	}
	if err != nil {
		return
	}
	// validate
	if cksums.given != nil {
		cksums.given.Finalize()
		if !cksums.given.Equal(cksums.expct) {
			err = cos.NewBadDataCksumError(cksums.expct, &cksums.given.Cksum, poi.lom.String())
			poi.t.statsT.AddMany(
				stats.NamedVal64{Name: stats.ErrCksumCount, Value: 1},
				stats.NamedVal64{Name: stats.ErrCksumSize, Value: written},
			)
			return
		}
	}
	cos.Close(lmfh)
	lmfh = nil
	// ok
	poi.lom.SetSize(written) // TODO: compare with non-zero lom.SizeBytes() that may have been set via oa.FromHeader()
	if cksums.store != nil {
		cksums.store.Finalize()
		poi.lom.SetCksum(&cksums.store.Cksum)
	}
	return
}

////////////////
// GET OBJECT //
////////////////

func (goi *getObjInfo) getObject() (errCode int, err error) {
	var (
		cs                                            fs.CapStatus
		doubleCheck, retry, retried, coldGet, capRead bool
	)
	// under lock: lom init, restore from cluster
	goi.lom.Lock(false)
do:
	err = goi.lom.Load(true /*cache it*/, true /*locked*/)
	if err != nil {
		coldGet = cmn.IsObjNotExist(err)
		if !coldGet {
			goi.lom.Unlock(false)
			errCode = http.StatusInternalServerError
			return
		}
		capRead = true // set flag to avoid calling later
		cs = fs.GetCapStatus()
		if cs.OOS {
			// Immediate return for no space left to restore object.
			goi.lom.Unlock(false)
			errCode, err = http.StatusInsufficientStorage, cs.Err
			return
		}
	}

	if coldGet && goi.lom.Bck().IsAIS() {
		// try lookup and restore
		goi.lom.Unlock(false)
		doubleCheck, errCode, err = goi.tryRestoreObject()
		if doubleCheck && err != nil {
			lom2 := &cluster.LOM{ObjName: goi.lom.ObjName}
			er2 := lom2.Init(goi.lom.Bucket())
			if er2 == nil {
				er2 = lom2.Load(true /*cache it*/, false /*locked*/)
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
		if goi.lom.VersionConf().ValidateWarmGet {
			var equal bool
			goi.lom.Unlock(false)
			if equal, errCode, err = goi.t.CompareObjects(goi.ctx, goi.lom); err != nil {
				goi.lom.Uncache(true /*delDirty*/)
				return
			}
			coldGet = !equal
			goi.lom.Lock(false)
		}
	}

	// checksum validation, if requested
	if !coldGet && goi.lom.CksumConf().ValidateWarmGet {
		coldGet, errCode, err = goi.tryRecoverObject()
		if err != nil {
			if !coldGet {
				goi.lom.Unlock(false)
				glog.Error(err)
				return
			}
			glog.Errorf("%v - proceeding to cold GET from %s", err, goi.lom.Bck())
		}
	}

	// 3. coldget
	if coldGet {
		if !capRead {
			capRead = true
			cs = fs.GetCapStatus()
		}
		if cs.OOS {
			// No space left to prefetch object.
			goi.lom.Unlock(false)
			errCode, err = http.StatusInsufficientStorage, cs.Err
			return
		}
		goi.lom.SetAtimeUnix(goi.started.UnixNano())
		if errCode, err = goi.t.GetCold(goi.ctx, goi.lom, cluster.GetCold); err != nil {
			return
		}
		goi.t.putMirror(goi.lom)
	}

	// 4. get locally and stream back
get:
	retry, errCode, err = goi.finalize(coldGet)
	if retry && !retried {
		debug.Assert(err != errSendingResp)
		glog.Warningf("GET %s: uncaching and retrying...", goi.lom)
		retried = true // NOTE: retry only once
		goi.lom.Uncache(true /*delDirty*/)
		goto do
	}

	goi.lom.Unlock(false)
	return
}

// validate checksum; if corrupted try to recover from other replicas or EC slices
func (goi *getObjInfo) tryRecoverObject() (coldGet bool, code int, err error) {
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
	if _, ok := err.(*cos.ErrBadCksum); !ok {
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
	cos.RemoveFile(lom.FQN) // TODO: ditto

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
		cos.RemoveFile(lom.FQN)
		_, code, err = goi.tryRestoreObject()
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
func (goi *getObjInfo) tryRestoreObject() (doubleCheck bool, errCode int, err error) {
	var (
		tsi, gfnNode         *cluster.Snode
		smap                 = goi.t.owner.smap.get()
		tname                = goi.t.si.String()
		marked               = xreg.GetResilverMarked()
		interrupted, running = marked.Interrupted, marked.Xact != nil
		gfnActive            = goi.t.res.IsActive()
		ecEnabled            = goi.lom.Bprops().EC.Enabled
	)
	tsi, err = cluster.HrwTargetAll(goi.lom.Uname(), &smap.Smap) // include targets in maintenance
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
	enoughECRestoreTargets := goi.lom.Bprops().EC.RequiredRestoreTargets() <= smap.CountActiveTargets()

	// cluster-wide lookup ("get from neighbor")
	marked = xreg.GetRebMarked()
	interrupted, running = marked.Interrupted, marked.Xact != nil
	if running {
		doubleCheck = true
	}
	gfnActive = reb.IsActiveGFN()
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
		ecErr = goi.lom.Load(true /*cache it*/, false /*locked*/) // TODO: optimize locking
		debug.AssertNoErr(ecErr)
		if ecErr == nil {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s: EC-recovered %s", tname, goi.lom)
			}
			return
		}
		err = fmt.Errorf(cmn.FmtErrFailed, tname, "load EC-recovered", goi.lom, ecErr)
	} else if ecErr != ec.ErrorECDisabled {
		err = fmt.Errorf(cmn.FmtErrFailed, tname, "EC-recover", goi.lom, ecErr)
	}

	if err != nil {
		err = fmt.Errorf("%s not found: %v", goi.lom.FullName(), err)
	} else {
		err = cmn.NewErrNotFound("%s: %s", goi.t.si, goi.lom.FullName())
	}
	errCode = http.StatusNotFound
	return
}

func (goi *getObjInfo) getFromNeighbor(lom *cluster.LOM, tsi *cluster.Snode) (ok bool) {
	header := make(http.Header)
	header.Add(cmn.HdrCallerID, goi.t.SID())
	header.Add(cmn.HdrCallerName, goi.t.Sname())
	query := url.Values{}
	query.Set(cmn.URLParamIsGFNRequest, "true")
	query = cmn.AddBckToQuery(query, lom.Bucket())
	reqArgs := cmn.ReqArgs{
		Method: http.MethodGet,
		Base:   tsi.URL(cmn.NetworkIntraData),
		Header: header,
		Path:   cmn.URLPathObjects.Join(lom.Bck().Name, lom.ObjName),
		Query:  query,
	}
	config := cmn.GCO.Get()
	req, _, cancel, err := reqArgs.ReqWithTimeout(config.Timeout.SendFile.D())
	if err != nil {
		glog.Errorf("failed to create request, err: %v", err)
		return
	}
	defer cancel()

	resp, err := goi.t.client.data.Do(req) // nolint:bodyclose // closed by `poi.putObject`
	if err != nil {
		glog.Errorf("GFN failure, URL %q, err: %v", reqArgs.URL(), err)
		return
	}
	cksumToUse := lom.ObjAttrs().FromHeader(resp.Header)
	workFQN := fs.CSM.GenContentFQN(lom, fs.WorkfileType, fs.WorkfileRemote)
	poi := &putObjInfo{
		t:          goi.t,
		lom:        lom,
		r:          resp.Body,
		recvType:   cluster.Migrated,
		workFQN:    workFQN,
		cksumToUse: cksumToUse,
	}
	if _, err := poi.putObject(); err != nil {
		glog.Error(err)
		return
	}
	ok = true
	return
}

func (goi *getObjInfo) finalize(coldGet bool) (retry bool, errCode int, err error) {
	var (
		lmfh    *os.File
		sgl     *memsys.SGL
		slab    *memsys.Slab
		buf     []byte
		reader  io.Reader
		csl     cos.ReadCloseSizer
		hdr     http.Header // if it is http request we will write also header
		written int64
	)
	defer func() { // cleanup
		if lmfh != nil {
			cos.Close(lmfh)
		}
		if buf != nil {
			slab.Free(buf)
		}
		if sgl != nil {
			sgl.Free()
		}
		if csl != nil {
			csl.Close()
		}
	}()
	if resp, ok := goi.w.(http.ResponseWriter); ok {
		hdr = resp.Header()
	}
	fqn := goi.lom.FQN
	if !coldGet && !goi.isGFN {
		// best-effort GET load balancing (see also mirror.findLeastUtilized())
		fqn = goi.lom.LBGet()
	}
	lmfh, err = os.Open(fqn)
	if err != nil {
		if os.IsNotExist(err) {
			errCode = http.StatusNotFound
			retry = true // (!lom.IsAIS() || lom.ECEnabled() || GFN...)
		} else {
			goi.t.fsErr(err, fqn)
			err = fmt.Errorf("%s: %w", goi.lom, err)
			errCode = http.StatusInternalServerError
		}
		return
	}

	var (
		rrange     *cmn.HTTPRange
		size       = goi.lom.SizeBytes()
		cksumConf  = goi.lom.CksumConf()
		cksumRange bool
	)
	// parse, validate, set response header
	if hdr != nil {
		// read range
		if goi.ranges.Range != "" {
			rsize := size
			if goi.ranges.Size > 0 {
				rsize = goi.ranges.Size
			}
			if rrange, errCode, err = goi.parseRange(hdr, rsize); err != nil {
				return
			}
			if rrange != nil {
				cksumRange = cksumConf.Type != cos.ChecksumNone && cksumConf.EnableReadRange
				size = rrange.Length // Content-Length
			}
		}
		goi.lom.ObjAttrs().ToHeader(hdr)
	}

	// set reader
	w := goi.w
	if rrange == nil {
		reader = lmfh
		if goi.archive.filename != "" {
			csl, err = goi.freadArch(lmfh)
			if err != nil {
				if _, ok := err.(*cmn.ErrNotFound); ok {
					errCode = http.StatusNotFound
				} else {
					err = fmt.Errorf(cmn.FmtErrFailed, goi.t.si,
						"extract "+goi.archive.filename+" from", goi.lom, err)
				}
				return
			}
			reader, size = csl, csl.Size() // Content-Length
			//
			// TODO: support checksumming extracted files
			//
		}
		if goi.chunked {
			// NOTE: hide `ReadFrom` of the `http.ResponseWriter` (in re: sendfile)
			w = cos.WriterOnly{Writer: goi.w}
			buf, slab = goi.t.gmm.AllocSize(size)
		}
	} else {
		buf, slab = goi.t.gmm.AllocSize(rrange.Length)
		reader = io.NewSectionReader(lmfh, rrange.Start, rrange.Length)
		if cksumRange {
			var (
				cksum *cos.CksumHash
				n     int64
			)
			sgl = slab.MMSA().NewSGL(rrange.Length, slab.Size())
			if n, cksum, err = cos.CopyAndChecksum(sgl, reader, buf, cksumConf.Type); err != nil {
				return
			}
			debug.Assert(n <= rrange.Length)
			hdr.Set(cmn.HdrObjCksumVal, cksum.Value())
			hdr.Set(cmn.HdrObjCksumType, cksumConf.Type)
			reader = sgl
		}
	}
	// set Content-Length
	if hdr != nil {
		hdr.Set(cmn.HdrContentLength, strconv.FormatInt(size, 10))
	}

	// transmit
	written, err = io.CopyBuffer(w, reader, buf)
	if err != nil {
		if !cos.IsRetriableConnErr(err) {
			goi.t.fsErr(err, fqn)
			goi.t.statsT.Add(stats.ErrGetCount, 1)
		}
		glog.Errorf(cmn.FmtErrFailed, goi.t.si, "GET", fqn, err)
		// at this point, error is already written into the response
		// return special to indicate just that
		err = errSendingResp
		return
	}

	// GFN: atime must be already set
	if !coldGet && !goi.isGFN {
		goi.lom.Load(false /*cache it*/, true /*locked*/)
		goi.lom.SetAtimeUnix(goi.started.UnixNano())
		goi.lom.ReCache(true) // GFN and cold GETs already did this
	}

	// Update objects which were sent during GFN. Thanks to this we will not
	// have to resend them in rebalance. In case of a race between rebalance
	// and GFN, the former wins and it will result in double send.
	if goi.isGFN {
		goi.t.reb.FilterAdd([]byte(goi.lom.Uname()))
	}

	delta := mono.SinceNano(goi.nanotim)
	goi.t.statsT.AddMany(
		stats.NamedVal64{Name: stats.GetThroughput, Value: written},
		stats.NamedVal64{Name: stats.GetLatency, Value: delta},
		stats.NamedVal64{Name: stats.GetCount, Value: 1},
	)
	return
}

// parse, validate, set response header
func (goi *getObjInfo) parseRange(hdr http.Header, size int64) (rrange *cmn.HTTPRange, errCode int, err error) {
	var ranges []cmn.HTTPRange

	ranges, err = cmn.ParseMultiRange(goi.ranges.Range, size)
	if err != nil {
		if _, ok := err.(*cmn.ErrRangeNoOverlap); ok {
			// https://datatracker.ietf.org/doc/html/rfc7233#section-4.2
			hdr.Set(cmn.HdrContentRange, fmt.Sprintf("%s*/%d", cmn.HdrContentRangeValPrefix, size))
		}
		errCode = http.StatusRequestedRangeNotSatisfiable
		return
	}
	if len(ranges) == 0 {
		return
	}
	if len(ranges) > 1 {
		err = fmt.Errorf(cmn.FmtErrUnsupported, goi.t.Snode(), "multi-range")
		errCode = http.StatusRequestedRangeNotSatisfiable
		return
	}
	if goi.archive.filename != "" {
		err = fmt.Errorf(cmn.FmtErrUnsupported, goi.t.Snode(), "range-reading archived files")
		errCode = http.StatusRequestedRangeNotSatisfiable
		return
	}
	rrange = &ranges[0]
	hdr.Set(cmn.HdrAcceptRanges, "bytes")
	hdr.Set(cmn.HdrContentRange, rrange.ContentRange(size))
	return
}

//////////////////////
// APPEND (to file) //
//////////////////////

func (aoi *appendObjInfo) appendObject() (newHandle string, errCode int, err error) {
	filePath := aoi.hi.filePath
	switch aoi.op {
	case cmn.AppendOp:
		var f *os.File
		if filePath == "" {
			filePath = fs.CSM.GenContentFQN(aoi.lom, fs.WorkfileType, fs.WorkfileAppend)
			f, err = aoi.lom.CreateFile(filePath)
			if err != nil {
				errCode = http.StatusInternalServerError
				return
			}
			aoi.hi.partialCksum = cos.NewCksumHash(aoi.lom.CksumConf().Type)
		} else {
			f, err = os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, cos.PermRWR)
			if err != nil {
				errCode = http.StatusInternalServerError
				return
			}
			cos.Assert(aoi.hi.partialCksum != nil)
		}

		var (
			buf  []byte
			slab *memsys.Slab
		)
		if aoi.size == 0 {
			buf, slab = aoi.t.gmm.Alloc()
		} else {
			buf, slab = aoi.t.gmm.AllocSize(aoi.size)
		}

		w := cos.NewWriterMulti(f, aoi.hi.partialCksum.H)
		_, err = io.CopyBuffer(w, aoi.r, buf)

		slab.Free(buf)
		cos.Close(f)
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
		cos.Assert(aoi.hi.partialCksum != nil)
		aoi.hi.partialCksum.Finalize()
		partialCksum := aoi.hi.partialCksum.Clone()
		if !aoi.cksum.IsEmpty() && !partialCksum.Equal(aoi.cksum) {
			err = cos.NewBadDataCksumError(partialCksum, aoi.cksum)
			errCode = http.StatusInternalServerError
			return
		}
		params := cluster.PromoteFileParams{
			SrcFQN:    filePath,
			Bck:       aoi.lom.Bck(),
			ObjName:   aoi.lom.ObjName,
			Cksum:     partialCksum,
			Overwrite: true,
			KeepOrig:  false,
		}
		var lom *cluster.LOM
		if lom, err = aoi.t.PromoteFile(params); err != nil {
			return
		}
		if lom != nil {
			cluster.FreeLOM(lom)
		}
	default:
		debug.AssertMsg(false, aoi.op)
	}

	delta := time.Since(aoi.started)
	aoi.t.statsT.AddMany(
		stats.NamedVal64{Name: stats.AppendCount, Value: 1},
		stats.NamedVal64{Name: stats.AppendLatency, Value: int64(delta)},
	)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("PUT %s: %s", aoi.lom, delta)
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
	hi.partialCksum = cos.NewCksumHash(p[2])
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

func combineAppendHandle(nodeID, filePath string, partialCksum *cos.CksumHash) string {
	buf, err := partialCksum.H.(encoding.BinaryMarshaler).MarshalBinary()
	debug.AssertNoErr(err)
	cksumTy := partialCksum.Type()
	cksumBinary := base64.StdEncoding.EncodeToString(buf)
	return nodeID + "|" + filePath + "|" + cksumTy + "|" + cksumBinary
}

/////////////////
// COPY OBJECT //
/////////////////

func (coi *copyObjInfo) copyObject(src *cluster.LOM, objNameTo string) (size int64, err error) {
	debug.Assert(coi.DP == nil)
	if src.Bck().IsRemote() || coi.BckTo.IsRemote() {
		// There will be no logic to create local copies etc, we can simply use copyReader
		coi.DP = &cluster.LDP{}
		return coi.copyReader(src, objNameTo)
	}
	si := coi.t.si
	if !coi.localOnly {
		smap := coi.t.owner.smap.Get()
		if si, err = cluster.HrwTarget(coi.BckTo.MakeUname(objNameTo), smap); err != nil {
			return
		}
	}
	// remote copy
	if si.ID() != coi.t.si.ID() {
		params := allocSendParams()
		{
			params.ObjNameTo = objNameTo
			params.Tsi = si
			params.DM = coi.DM
		}
		size, err = coi.putRemote(src, params)
		freeSendParams(params)
		return
	}
	// dry-run
	if coi.DryRun {
		// TODO: replace with something similar to src.FQN == dst.FQN, but dstBck might not exist.
		if src.Bucket().Equal(coi.BckTo.Bck) && src.ObjName == objNameTo {
			return 0, nil
		}
		return src.SizeBytes(), nil
	}
	// local copy
	dst := cluster.AllocLOM(objNameTo)
	defer cluster.FreeLOM(dst)
	err = dst.Init(coi.BckTo.Bck)
	if err != nil {
		return
	}
	if src.FQN == dst.FQN { // resilvering with a single mountpath?
		return
	}

	exclusive := src.Uname() == dst.Uname()
	src.Lock(exclusive)
	defer src.Unlock(exclusive)
	if err = src.Load(false /*cache it*/, true /*locked*/); err != nil {
		if !cmn.IsObjNotExist(err) {
			err = fmt.Errorf("%s: err: %w", src, err)
		}
		return
	}

	// unless overwriting the source w-lock the destination (see `exclusive`)
	if src.Uname() != dst.Uname() {
		dst.Lock(true)
		defer dst.Unlock(true)
		if err = dst.Load(false /*cache it*/, true /*locked*/); err == nil {
			if src.EqCksum(dst.Checksum()) {
				return
			}
		} else if cmn.IsErrBucketNought(err) {
			return
		}
	}
	dst2, err2 := src.CopyObject(dst.FQN, coi.Buf)
	if err2 == nil {
		size = src.SizeBytes()
		if coi.finalize {
			coi.t.putMirror(dst2)
		}
	}
	err = err2
	cluster.FreeLOM(dst2)
	return
}

/////////////////
// COPY READER //
/////////////////

// copyReader puts a new object to a cluster, according to a reader taken from coi.DP.Reader(lom) The reader returned
// from coi.DP is responsible for any locking or source LOM, if necessary. If the reader doesn't take any locks, it has
// to consider object content changing in the middle of copying.
//
// LOM can be meta of a cloud object. It creates some problems. However, it's DP who is responsible for providing a reader,
// so DP should tak any steps necessary to do so. It includes handling cold get, warm get etc.
//
// If destination bucket is remote bucket, copyReader will always create a cached copy of an object on one of the
// targets as well as make put to the relevant backend provider.
// TODO: support not storing an object from a cloud bucket.
func (coi *copyObjInfo) copyReader(lom *cluster.LOM, objNameTo string) (size int64, err error) {
	var (
		reader cos.ReadOpenCloser
		si     *cluster.Snode
	)
	debug.Assert(coi.DP != nil)
	if si, err = cluster.HrwTarget(coi.BckTo.MakeUname(objNameTo), coi.t.owner.smap.Get()); err != nil {
		return
	}

	if si.ID() != coi.t.si.ID() {
		params := allocSendParams()
		{
			params.ObjNameTo = objNameTo
			params.Tsi = si
			params.DM = coi.DM
		}
		size, err = coi.putRemote(lom, params)
		freeSendParams(params)
		return
	}

	if err = lom.Load(false /*cache it*/, false /*locked*/); err != nil {
		return
	}
	// DryRun: just get a reader and discard it. Init on dstLOM would cause and error as dstBck doesn't exist.
	if coi.DryRun {
		return coi.dryRunCopyReader(lom)
	}
	dst := cluster.AllocLOM(objNameTo)
	defer cluster.FreeLOM(dst)
	if err = dst.Init(coi.BckTo.Bck); err != nil {
		return
	}
	if lom.Bucket().Equal(coi.BckTo.Bck) {
		dst.SetVersion(lom.Version())
	}

	if reader, _, err = coi.DP.Reader(lom); err != nil {
		return 0, err
	}

	// Set the correct recvType: some transactions must update the object
	// in the Cloud(if destination is a Cloud bucket).
	recvType := cluster.Migrated
	if coi.DM != nil {
		recvType = coi.DM.RecvType()
	}
	params := cluster.PutObjectParams{
		Tag:      "copy-dp",
		Reader:   reader,
		RecvType: recvType,
	}
	if err := coi.t.PutObject(dst, params); err != nil {
		return 0, err
	}

	return lom.SizeBytes(), nil
}

func (coi *copyObjInfo) dryRunCopyReader(lom *cluster.LOM) (size int64, err error) {
	debug.Assert(coi.DryRun)
	debug.Assert(coi.DP != nil)

	var reader io.ReadCloser
	if reader, _, err = coi.DP.Reader(lom); err != nil {
		return 0, err
	}
	defer reader.Close()
	return io.Copy(io.Discard, reader)
}

// PUT object onto designated target
func (coi *copyObjInfo) putRemote(lom *cluster.LOM, params *cluster.SendToParams) (size int64, err error) {
	if coi.DM != nil {
		// We need to clone the `lom` to use it in async operation.
		// The `lom` is freed upon callback in `_sendObjDM`.
		lom = lom.Clone(lom.FQN)
	}
	if coi.DP == nil {
		// NOTE: not the best way to determine whether `lom` is an object or just
		// a regular file. Ideally, the parameter shouldn't be `lom` at all,
		// rather something more general like `cluster.CT`.
		var reader cos.ReadOpenCloser
		if !coi.promoteFile {
			lom.Lock(false)
			if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
				lom.Unlock(false)
				return 0, nil
			}
			if coi.DryRun {
				lom.Unlock(false)
				return lom.SizeBytes(), nil
			}
			fh, err := cos.NewFileHandle(lom.FQN)
			if err != nil {
				lom.Unlock(false)
				return 0, fmt.Errorf(cmn.FmtErrFailed, coi.t.Snode(), "open", lom.FQN, err)
			}
			size = lom.SizeBytes()
			reader = cos.NewDeferROC(fh, func() { lom.Unlock(false) })
		} else {
			debug.Assert(!coi.DryRun)
			fh, err := cos.NewFileHandle(lom.FQN)
			if err != nil {
				return 0, fmt.Errorf(cmn.FmtErrFailed, coi.t.Snode(), "open", lom.FQN, err)
			}
			fi, err := fh.Stat()
			if err != nil {
				fh.Close()
				return 0, fmt.Errorf("failed to stat %s: %w", lom.FQN, err)
			}
			size = fi.Size()
			reader = fh
		}
		params.Reader = reader
		params.ObjAttrs = lom
	} else {
		if params.Reader, params.ObjAttrs, err = coi.DP.Reader(lom); err != nil {
			return
		}
		if coi.DryRun {
			size, err = io.Copy(io.Discard, params.Reader)
			cos.Close(params.Reader)
			return
		}
		// NOTE: returns the cos.ContentLengthUnknown (-1) if post-transform size not known.
		size = params.ObjAttrs.SizeBytes()
	}
	debug.Assert(params.ObjAttrs != nil)
	params.BckTo = coi.BckTo
	// either stream or PUT
	if params.DM != nil {
		err = _sendObjDM(lom, params)
	} else {
		err = coi.t._sendPUT(params)
	}
	return
}

// streaming send via bundle.DataMover
func _sendObjDM(lom *cluster.LOM, params *cluster.SendToParams) error {
	o := transport.AllocSend()
	hdr, oa := &o.Hdr, params.ObjAttrs
	{
		hdr.Bck = params.BckTo.Bck
		hdr.ObjName = params.ObjNameTo
		hdr.ObjAttrs.CopyFrom(oa)
	}
	o.Callback = func(_ transport.ObjHdr, _ io.ReadCloser, _ interface{}, _ error) {
		cluster.FreeLOM(lom)
	}
	return params.DM.Send(o, params.Reader, params.Tsi)
}

/////////////////////////
// APPEND (to archive) //
/////////////////////////

func (aaoi *appendArchObjInfo) appendToArch(fqn string) error {
	fh, err := cos.OpenTarForAppend(aaoi.lom.String(), fqn)
	if err != nil {
		return err
	}
	buf, slab := aaoi.t.gmm.AllocSize(aaoi.size)
	tw := tar.NewWriter(fh)
	hdr := tar.Header{
		Typeflag: tar.TypeReg,
		Name:     aaoi.filename,
		Size:     aaoi.size,
		ModTime:  aaoi.started,
		Mode:     int64(cos.PermRWR), // default value '0' - no access at all
	}
	tw.WriteHeader(&hdr)
	_, err = io.CopyBuffer(tw, aaoi.r, buf)
	tw.Close()
	cos.Close(fh)
	slab.Free(buf)
	if err != nil {
		return err
	}

	st, err := os.Stat(fqn)
	if err != nil {
		return err
	}
	aaoi.lom.SetSize(st.Size())
	aaoi.lom.SetCksum(cos.NewCksum(cos.ChecksumNone, ""))
	aaoi.lom.SetAtimeUnix(aaoi.started.UnixNano())
	return nil
}

func (aaoi *appendArchObjInfo) finalize(fqn string) error {
	if err := os.Rename(fqn, aaoi.lom.FQN); err != nil {
		return err
	}
	if err := aaoi.lom.Persist(true); err != nil {
		return err
	}
	if aaoi.lom.Bprops().EC.Enabled {
		if err := ec.ECM.EncodeObject(aaoi.lom); err != nil && err != ec.ErrorECDisabled {
			return err
		}
	}
	aaoi.t.putMirror(aaoi.lom)
	return nil
}

func (aaoi *appendArchObjInfo) begin() (string, error) {
	workFQN := fs.CSM.GenContentFQN(aaoi.lom, fs.WorkfileType, fs.WorkfileAppendToArch)
	if err := os.Rename(aaoi.lom.FQN, workFQN); err != nil {
		return "", err
	}
	return workFQN, nil
}

func (aaoi *appendArchObjInfo) abort(fqn string) {
	aaoi.lom.Uncache(true)
	if err := os.Rename(aaoi.lom.FQN, fqn); err != nil {
		glog.Errorf("nested error while rolling back %s: %v", aaoi.lom.ObjName, err)
	}
}

func (aaoi *appendArchObjInfo) appendObject() (errCode int, err error) {
	if aaoi.filename == "" {
		return http.StatusBadRequest, errors.New("archive path is not defined")
	}
	// Standard library does not support appending to archive of any type.
	// For TAR there is a working workaround, so allow appending to TAR only.
	if aaoi.mime != cos.ExtTar {
		return http.StatusBadRequest, fmt.Errorf("append is supported only for %s archives", cos.ExtTar)
	}
	workFQN, err := aaoi.begin()
	if err != nil {
		return http.StatusInternalServerError, err
	}
	if err = aaoi.appendToArch(workFQN); err == nil {
		if err = aaoi.finalize(workFQN); err == nil {
			return 0, nil
		}
	}
	aaoi.abort(workFQN)
	return http.StatusInternalServerError, err
}

// PUT(lom) => destination target
// NOTE: always closes params.Reader, either explicitly or via Do()
func (t *targetrunner) _sendPUT(params *cluster.SendToParams) error {
	var (
		hdr   = make(http.Header, 8)
		query = cmn.AddBckToQuery(nil, params.BckTo.Bck)
	)
	cmn.ToHeader(params.ObjAttrs, hdr)
	hdr.Set(cmn.HdrPutterID, t.si.ID())
	query.Set(cmn.URLParamRecvType, strconv.Itoa(int(cluster.Migrated)))
	reqArgs := cmn.ReqArgs{
		Method: http.MethodPut,
		Base:   params.Tsi.URL(cmn.NetworkIntraData),
		Path:   cmn.URLPathObjects.Join(params.BckTo.Name, params.ObjNameTo),
		Query:  query,
		Header: hdr,
		BodyR:  params.Reader,
	}
	config := cmn.GCO.Get()
	req, _, cancel, err := reqArgs.ReqWithTimeout(config.Timeout.SendFile.D())
	if err != nil {
		cos.Close(params.Reader)
		return fmt.Errorf("unexpected failure to create request, err: %w", err)
	}
	defer cancel()
	resp, err := t.client.data.Do(req)
	if err != nil {
		return fmt.Errorf(cmn.FmtErrFailed, t.si, "PUT to", reqArgs.URL(), err)
	}
	cos.DrainReader(resp.Body)
	resp.Body.Close()
	return nil
}

///////////////
// mem pools //
///////////////

var (
	goiPool, poiPool, coiPool, sndPool sync.Pool

	goi0 getObjInfo
	poi0 putObjInfo
	coi0 copyObjInfo
	snd0 cluster.SendToParams
)

func allocGetObjInfo() (a *getObjInfo) {
	if v := goiPool.Get(); v != nil {
		a = v.(*getObjInfo)
		return
	}
	return &getObjInfo{}
}

func freeGetObjInfo(a *getObjInfo) {
	*a = goi0
	goiPool.Put(a)
}

func allocPutObjInfo() (a *putObjInfo) {
	if v := poiPool.Get(); v != nil {
		a = v.(*putObjInfo)
		return
	}
	return &putObjInfo{}
}

func freePutObjInfo(a *putObjInfo) {
	*a = poi0
	poiPool.Put(a)
}

func allocCopyObjInfo() (a *copyObjInfo) {
	if v := coiPool.Get(); v != nil {
		a = v.(*copyObjInfo)
		return
	}
	return &copyObjInfo{}
}

func freeCopyObjInfo(a *copyObjInfo) {
	*a = coi0
	coiPool.Put(a)
}

func allocSendParams() (a *cluster.SendToParams) {
	if v := sndPool.Get(); v != nil {
		a = v.(*cluster.SendToParams)
		return
	}
	return &cluster.SendToParams{}
}

func freeSendParams(a *cluster.SendToParams) {
	*a = snd0
	sndPool.Put(a)
}
