// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
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
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
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
	"github.com/NVIDIA/aistore/xact/xreg"
)

//
// PUT, GET, APPEND (to file | to archive), and COPY object
//

const fmtNested = "%s: nested (%v): failed to %s %q: %v"

type (
	putObjInfo struct {
		atime      time.Time
		t          *target
		lom        *cluster.LOM
		r          io.ReadCloser // reader that has the content
		cksumToUse *cos.Cksum    // if available (not `none`), can be validated and will be stored
		size       int64         // object size aka Content-Length
		workFQN    string        // temp fqn to be renamed
		xctn       cluster.Xact  // xaction that puts
		owt        cmn.OWT       // object write transaction enum { OwtPut, ..., OwtGet* }
		restful    bool          // being invoked via RESTful API
		t2t        bool          // by another target
		skipEC     bool          // do not erasure-encode when finalizing
		skipVC     bool          // skip loading existing Version and skip comparing Checksums (skip VC)
	}

	getObjInfo struct {
		atime    int64
		nanotim  int64
		t        *target
		lom      *cluster.LOM
		w        io.Writer       // not necessarily http.ResponseWriter
		ctx      context.Context // context used when getting object from remote backend (access creds)
		ranges   byteRanges      // range read (see https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35)
		archive  archiveQuery    // archive query
		isGFN    bool            // is GFN request
		chunked  bool            // chunked transfer (en)coding: https://tools.ietf.org/html/rfc7230#page-36
		unlocked bool
	}

	// Contains information packed in append handle.
	handleInfo struct {
		nodeID       string
		filePath     string
		partialCksum *cos.CksumHash
	}

	appendObjInfo struct {
		started time.Time // started time of receiving - used to calculate the recv duration
		t       *target
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
		t        *target
		owt      cmn.OWT
		finalize bool // copies and EC (as in poi.finalize())
		dryRun   bool
	}
	sendArgs struct {
		reader    cos.ReadOpenCloser
		bckTo     *cluster.Bck
		objNameTo string
		tsi       *cluster.Snode
		dm        cluster.DataMover
		objAttrs  cmn.ObjAttrsHolder
		owt       cmn.OWT
	}

	appendArchObjInfo struct {
		started time.Time // started time of receiving - used to calculate the recv duration
		t       *target
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

func (poi *putObjInfo) do(r *http.Request, dpq *dpq) (int, error) {
	{
		poi.r = r.Body
		poi.workFQN = fs.CSM.Gen(poi.lom, fs.WorkfileType, fs.WorkfilePut)
		poi.cksumToUse = poi.lom.ObjAttrs().FromHeader(r.Header)
		poi.owt = cmn.OwtPut // default
	}
	if dpq.owt != "" {
		poi.owt.FromS(dpq.owt)
	}
	if dpq.uuid != "" {
		// resolve cluster-wide xact "behind" this PUT (promote via a single target won't show up)
		if xctn := xreg.GetXact(dpq.uuid); xctn != nil {
			poi.xctn = xctn
		}
	}
	if sizeStr := r.Header.Get(cmn.HdrContentLength); sizeStr != "" {
		if size, ers := strconv.ParseInt(sizeStr, 10, 64); ers == nil {
			poi.size = size
		}
	}
	return poi.putObject()
}

func (poi *putObjInfo) putObject() (int, error) {
	lom := poi.lom
	// PUT is a no-op if the checksums do match
	if !poi.skipVC && !poi.cksumToUse.IsEmpty() {
		if lom.EqCksum(poi.cksumToUse) {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("destination %s has identical %s: PUT is a no-op", lom, poi.cksumToUse)
			}
			cos.DrainReader(poi.r)
			return 0, nil
		}
	}
	if err := poi.write(); err != nil {
		return http.StatusInternalServerError, err
	}
	if errCode, err := poi.finalize(); err != nil {
		return errCode, err
	}
	// NOTE: counting only user PUTs
	if poi.owt == cmn.OwtPut && poi.restful && !poi.t2t {
		delta := time.Since(poi.atime)
		poi.t.statsT.AddMany(
			cos.NamedVal64{Name: stats.PutCount, Value: 1},
			cos.NamedVal64{Name: stats.PutLatency, Value: int64(delta)},
		)
	}
	// xaction in-objs counters, promote first
	if poi.t2t && poi.xctn != nil && poi.owt == cmn.OwtPromote {
		poi.xctn.InObjsAdd(1, poi.lom.SizeBytes())
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof(poi.loghdr())
	}
	return 0, nil
}

func (poi *putObjInfo) loghdr() string {
	s := poi.owt.String() + ", " + poi.lom.String()
	if poi.xctn != nil { // may not be showing remote xaction (see doPut)
		s += ", " + poi.xctn.String()
	}
	if poi.skipVC {
		s += ", skip-vc"
	}
	if poi.t2t {
		s += ", t2t"
	}
	return s
}

func (poi *putObjInfo) finalize() (errCode int, err error) {
	if errCode, err = poi.fini(); err != nil {
		if err1 := cos.Stat(poi.workFQN); err1 == nil || !os.IsNotExist(err1) {
			if err1 == nil {
				err1 = err
			}
			poi.t.fsErr(err1, poi.workFQN)
			if err2 := cos.RemoveFile(poi.workFQN); err2 != nil {
				glog.Errorf(fmtNested, poi.t, err1, "remove", poi.workFQN, err2)
			}
		}
		poi.lom.Uncache(true /*delDirty*/)
		return
	}
	if !poi.skipEC {
		if ecErr := ec.ECM.EncodeObject(poi.lom); ecErr != nil && ecErr != ec.ErrorECDisabled {
			err = ecErr
			if cmn.IsErrCapacityExceeded(err) {
				errCode = http.StatusInsufficientStorage
			}
			return
		}
	}
	poi.t.putMirror(poi.lom)
	return
}

// poi.workFQN => LOM
func (poi *putObjInfo) fini() (errCode int, err error) {
	var (
		lom = poi.lom
		bck = lom.Bck()
		bmd = poi.t.owner.bmd.Get()
	)
	// remote versioning
	if bck.IsRemote() && (poi.owt == cmn.OwtPut || poi.owt == cmn.OwtFinalize || poi.owt == cmn.OwtPromote) {
		errCode, err = poi.putRemote()
		if err != nil {
			loghdr := poi.loghdr()
			glog.Errorf("PUT (%s): %v(%d)", loghdr, err, errCode)
			if errCode != http.StatusServiceUnavailable {
				return
			}
			// e.g.: "googleapi: Error 503: We encountered an internal error. Please try again."
			time.Sleep(time.Second)
			errCode, err = poi.putRemote()
			if err != nil {
				return
			}
			glog.Infof("PUT (%s): retried OK", loghdr)
		}
	}
	if _, present := bmd.Get(bck); !present {
		err = fmt.Errorf("PUT (%s): %q does not exist", poi.loghdr(), bck)
		errCode = http.StatusBadRequest
		return
	}

	// NOTE: see GetCold() implementation and cmn.OWT
	switch poi.owt {
	case cmn.OwtGetTryLock, cmn.OwtGetLock, cmn.OwtGet:
		debug.AssertFunc(func() bool { _, exclusive := lom.IsLocked(); return exclusive })
	case cmn.OwtGetPrefetchLock:
		if !lom.TryLock(true) {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Warningf("(%s) is busy", poi.loghdr())
			}
			return 0, cmn.ErrSkip // e.g. prefetch can skip it and keep on going
		}
		defer lom.Unlock(true)
	default:
		lom.Lock(true)
		defer lom.Unlock(true)
	}

	// ais versioning
	if bck.IsAIS() && lom.VersionConf().Enabled {
		if poi.owt == cmn.OwtPut || poi.owt == cmn.OwtFinalize || poi.owt == cmn.OwtPromote {
			if poi.skipVC {
				err = lom.IncVersion()
				debug.Assert(err == nil)
			} else if remSrc, ok := lom.GetCustomKey(cmn.SourceObjMD); !ok || remSrc == "" {
				if err = lom.IncVersion(); err != nil {
					glog.Error(err)
				}
			}
		}
	}
	if err = cos.Rename(poi.workFQN, lom.FQN); err != nil {
		err = cmn.NewErrFailedTo(poi.t, "rename", lom, err)
		return
	}
	if lom.HasCopies() {
		if errdc := lom.DelAllCopies(); errdc != nil {
			glog.Errorf("PUT (%s): failed to delete old copies [%v], proceeding to PUT anyway...", poi.loghdr(), errdc)
		}
	}
	if lom.AtimeUnix() == 0 {
		lom.SetAtimeUnix(poi.atime.UnixNano())
		debug.Assert(lom.AtimeUnix() != 0)
	}
	err = lom.Persist()
	return
}

// via backend.PutObj()
func (poi *putObjInfo) putRemote() (errCode int, err error) {
	var (
		lom     = poi.lom
		backend = poi.t.Backend(lom.Bck())
	)
	lmfh, err := cos.NewFileHandle(poi.workFQN)
	if err != nil {
		err = cmn.NewErrFailedTo(poi.t, "open", poi.workFQN, err)
		return
	}
	if poi.owt == cmn.OwtPut && !lom.Bck().IsRemoteAIS() {
		// some/all of those are set by the backend.PutObj()
		lom.ObjAttrs().DelCustomKeys(cmn.SourceObjMD, cmn.CRC32CObjMD, cmn.ETag, cmn.MD5ObjMD, cmn.VersionObjMD)
	}
	errCode, err = backend.PutObj(lmfh, lom)
	if err == nil && !lom.Bck().IsRemoteAIS() {
		lom.SetCustomKey(cmn.SourceObjMD, backend.Provider())
	}
	return
}

// LOM is updated at the end of this call with size and checksum.
// `poi.r` (reader) is also closed upon exit.
func (poi *putObjInfo) write() (err error) {
	var (
		written int64
		buf     []byte
		slab    *memsys.Slab
		lmfh    *os.File
		writer  io.Writer
		writers = make([]io.Writer, 0, 4)
		cksums  = struct {
			store *cos.CksumHash // store with LOM
			given *cos.CksumHash // compute additionally
			expct *cos.Cksum     // and validate against `expct` if required/available
		}{}
		ckconf = poi.lom.CksumConf()
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
	defer func() {
		poi._cleanup(buf, slab, lmfh, err)
	}()
	// checksums
	if ckconf.Type == cos.ChecksumNone {
		poi.lom.SetCksum(cos.NoneCksum)
		goto write
	}
	if !poi.cksumToUse.IsEmpty() && !poi.validateCksum(ckconf) {
		// if the corresponding validation is not configured/enabled we just go ahead
		// and use the checksum that has arrived with the object
		poi.lom.SetCksum(poi.cksumToUse)
		goto write
	}

	// compute checksum and save it as part of the object metadata
	cksums.store = cos.NewCksumHash(ckconf.Type)
	writers = append(writers, cksums.store.H)
	if !poi.skipVC && !poi.cksumToUse.IsEmpty() && poi.validateCksum(ckconf) {
		// if validate-cold-get and the cksum is provided we should also check md5 hash (aws, gcp)
		// or if the object is migrated, and `ckconf.ValidateObjMove` we should check with existing checksum
		cksums.expct = poi.cksumToUse
		cksums.given = cos.NewCksumHash(poi.cksumToUse.Type())
		writers = append(writers, cksums.given.H)
	}
write:
	if len(writers) == 0 {
		written, err = io.CopyBuffer(writer, poi.r /*reader*/, buf)
	} else {
		writers = append(writers, writer)
		written, err = io.CopyBuffer(cos.NewWriterMulti(writers...), poi.r /*reader*/, buf)
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
				cos.NamedVal64{Name: stats.ErrCksumCount, Value: 1},
				cos.NamedVal64{Name: stats.ErrCksumSize, Value: written},
			)
			return
		}
	}

	// ok
	cos.Close(lmfh)
	lmfh = nil
	poi.lom.SetSize(written) // TODO: compare with non-zero lom.SizeBytes() that may have been set via oa.FromHeader()
	if cksums.store != nil {
		cksums.store.Finalize()
		poi.lom.SetCksum(&cksums.store.Cksum)
	}
	return
}

// post-write close & cleanup
func (poi *putObjInfo) _cleanup(buf []byte, slab *memsys.Slab, lmfh *os.File, err error) {
	if buf != nil {
		slab.Free(buf)
	}
	if err == nil {
		debug.Assert(lmfh == nil)
		cos.Close(poi.r)
		return // ok
	}

	// not ok
	poi.r.Close()
	debug.Assert(lmfh != nil)
	if nerr := lmfh.Close(); nerr != nil {
		glog.Errorf(fmtNested, poi.t, err, "close", poi.workFQN, nerr)
	}
	if nerr := cos.RemoveFile(poi.workFQN); nerr != nil {
		glog.Errorf(fmtNested, poi.t, err, "remove", poi.workFQN, nerr)
	}
}

func (poi *putObjInfo) validateCksum(c *cmn.CksumConf) (v bool) {
	switch poi.owt {
	case cmn.OwtMigrate, cmn.OwtPromote, cmn.OwtFinalize:
		v = c.ValidateObjMove
	case cmn.OwtPut, cmn.OwtGetTryLock, cmn.OwtGetLock, cmn.OwtGet:
		v = c.ValidateColdGet
	case cmn.OwtGetPrefetchLock:
	default:
		debug.Assert(false)
	}
	return
}

////////////////
// GET OBJECT //
////////////////

func (goi *getObjInfo) getObject() (errCode int, err error) {
	debug.Assert(!goi.unlocked)
	goi.lom.Lock(false)
	errCode, err = goi.get()
	if !goi.unlocked {
		goi.lom.Unlock(false)
	}
	return errCode, err
}

// is under rlock
func (goi *getObjInfo) get() (errCode int, err error) {
	var (
		cs                          fs.CapStatus
		doubleCheck, retry, retried bool
		cold                        bool
	)
do:
	err = goi.lom.Load(true /*cache it*/, true /*locked*/)
	if err != nil {
		cold = cmn.IsObjNotExist(err)
		if !cold {
			errCode = http.StatusInternalServerError
			return
		}
		cs = fs.GetCapStatus()
		if cs.OOS {
			errCode, err = http.StatusInsufficientStorage, cs.Err
			return
		}
	}

	if cold {
		if goi.lom.Bck().IsAIS() { // ais bucket with no backend - try lookup and restore
			goi.lom.Unlock(false)
			doubleCheck, errCode, err = goi.restoreFromAny(false /*skipLomRestore*/)
			if doubleCheck && err != nil {
				lom2 := cluster.AllocLOM(goi.lom.ObjName)
				er2 := lom2.InitBck(goi.lom.Bucket())
				if er2 == nil {
					er2 = lom2.Load(true /*cache it*/, false /*locked*/)
				}
				if er2 == nil {
					cluster.FreeLOM(goi.lom)
					goi.lom = lom2
					err = nil
				} else {
					cluster.FreeLOM(lom2)
				}
			}
			if err != nil {
				goi.unlocked = true
				return
			}
			goi.lom.Lock(false)
			if err = goi.lom.Load(true /*cache it*/, true /*locked*/); err != nil {
				return
			}
			goto fin
		}
	} else if goi.lom.Bck().IsRemote() && goi.lom.VersionConf().ValidateWarmGet { // check remote version
		var equal bool
		goi.lom.Unlock(false)
		if equal, errCode, err = goi.t.CompareObjects(goi.ctx, goi.lom); err != nil {
			goi.lom.Uncache(true /*delDirty*/)
			goi.unlocked = true
			return
		}
		cold = !equal
		if cold {
			if err = goi.lom.AllowDisconnectedBackend(true /*loaded*/); err != nil {
				goi.unlocked = true
				return
			}
		}
		goi.lom.Lock(false)
	}

	if !cold && goi.lom.CksumConf().ValidateWarmGet { // validate checksums and recover (self-heal) if corrupted
		cold, errCode, err = goi.validateRecover()
		if err != nil {
			if !cold {
				glog.Error(err)
				return
			}
			glog.Errorf("%v - proceeding to cold-GET from %s", err, goi.lom.Bck())
		}
	}

	// cold GET
	if cold {
		if cs.IsNil() {
			cs = fs.GetCapStatus()
		}
		if cs.OOS {
			errCode, err = http.StatusInsufficientStorage, cs.Err
			return
		}
		goi.lom.SetAtimeUnix(goi.atime)
		// (will upgrade rlock => wlock)
		if errCode, err = goi.t.GetCold(goi.ctx, goi.lom, cmn.OwtGet); err != nil {
			goi.unlocked = true
			return
		}
	}

	// read locally and stream back
fin:
	retry, errCode, err = goi.finalize(cold)
	if retry && !retried {
		debug.Assert(err != errSendingResp)
		glog.Warningf("GET %s: retrying...", goi.lom)
		retried = true // only once
		goi.lom.Uncache(true /*delDirty*/)
		goto do
	}
	return
}

// - validate checksums
// - if corrupted and IsAIS, try to recover from redundant replicas or EC slices
// - otherwise, rely on the remote backend for recovery (tradeoff; TODO: make it configurable)
func (goi *getObjInfo) validateRecover() (coldGet bool, code int, err error) {
	var (
		lom     = goi.lom
		retried bool
	)
validate:
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
		if erl := lom.Remove(true /*force through rlock*/); erl != nil {
			glog.Warningf("%s: failed to remove corrupted %s, err: %v", goi.t, lom, erl)
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
		restored := lom.RestoreToLocation()
		goi.lom.Lock(false)
		if restored {
			glog.Warningf("%s: recovered corrupted %s from local replica", goi.t, lom)
			code = 0
			goto validate
		}
	}
	if lom.Bprops().EC.Enabled {
		retried = true
		goi.lom.Unlock(false)
		cos.RemoveFile(lom.FQN)
		_, code, err = goi.restoreFromAny(true /*skipLomRestore*/)
		goi.lom.Lock(false)
		if err == nil {
			glog.Warningf("%s: recovered corrupted %s from EC slices", goi.t, lom)
			code = 0
			goto validate
		}
	}

	// TODO: ditto
	if erl := lom.Remove(true /*force through rlock*/); erl != nil {
		glog.Warningf("%s: failed to remove corrupted %s, err: %v", goi.t, lom, erl)
	}
	return
}

// attempt to restore an object from any/all of the below:
// 1) local copies (other FSes on this target)
// 2) other targets (when resilvering or rebalancing is running (aka GFN))
// 3) other targets if the bucket erasure coded
// 4) Cloud
func (goi *getObjInfo) restoreFromAny(skipLomRestore bool) (doubleCheck bool, errCode int, err error) {
	var (
		tsi   *cluster.Snode
		smap  = goi.t.owner.smap.get()
		tname = goi.t.si.String()
	)
	tsi, err = cluster.HrwTargetAll(goi.lom.Uname(), &smap.Smap) // including targets in maintenance
	if err != nil {
		return
	}
	if !skipLomRestore {
		// when resilvering:
		// (whether or not resilvering is active depends on the context: mountpath events vs GET)
		var (
			marked               = xreg.GetResilverMarked()
			interrupted, running = marked.Interrupted, marked.Xact != nil
			gfnActive            = goi.t.res.IsActive(3 /*interval-of-inactivity multiplier*/)
		)
		if interrupted || running || gfnActive {
			if goi.lom.RestoreToLocation() { // from copies
				if glog.FastV(4, glog.SmoduleAIS) {
					glog.Infof("%s restored", goi.lom)
				}
				return
			}
			doubleCheck = running
		}
	}

	// when rebalancing: cluster-wide lookup (aka "get from neighbor" or GFN)
	var (
		gfnNode              *cluster.Snode
		marked               = xreg.GetRebMarked()
		interrupted, running = marked.Interrupted, marked.Xact != nil
		gfnActive            = reb.IsActiveGFN() // GFN(global rebalance)
		ecEnabled            = goi.lom.Bprops().EC.Enabled
		// TODO: if there're not enough EC targets to restore a sliced object,
		//       we might still be able to restore it from its full replica
		enoughECRestoreTargets = goi.lom.Bprops().EC.RequiredRestoreTargets() <= smap.CountActiveTargets()
	)
	if running {
		doubleCheck = true
	}
	if running && tsi.ID() != goi.t.si.ID() {
		if goi.t.HeadObjT2T(goi.lom, tsi) {
			gfnNode = tsi
			goto gfn
		}
	}
	if running || !enoughECRestoreTargets || ((interrupted || gfnActive) && !ecEnabled) {
		gfnNode = goi.t.headObjBcast(goi.lom, smap)
	}
gfn:
	if gfnNode != nil {
		if goi.getFromNeighbor(goi.lom, gfnNode) {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s: gfn %s <= %s", tname, goi.lom, gfnNode)
			}
			return
		}
	}

	// restore from existing EC slices, if possible
	ecErr := ec.ECM.RestoreObject(goi.lom)
	if ecErr == nil {
		ecErr = goi.lom.Load(true /*cache it*/, false /*locked*/) // TODO: optimize locking
		debug.AssertNoErr(ecErr)
		if ecErr == nil {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s: EC-recovered %s", tname, goi.lom)
			}
			return
		}
		err = cmn.NewErrFailedTo(tname, "load EC-recovered", goi.lom, ecErr)
	} else if ecErr != ec.ErrorECDisabled {
		err = cmn.NewErrFailedTo(tname, "EC-recover", goi.lom, ecErr)
		if cmn.IsErrCapacityExceeded(ecErr) {
			errCode = http.StatusInsufficientStorage
		}
		return
	}

	if err != nil {
		err = cmn.NewErrFailedTo(goi.t, "goi-restore-any", goi.lom, err)
	} else {
		err = cmn.NewErrNotFound("%s: %s", goi.t.si, goi.lom.FullName())
	}
	errCode = http.StatusNotFound
	return
}

func (goi *getObjInfo) getFromNeighbor(lom *cluster.LOM, tsi *cluster.Snode) bool {
	query := lom.Bck().AddToQuery(nil)
	query.Set(apc.QparamIsGFNRequest, "true")
	reqArgs := cmn.AllocHra()
	{
		reqArgs.Method = http.MethodGet
		reqArgs.Base = tsi.URL(cmn.NetIntraData)
		reqArgs.Header = http.Header{
			apc.HdrCallerID:   []string{goi.t.SID()},
			apc.HdrCallerName: []string{goi.t.callerName()},
		}
		reqArgs.Path = apc.URLPathObjects.Join(lom.Bck().Name, lom.ObjName)
		reqArgs.Query = query
	}
	config := cmn.GCO.Get()
	req, _, cancel, err := reqArgs.ReqWithTimeout(config.Timeout.SendFile.D())
	if err != nil {
		debug.AssertNoErr(err)
		return false
	}
	defer cancel()

	resp, err := goi.t.client.data.Do(req) // nolint:bodyclose // closed by `poi.putObject`
	cmn.FreeHra(reqArgs)
	if err != nil {
		glog.Errorf("%s: gfn failure, %s %q, err: %v", goi.t, tsi, lom, err)
		return false
	}

	cksumToUse := lom.ObjAttrs().FromHeader(resp.Header)
	workFQN := fs.CSM.Gen(lom, fs.WorkfileType, fs.WorkfileRemote)
	poi := allocPutObjInfo()
	{
		poi.t = goi.t
		poi.lom = lom
		poi.r = resp.Body
		poi.owt = cmn.OwtMigrate
		poi.workFQN = workFQN
		poi.cksumToUse = cksumToUse
	}
	errCode, erp := poi.putObject()
	freePutObjInfo(poi)
	if erp == nil {
		return true
	}
	glog.Errorf("%s: gfn-GET failed to PUT locally: %v(%d)", goi.t, erp, errCode)
	return false
}

func (goi *getObjInfo) finalize(coldGet bool) (retry bool, errCode int, err error) {
	var (
		lmfh    *os.File
		slab    *memsys.Slab
		buf     []byte
		hdr     http.Header
		written int64
	)
	if resp, ok := goi.w.(http.ResponseWriter); ok {
		hdr = resp.Header()
	}
	fqn := goi.lom.FQN
	if !coldGet && !goi.isGFN {
		// best-effort GET load balancing (see also mirror.findLeastUtilized())
		fqn = goi.lom.LBGet()
	}
	// open
	lmfh, err = os.Open(fqn)
	if err != nil {
		if os.IsNotExist(err) {
			errCode = http.StatusNotFound
			retry = true // (!lom.IsAIS() || lom.ECEnabled() || GFN...)
		} else {
			goi.t.fsErr(err, fqn)
			errCode = http.StatusInternalServerError
			err = cmn.NewErrFailedTo(goi.t, "goi-finalize", goi.lom, err, errCode)
		}
		return
	}

	var (
		rrange     *cmn.HTTPRange
		reader     io.Reader = lmfh
		size                 = goi.lom.SizeBytes()
		cksumConf            = goi.lom.CksumConf()
		cksumRange bool
	)
	defer func() {
		if lmfh != nil {
			cos.Close(lmfh)
		}
		if buf != nil {
			slab.Free(buf)
		}
	}()
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
		cmn.ToHeader(goi.lom.ObjAttrs(), hdr)
	}

	// reader
	w := goi.w
	if rrange == nil {
		if goi.archive.filename != "" {
			var csl cos.ReadCloseSizer
			csl, err = goi.freadArch(lmfh)
			if err != nil {
				if cmn.IsErrNotFound(err) {
					errCode = http.StatusNotFound
				} else {
					err = cmn.NewErrFailedTo(goi.t, "extract "+goi.archive.filename+" from", goi.lom, err)
				}
				return
			}
			defer func() {
				csl.Close()
			}()
			reader, size = csl, csl.Size() // Content-Length
		}
		if goi.chunked {
			// NOTE: hide `ReadFrom` of the `http.ResponseWriter` (in re: sendfile)
			w = cos.WriterOnly{Writer: goi.w}
			if lmfh != nil {
				buf, slab = goi.t.gmm.AllocSize(size)
			}
		}
	} else {
		buf, slab = goi.t.gmm.AllocSize(rrange.Length)
		reader = io.NewSectionReader(lmfh, rrange.Start, rrange.Length)
		if cksumRange {
			var (
				cksum *cos.CksumHash
				n     int64
				sgl   = slab.MMSA().NewSGL(rrange.Length, slab.Size())
			)
			defer func() {
				sgl.Free()
			}()
			if n, cksum, err = cos.CopyAndChecksum(sgl, reader, buf, cksumConf.Type); err != nil {
				return
			}
			debug.Assert(n <= rrange.Length)
			hdr.Set(apc.HdrObjCksumVal, cksum.Value())
			hdr.Set(apc.HdrObjCksumType, cksumConf.Type)
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
		glog.Error(cmn.NewErrFailedTo(goi.t, "GET", fqn, err))
		// at this point, error is already written into the response
		// return special to indicate just that
		err = errSendingResp
		return
	}

	// GFN: atime must be already set
	if !coldGet && !goi.isGFN {
		goi.lom.Load(false /*cache it*/, true /*locked*/)
		goi.lom.SetAtimeUnix(goi.atime)
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
		cos.NamedVal64{Name: stats.GetThroughput, Value: written},
		cos.NamedVal64{Name: stats.GetLatency, Value: delta},
		cos.NamedVal64{Name: stats.GetCount, Value: 1},
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
		err = fmt.Errorf(cmn.FmtErrUnsupported, goi.t, "multi-range")
		errCode = http.StatusRequestedRangeNotSatisfiable
		return
	}
	if goi.archive.filename != "" {
		err = fmt.Errorf(cmn.FmtErrUnsupported, goi.t, "range-reading archived files")
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
	case apc.AppendOp:
		var f *os.File
		if filePath == "" {
			filePath = fs.CSM.Gen(aoi.lom, fs.WorkfileType, fs.WorkfileAppend)
			f, err = aoi.lom.CreateFile(filePath)
			if err != nil {
				errCode = http.StatusInternalServerError
				return
			}
			aoi.hi.partialCksum = cos.NewCksumHash(aoi.lom.CksumType())
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
	case apc.FlushOp:
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
		params := cluster.PromoteParams{
			Bck:   aoi.lom.Bck(),
			Cksum: partialCksum,
			PromoteArgs: cluster.PromoteArgs{
				SrcFQN:       filePath,
				ObjName:      aoi.lom.ObjName,
				OverwriteDst: true,
				DeleteSrc:    true, // NOTE: always overwrite and remove
			},
		}
		if errCode, err = aoi.t.Promote(params); err != nil {
			return
		}
	default:
		debug.AssertMsg(false, aoi.op)
	}

	delta := time.Since(aoi.started)
	aoi.t.statsT.AddMany(
		cos.NamedVal64{Name: stats.AppendCount, Value: 1},
		cos.NamedVal64{Name: stats.AppendLatency, Value: int64(delta)},
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
		return hi, fmt.Errorf("invalid APPEND handle: %q", handle)
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

func (coi *copyObjInfo) copyObject(lom *cluster.LOM, objNameTo string) (size int64, err error) {
	debug.Assert(coi.DP == nil)
	// remote to remote: no need to create local copies - use copyReader
	if lom.Bck().IsRemote() || coi.BckTo.IsRemote() {
		coi.DP = &cluster.LDP{}
		return coi.copyReader(lom, objNameTo)
	}
	if coi.dryRun {
		defer func() {
			if err == nil && coi.Xact != nil {
				coi.Xact.ObjsAdd(1, size)
			}
		}()
	}

	// remote
	smap := coi.t.owner.smap.Get()
	tsi, err := cluster.HrwTarget(coi.BckTo.MakeUname(objNameTo), smap)
	if err != nil {
		return 0, err
	}
	if tsi.ID() != coi.t.si.ID() {
		return coi.sendRemote(lom, objNameTo, tsi)
	}

	// dry-run
	if coi.dryRun {
		// TODO: replace with something similar to lom.FQN == dst.FQN, but dstBck might not exist.
		if lom.Bck().Equal(coi.BckTo, true /*same ID*/, true /*same backend*/) && lom.ObjName == objNameTo {
			return 0, nil
		}
		return lom.SizeBytes(), nil
	}

	// local
	dst := cluster.AllocLOM(objNameTo)
	defer cluster.FreeLOM(dst)
	if err = dst.InitBck(coi.BckTo.Bucket()); err != nil {
		return
	}
	if lom.FQN == dst.FQN { // resilvering with a single mountpath?
		return
	}
	exclusive := lom.Uname() == dst.Uname()
	lom.Lock(exclusive)
	defer lom.Unlock(exclusive)
	if err = lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		if !cmn.IsObjNotExist(err) {
			err = cmn.NewErrFailedTo(coi.t, "coi-load", lom, err)
		}
		return
	}
	// w-lock the destination unless overwriting the source
	if lom.Uname() != dst.Uname() {
		dst.Lock(true)
		defer dst.Unlock(true)
		if err = dst.Load(false /*cache it*/, true /*locked*/); err == nil {
			if lom.EqCksum(dst.Checksum()) {
				return
			}
		} else if cmn.IsErrBucketNought(err) {
			return
		}
	}
	dst2, err2 := lom.Copy2FQN(dst.FQN, coi.Buf)
	if err2 == nil {
		size = lom.SizeBytes()
		if coi.finalize {
			coi.t.putMirror(dst2)
		}
	}
	err = err2
	if dst2 != nil {
		cluster.FreeLOM(dst2)
	}
	// xaction stats: inc locally processed (and see data mover for in and out objs)
	if coi.Xact != nil {
		debug.Assert(coi.DM == nil || coi.Xact == coi.DM.GetXact())
		coi.Xact.ObjsAdd(1, lom.SizeBytes())
	}
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
//
// TODO: consider adding support for _not_ storing (aka "caching") an object from bucket that has remote backend.
func (coi *copyObjInfo) copyReader(lom *cluster.LOM, objNameTo string) (size int64, err error) {
	var (
		reader cos.ReadOpenCloser
		tsi    *cluster.Snode
	)
	debug.Assert(coi.DP != nil)
	if tsi, err = cluster.HrwTarget(coi.BckTo.MakeUname(objNameTo), coi.t.owner.smap.Get()); err != nil {
		return
	}
	if coi.dryRun {
		defer func() {
			if err == nil && coi.Xact != nil {
				coi.Xact.ObjsAdd(1, size)
			}
		}()
	}
	if tsi.ID() != coi.t.si.ID() {
		// remote
		return coi.sendRemote(lom, objNameTo, tsi)
	}

	// local
	if err = lom.Load(false /*cache it*/, false /*locked*/); err != nil {
		return
	}
	if coi.dryRun {
		// discard the reader and be done
		return coi.dryRunCopyReader(lom)
	}
	dst := cluster.AllocLOM(objNameTo)
	defer cluster.FreeLOM(dst)
	if err = dst.InitBck(coi.BckTo.Bucket()); err != nil {
		return
	}
	if lom.Bck().Equal(coi.BckTo, true, true) {
		dst.SetVersion(lom.Version())
	}
	if reader, _, err = coi.DP.Reader(lom); err != nil {
		return 0, err
	}
	params := cluster.AllocPutObjParams()
	{
		params.WorkTag = "copy-dp"
		params.Reader = reader
		// owt: some transactions must update the object in the Cloud(iff the destination is a Cloud bucket)
		if coi.DM != nil {
			params.OWT = coi.DM.OWT()
		} else {
			params.OWT = cmn.OwtMigrate
		}
		params.Atime = lom.Atime()
	}
	err = coi.t.PutObject(dst, params)
	cluster.FreePutObjParams(params)
	if err != nil {
		return
	}
	// xaction stats: inc locally processed (and see data mover for in and out objs)
	size = lom.SizeBytes()
	if coi.Xact != nil {
		debug.Assert(coi.DM == nil || coi.Xact == coi.DM.GetXact())
		coi.Xact.ObjsAdd(1, size)
	}
	return
}

func (coi *copyObjInfo) dryRunCopyReader(lom *cluster.LOM) (size int64, err error) {
	var reader io.ReadCloser
	if reader, _, err = coi.DP.Reader(lom); err != nil {
		return 0, err
	}
	defer reader.Close()
	return io.Copy(io.Discard, reader)
}

func (coi *copyObjInfo) sendRemote(lom *cluster.LOM, objNameTo string, tsi *cluster.Snode) (size int64, err error) {
	debug.Assert(coi.owt > 0)
	sargs := allocSnda()
	{
		sargs.objNameTo = objNameTo
		sargs.tsi = tsi
		sargs.dm = coi.DM
		if coi.DM != nil {
			sargs.owt = coi.DM.OWT() // takes precedence
		} else {
			sargs.owt = coi.owt
		}
	}
	size, err = coi.doSend(lom, sargs)
	freeSnda(sargs)
	return
}

// send object => designated target
// * source is a LOM or a reader (that may be reading from remote)
// * one of the two equivalent transmission mechanisms: PUT or transport Send
func (coi *copyObjInfo) doSend(lom *cluster.LOM, sargs *sendArgs) (size int64, err error) {
	if coi.DM != nil {
		// clone the `lom` to use it in the async operation (free it via `_sendObjDM` callback)
		lom = lom.CloneMD(lom.FQN)
	}
	if coi.DP == nil { // read from local file
		var reader cos.ReadOpenCloser
		if coi.owt != cmn.OwtPromote {
			// migrate/replicate
			lom.Lock(false)
			if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
				lom.Unlock(false)
				return 0, nil
			}
			if coi.dryRun {
				lom.Unlock(false)
				return lom.SizeBytes(), nil
			}
			fh, err := cos.NewFileHandle(lom.FQN)
			if err != nil {
				lom.Unlock(false)
				return 0, cmn.NewErrFailedTo(coi.t, "open", lom.FQN, err)
			}
			size = lom.SizeBytes()
			reader = cos.NewDeferROC(fh, func() { lom.Unlock(false) })
		} else {
			// promote
			debug.Assert(!coi.dryRun)
			debug.Assert(sargs.owt == cmn.OwtPromote)
			fh, err := cos.NewFileHandle(lom.FQN)
			if err != nil {
				if os.IsNotExist(err) {
					return 0, nil
				}
				return 0, cmn.NewErrFailedTo(coi.t, "open", lom.FQN, err)
			}
			fi, err := fh.Stat()
			if err != nil {
				fh.Close()
				return 0, cmn.NewErrFailedTo(coi.t, "fstat", lom.FQN, err)
			}
			size = fi.Size()
			reader = fh
		}
		sargs.reader = reader
		sargs.objAttrs = lom
	} else {
		if sargs.reader, sargs.objAttrs, err = coi.DP.Reader(lom); err != nil {
			return
		}
		if coi.dryRun {
			size, err = io.Copy(io.Discard, sargs.reader)
			cos.Close(sargs.reader)
			return
		}
		// NOTE: returns the cos.ContentLengthUnknown (-1) if post-transform size not known.
		size = sargs.objAttrs.SizeBytes()
	}

	// do
	sargs.bckTo = coi.BckTo
	if sargs.dm != nil {
		err = coi.dm(lom /*for attrs*/, sargs)
	} else {
		err = coi.put(sargs)
	}
	return
}

// transmit object to another target via data mover (compare with coi.put())
func (coi *copyObjInfo) dm(lom *cluster.LOM, sargs *sendArgs) error {
	debug.Assert(sargs.dm.OWT() == sargs.owt)
	debug.Assert(sargs.dm.GetXact() == coi.Xact || sargs.dm.GetXact().ID() == coi.Xact.ID())
	o := transport.AllocSend()
	hdr, oa := &o.Hdr, sargs.objAttrs
	{
		hdr.Bck.Copy(sargs.bckTo.Bucket())
		hdr.ObjName = sargs.objNameTo
		hdr.ObjAttrs.CopyFrom(oa)
	}
	o.Callback = func(_ transport.ObjHdr, _ io.ReadCloser, _ interface{}, _ error) {
		cluster.FreeLOM(lom)
	}
	return sargs.dm.Send(o, sargs.reader, sargs.tsi)
}

// PUT(lom) => destination target (compare with coi.dm())
// always closes params.Reader, either explicitly or via Do()
func (coi *copyObjInfo) put(sargs *sendArgs) error {
	var (
		hdr   = make(http.Header, 8)
		query = sargs.bckTo.AddToQuery(nil)
	)
	cmn.ToHeader(sargs.objAttrs, hdr)
	hdr.Set(apc.HdrT2TPutterID, coi.t.si.ID())
	query.Set(apc.QparamOWT, sargs.owt.ToS())
	if coi.Xact != nil {
		query.Set(apc.QparamUUID, coi.Xact.ID())
	}
	reqArgs := cmn.HreqArgs{
		Method: http.MethodPut,
		Base:   sargs.tsi.URL(cmn.NetIntraData),
		Path:   apc.URLPathObjects.Join(sargs.bckTo.Name, sargs.objNameTo),
		Query:  query,
		Header: hdr,
		BodyR:  sargs.reader,
	}
	config := cmn.GCO.Get()
	req, _, cancel, err := reqArgs.ReqWithTimeout(config.Timeout.SendFile.D())
	if err != nil {
		cos.Close(sargs.reader)
		return fmt.Errorf("unexpected failure to create request, err: %w", err)
	}
	defer cancel()
	resp, err := coi.t.client.data.Do(req)
	if err != nil {
		return cmn.NewErrFailedTo(coi.t, "coi.put "+sargs.bckTo.Name+"/"+sargs.objNameTo, sargs.tsi, err)
	}
	cos.DrainReader(resp.Body)
	resp.Body.Close()
	return nil
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
	aaoi.lom.SetAtimeUnix(aaoi.started.UnixNano())
	if err := aaoi.lom.Persist(); err != nil {
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
	workFQN := fs.CSM.Gen(aaoi.lom, fs.WorkfileType, fs.WorkfileAppendToArch)
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
	errCode = http.StatusInternalServerError
	if cmn.IsErrCapacityExceeded(err) {
		errCode = http.StatusInsufficientStorage
	}
	return
}

///////////////
// mem pools //
///////////////

var (
	goiPool, poiPool, coiPool, sndPool sync.Pool

	goi0 getObjInfo
	poi0 putObjInfo
	coi0 copyObjInfo
	snd0 sendArgs
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

func allocSnda() (a *sendArgs) {
	if v := sndPool.Get(); v != nil {
		a = v.(*sendArgs)
		return
	}
	return &sendArgs{}
}

func freeSnda(a *sendArgs) {
	*a = snd0
	sndPool.Put(a)
}
