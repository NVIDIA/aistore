// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
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

type (
	putOI struct {
		r          io.ReadCloser // reader that has the content
		xctn       cluster.Xact  // xaction that puts
		t          *target       // this
		lom        *cluster.LOM  // obj
		cksumToUse *cos.Cksum    // if available (not `none`), can be validated and will be stored
		resphdr    http.Header   // as implied
		workFQN    string        // temp fqn to be renamed
		atime      int64         // access time
		size       int64         // aka Content-Length
		owt        cmn.OWT       // object write transaction enum { OwtPut, ..., OwtGet* }
		restful    bool          // being invoked via RESTful API
		t2t        bool          // by another target
		skipEC     bool          // do not erasure-encode when finalizing
		skipVC     bool          // skip loading existing Version and skip comparing Checksums (skip VC)
	}

	getOI struct {
		w          http.ResponseWriter
		ctx        context.Context // context used when getting object from remote backend (access creds)
		t          *target         // this
		lom        *cluster.LOM    // obj
		archive    archiveQuery    // archive query
		ranges     byteRanges      // range read (see https://www.rfc-editor.org/rfc/rfc7233#section-2.1)
		atime      int64           // access time
		latency    int64           // get vnanoseconds
		isGFN      bool            // is GFN
		chunked    bool            // chunked transfer (en)coding: https://tools.ietf.org/html/rfc7230#page-36
		unlocked   bool            // internal
		verchanged bool            // version changed
		retry      bool            // once
	}

	// append handle (packed)
	aoHdl struct {
		partialCksum *cos.CksumHash
		nodeID       string
		filePath     string
	}
	// (see also apndArchI)
	apndOI struct {
		started time.Time     // started time of receiving - used to calculate the recv duration
		r       io.ReadCloser // reader with the content of the object.
		t       *target       // this
		lom     *cluster.LOM  // append to
		cksum   *cos.Cksum    // expected checksum of the final object.
		hdl     aoHdl         // packed
		op      string        // operation (Append | Flush)
		size    int64         // Content-Length
	}

	copyOI struct {
		t *target
		cluster.CopyObjectParams
		owt      cmn.OWT
		finalize bool // copies and EC (as in poi.finalize())
		dryRun   bool
	}
	sendArgs struct {
		reader    cos.ReadOpenCloser
		dm        cluster.DataMover
		objAttrs  cos.OAH
		tsi       *meta.Snode
		bckTo     *meta.Bck
		objNameTo string
		owt       cmn.OWT
	}

	apndArchI struct {
		r        io.ReadCloser // read bytes to append
		t        *target       // this
		lom      *cluster.LOM  // resulting shard
		filename string        // fqn inside
		mime     string        // format
		started  time.Time     // time of receiving
		size     int64         // aka Content-Length
		put      bool          // apc.HdrPutIfNotExist
	}
)

//
// PUT(object)
//

func (poi *putOI) do(resphdr http.Header, r *http.Request, dpq *dpq) (int, error) {
	{
		poi.r = r.Body
		poi.resphdr = resphdr
		poi.workFQN = fs.CSM.Gen(poi.lom, fs.WorkfileType, fs.WorkfilePut)
		poi.cksumToUse = poi.lom.ObjAttrs().FromHeader(r.Header)
		poi.owt = cmn.OwtPut // default
	}
	if dpq.owt != "" {
		poi.owt.FromS(dpq.owt)
	}
	if dpq.uuid != "" {
		// resolve cluster-wide xact "behind" this PUT (promote via a single target won't show up)
		xctn, err := xreg.GetXact(dpq.uuid)
		if err != nil {
			glog.Error(err)
			return 0, err
		}
		if xctn != nil {
			poi.xctn = xctn
		}
	}
	if sizeStr := r.Header.Get(cos.HdrContentLength); sizeStr != "" {
		if size, ers := strconv.ParseInt(sizeStr, 10, 64); ers == nil {
			poi.size = size
		}
	}
	return poi.putObject()
}

func (poi *putOI) putObject() (errCode int, err error) {
	// PUT is a no-op if the checksums do match
	if !poi.skipVC && !poi.cksumToUse.IsEmpty() {
		if poi.lom.EqCksum(poi.cksumToUse) {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("destination %s has identical %s: PUT is a no-op", poi.lom, poi.cksumToUse)
			}
			cos.DrainReader(poi.r)
			return 0, nil
		}
	}
	if err = poi.write(); err != nil {
		errCode = http.StatusInternalServerError
		goto rerr
	}
	if errCode, err = poi.finalize(); err != nil {
		goto rerr
	}
	// stats
	if !poi.t2t {
		// NOTE: counting only user PUTs; ignoring EC and copies, on the one hand, and
		// same-checksum-skip-writing, on the other
		if poi.owt == cmn.OwtPut && poi.restful {
			debug.Assert(cos.IsValidAtime(poi.atime), poi.atime)
			now := time.Now().UnixNano()
			poi.t.statsT.AddMany(
				cos.NamedVal64{Name: stats.PutCount, Value: 1},
				cos.NamedVal64{Name: stats.PutThroughput, Value: poi.lom.SizeBytes()},
				cos.NamedVal64{Name: stats.PutLatency, Value: now - poi.atime},
			)
			// via /s3 (TODO: revisit)
			if poi.resphdr != nil {
				cmn.ToHeader(poi.lom.ObjAttrs(), poi.resphdr)
			}
		}
	} else if poi.xctn != nil && poi.owt == cmn.OwtPromote {
		// xaction in-objs counters, promote first
		poi.xctn.InObjsAdd(1, poi.lom.SizeBytes())
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof(poi.loghdr())
	}
	return
rerr:
	if poi.owt == cmn.OwtPut && poi.restful && !poi.t2t {
		poi.t.statsT.IncErr(stats.PutCount)
	}
	return
}

func (poi *putOI) loghdr() string {
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

func (poi *putOI) finalize() (errCode int, err error) {
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
func (poi *putOI) fini() (errCode int, err error) {
	var (
		lom = poi.lom
		bck = lom.Bck()
	)
	// put remote
	if bck.IsRemote() && (poi.owt == cmn.OwtPut || poi.owt == cmn.OwtFinalize || poi.owt == cmn.OwtPromote) {
		errCode, err = poi.putRemote()
		if err != nil {
			loghdr := poi.loghdr()
			glog.Errorf("PUT (%s): %v(%d)", loghdr, err, errCode)
			if errCode != http.StatusServiceUnavailable {
				return
			}
			// (googleapi: "Error 503: We encountered an internal error. Please try again.")
			time.Sleep(time.Second)
			errCode, err = poi.putRemote()
			if err != nil {
				return
			}
			glog.Infof("PUT (%s): retried OK", loghdr)
		}
	}

	// locking strategies: optimistic and otherwise
	// (see GetCold() implementation and cmn.OWT enum)
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
		// expecting valid atime passed with `poi`
		debug.Assert(cos.IsValidAtime(poi.atime), poi.atime)
		lom.Lock(true)
		defer lom.Unlock(true)
		lom.SetAtimeUnix(poi.atime)
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

	// done
	if err = lom.RenameFrom(poi.workFQN); err != nil {
		return
	}
	if lom.HasCopies() {
		if errdc := lom.DelAllCopies(); errdc != nil {
			glog.Errorf("PUT (%s): failed to delete old copies [%v], proceeding to PUT anyway...", poi.loghdr(), errdc)
		}
	}
	if lom.AtimeUnix() == 0 { // (is set when migrating within cluster; prefetch special case)
		lom.SetAtimeUnix(poi.atime)
	}
	err = lom.PersistMain()
	return
}

// via backend.PutObj()
func (poi *putOI) putRemote() (errCode int, err error) {
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
func (poi *putOI) write() (err error) {
	var (
		written int64
		buf     []byte
		slab    *memsys.Slab
		lmfh    *os.File
		writer  io.Writer
		writers = make([]io.Writer, 0, 4)
		cksums  = struct {
			store     *cos.CksumHash // store with LOM
			expct     *cos.Cksum     // caller-provided (aka "end-to-end protection")
			compt     *cos.CksumHash // compute to validate `expct` - iff provided
			finalized bool           // to avoid computing the same checksum type twice
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
	// additional scenarios:
	// - validate-cold-get + cksums.expct (validate against cloud checksum)
	// - object migrated + `ckconf.ValidateObjMove`

	cksums.store = cos.NewCksumHash(ckconf.Type) // always according to the bucket
	writers = append(writers, cksums.store.H)
	if !poi.skipVC && !poi.cksumToUse.IsEmpty() && poi.validateCksum(ckconf) {
		cksums.expct = poi.cksumToUse
		if poi.cksumToUse.Type() == cksums.store.Type() {
			cksums.compt = cksums.store
		} else {
			// otherwise, compute separately
			cksums.compt = cos.NewCksumHash(poi.cksumToUse.Type())
			writers = append(writers, cksums.compt.H)
		}
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
	if cksums.compt != nil {
		cksums.finalized = cksums.compt == cksums.store
		cksums.compt.Finalize()
		if !cksums.compt.Equal(cksums.expct) {
			err = cos.NewBadDataCksumError(cksums.expct, &cksums.compt.Cksum, poi.lom.String())
			poi.t.statsT.AddMany(
				cos.NamedVal64{Name: stats.ErrCksumCount, Value: 1},
				cos.NamedVal64{Name: stats.ErrCksumSize, Value: written},
			)
			return
		}
	}

	// ok
	if cmn.Features.IsSet(feat.FsyncPUT) {
		err = lmfh.Sync() // compare w/ cos.FlushClose
		debug.AssertNoErr(err)
	}
	cos.Close(lmfh)
	lmfh = nil
	poi.lom.SetSize(written) // TODO: compare with non-zero lom.SizeBytes() that may have been set via oa.FromHeader()
	if cksums.store != nil {
		if !cksums.finalized {
			cksums.store.Finalize()
		}
		poi.lom.SetCksum(&cksums.store.Cksum)
	}
	return
}

// post-write close & cleanup
func (poi *putOI) _cleanup(buf []byte, slab *memsys.Slab, lmfh *os.File, err error) {
	if buf != nil {
		slab.Free(buf)
	}
	if err == nil {
		cos.Close(poi.r)
		return // ok
	}

	// not ok
	poi.r.Close()
	if nerr := lmfh.Close(); nerr != nil {
		glog.Errorf(fmtNested, poi.t, err, "close", poi.workFQN, nerr)
	}
	if nerr := cos.RemoveFile(poi.workFQN); nerr != nil {
		glog.Errorf(fmtNested, poi.t, err, "remove", poi.workFQN, nerr)
	}
}

func (poi *putOI) validateCksum(c *cmn.CksumConf) (v bool) {
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

//
// GET(object)
//

func (goi *getOI) getObject() (errCode int, err error) {
	debug.Assert(!goi.unlocked)
	goi.lom.Lock(false)
	errCode, err = goi.get()
	if !goi.unlocked {
		goi.lom.Unlock(false)
	}
	return errCode, err
}

// is under rlock
func (goi *getOI) get() (errCode int, err error) {
	var (
		cs          fs.CapStatus
		doubleCheck bool
		retried     bool
		cold        bool
	)
do:
	err = goi.lom.Load(true /*cache it*/, true /*locked*/)
	if err != nil {
		cold = cmn.IsObjNotExist(err)
		if !cold {
			errCode = http.StatusInternalServerError
			return
		}
		cs = fs.Cap()
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
		if !equal {
			cold, goi.verchanged = true, true
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
			cs = fs.Cap()
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
	errCode, err = goi.finalize(cold)
	if err == nil {
		return
	}
	goi.lom.Uncache(true /*delDirty*/)
	if goi.retry {
		goi.retry = false
		if !retried {
			glog.Warningf("GET %s: retrying...", goi.lom)
			retried = true // only once
			goto do
		}
		glog.Warningf("GET %s: failed retrying %v(%d)", goi.lom, err, errCode)
	}
	return
}

// - validate checksums
// - if corrupted and IsAIS, try to recover from redundant replicas or EC slices
// - otherwise, rely on the remote backend for recovery (tradeoff; TODO: make it configurable)
func (goi *getOI) validateRecover() (coldGet bool, code int, err error) {
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
func (goi *getOI) restoreFromAny(skipLomRestore bool) (doubleCheck bool, errCode int, err error) {
	var (
		tsi   *meta.Snode
		smap  = goi.t.owner.smap.get()
		tname = goi.t.String()
	)
	tsi, err = cluster.HrwTargetAll(goi.lom.Uname(), &smap.Smap) // including targets in maintenance
	if err != nil {
		return
	}
	if !skipLomRestore {
		// when resilvering:
		// (whether or not resilvering is active depends on the context: mountpath events vs GET)
		var (
			resMarked = xreg.GetResilverMarked()
			running   = resMarked.Xact != nil
			gfnActive = goi.t.res.IsActive(3 /*interval-of-inactivity multiplier*/)
		)
		if resMarked.Interrupted || running || gfnActive {
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
		gfnNode   *meta.Snode
		marked    = xreg.GetRebMarked()
		running   = marked.Xact != nil
		gfnActive = reb.IsGFN() // GFN(global rebalance)
		ecEnabled = goi.lom.Bprops().EC.Enabled
		// TODO: when not enough EC targets to restore a sliced object,
		// we might still be able to restore from the object's full replica
		enoughECRestoreTargets = goi.lom.Bprops().EC.RequiredRestoreTargets() <= smap.CountActiveTs()
	)
	if running {
		doubleCheck = true
	}
	if running && tsi.ID() != goi.t.SID() {
		if goi.t.HeadObjT2T(goi.lom, tsi) {
			gfnNode = tsi
			goto gfn
		}
	}
	if running || !enoughECRestoreTargets ||
		((marked.Interrupted || marked.Restarted || gfnActive) && !ecEnabled) {
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
		err = cos.NewErrNotFound("%s: %s", goi.t.si, goi.lom.Cname())
	}
	errCode = http.StatusNotFound
	return
}

func (goi *getOI) getFromNeighbor(lom *cluster.LOM, tsi *meta.Snode) bool {
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

	resp, err := goi.t.client.data.Do(req) //nolint:bodyclose // closed by `poi.putObject`
	cmn.FreeHra(reqArgs)
	if err != nil {
		glog.Errorf("%s: gfn failure, %s %q, err: %v", goi.t, tsi, lom, err)
		return false
	}

	cksumToUse := lom.ObjAttrs().FromHeader(resp.Header)
	workFQN := fs.CSM.Gen(lom, fs.WorkfileType, fs.WorkfileRemote)
	poi := allocPOI()
	{
		poi.t = goi.t
		poi.lom = lom
		poi.r = resp.Body
		poi.owt = cmn.OwtMigrate
		poi.workFQN = workFQN
		poi.atime = lom.ObjAttrs().Atime
		poi.cksumToUse = cksumToUse
	}
	errCode, erp := poi.putObject()
	freePOI(poi)
	if erp == nil {
		return true
	}
	glog.Errorf("%s: gfn-GET failed to PUT locally: %v(%d)", goi.t, erp, errCode)
	return false
}

func (goi *getOI) finalize(coldGet bool) (errCode int, err error) {
	var (
		lmfh *os.File
		hrng *htrange
		fqn  = goi.lom.FQN
	)
	if !coldGet && !goi.isGFN {
		fqn = goi.lom.LBGet() // best-effort GET load balancing (see also mirror.findLeastUtilized())
	}
	lmfh, err = os.Open(fqn)
	if err != nil {
		if os.IsNotExist(err) {
			errCode = http.StatusNotFound
			goi.retry = true // (!lom.IsAIS() || lom.ECEnabled() || GFN...)
		} else {
			goi.t.fsErr(err, fqn)
			errCode = http.StatusInternalServerError
			err = cmn.NewErrFailedTo(goi.t, "goi-finalize", goi.lom, err, errCode)
		}
		return
	}

	hdr := goi.w.Header()
	if goi.ranges.Range != "" {
		rsize := goi.lom.SizeBytes()
		if goi.ranges.Size > 0 {
			rsize = goi.ranges.Size
		}
		if hrng, errCode, err = goi.parseRange(hdr, rsize); err != nil {
			goto ret
		}
		if goi.archive.filename != "" {
			err = cmn.NewErrUnsupp("range-read archived file", goi.archive.filename)
			errCode = http.StatusRequestedRangeNotSatisfiable
			goto ret
		}
	}
	errCode, err = goi.fini(fqn, lmfh, hdr, hrng, coldGet)
ret:
	cos.Close(lmfh)
	return
}

// in particular, setup reader and writer and set headers
func (goi *getOI) fini(fqn string, lmfh *os.File, hdr http.Header, hrng *htrange, coldGet bool) (errCode int, err error) {
	var (
		slab   *memsys.Slab
		buf    []byte
		size   int64
		reader io.Reader = lmfh
	)

	cmn.ToHeader(goi.lom.ObjAttrs(), hdr) // (defaults)

	switch {
	case goi.archive.filename != "": // archive
		var mime string
		mime, err = archive.MimeFile(lmfh, goi.t.smm, goi.archive.mime, goi.lom.ObjName)
		if err != nil {
			return
		}
		var csl cos.ReadCloseSizer
		csl, err = archive.GetReader(lmfh, goi.lom.Cname(), goi.archive.filename, mime, goi.lom.SizeBytes())
		if err != nil {
			if cos.IsErrNotFound(err) {
				errCode = http.StatusNotFound
			} else {
				err = cmn.NewErrFailedTo(goi.t, "extract "+goi.archive.filename+" from", goi.lom, err)
			}
			return
		}
		defer func() {
			csl.Close()
		}()
		reader, size = csl, csl.Size()
		hdr.Del(apc.HdrObjCksumVal)
		hdr.Del(apc.HdrObjCksumType)
		hdr.Set(apc.HdrArchmime, mime)
		hdr.Set(apc.HdrArchpath, goi.archive.filename)
		hdr.Set(cos.HdrContentLength, strconv.FormatInt(size, 10))

		buf, slab = goi.t.gmm.AllocSize(size)
	case hrng != nil: // range
		cksumConf := goi.lom.CksumConf()
		cksumRange := cksumConf.Type != cos.ChecksumNone && cksumConf.EnableReadRange
		size = hrng.Length
		buf, slab = goi.t.gmm.AllocSize(hrng.Length)
		reader = io.NewSectionReader(lmfh, hrng.Start, hrng.Length)
		if cksumRange {
			sgl := slab.MMSA().NewSGL(hrng.Length, slab.Size())
			defer func() {
				sgl.Free()
			}()
			var cksum *cos.CksumHash
			if _, cksum, err = cos.CopyAndChecksum(sgl, reader, buf, cksumConf.Type); err != nil {
				slab.Free(buf)
				return
			}
			hdr.Set(apc.HdrObjCksumVal, cksum.Value())
			hdr.Set(apc.HdrObjCksumType, cksumConf.Type)
			reader = sgl
		}
		hdr.Set(cos.HdrContentLength, strconv.FormatInt(size, 10))
	default:
		size = goi.lom.SizeBytes()
		buf, slab = goi.t.gmm.AllocSize(size)
	}

	err = goi.transmit(reader, buf, fqn, coldGet)
	slab.Free(buf)
	return
}

func (goi *getOI) transmit(r io.Reader, buf []byte, fqn string, coldGet bool) error {
	// NOTE: hide `ReadFrom` of the `http.ResponseWriter`
	// (in re: sendfile; see also cos.WriterOnly comment)
	w := cos.WriterOnly{Writer: io.Writer(goi.w)}
	written, err := io.CopyBuffer(w, r, buf)
	if err != nil {
		if !cos.IsRetriableConnErr(err) {
			goi.t.fsErr(err, fqn)
		}
		glog.Error(cmn.NewErrFailedTo(goi.t, "GET", fqn, err))
		// at this point, error is already written into the response -
		// return special code to indicate just that
		return errSendingResp
	}
	// GFN: atime must be already set
	if !coldGet && !goi.isGFN {
		if err := goi.lom.Load(false /*cache it*/, true /*locked*/); err != nil {
			glog.Errorf("%s: GET post-transmission failure: %v", goi.t, err)
			return errSendingResp
		}
		goi.lom.SetAtimeUnix(goi.atime)
		goi.lom.Recache() // GFN and cold GETs have already done this
	}
	// Update objects which were sent during GFN. Thanks to this we will not
	// have to resend them in rebalance. In case of a race between rebalance
	// and GFN, the former wins and it will result in double send.
	if goi.isGFN {
		goi.t.reb.FilterAdd([]byte(goi.lom.Uname()))
	}
	// stats
	goi.t.statsT.AddMany(
		cos.NamedVal64{Name: stats.GetThroughput, Value: written},
		cos.NamedVal64{Name: stats.GetLatency, Value: mono.SinceNano(goi.latency)},
		cos.NamedVal64{Name: stats.GetCount, Value: 1},
	)
	if goi.verchanged {
		goi.t.statsT.AddMany(
			cos.NamedVal64{Name: stats.VerChangeCount, Value: 1},
			cos.NamedVal64{Name: stats.VerChangeSize, Value: goi.lom.SizeBytes()},
		)
	}
	return nil
}

// parse & validate user-spec-ed goi.ranges, and set response header
func (goi *getOI) parseRange(resphdr http.Header, size int64) (hrng *htrange, errCode int, err error) {
	var ranges []htrange
	ranges, err = parseMultiRange(goi.ranges.Range, size)
	if err != nil {
		if _, ok := err.(*errRangeNoOverlap); ok {
			// https://datatracker.ietf.org/doc/html/rfc7233#section-4.2
			resphdr.Set(cos.HdrContentRange, fmt.Sprintf("%s*/%d", cos.HdrContentRangeValPrefix, size))
		}
		errCode = http.StatusRequestedRangeNotSatisfiable
		return
	}
	if len(ranges) == 0 {
		return
	}
	if len(ranges) > 1 {
		err = cmn.NewErrUnsupp("multi-range read", goi.lom.Cname())
		errCode = http.StatusRequestedRangeNotSatisfiable
		return
	}
	if goi.archive.filename != "" {
		err = cmn.NewErrUnsupp("range-read archived file", goi.archive.filename)
		errCode = http.StatusRequestedRangeNotSatisfiable
		return
	}

	// set response header
	hrng = &ranges[0]
	resphdr.Set(cos.HdrAcceptRanges, "bytes")
	resphdr.Set(cos.HdrContentRange, hrng.contentRange(size))
	return
}

//
// APPEND to existing object (as file)
//

func (a *apndOI) do() (newHandle string, errCode int, err error) {
	filePath := a.hdl.filePath
	switch a.op {
	case apc.AppendOp:
		var f *os.File
		if filePath == "" {
			filePath = fs.CSM.Gen(a.lom, fs.WorkfileType, fs.WorkfileAppend)
			f, err = a.lom.CreateFile(filePath)
			if err != nil {
				errCode = http.StatusInternalServerError
				return
			}
			a.hdl.partialCksum = cos.NewCksumHash(a.lom.CksumType())
		} else {
			f, err = os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, cos.PermRWR)
			if err != nil {
				errCode = http.StatusInternalServerError
				return
			}
			debug.Assert(a.hdl.partialCksum != nil)
		}

		var (
			buf  []byte
			slab *memsys.Slab
		)
		if a.size == 0 {
			buf, slab = a.t.gmm.Alloc()
		} else {
			buf, slab = a.t.gmm.AllocSize(a.size)
		}

		w := cos.NewWriterMulti(f, a.hdl.partialCksum.H)
		_, err = io.CopyBuffer(w, a.r, buf)

		slab.Free(buf)
		cos.Close(f)
		if err != nil {
			errCode = http.StatusInternalServerError
			return
		}

		newHandle = combineAppendHandle(a.t.SID(), filePath, a.hdl.partialCksum)
	case apc.FlushOp:
		if filePath == "" {
			err = fmt.Errorf("failed to finalize append-file operation: empty source in the %+v handle", a.hdl)
			errCode = http.StatusBadRequest
			return
		}
		debug.Assert(a.hdl.partialCksum != nil)
		a.hdl.partialCksum.Finalize()
		partialCksum := a.hdl.partialCksum.Clone()
		if !a.cksum.IsEmpty() && !partialCksum.Equal(a.cksum) {
			err = cos.NewBadDataCksumError(partialCksum, a.cksum)
			errCode = http.StatusInternalServerError
			return
		}
		params := cluster.PromoteParams{
			Bck:   a.lom.Bck(),
			Cksum: partialCksum,
			PromoteArgs: cluster.PromoteArgs{
				SrcFQN:       filePath,
				ObjName:      a.lom.ObjName,
				OverwriteDst: true,
				DeleteSrc:    true, // NOTE: always overwrite and remove
			},
		}
		if errCode, err = a.t.Promote(params); err != nil {
			return
		}
	default:
		debug.Assert(false, a.op)
	}

	delta := time.Since(a.started)
	a.t.statsT.AddMany(
		cos.NamedVal64{Name: stats.AppendCount, Value: 1},
		cos.NamedVal64{Name: stats.AppendLatency, Value: int64(delta)},
	)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("PUT %s: %s", a.lom, delta)
	}
	return
}

func parseAppendHandle(handle string) (hdl aoHdl, err error) {
	if handle == "" {
		return
	}
	p := strings.SplitN(handle, "|", 4)
	if len(p) != 4 {
		return hdl, fmt.Errorf("invalid APPEND handle: %q", handle)
	}
	hdl.partialCksum = cos.NewCksumHash(p[2])
	buf, err := base64.StdEncoding.DecodeString(p[3])
	if err != nil {
		return hdl, err
	}
	err = hdl.partialCksum.H.(encoding.BinaryUnmarshaler).UnmarshalBinary(buf)
	if err != nil {
		return hdl, err
	}
	hdl.nodeID = p[0]
	hdl.filePath = p[1]
	return
}

func combineAppendHandle(nodeID, filePath string, partialCksum *cos.CksumHash) string {
	buf, err := partialCksum.H.(encoding.BinaryMarshaler).MarshalBinary()
	debug.AssertNoErr(err)
	cksumTy := partialCksum.Type()
	cksumBinary := base64.StdEncoding.EncodeToString(buf)
	return nodeID + "|" + filePath + "|" + cksumTy + "|" + cksumBinary
}

//
// COPY(object)
//

func (coi *copyOI) objsAdd(size int64, err error) {
	if err == nil && coi.Xact != nil {
		coi.Xact.ObjsAdd(1, size)
	}
}

func (coi *copyOI) copyObject(lom *cluster.LOM, objNameTo string) (size int64, err error) {
	debug.Assert(coi.DP == nil)

	if lom.Bck().IsRemote() || coi.BckTo.IsRemote() {
		// when either one or both buckets are remote
		coi.DP = &cluster.LDP{}
		return coi.copyReader(lom, objNameTo)
	}

	smap := coi.t.owner.smap.Get()
	tsi, err := cluster.HrwTarget(coi.BckTo.MakeUname(objNameTo), smap)
	if err != nil {
		return 0, err
	}
	if tsi.ID() != coi.t.SID() {
		// dst location is tsi
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

	// copy here
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
	return
}

/////////////////
// COPY READER //
/////////////////

// copyReader writes a new object that it reads using a special reader returned by
// coi.DP.Reader(lom).
//
// The reader is responsible for any read-locking of the source LOM, if necessary.
// (If the reader doesn't rlock the object's content may be subject to changing in the middle
// of copying/transforming, etc.)
//
// LOM can be a "pure" metadata of a (non-existing) Cloud object. Accordingly, DP's reader must
// be able to hande cold get, warm get, etc.
//
// If destination bucket is remote, copyReader will:
// - create a local replica of the object on one of the targets, and
// - PUT to the relevant backend
// An option for _not_ storing the object _in_ the cluster would be a _feature_ that can be
// further debated.
func (coi *copyOI) copyReader(lom *cluster.LOM, objNameTo string) (size int64, err error) {
	var tsi *meta.Snode
	if tsi, err = cluster.HrwTarget(coi.BckTo.MakeUname(objNameTo), coi.t.owner.smap.Get()); err != nil {
		return
	}
	if tsi.ID() != coi.t.SID() {
		// remote dst
		return coi.sendRemote(lom, objNameTo, tsi)
	}

	if coi.dryRun {
		// discard the reader and be done
		return coi.dryRunCopyReader(lom)
	}

	// local dst (NOTE: no assumpions on whether the (src) lom is present)
	dst := cluster.AllocLOM(objNameTo)
	size, err = coi.putReader(lom, dst)
	cluster.FreeLOM(dst)

	return
}

func (coi *copyOI) putReader(lom, dst *cluster.LOM) (size int64, err error) {
	var (
		reader cos.ReadOpenCloser
		oah    cos.OAH
	)
	if err = dst.InitBck(coi.BckTo.Bucket()); err != nil {
		return
	}
	if reader, oah, err = coi.DP.Reader(lom); err != nil {
		return
	}
	if lom.Bck().Equal(coi.BckTo, true, true) {
		dst.SetVersion(oah.Version())
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
	size = oah.SizeBytes()
	return
}

func (coi *copyOI) dryRunCopyReader(lom *cluster.LOM) (size int64, err error) {
	var reader io.ReadCloser
	if reader, _, err = coi.DP.Reader(lom); err != nil {
		return 0, err
	}
	defer reader.Close()
	return io.Copy(io.Discard, reader)
}

func (coi *copyOI) sendRemote(lom *cluster.LOM, objNameTo string, tsi *meta.Snode) (size int64, err error) {
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
func (coi *copyOI) doSend(lom *cluster.LOM, sargs *sendArgs) (size int64, err error) {
	if coi.DM != nil {
		// clone the `lom` to use it in the async operation (free it via `_sendObjDM` callback)
		lom = lom.CloneMD(lom.FQN)
	}
	if coi.DP == nil {
		var reader cos.ReadOpenCloser
		if coi.owt != cmn.OwtPromote {
			// 1. migrate/replicate lom
			lom.Lock(false)
			err := lom.Load(false /*cache it*/, true /*locked*/)
			if err != nil {
				lom.Unlock(false)
				return 0, nil
			}
			if coi.dryRun {
				lom.Unlock(false)
				return lom.SizeBytes(), nil
			}
			if reader, err = lom.NewDeferROC(); err != nil {
				return 0, err
			}
			size = lom.SizeBytes()
		} else {
			// 2. promote local file
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
		// 3. get a reader for this object - utilize backend's `GetObjReader`
		// unless the object is present
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

// use data mover to transmit objects to other targets
// (compare with coi.put())
func (coi *copyOI) dm(lom *cluster.LOM, sargs *sendArgs) error {
	debug.Assert(sargs.dm.OWT() == sargs.owt)
	debug.Assert(sargs.dm.GetXact() == coi.Xact || sargs.dm.GetXact().ID() == coi.Xact.ID())
	o := transport.AllocSend()
	hdr, oa := &o.Hdr, sargs.objAttrs
	{
		hdr.Bck.Copy(sargs.bckTo.Bucket())
		hdr.ObjName = sargs.objNameTo
		hdr.ObjAttrs.CopyFrom(oa)
	}
	o.Callback = func(_ transport.ObjHdr, _ io.ReadCloser, _ any, _ error) {
		cluster.FreeLOM(lom)
	}
	return sargs.dm.Send(o, sargs.reader, sargs.tsi)
}

// PUT(lom) => destination target (compare with coi.dm())
// always closes params.Reader, either explicitly or via Do()
func (coi *copyOI) put(sargs *sendArgs) error {
	var (
		hdr   = make(http.Header, 8)
		query = sargs.bckTo.AddToQuery(nil)
	)
	cmn.ToHeader(sargs.objAttrs, hdr)
	hdr.Set(apc.HdrT2TPutterID, coi.t.SID())
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

//
// APPEND to archive aka shard (read/write/list via cmn/archive pkg)
//

func (a *apndArchI) do() (int, error) {
	if a.filename == "" {
		return 0, errors.New("archive path is not defined")
	}
	// standard library does not support appending to tgz and zip;
	// for TAR there is an optimizing workaround not requiring full copy
	if a.mime == archive.ExtTar && !a.put {
		var (
			err     error
			fh      *os.File
			size    int64
			workFQN = fs.CSM.Gen(a.lom, fs.WorkfileType, fs.WorkfileAppendToArch)
		)
		if err = os.Rename(a.lom.FQN, workFQN); err != nil {
			return http.StatusInternalServerError, err
		}
		fh, err = archive.OpenTarSeekEnd(a.lom.Cname(), workFQN)
		if err != nil {
			if errV := a.lom.RenameFrom(workFQN); errV != nil {
				return http.StatusInternalServerError, errV
			}
			if err == archive.ErrTarIsEmpty {
				a.put = true
				goto cpap
			}
			return http.StatusInternalServerError, err
		}
		// do - fast
		if size, err = a.fast(fh); err == nil {
			// NOTE: checksum traded off
			if err = a.finalize(size, cos.NoneCksum, workFQN); err == nil {
				return http.StatusInternalServerError, nil // ok
			}
		}
		if errV := a.lom.RenameFrom(workFQN); errV != nil {
			glog.Errorf(fmtNested, a.t, err, "append and rename back", workFQN, errV)
		}
		return http.StatusInternalServerError, err
	}

cpap: // copy + append
	var (
		err       error
		lmfh, wfh *os.File
		workFQN   string
		cksum     cos.CksumHashSize
		writer    archive.Writer
	)
	workFQN = fs.CSM.Gen(a.lom, fs.WorkfileType, fs.WorkfileAppendToArch)
	wfh, err = os.OpenFile(workFQN, os.O_CREATE|os.O_WRONLY, cos.PermRWR)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	// currently, arch writers only use size and time but it may change
	oah := cos.SimpleOAH{Size: a.size, Atime: a.started.UnixNano()}
	if a.put {
		// when append becomes PUT (TODO: checksum type)
		cksum.Init(cos.ChecksumXXHash)
		writer = archive.NewWriter(a.mime, wfh, &cksum, nil /*opts*/)
		err = writer.Write(a.filename, oah, a.r)
		writer.Fini()
	} else {
		// copy + append
		lmfh, err = os.Open(a.lom.FQN)
		if err != nil {
			cos.Close(wfh)
			return http.StatusNotFound, err
		}
		cksum.Init(a.lom.CksumType())
		writer = archive.NewWriter(a.mime, wfh, &cksum, nil)
		err = writer.Copy(lmfh, a.lom.SizeBytes())
		if err == nil {
			err = writer.Write(a.filename, oah, a.r)
		}
		writer.Fini()
		cos.Close(lmfh)
	}

	// finalize
	cos.Close(wfh)
	if err == nil {
		cksum.Finalize()
		err = a.finalize(cksum.Size, cksum.Clone(), workFQN)
	} else {
		cos.RemoveFile(workFQN)
	}
	return a.reterr(err)
}

// TAR only - fast & direct
func (a *apndArchI) fast(rwfh *os.File) (size int64, err error) {
	var (
		buf, slab = a.t.gmm.AllocSize(a.size)
		tw        = tar.NewWriter(rwfh)
		hdr       = tar.Header{
			Typeflag: tar.TypeReg,
			Name:     a.filename,
			Size:     a.size,
			ModTime:  a.started,
			Mode:     int64(cos.PermRWRR),
		}
	)
	tw.WriteHeader(&hdr)
	_, err = io.CopyBuffer(tw, a.r, buf) // append
	cos.Close(tw)
	if err == nil {
		size, err = rwfh.Seek(0, io.SeekCurrent)
	}
	slab.Free(buf)
	cos.Close(rwfh)
	return
}

func (*apndArchI) reterr(err error) (int, error) {
	errCode := http.StatusInternalServerError
	if cmn.IsErrCapacityExceeded(err) {
		errCode = http.StatusInsufficientStorage
	}
	return errCode, err
}

func (a *apndArchI) finalize(size int64, cksum *cos.Cksum, fqn string) error {
	debug.Func(func() {
		finfo, err := os.Stat(fqn)
		debug.AssertNoErr(err)
		debug.Assertf(finfo.Size() == size, "%d != %d", finfo.Size(), size)
	})
	// done
	if err := a.lom.RenameFrom(fqn); err != nil {
		return err
	}
	a.lom.SetSize(size)
	a.lom.SetCksum(cksum)
	a.lom.SetAtimeUnix(a.started.UnixNano())
	if err := a.lom.Persist(); err != nil {
		return err
	}
	if a.lom.Bprops().EC.Enabled {
		if err := ec.ECM.EncodeObject(a.lom); err != nil && err != ec.ErrorECDisabled {
			return err
		}
	}
	a.t.putMirror(a.lom)
	return nil
}

//
// mem pools
//

var (
	goiPool, poiPool, coiPool, sndPool sync.Pool

	goi0 getOI
	poi0 putOI
	coi0 copyOI
	snd0 sendArgs
)

func allocGOI() (a *getOI) {
	if v := goiPool.Get(); v != nil {
		a = v.(*getOI)
		return
	}
	return &getOI{}
}

func freeGOI(a *getOI) {
	*a = goi0
	goiPool.Put(a)
}

func allocPOI() (a *putOI) {
	if v := poiPool.Get(); v != nil {
		a = v.(*putOI)
		return
	}
	return &putOI{}
}

func freePOI(a *putOI) {
	*a = poi0
	poiPool.Put(a)
}

func allocCOI() (a *copyOI) {
	if v := coiPool.Get(); v != nil {
		a = v.(*copyOI)
		return
	}
	return &copyOI{}
}

func freeCOI(a *copyOI) {
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
