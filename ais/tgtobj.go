// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xact/xs"
)

//
// PUT, GET, APPEND (to file | to archive), and COPY object
//

type (
	putOI struct {
		oreq       *http.Request
		r          io.ReadCloser // content reader
		xctn       core.Xact     // xaction that puts
		t          *target       // this
		lom        *core.LOM     // obj
		cksumToUse *cos.Cksum    // if available (not `none`), can be validated and will be stored
		config     *cmn.Config   // (during this request)
		resphdr    http.Header   // as implied
		workFQN    string        // temp fqn to be renamed
		atime      int64         // access time.Now()
		ltime      int64         // mono.NanoTime, to measure latency
		rltime     int64         // mono.NanoTime, to measure remote bucket latency
		size       int64         // aka Content-Length
		owt        cmn.OWT       // object write transaction enum { OwtPut, ..., OwtGet* }
		restful    bool          // being invoked via RESTful API
		t2t        bool          // by another target
		skipEC     bool          // do not erasure-encode when finalizing
		skipVC     bool          // skip loading existing Version and skip comparing Checksums (skip VC)
		coldGET    bool          // (one implication: proceed to write)
		remoteErr  bool          // to exclude `putRemote` errors when counting soft IO errors
	}

	getOI struct {
		req        *http.Request
		w          http.ResponseWriter
		ctx        context.Context // context used when getting object from remote backend (access creds)
		t          *target         // this
		lom        *core.LOM       // obj
		dpq        *dpq
		ranges     byteRanges // range read (see https://www.rfc-editor.org/rfc/rfc7233#section-2.1)
		atime      int64      // access time.Now()
		ltime      int64      // mono.NanoTime, to measure latency
		rstarttime int64      // mono.NanoTime, mark start of remote GET to measure latency
		rltime     int64      // mono.NanoTime, to measure remote bucket latency
		chunked    bool       // chunked transfer (en)coding: https://tools.ietf.org/html/rfc7230#page-36
		unlocked   bool       // internal
		verchanged bool       // version changed
		retry      bool       // once
		cold       bool       // true if executed backend.Get
		latestVer  bool       // QparamLatestVer || 'versioning.*_warm_get'
		isIOErr    bool       // to count GET error as a "IO error"; see `Trunner._softErrs()`
	}
	_uplock struct {
		config  *cmn.Config
		timeout time.Duration
		elapsed time.Duration
		sleep   time.Duration
	}

	// textbook append: (packed) handle and control structure (see also `putA2I` arch below)
	aoHdl struct {
		partialCksum *cos.CksumHash
		nodeID       string
		workFQN      string
	}
	apndOI struct {
		r       io.ReadCloser // content reader
		t       *target       // this
		config  *cmn.Config   // (during this request)
		lom     *core.LOM     // append to or _as_
		cksum   *cos.Cksum    // checksum expected once Flush-ed
		hdl     aoHdl         // (packed)
		op      string        // enum {apc.AppendOp, apc.FlushOp}
		started int64         // start time (nanoseconds)
		size    int64         // Content-Length
	}

	coi xs.CoiParams

	sendArgs struct {
		reader    cos.ReadOpenCloser
		dm        *bundle.DataMover
		objAttrs  cos.OAH
		tsi       *meta.Snode
		bckTo     *meta.Bck
		objNameTo string
		owt       cmn.OWT
	}

	// put/append-to arch
	putA2I struct {
		r        io.ReadCloser // read bytes to append
		t        *target       // this
		lom      *core.LOM     // resulting shard
		filename string        // fqn inside
		mime     string        // format
		started  int64         // time of receiving
		size     int64         // aka Content-Length
		put      bool          // overwrite
	}
)

//
// PUT(object)
//

// poi.restful entry point
func (poi *putOI) do(resphdr http.Header, r *http.Request, dpq *dpq) (int, error) {
	{
		poi.oreq = r
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
			nlog.Errorln(err)
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

func (poi *putOI) putObject() (ecode int, err error) {
	if lom := poi.lom; lom.IsFntl() {
		// fixup fntl
		var (
			short = lom.ShortenFntl()
			saved = lom.PushFntl(short)
		)
		lom.SetCustomKey(cmn.OrigFntl, saved[0])
		poi.workFQN = fs.CSM.Gen(lom, fs.WorkfileType, "fntl-0x24")
	}

	poi.ltime = mono.NanoTime()

	// if checksums match PUT is a no-op
	if !poi.skipVC && !poi.coldGET {
		if poi.lom.EqCksum(poi.cksumToUse) {
			if cmn.Rom.FastV(4, cos.SmoduleAIS) {
				nlog.Infoln(poi.lom.String(), "has identical", poi.cksumToUse.String(), "- PUT is a no-op")
			}
			cos.DrainReader(poi.r)
			return 0, nil
		}
	}

	buf, slab, lmfh, erw := poi.write()
	poi._cleanup(buf, slab, lmfh, erw)
	if erw != nil {
		err, ecode = erw, http.StatusInternalServerError
		goto rerr
	}

	if ecode, err = poi.finalize(); err != nil {
		goto rerr
	}

	// NOTE stats: counting xactions and user PUTs; not counting (cold-GET -> PUT)
	if poi.xctn != nil {
		poi.stats()
		if poi.owt == cmn.OwtPromote {
			poi.xctn.InObjsAdd(1, poi.lom.Lsize())
		}
	} else if !poi.t2t && poi.owt == cmn.OwtPut && poi.restful {
		// user PUT
		debug.Assert(cos.IsValidAtime(poi.atime), poi.atime)
		poi.stats()
		// response header
		if poi.resphdr != nil {
			cmn.ToHeader(poi.lom.ObjAttrs(), poi.resphdr, 0 /*skip setting content-length*/)
		}
	}

	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		nlog.Infoln(poi.loghdr())
	}
	return 0, nil
rerr:
	if poi.owt == cmn.OwtPut && poi.restful && !poi.t2t {
		vlabs := poi._vlabs()
		if err != cmn.ErrSkip && !poi.remoteErr && err != io.ErrUnexpectedEOF &&
			!cos.IsRetriableConnErr(err) && !cos.IsErrMv(err) {
			poi.t.statsT.AddWith(
				cos.NamedVal64{Name: stats.ErrPutCount, Value: 1, VarLabs: vlabs},
				cos.NamedVal64{Name: stats.IOErrPutCount, Value: 1, VarLabs: vlabs},
			)
			if cmn.Rom.FastV(4, cos.SmoduleAIS) {
				nlog.Warningln("io-error [", err, "]", poi.loghdr())
			}
		} else {
			poi.t.statsT.AddWith(
				cos.NamedVal64{Name: stats.ErrPutCount, Value: 1, VarLabs: vlabs},
			)
		}
	}
	return ecode, err
}

func (poi *putOI) _vlabs() map[string]string {
	vlabs := map[string]string{stats.VarlabBucket: poi.lom.Bck().Cname(""), stats.VarlabXactKind: "", stats.VarlabXactID: ""}
	if poi.xctn != nil {
		vlabs[stats.VarlabXactKind] = poi.xctn.Kind()
		vlabs[stats.VarlabXactID] = poi.xctn.ID()
	}
	return vlabs
}

func (poi *putOI) stats() {
	var (
		bck   = poi.lom.Bck()
		size  = poi.lom.Lsize()
		delta = mono.SinceNano(poi.ltime)
		vlabs = poi._vlabs()
	)
	poi.t.statsT.AddWith(
		cos.NamedVal64{Name: stats.PutCount, Value: 1, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.PutSize, Value: size, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.PutThroughput, Value: size, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.PutLatency, Value: delta, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.PutLatencyTotal, Value: delta, VarLabs: vlabs},
	)
	if poi.rltime > 0 {
		debug.Assert(bck.IsRemote())
		backend := poi.t.Backend(bck)
		poi.t.statsT.AddWith(
			cos.NamedVal64{Name: backend.MetricName(stats.PutCount), Value: 1, VarLabs: vlabs},
			cos.NamedVal64{Name: backend.MetricName(stats.PutLatencyTotal), Value: poi.rltime, VarLabs: vlabs},
			cos.NamedVal64{Name: backend.MetricName(stats.PutE2ELatencyTotal), Value: delta, VarLabs: vlabs},
			cos.NamedVal64{Name: backend.MetricName(stats.PutSize), Value: size, VarLabs: vlabs},
		)
	}
}

// verbose only
func (poi *putOI) loghdr() string {
	var (
		sb strings.Builder
		l  = 128
	)
	sb.Grow(l)
	sb.WriteString(poi.owt.String())
	sb.WriteString(", ")
	sb.WriteString(poi.lom.Cname())
	if poi.xctn != nil {
		sb.WriteString(", ")
		sb.WriteString(poi.xctn.String())
	}
	if poi.skipVC {
		sb.WriteString(", skip-vc")
	}
	if poi.coldGET {
		sb.WriteString(", cold-get")
	}
	if poi.t2t {
		sb.WriteString(", t2t")
	}
	return sb.String()
}

func (poi *putOI) finalize() (ecode int, err error) {
	if ecode, err = poi.fini(); err != nil {
		if err1 := cos.Stat(poi.workFQN); err1 == nil || !os.IsNotExist(err1) {
			// cleanup: rm work-fqn
			if err1 == nil {
				err1 = err
			}
			if err2 := cos.RemoveFile(poi.workFQN); err2 != nil && !os.IsNotExist(err2) {
				nlog.Errorf(fmtNested, poi.t, err1, "remove", poi.workFQN, err2)
			}
		}
		poi.lom.Uncache()
		if ecode != http.StatusInsufficientStorage && cmn.IsErrCapExceeded(err) {
			ecode = http.StatusInsufficientStorage
		}
		return ecode, err
	}
	if !poi.skipEC {
		if ecErr := ec.ECM.EncodeObject(poi.lom, nil); ecErr != nil && ecErr != ec.ErrorECDisabled {
			err = ecErr
			if ecode != http.StatusInsufficientStorage && cmn.IsErrCapExceeded(err) {
				ecode = http.StatusInsufficientStorage
			}
			return ecode, err
		}
	}
	poi.t.putMirror(poi.lom)
	return 0, nil
}

// poi.workFQN => LOM
func (poi *putOI) fini() (ecode int, err error) {
	var (
		lom = poi.lom
		bck = lom.Bck()
	)
	// put remote
	if bck.IsRemote() && poi.owt < cmn.OwtRebalance {
		ecode, err = poi.putRemote()
		if err != nil {
			loghdr := poi.loghdr()
			nlog.Errorf("PUT (%s): %v(%d)", loghdr, err, ecode)
			if ecode != http.StatusServiceUnavailable {
				return ecode, err
			}
			// retry 503 only once; e.g. error message:
			// (googleapi: "Error 503: We encountered an internal error. Please try again.")
			time.Sleep(time.Second)
			ecode, err = poi.putRemote()
			if err != nil {
				return ecode, err
			}
			nlog.Infof("PUT (%s): retried OK", loghdr)
		}
	}

	// locking strategies: optimistic and otherwise
	// (see GetCold() implementation and cmn.OWT enum)
	switch poi.owt {
	case cmn.OwtGetTryLock, cmn.OwtGetLock, cmn.OwtGet:
		// do nothing: lom is already wlocked
	case cmn.OwtGetPrefetchLock:
		if !lom.TryLock(true) {
			nlog.Warningln(poi.loghdr(), "is busy")
			return 0, cmn.ErrSkip // e.g. prefetch can skip it and keep on going
		}
		defer lom.Unlock(true)
	default:
		debug.Assert(cos.IsValidAtime(poi.atime), poi.atime) // expecting valid atime
		lom.Lock(true)
		defer lom.Unlock(true)
		lom.SetAtimeUnix(poi.atime)
	}

	// ais versioning
	if bck.IsAIS() && lom.VersionConf().Enabled {
		if poi.owt < cmn.OwtRebalance {
			if poi.skipVC {
				err = lom.IncVersion()
				debug.AssertNoErr(err)
			} else if remSrc, ok := lom.GetCustomKey(cmn.SourceObjMD); !ok || remSrc == "" {
				if err = lom.IncVersion(); err != nil {
					nlog.Errorln(err) // (unlikely)
				}
			}
		}
	}

	// done
	if err = lom.RenameFinalize(poi.workFQN); err != nil {
		return 0, err
	}
	if lom.HasCopies() {
		if errdc := lom.DelAllCopies(); errdc != nil {
			nlog.Errorf("PUT (%s): failed to delete old copies [%v], proceeding anyway...", poi.loghdr(), errdc)
		}
	}
	if lom.AtimeUnix() == 0 { // (is set when migrating within cluster; prefetch special case)
		lom.SetAtimeUnix(poi.atime)
	}
	return 0, lom.PersistMain()
}

// via backend.PutObj()
func (poi *putOI) putRemote() (int, error) {
	var (
		lom       = poi.lom
		startTime = mono.NanoTime()
	)
	lmfh, err := cos.NewFileHandle(poi.workFQN)
	if err != nil {
		return 0, cmn.NewErrFailedTo(poi.t, "open", poi.workFQN, err)
	}
	if poi.owt == cmn.OwtPut && !lom.Bck().IsRemoteAIS() {
		lom.ObjAttrs().DelStdCustom() // backend.PutObj() will set updated values
	}
	var (
		ecode   int
		backend = poi.t.Backend(lom.Bck())
	)
	ecode, err = backend.PutObj(lmfh, lom, poi.oreq)
	if err == nil {
		if !lom.Bck().IsRemoteAIS() {
			lom.SetCustomKey(cmn.SourceObjMD, backend.Provider())
		}
		poi.rltime = mono.SinceNano(startTime)
		return 0, nil
	}
	poi.remoteErr = true
	return ecode, err
}

// LOM is updated at the end of this call with size and checksum.
// `poi.r` (reader) is also closed upon exit.
func (poi *putOI) write() (buf []byte, slab *memsys.Slab, lmfh cos.LomWriter, err error) {
	var (
		written int64
		cksums  = struct {
			store     *cos.CksumHash // store with LOM
			expct     *cos.Cksum     // caller-provided (aka "end-to-end protection")
			compt     *cos.CksumHash // compute to validate `expct` - iff provided
			finalized bool           // to avoid computing the same checksum type twice
		}{}
		ckconf = poi.lom.CksumConf()
	)
	if lmfh, err = poi.lom.CreateWork(poi.workFQN); err != nil {
		return
	}
	if poi.size <= 0 {
		buf, slab = poi.t.gmm.Alloc()
	} else {
		buf, slab = poi.t.gmm.AllocSize(poi.size)
	}

	switch {
	case ckconf.Type == cos.ChecksumNone:
		poi.lom.SetCksum(cos.NoneCksum)
		// not using `ReadFrom` of the `*os.File` -
		// ultimately, https://github.com/golang/go/blob/master/src/internal/poll/copy_file_range_linux.go#L100
		written, err = cos.CopyBuffer(lmfh, poi.r, buf)
	case !poi.cksumToUse.IsEmpty() && !poi.validateCksum(ckconf):
		// if the corresponding validation is not configured/enabled we just go ahead
		// and use the checksum that has arrived with the object
		poi.lom.SetCksum(poi.cksumToUse)
		// (ditto)
		written, err = cos.CopyBuffer(lmfh, poi.r, buf)
	default:
		writers := make([]io.Writer, 0, 3)
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
		writers = append(writers, lmfh)
		written, err = cos.CopyBuffer(cos.NewWriterMulti(writers...), poi.r, buf) // (ditto)
	}
	if err != nil {
		return
	}

	// validate
	if cksums.compt != nil {
		cksums.finalized = cksums.compt == cksums.store
		cksums.compt.Finalize()
		if !cksums.compt.Equal(cksums.expct) {
			err = cos.NewErrDataCksum(cksums.expct, &cksums.compt.Cksum, poi.lom.Cname())
			poi.t.statsT.AddWith(
				cos.NamedVal64{Name: stats.ErrPutCksumCount, Value: 1, VarLabs: poi._vlabs()},
			)
			return
		}
	}

	// ok
	if poi.lom.IsFeatureSet(feat.FsyncPUT) {
		err = lmfh.Sync() // compare w/ cos.FlushClose
		debug.AssertNoErr(err)
	}

	cos.Close(lmfh)
	lmfh = nil

	poi.lom.SetSize(written) // TODO: compare with non-zero lom.Lsize() that may have been set via oa.FromHeader()
	if cksums.store != nil {
		if !cksums.finalized {
			cksums.store.Finalize()
		}
		poi.lom.SetCksum(&cksums.store.Cksum)
	}
	return
}

// post-write close & cleanup
func (poi *putOI) _cleanup(buf []byte, slab *memsys.Slab, lmfh cos.LomWriter, err error) {
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
		nlog.Errorf(fmtNested, poi.t, err, "close", poi.workFQN, nerr)
	}
	if nerr := cos.RemoveFile(poi.workFQN); nerr != nil && !os.IsNotExist(nerr) {
		nlog.Errorf(fmtNested, poi.t, err, "remove", poi.workFQN, nerr)
	}
}

func (poi *putOI) validateCksum(c *cmn.CksumConf) (v bool) {
	switch poi.owt {
	case cmn.OwtRebalance, cmn.OwtCopy, cmn.OwtCopySameBucket:
		v = c.ValidateObjMove
	case cmn.OwtPut:
		v = true
	case cmn.OwtGetTryLock, cmn.OwtGetLock, cmn.OwtGet:
		v = c.ValidateColdGet
	case cmn.OwtGetPrefetchLock:
	default:
		debug.Assert(false, poi.owt)
	}
	return
}

//
// GET(object)
//

func (goi *getOI) getObject() (ecode int, err error) {
	debug.Assert(!goi.unlocked)
	goi.lom.Lock(false)
	ecode, err = goi.get()
	if !goi.unlocked {
		goi.lom.Unlock(false)
	}
	return ecode, err
}

// is under rlock
func (goi *getOI) get() (ecode int, err error) {
	var (
		uplock      *_uplock
		cs          fs.CapStatus
		doubleCheck bool
		retried     bool
		cold        bool
	)
do:
	err = goi.lom.Load(true /*cache it*/, true /*locked*/)
	if err != nil {
		cold = cos.IsNotExist(err, 0)
		if !cold {
			goi.isIOErr = true
			return http.StatusInternalServerError, err
		}
		if goi.lom.IsFeatureSet(feat.DisableColdGET) && goi.lom.Bck().IsRemote() {
			return http.StatusNotFound, fmt.Errorf("%w (cold GET disabled)", err)
		}
		cs = fs.Cap()
		if cs.IsOOS() {
			return http.StatusInsufficientStorage, cs.Err()
		}
	}

	switch {
	case cold && goi.lom.Bck().IsAIS():
		// ais bucket with no backend - try recover
		goi.lom.Unlock(false)
		doubleCheck, ecode, err = goi.restoreFromAny(false /*skipLomRestore*/)
		if doubleCheck && err != nil {
			lom2 := core.AllocLOM(goi.lom.ObjName)
			er2 := lom2.InitBck(goi.lom.Bucket())
			if er2 == nil {
				er2 = lom2.Load(true /*cache it*/, false /*locked*/)
				goi.isIOErr = true
			}
			if er2 == nil {
				core.FreeLOM(goi.lom)
				goi.lom = lom2
				err = nil
			} else {
				core.FreeLOM(lom2)
			}
		}
		if err != nil {
			goi.unlocked = true
			return ecode, err
		}
		goi.lom.Lock(false)
		if err = goi.lom.Load(true /*cache it*/, true /*locked*/); err != nil {
			goi.isIOErr = true
			return 0, err
		}
		goto fin // ok, done
	case cold:
		// have remote backend - use it
	case goi.latestVer:
		// apc.QparamLatestVer or 'versioning.validate_warm_get'
		res := goi.lom.CheckRemoteMD(true /* rlocked */, false /*synchronize*/, goi.req)
		if res.Err != nil {
			return res.ErrCode, res.Err
		}
		if !res.Eq {
			cold, goi.verchanged = true, true
		}
		// TODO: utilize res.ObjAttrs
	}

	// validate checksums and recover (a.k.a. self-heal) if corrupted
	if !cold && goi.lom.CksumConf().ValidateWarmGet {
		cold, ecode, err = goi.validateRecover()
		if err != nil {
			if !cold {
				nlog.Errorln(err)
				return ecode, err
			}
			nlog.Errorln(err, "- proceeding to cold-GET from", goi.lom.Bck().Cname(""))
		}
	}

	// cold-GET: upgrade rlock => wlock and call t.Backend.GetObjReader
	if cold {
		var (
			res     core.GetReaderResult
			ckconf  = goi.lom.CksumConf()
			backend = goi.t.Backend(goi.lom.Bck())
		)
		if cs.IsNil() {
			cs = fs.Cap()
		}
		if cs.IsOOS() {
			return http.StatusInsufficientStorage, cs.Err()
		}

		// try upgrading rlock => wlock; poll for a while
		if !goi.lom.UpgradeLock() {
			if uplock == nil {
				c := cmn.GCO.Get()
				uplock = &_uplock{config: c, sleep: time.Second}
				uplock.timeout = max(c.Timeout.MaxHostBusy.D()-c.Timeout.CplaneOperation.D(), 4*time.Second)

				nlog.Warningln(uplockWarn, goi.lom.String())
			}
			if err := uplock.do(goi.lom); err != nil {
				return http.StatusConflict, err
			}
			goto do
		}

		goi.lom.SetAtimeUnix(goi.atime)
		// zero-out prev. version custom metadata, if any
		goi.lom.SetCustomMD(nil)

		goi.rstarttime = mono.NanoTime()
		// get remote reader (compare w/ t.GetCold)
		res = backend.GetObjReader(goi.ctx, goi.lom, 0, 0)
		if res.Err != nil {
			goi.lom.Unlock(true)
			goi.unlocked = true
			if !cos.IsNotExist(res.Err, res.ErrCode) {
				nlog.Infoln(ftcg, "(read)", goi.lom.Cname(), res.Err, res.ErrCode)
			}
			return res.ErrCode, res.Err
		}
		goi.cold = true

		// 3 alternative ways to perform cold GET
		if goi.dpq.arch.path == "" && goi.dpq.arch.regx == "" &&
			(ckconf.Type == cos.ChecksumNone || (!ckconf.ValidateColdGet && !ckconf.EnableReadRange)) {
			if goi.ranges.Range == "" && goi.lom.IsFeatureSet(feat.StreamingColdGET) {
				err = goi.coldStream(&res)
			} else {
				err = goi.coldReopen(&res)
			}
			goi.unlocked = true // always
			return 0, err
		}
		// otherwise, regular path
		ecode, err = goi._coldPut(&res)
		if err != nil {
			goi.unlocked = true
			return ecode, err
		}
		goi.rltime = mono.SinceNano(goi.rstarttime)
	}

	// read locally and stream back
fin:
	ecode, err = goi.txfini()
	if err == nil {
		return 0, nil
	}
	goi.lom.Uncache()
	if goi.retry {
		goi.retry = false
		if !retried {
			nlog.Warningln("retrying", goi.lom.String(), err)
			retried = true
			goto do
		}
	}
	return ecode, err
}

func (goi *getOI) _coldPut(res *core.GetReaderResult) (int, error) {
	var (
		t, lom = goi.t, goi.lom
		poi    = allocPOI()
	)
	{
		poi.t = t
		poi.lom = lom
		poi.config = cmn.GCO.Get()
		poi.r = res.R
		poi.size = res.Size
		poi.workFQN = fs.CSM.Gen(lom, fs.WorkfileType, fs.WorkfileColdget)
		poi.atime = goi.atime
		poi.owt = cmn.OwtGet
		poi.cksumToUse = res.ExpCksum // expected checksum (to validate if the bucket's `validate_cold_get == true`)
		poi.coldGET = true
	}
	code, err := poi.putObject()
	freePOI(poi)

	if err != nil {
		lom.Unlock(true)
		nlog.Infoln(ftcg, "(put)", lom.Cname(), err)
		return code, err
	}

	// load, downgrade lock, inc stats
	if err = lom.Load(true /*cache it*/, true /*locked*/); err != nil {
		lom.Unlock(true)
		err = fmt.Errorf("unexpected failure to load %s: %w", lom, err) // (unlikely)
		nlog.Errorln(err)
		return http.StatusInternalServerError, err
	}

	lom.DowngradeLock()
	return 0, nil
}

// validateRecover first validates and tries to recover a corrupted object:
//   - validate checksums
//   - if corrupted and IsAIS or coldGET not permitted, try to recover from redundant
//     replicas or EC slices
//   - otherwise, rely on the remote backend for recovery (tradeoff; TODO: make it configurable)
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
	if !lom.Bck().IsAIS() && !goi.lom.IsFeatureSet(feat.DisableColdGET) {
		coldGet = true
		return
	}

	nlog.Warningln(err)
	redundant := lom.HasCopies() || lom.ECEnabled()
	//
	// return err if there's no redundancy OR already recovered once (and failed)
	//
	if retried || !redundant {
		//
		// TODO: mark `deleted` and postpone actual deletion
		//
		if erl := lom.RemoveObj(true /*force through rlock*/); erl != nil {
			nlog.Warningf("%s: failed to remove corrupted %s, err: %v", goi.t, lom, erl)
		}
		return
	}
	//
	// try to recover from BAD CHECKSUM
	//
	lom.RemoveMain()

	if lom.HasCopies() {
		retried = true
		goi.lom.Unlock(false)
		// lookup and restore the object from local replicas
		restored := lom.RestoreToLocation()
		goi.lom.Lock(false)
		if restored {
			nlog.Warningf("%s: recovered corrupted %s from local replica", goi.t, lom)
			code = 0
			goto validate
		}
	}
	if lom.ECEnabled() {
		retried = true
		goi.lom.Unlock(false)
		lom.RemoveMain()
		_, code, err = goi.restoreFromAny(true /*skipLomRestore*/)
		goi.lom.Lock(false)
		if err == nil {
			nlog.Warningf("%s: recovered corrupted %s from EC slices", goi.t, lom)
			code = 0
			goto validate
		}
	}

	// TODO: ditto
	if erl := lom.RemoveObj(true /*force through rlock*/); erl != nil {
		nlog.Warningf("%s: failed to remove corrupted %s, err: %v", goi.t, lom, erl)
	}
	return
}

// attempt to restore an object from any/all of the below:
// 1) local copies (other FSes on this target)
// 2) other targets (when resilvering or rebalancing is running (aka GFN))
// 3) other targets if the bucket erasure coded
// 4) Cloud
func (goi *getOI) restoreFromAny(skipLomRestore bool) (doubleCheck bool, ecode int, err error) {
	var (
		tsi  *meta.Snode
		smap = goi.t.owner.smap.get()
	)
	// NOTE: including targets 'in maintenance mode'
	tsi, err = smap.HrwHash2Tall(goi.lom.Digest())
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
				nlog.Infof("%s restored to location", goi.lom)
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
		ecEnabled = goi.lom.ECEnabled()
		// TODO: when not enough EC targets to restore a sliced object,
		// we might still be able to restore from the object's full replica
		enoughECRestoreTargets = goi.lom.Bprops().EC.RequiredRestoreTargets() <= smap.CountActiveTs()
	)
	if running {
		doubleCheck = true
	}
	if running && tsi.ID() != goi.t.SID() {
		if goi.t.headt2t(goi.lom, tsi, smap) {
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
			return
		}
	}

	// restore from existing EC slices
	ecErr := ec.ECM.Recover(goi.lom)
	if ecErr == nil {
		ecErr = goi.lom.Load(true /*cache it*/, false /*locked*/) // TODO: optimize locking
		if ecErr == nil {
			nlog.Infoln(goi.t.String(), "EC-recovered", goi.lom.Cname())
			return
		}
		err = cmn.NewErrFailedTo(goi.t, "load EC-recovered", goi.lom.Cname(), ecErr)
		nlog.Errorln(ecErr)
	} else if ecErr != ec.ErrorECDisabled {
		err = cmn.NewErrFailedTo(goi.t, "EC-recover", goi.lom.Cname(), ecErr)
		if cmn.IsErrCapExceeded(ecErr) {
			ecode = http.StatusInsufficientStorage
		}
		return
	}

	if err != nil {
		if _, ok := err.(*cmn.ErrFailedTo); !ok {
			err = cmn.NewErrFailedTo(goi.t, "goi-restore-any", goi.lom.Cname(), err)
		}
	} else {
		err = cos.NewErrNotFound(goi.t, goi.lom.Cname())
	}
	ecode = http.StatusNotFound
	return
}

func (goi *getOI) getFromNeighbor(lom *core.LOM, tsi *meta.Snode) bool {
	query := lom.Bck().NewQuery()
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

	resp, err := g.client.data.Do(req) //nolint:bodyclose // closed by `poi.putObject`
	cmn.FreeHra(reqArgs)
	if err != nil {
		nlog.Errorf("%s: gfn failure, %s %q, err: %v", goi.t, tsi, lom, err)
		return false
	}

	cksumToUse := lom.ObjAttrs().FromHeader(resp.Header)
	workFQN := fs.CSM.Gen(lom, fs.WorkfileType, fs.WorkfileRemote)
	poi := allocPOI()
	{
		poi.t = goi.t
		poi.lom = lom
		poi.config = config
		poi.r = resp.Body
		poi.owt = cmn.OwtRebalance
		poi.workFQN = workFQN
		poi.atime = lom.ObjAttrs().Atime
		poi.cksumToUse = cksumToUse
	}
	ecode, erp := poi.putObject()
	freePOI(poi)
	if erp == nil {
		if cmn.Rom.FastV(5, cos.SmoduleAIS) {
			nlog.Infoln(goi.t.String(), "gfn", goi.lom.String(), "<=", tsi.StringEx())
		}
		return true
	}
	nlog.Errorf("%s: gfn-GET failed to PUT locally: %v(%d)", goi.t, erp, ecode)
	return false
}

func (goi *getOI) txfini() (ecode int, err error) {
	var (
		lmfh *os.File
		hrng *htrange
		fqn  = goi.lom.FQN
		dpq  = goi.dpq
	)
	if !goi.cold && !dpq.isGFN && !goi.lom.IsChunked() {
		fqn = goi.lom.LBGet() // best-effort GET load balancing (see also mirror.findLeastUtilized())
	}
	// open
	lmfh, err = goi.lom.OpenFile()
	if err != nil {
		if os.IsNotExist(err) {
			// NOTE: retry only once and only when ec-enabled - see goi.restoreFromAny()
			ecode = http.StatusNotFound
			goi.retry = goi.lom.ECEnabled()
		} else {
			goi.t.FSHC(err, goi.lom.Mountpath(), fqn)
			ecode = http.StatusInternalServerError
			err = cmn.NewErrFailedTo(goi.t, "goi-finalize", goi.lom.Cname(), err, ecode)
		}
		return ecode, err
	}

	whdr := goi.w.Header()

	// transmit (range, arch, regular)
	switch {
	case goi.ranges.Range != "":
		debug.Assert(!dpq.isArch())
		rsize := goi.lom.Lsize()
		if goi.ranges.Size > 0 {
			rsize = goi.ranges.Size
		}
		if hrng, ecode, err = goi.rngToHeader(whdr, rsize); err != nil {
			break
		}
		err = goi._txrng(fqn, lmfh, whdr, hrng)
	case dpq.isArch():
		err = goi._txarch(fqn, lmfh, whdr)
	default:
		err = goi._txreg(fqn, lmfh, whdr)
	}

	cos.Close(lmfh)
	return ecode, err
}

func (goi *getOI) _txrng(fqn string, lmfh *os.File, whdr http.Header, hrng *htrange) (err error) {
	var (
		r     io.Reader
		lom   = goi.lom
		sgl   *memsys.SGL
		cksum = lom.Checksum()
		size  int64
	)
	ckconf := lom.CksumConf()
	cksumRange := ckconf.Type != cos.ChecksumNone && ckconf.EnableReadRange
	size = hrng.Length
	r = io.NewSectionReader(lmfh, hrng.Start, hrng.Length)
	if cksumRange {
		sgl = goi.t.gmm.NewSGL(size)
		_, cksumH, err := cos.CopyAndChecksum(sgl /*as ReaderFrom*/, r, nil, ckconf.Type)
		if err != nil {
			sgl.Free()
			goi.isIOErr = true
			return err
		}
		r = sgl
		if cksumH != nil {
			cksum = &cksumH.Cksum
		}
	}

	// set response header
	whdr.Set(cos.HdrContentType, cos.ContentBinary)
	cmn.ToHeader(lom.ObjAttrs(), whdr, size, cksum)
	if goi.dpq.isS3 {
		s3.SetS3Headers(whdr, lom)
	}

	buf, slab := goi.t.gmm.AllocSize(min(size, memsys.DefaultBuf2Size))
	err = goi.transmit(r, buf, fqn)
	slab.Free(buf)
	if sgl != nil {
		sgl.Free()
	}
	return err
}

// in particular, setup reader and writer and set headers
func (goi *getOI) _txreg(fqn string, lmfh *os.File, whdr http.Header) (err error) {
	var (
		dpq   = goi.dpq
		lom   = goi.lom
		cksum = lom.Checksum()
		size  = lom.Lsize()
	)
	// set response header
	whdr.Set(cos.HdrContentType, cos.ContentBinary)
	cmn.ToHeader(lom.ObjAttrs(), whdr, size, cksum)
	if dpq.isS3 {
		// (expecting user to set bucket checksum = md5)
		s3.SetS3Headers(whdr, lom)
	}

	buf, slab := goi.t.gmm.AllocSize(min(size, memsys.DefaultBuf2Size))
	err = goi.transmit(lmfh, buf, fqn)
	slab.Free(buf)
	return err
}

// TODO: checksum
func (goi *getOI) _txarch(fqn string, lmfh *os.File, whdr http.Header) error {
	var (
		ar  archive.Reader
		dpq = goi.dpq
		lom = goi.lom
	)
	mime, err := archive.MimeFile(lmfh, goi.t.smm, dpq.arch.mime, lom.ObjName)
	if err != nil {
		return err
	}
	ar, err = archive.NewReader(mime, lmfh, lom.Lsize())
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", lom.Cname(), err)
	}

	// single
	if dpq.arch.path != "" {
		debug.Assert(dpq.arch.mmode == "", dpq.arch.mmode)
		var csl cos.ReadCloseSizer
		csl, err = ar.ReadOne(dpq.arch.path)
		if err != nil {
			goi.isIOErr = true
			return cmn.NewErrFailedTo(goi.t, "extract "+dpq._archstr()+" from", lom.Cname(), err)
		}
		if csl == nil {
			return cos.NewErrNotFound(goi.t, dpq._archstr()+" in "+lom.Cname())
		}
		// found
		whdr.Set(cos.HdrContentType, cos.ContentBinary)
		buf, slab := goi.t.gmm.AllocSize(min(csl.Size(), memsys.DefaultBuf2Size))
		err = goi.transmit(csl, buf, fqn)
		slab.Free(buf)
		csl.Close()
		return err
	}

	// multi match; writing & streaming tar =>(directly)=> response writer
	debug.Assert(dpq.arch.mmode != "")
	rcb := _newRcb(goi.w)
	whdr.Set(cos.HdrContentType, cos.ContentTar)
	err = ar.ReadUntil(rcb, dpq.arch.regx, dpq.arch.mmode)
	if err != nil {
		goi.isIOErr = true
		err = cmn.NewErrFailedTo(goi.t, "extract files that match "+dpq._archstr()+" from", lom.Cname(), err)
	}
	if err == nil && rcb.num == 0 {
		// none found
		return cos.NewErrNotFound(goi.t, dpq._archstr()+" in "+lom.Cname())
	}
	rcb.fini()
	return err
}

func (goi *getOI) transmit(r io.Reader, buf []byte, fqn string) error {
	written, err := cos.CopyBuffer(goi.w, r, buf)
	if err != nil {
		if !cos.IsRetriableConnErr(err) || cmn.Rom.FastV(5, cos.SmoduleAIS) {
			nlog.Warningln("failed to GET (Tx)", goi.lom.Cname(), err)
			goi.t.FSHC(err, goi.lom.Mountpath(), fqn)
		}

		// at this point, error is already written into the response -
		// return special code to indicate just that
		return errSendingResp
	}
	// Update objects sent during GFN. Thanks to this we will not
	// have to resend them in rebalance. In case of a race between rebalance
	// and GFN the former wins, resulting in duplicated transmission.
	if goi.dpq.isGFN {
		uname := goi.lom.UnamePtr()
		bname := cos.UnsafeBptr(uname)
		goi.t.reb.FilterAdd(*bname)
	} else if !goi.cold { // GFN & cold-GET: must be already loaded w/ atime set
		if err := goi.lom.Load(false /*cache it*/, true /*locked*/); err != nil {
			fs.CleanPathErr(err)
			nlog.Errorln(goi.t.String(), "GET post-transmission failure:", err)
			return errSendingResp
		}
		goi.lom.SetAtimeUnix(goi.atime)
		goi.lom.Recache()
	}
	//
	// stats
	//
	goi.stats(written)
	return nil
}

func (goi *getOI) stats(written int64) {
	vlabs := map[string]string{stats.VarlabBucket: goi.lom.Bck().Cname("")}
	delta := mono.SinceNano(goi.ltime)
	goi.t.statsT.AddWith(
		cos.NamedVal64{Name: stats.GetCount, Value: 1, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.GetSize, Value: written, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.GetThroughput, Value: written, VarLabs: vlabs}, // vis-à-vis user (as written m.b. range)
		cos.NamedVal64{Name: stats.GetLatency, Value: delta, VarLabs: vlabs},      // see also: per-backend *LatencyTotal below
		cos.NamedVal64{Name: stats.GetLatencyTotal, Value: delta, VarLabs: vlabs}, // ditto
	)
	if goi.verchanged {
		goi.t.statsT.AddWith(
			cos.NamedVal64{Name: stats.VerChangeCount, Value: 1, VarLabs: vlabs},
			cos.NamedVal64{Name: stats.VerChangeSize, Value: goi.lom.Lsize(), VarLabs: vlabs},
		)
	}

	if goi.rltime > 0 {
		bck := goi.lom.Bck()
		backend := goi.t.Backend(bck)
		goi.t.statsT.AddWith(
			cos.NamedVal64{Name: backend.MetricName(stats.GetCount), Value: 1, VarLabs: vlabs},
			cos.NamedVal64{Name: backend.MetricName(stats.GetE2ELatencyTotal), Value: delta, VarLabs: vlabs},
			cos.NamedVal64{Name: backend.MetricName(stats.GetLatencyTotal), Value: goi.rltime, VarLabs: vlabs},
			cos.NamedVal64{Name: backend.MetricName(stats.GetSize), Value: written, VarLabs: vlabs},
		)
		if goi.verchanged {
			goi.t.statsT.AddWith(
				cos.NamedVal64{Name: backend.MetricName(stats.VerChangeCount), Value: 1, VarLabs: vlabs},
				cos.NamedVal64{Name: backend.MetricName(stats.VerChangeSize), Value: goi.lom.Lsize(), VarLabs: vlabs},
			)
		}
	}
}

// - parse and validate user specified read range (goi.ranges)
// - set response header accordingly
func (goi *getOI) rngToHeader(resphdr http.Header, size int64) (hrng *htrange, ecode int, err error) {
	var ranges []htrange
	ranges, err = parseMultiRange(goi.ranges.Range, size)
	if err != nil {
		if cmn.IsErrRangeNotSatisfiable(err) {
			// https://datatracker.ietf.org/doc/html/rfc7233#section-4.2
			resphdr.Set(cos.HdrContentRange, fmt.Sprintf("%s*/%d", cos.HdrContentRangeValPrefix, size))
		}
		ecode = http.StatusRequestedRangeNotSatisfiable
		return
	}
	if len(ranges) == 0 {
		return
	}
	if len(ranges) > 1 {
		err = cmn.NewErrUnsupp("multi-range read", goi.lom.Cname())
		ecode = http.StatusRequestedRangeNotSatisfiable
		return
	}
	if goi.dpq.arch.path != "" {
		err = cmn.NewErrUnsupp("range-read archived file", goi.dpq.arch.path)
		ecode = http.StatusRequestedRangeNotSatisfiable
		return
	}

	// set response header
	hrng = &ranges[0]
	resphdr.Set(cos.HdrAcceptRanges, "bytes")
	resphdr.Set(cos.HdrContentRange, hrng.contentRange(size))
	return
}

//
// APPEND a file or multiple files:
// - as a new object, if doesn't exist
// - to an existing object, if exists
//

func (a *apndOI) do(r *http.Request) (packedHdl string, ecode int, err error) {
	var (
		cksumValue    = r.Header.Get(apc.HdrObjCksumVal)
		cksumType     = r.Header.Get(apc.HdrObjCksumType)
		contentLength = r.Header.Get(cos.HdrContentLength)
	)
	if contentLength != "" {
		if size, ers := strconv.ParseInt(contentLength, 10, 64); ers == nil {
			a.size = size
		}
	}
	if cksumValue != "" {
		a.cksum = cos.NewCksum(cksumType, cksumValue)
	}

	switch a.op {
	case apc.AppendOp:
		buf, slab := a.t.gmm.Alloc()
		packedHdl, ecode, err = a.apnd(buf)
		slab.Free(buf)
	case apc.FlushOp:
		ecode, err = a.flush()
	default:
		err = fmt.Errorf("invalid operation %q (expecting either %q or %q) - check %q query",
			a.op, apc.AppendOp, apc.FlushOp, apc.QparamAppendType)
	}

	return packedHdl, ecode, err
}

func (a *apndOI) apnd(buf []byte) (packedHdl string, ecode int, err error) {
	var (
		fh      cos.LomWriter
		workFQN = a.hdl.workFQN
	)
	if workFQN == "" {
		workFQN = fs.CSM.Gen(a.lom, fs.WorkfileType, fs.WorkfileAppend)
		a.lom.Lock(false)
		if a.lom.Load(false /*cache it*/, false /*locked*/) == nil {
			_, a.hdl.partialCksum, err = cos.CopyFile(a.lom.FQN, workFQN, buf, a.lom.CksumType())
			a.lom.Unlock(false)
			if err != nil {
				ecode = http.StatusInternalServerError
				return
			}
			fh, err = a.lom.AppendWork(workFQN)
		} else {
			a.lom.Unlock(false)
			a.hdl.partialCksum = cos.NewCksumHash(a.lom.CksumType())
			fh, err = a.lom.CreateWork(workFQN)
		}
	} else {
		fh, err = a.lom.AppendWork(workFQN)
		debug.Assert(a.hdl.partialCksum != nil)
	}
	if err != nil { // failed to open or create
		ecode = http.StatusInternalServerError
		return
	}

	w := cos.NewWriterMulti(fh, a.hdl.partialCksum.H)
	_, err = cos.CopyBuffer(w, a.r, buf)
	cos.Close(fh)
	if err != nil {
		ecode = http.StatusInternalServerError
		return
	}

	packedHdl = a.pack(workFQN)

	// stats (TODO: add `stats.FlushCount` for symmetry)
	lat := time.Now().UnixNano() - a.started
	vlabs := map[string]string{stats.VarlabBucket: a.lom.Bck().Cname("")}
	a.t.statsT.AddWith(
		cos.NamedVal64{Name: stats.AppendCount, Value: 1, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.AppendLatency, Value: lat, VarLabs: vlabs},
	)
	if cmn.Rom.FastV(4, cos.SmoduleAIS) {
		nlog.Infoln("APPEND", a.lom.String(), time.Duration(lat))
	}
	return
}

func (a *apndOI) flush() (int, error) {
	if a.hdl.workFQN == "" {
		return 0, fmt.Errorf("failed to finalize append-file operation: empty source in the %+v handle", a.hdl)
	}

	// finalize checksum
	debug.Assert(a.hdl.partialCksum != nil)
	a.hdl.partialCksum.Finalize()
	partialCksum := a.hdl.partialCksum.Clone()
	if !a.cksum.IsEmpty() && !partialCksum.Equal(a.cksum) {
		return http.StatusInternalServerError, cos.NewErrDataCksum(partialCksum, a.cksum)
	}

	params := core.PromoteParams{
		Bck:    a.lom.Bck(),
		Cksum:  partialCksum,
		Config: a.config,
		PromoteArgs: apc.PromoteArgs{
			SrcFQN:       a.hdl.workFQN,
			ObjName:      a.lom.ObjName,
			OverwriteDst: true,
			DeleteSrc:    true, // NOTE: always overwrite and remove
		},
	}
	return a.t.Promote(&params)
}

func (a *apndOI) parse(packedHdl string) error {
	if packedHdl == "" {
		return nil
	}
	items, err := preParse(packedHdl)
	if err != nil {
		return err
	}
	a.hdl.partialCksum = cos.NewCksumHash(items[2])
	buf, err := base64.StdEncoding.DecodeString(items[3])
	if err != nil {
		return err
	}
	if err := a.hdl.partialCksum.H.(encoding.BinaryUnmarshaler).UnmarshalBinary(buf); err != nil {
		return err
	}

	a.hdl.nodeID = items[0]
	a.hdl.workFQN = items[1]
	return nil
}

func (a *apndOI) pack(workFQN string) string {
	buf, err := a.hdl.partialCksum.H.(encoding.BinaryMarshaler).MarshalBinary()
	debug.AssertNoErr(err)
	cksumTy := a.hdl.partialCksum.Type()
	cksumBinary := base64.StdEncoding.EncodeToString(buf)
	return a.t.SID() + appendHandleSepa + workFQN + appendHandleSepa + cksumTy + appendHandleSepa + cksumBinary
}

//
// COPY (object | reader)
//

// main method
func (coi *coi) do(t *target, dm *bundle.DataMover, lom *core.LOM) (size int64, err error) {
	if coi.DryRun {
		return coi._dryRun(lom, coi.ObjnameTo)
	}

	// DP == nil: use default (no-op transform) if source bucket is remote
	if coi.DP == nil && (lom.Bck().IsRemote() || coi.BckTo.IsRemote()) {
		coi.DP = &core.LDP{}
	}

	// 1: dst location
	smap := t.owner.smap.Get()
	tsi, errN := smap.HrwName2T(coi.BckTo.MakeUname(coi.ObjnameTo))
	if errN != nil {
		return 0, errN
	}
	if tsi.ID() != t.SID() {
		return coi.send(t, dm, lom, coi.ObjnameTo, tsi)
	}

	// dst is this target
	// 2, 3: with transformation and without
	dst := core.AllocLOM(coi.ObjnameTo)
	defer core.FreeLOM(dst)

	if err := dst.InitBck(coi.BckTo.Bucket()); err != nil {
		return 0, err
	}
	// check for no-op
	if coi.isNOP(lom, dst, dm) {
		if cmn.Rom.FastV(5, cos.SmoduleAIS) {
			nlog.Infoln("copying", lom.String(), "=>", dst.String(), "is a no-op: destination exists and is identical")
		}
		return
	}
	// do
	if coi.DP != nil {
		var ecode int
		size, ecode, err = coi._reader(t, dm, lom, dst)
		debug.Assert(ecode != http.StatusNotFound || cos.IsNotExist(err, 0), err, ecode)
	} else {
		// fast path (Copy2FQN)
		size, err = coi._regular(t, lom, dst)
	}
	return size, err
}

func (coi *coi) isNOP(lom, dst *core.LOM, dm *bundle.DataMover) bool {
	if coi.LatestVer || coi.Sync {
		return false
	}
	owt := coi.OWT
	if dm != nil {
		owt = dm.OWT()
	}
	if owt != cmn.OwtCopy {
		return false
	}
	if err := lom.Load(true, false); err != nil {
		return false
	}
	if err := dst.Load(true, false); err != nil {
		return false
	}
	if lom.CheckEq(dst) != nil {
		return false
	}
	res := dst.CheckRemoteMD(false /*locked*/, false, nil /*origReq*/)
	return res.Eq
}

func (coi *coi) _dryRun(lom *core.LOM, objnameTo string) (size int64, err error) {
	if coi.DP == nil {
		uname := coi.BckTo.MakeUname(objnameTo)
		if lom.Uname() != cos.UnsafeS(uname) {
			size = lom.Lsize()
		}
		return size, nil
	}

	// discard the reader and be done
	var reader io.ReadCloser
	if reader, _, err = coi.DP.Reader(lom, false, false); err != nil {
		return 0, err
	}
	size, err = io.Copy(io.Discard, reader)
	reader.Close()
	return size, err
}

// PUT DP(lom) => dst
// The DP reader is responsible for any read-locking of the source lom.
//
// NOTE: no assumpions are being made on whether the source lom is present in cluster.
// (can be a "pure" metadata of a (non-existing) Cloud object; accordingly, DP's reader must
// be able to hande cold get, warm get, etc.)
//
// If destination bucket is remote:
// - create a local replica of the object on one of the targets, and
// - putRemote (with one exception below)
//
// An option for _not_ storing the object _in_ the cluster would be a _feature_ that can be
// further debated.
func (coi *coi) _reader(t *target, dm *bundle.DataMover, lom, dst *core.LOM) (size int64, _ int, _ error) {
	reader, oah, errN := coi.DP.Reader(lom, coi.LatestVer, coi.Sync)
	if errN != nil {
		return 0, 0, errN
	}
	poi := allocPOI()
	{
		poi.t = t
		poi.lom = dst
		poi.config = coi.Config
		poi.r = reader
		poi.xctn = coi.Xact // on behalf of
		poi.workFQN = fs.CSM.Gen(dst, fs.WorkfileType, "copy-dp")
		poi.atime = oah.AtimeUnix()
		poi.cksumToUse = oah.Checksum()

		poi.owt = coi.OWT
		if dm != nil {
			poi.owt = dm.OWT() // (precedence; cmn.OwtCopy, cmn.OwtTransform - what else?)
		}
	}
	if poi.owt == cmn.OwtCopy {
		// preserve src metadata when copying (vs. transforming)
		dst.CopyVersion(lom)
		dst.SetCustomMD(lom.GetCustomMD())

		// [special] when src == dst (`ais cp s3://data s3://data --all`)
		if backend := lom.Bck().RemoteBck(); backend != nil && backend.Equal(coi.BckTo.Bucket()) {
			poi.owt = cmn.OwtCopySameBucket
		}
	}

	ecode, err := poi.putObject()
	freePOI(poi)
	if err == nil {
		// xaction stats: inc locally processed (and see data mover for in and out objs)
		size = oah.Lsize()
	}
	return size, ecode, err
}

func (coi *coi) _regular(t *target, lom, dst *core.LOM) (size int64, _ error) {
	if lom.FQN == dst.FQN { // resilvering with a single mountpath?
		return
	}
	lcopy := lom.Uname() == dst.Uname() // n-way copy
	lom.Lock(lcopy)
	defer lom.Unlock(lcopy)

	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		if !cos.IsNotExist(err, 0) {
			err = cmn.NewErrFailedTo(t, "coi-load", lom.Cname(), err)
		}
		return 0, err
	}

	// w-lock the destination unless already locked (above)
	if !lcopy {
		dst.Lock(true)
		defer dst.Unlock(true)
		if err := dst.Load(false /*cache it*/, true /*locked*/); err == nil {
			if lom.EqCksum(dst.Checksum()) {
				return 0, nil
			}
		} else if cmn.IsErrBucketNought(err) {
			return 0, err
		}
	}

	// TODO: add a metric to count and size local copying
	dst2, err := lom.Copy2FQN(dst.FQN, coi.Buf)
	if err == nil {
		size = lom.Lsize()
		if coi.Finalize {
			t.putMirror(dst2)
		}
	}
	if dst2 != nil {
		core.FreeLOM(dst2)
	}
	return size, err
}

// send object => designated target
// * source is a LOM or a reader (that may be reading from remote)
// * one of the two equivalent transmission mechanisms: PUT or transport Send
func (coi *coi) send(t *target, dm *bundle.DataMover, lom *core.LOM, objNameTo string, tsi *meta.Snode) (size int64, err error) {
	debug.Assert(coi.OWT > 0)
	sargs := allocSnda()
	{
		sargs.objNameTo = objNameTo
		sargs.tsi = tsi
		sargs.dm = dm

		sargs.owt = coi.OWT
		if dm != nil {
			sargs.owt = dm.OWT() // (precedence; cmn.OwtCopy, cmn.OwtTransform - what else?)
		}
	}
	size, err = coi._send(t, lom, sargs)
	freeSnda(sargs)
	return
}

func (coi *coi) _send(t *target, lom *core.LOM, sargs *sendArgs) (size int64, _ error) {
	debug.Assert(!coi.DryRun)
	if sargs.dm != nil {
		// clone the `lom` to use it in the async operation (free it via `_sendObjDM` callback)
		lom = lom.CloneMD(lom.FQN)
	}

	switch {
	case coi.OWT == cmn.OwtPromote:
		// 1. promote
		debug.Assert(coi.DP == nil)
		debug.Assert(sargs.owt == cmn.OwtPromote)

		fh, err := cos.NewFileHandle(lom.FQN)
		if err != nil {
			if os.IsNotExist(err) {
				return 0, nil
			}
			return 0, cmn.NewErrFailedTo(t, "open", lom.Cname(), err)
		}
		fi, err := fh.Stat()
		if err != nil {
			fh.Close()
			return 0, cmn.NewErrFailedTo(t, "fstat", lom.Cname(), err)
		}
		size = fi.Size()
		sargs.reader, sargs.objAttrs = fh, lom
	case coi.DP == nil:
		// 2. migrate/replicate lom

		lom.Lock(false)
		if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
			lom.Unlock(false)
			return 0, nil
		}
		reader, err := lom.NewDeferROC()
		if err != nil {
			return 0, err
		}
		size = lom.Lsize()
		sargs.reader, sargs.objAttrs = reader, lom
	default:
		// 3. DP transform (possibly, no-op)
		// If the object is not present call t.Backend.GetObjReader
		reader, oah, err := coi.DP.Reader(lom, coi.LatestVer, coi.Sync)
		if err != nil {
			return
		}
		// returns cos.ContentLengthUnknown (-1) if post-transform size is unknown
		size = oah.Lsize()
		sargs.reader, sargs.objAttrs = reader, oah
	}

	// do
	var err error
	sargs.bckTo = coi.BckTo
	if sargs.dm != nil {
		err = coi._dm(lom /*for attrs*/, sargs)
	} else {
		err = coi.put(t, sargs)
	}
	return size, err
}

// use data mover to transmit objects to other targets
// (compare with coi.put())
func (coi *coi) _dm(lom *core.LOM, sargs *sendArgs) error {
	debug.Assert(sargs.dm.OWT() == sargs.owt)
	debug.Assert(sargs.dm.GetXact() == coi.Xact || sargs.dm.GetXact().ID() == coi.Xact.ID())
	o := transport.AllocSend()
	hdr, oa := &o.Hdr, sargs.objAttrs
	{
		hdr.Bck.Copy(sargs.bckTo.Bucket())
		hdr.ObjName = sargs.objNameTo
		hdr.ObjAttrs.CopyFrom(oa, false /*skip cksum*/)
	}
	o.Callback = func(_ *transport.ObjHdr, _ io.ReadCloser, _ any, _ error) {
		core.FreeLOM(lom)
	}
	return sargs.dm.Send(o, sargs.reader, sargs.tsi)
}

// PUT(lom) => destination target (compare with coi.dm())
// always closes params.Reader, either explicitly or via Do()
func (coi *coi) put(t *target, sargs *sendArgs) error {
	var (
		hdr   = make(http.Header, 8)
		query = sargs.bckTo.NewQuery()
	)
	cmn.ToHeader(sargs.objAttrs, hdr, sargs.objAttrs.Lsize(true))
	hdr.Set(apc.HdrT2TPutterID, t.SID())
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
	req, _, cancel, err := reqArgs.ReqWithTimeout(coi.Config.Timeout.SendFile.D())
	if err != nil {
		cos.Close(sargs.reader)
		return fmt.Errorf("unexpected failure to create request, err: %w", err)
	}
	defer cancel()
	resp, err := g.client.data.Do(req)
	if err != nil {
		return cmn.NewErrFailedTo(t, "coi.put "+sargs.bckTo.Name+"/"+sargs.objNameTo, sargs.tsi, err)
	}
	cos.DrainReader(resp.Body)
	resp.Body.Close()
	return nil
}

func (coi *coi) stats(size int64, err error) {
	if err == nil && coi.Xact != nil && size > 0 {
		coi.Xact.ObjsAdd(1, size)
	}
}

//
// PUT a new shard _or_ APPEND to an existing one (w/ read/write/list via cmn/archive)
//

func (a *putA2I) do() (int, error) {
	if a.filename == "" {
		return 0, errors.New("archive path is not defined")
	}
	// standard library does not support appending to tgz, zip, and such;
	// for TAR there is an optimizing workaround not requiring a full copy
	if a.mime == archive.ExtTar && !a.put /*append*/ && !a.lom.IsChunked() {
		var (
			err       error
			fh        *os.File
			offset    int64
			size      int64
			tarFormat tar.Format
			workFQN   = fs.CSM.Gen(a.lom, fs.WorkfileType, fs.WorkfileAppendToArch)
		)
		if err = a.lom.RenameMainTo(workFQN); err != nil {
			return http.StatusInternalServerError, err
		}
		fh, tarFormat, offset, err = archive.OpenTarForAppend(a.lom.Cname(), workFQN)
		if err != nil {
			if errV := a.lom.RenameToMain(workFQN); errV != nil {
				return http.StatusInternalServerError, errV
			}
			if err == archive.ErrTarIsEmpty {
				a.put = true
				goto cpap
			}
			return http.StatusInternalServerError, err
		}
		// do - fast
		if size, err = a.fast(fh, tarFormat, offset); err == nil {
			// TODO: checksum NIY
			if err = a.finalize(size, cos.NoneCksum, workFQN); err == nil {
				return http.StatusInternalServerError, nil // ok
			}
		} else if errV := a.lom.RenameToMain(workFQN); errV != nil {
			nlog.Errorf(fmtNested, a.t, err, "append and rename back", workFQN, errV)
		}
		return http.StatusInternalServerError, err
	}

cpap: // copy + append
	var (
		err     error
		wfh     *os.File
		lmfh    cos.LomReader
		workFQN string
		cksum   cos.CksumHashSize
		aw      archive.Writer
	)
	workFQN = fs.CSM.Gen(a.lom, fs.WorkfileType, fs.WorkfileAppendToArch)
	wfh, err = os.OpenFile(workFQN, os.O_CREATE|os.O_WRONLY, cos.PermRWR)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	// currently, arch writers only use size and time but it may change
	oah := cos.SimpleOAH{Size: a.size, Atime: a.started}
	if a.put {
		// when append becomes PUT (TODO: checksum type)
		cksum.Init(cos.ChecksumXXHash)
		aw = archive.NewWriter(a.mime, wfh, &cksum, nil /*opts*/)
		err = aw.Write(a.filename, oah, a.r)
		aw.Fini()
	} else {
		// copy + append
		lmfh, err = a.lom.Open()
		if err != nil {
			cos.Close(wfh)
			return http.StatusNotFound, err
		}
		cksum.Init(a.lom.CksumType())
		aw = archive.NewWriter(a.mime, wfh, &cksum, nil)
		err = aw.Copy(lmfh, a.lom.Lsize())
		if err == nil {
			err = aw.Write(a.filename, oah, a.r)
		}
		aw.Fini() // in that order
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
func (a *putA2I) fast(rwfh *os.File, tarFormat tar.Format, offset int64) (size int64, err error) {
	var (
		buf, slab = a.t.gmm.AllocSize(a.size)
		tw        = tar.NewWriter(rwfh)
		hdr       = tar.Header{
			Typeflag: tar.TypeReg,
			Name:     a.filename,
			Size:     a.size,
			ModTime:  time.Unix(0, a.started),
			Mode:     int64(cos.PermRWRR),
			Format:   tarFormat,
		}
	)
	tw.WriteHeader(&hdr)
	_, err = io.CopyBuffer(tw, a.r, buf) // append
	cos.Close(tw)
	if err == nil {
		size, err = rwfh.Seek(0, io.SeekCurrent)
		debug.Assert(err != nil || size > offset, size, " vs ", offset)
	}
	slab.Free(buf)
	cos.Close(rwfh)
	return
}

func (*putA2I) reterr(err error) (int, error) {
	ecode := http.StatusInternalServerError
	if cmn.IsErrCapExceeded(err) {
		ecode = http.StatusInsufficientStorage
	}
	return ecode, err
}

func (a *putA2I) finalize(size int64, cksum *cos.Cksum, fqn string) error {
	debug.Func(func() {
		finfo, err := os.Stat(fqn)
		debug.AssertNoErr(err)
		debug.Assertf(finfo.Size() == size, "%d != %d", finfo.Size(), size)
	})
	// done
	if err := a.lom.RenameFinalize(fqn); err != nil {
		return err
	}
	a.lom.SetSize(size)
	a.lom.SetCksum(cksum)
	a.lom.SetAtimeUnix(a.started)
	if err := a.lom.Persist(); err != nil {
		return err
	}
	if a.lom.ECEnabled() {
		if err := ec.ECM.EncodeObject(a.lom, nil); err != nil && err != ec.ErrorECDisabled {
			return err
		}
	}
	a.t.putMirror(a.lom)
	return nil
}

//
// put mirorr (main)
//

func (t *target) putMirror(lom *core.LOM) {
	mconfig := lom.MirrorConf()
	if !mconfig.Enabled {
		return
	}
	if mpathCnt := fs.NumAvail(); mpathCnt < int(mconfig.Copies) {
		// removed: inc stats.ErrPutMirrorCount
		nanotim := mono.NanoTime()
		if nanotim&0x7 == 7 {
			if mpathCnt == 0 {
				nlog.Errorf("%s: %v", t, cmn.ErrNoMountpaths)
			} else {
				nlog.Errorf(fmtErrInsuffMpaths2, t, mpathCnt, lom, mconfig.Copies)
			}
		}
		return
	}
	rns := xreg.RenewPutMirror(lom)
	if rns.Err != nil {
		nlog.Errorf("%s: %s %v", t, lom, rns.Err)
		debug.AssertNoErr(rns.Err)
		return
	}
	xctn := rns.Entry.Get()
	xputlrep := xctn.(*mirror.XactPut)
	xputlrep.Repl(lom)
}

//
// uplock
//

const uplockWarn = "conflict getting remote"

func (u *_uplock) do(lom *core.LOM) error {
	if u.elapsed > u.timeout {
		err := cmn.NewErrBusy("node", core.T.String(), uplockWarn+" '"+lom.Cname()+"'")
		nlog.ErrorDepth(1, err)
		return err
	}

	lom.Unlock(false)
	runtime.Gosched()
	time.Sleep(u.sleep)
	lom.Lock(false) // all over again: try load and check all respective conditions

	u.elapsed += u.sleep
	if u.elapsed == 3*u.sleep && u.sleep < u.config.Timeout.CplaneOperation.D() {
		u.sleep <<= 1
	}
	return nil
}

// TODO:
// - CopyBuffer
// - currently, only tar - add message pack (what else?)
// - Call(..., *tar.Header) to avoid typecast

type rcbCtx struct {
	w   io.Writer
	tw  *tar.Writer
	num int
}

var _ archive.ArchRCB = (*rcbCtx)(nil)

func _newRcb(w io.Writer) (c *rcbCtx) {
	c = &rcbCtx{w: w}
	return c
}

func (c *rcbCtx) Call(_ string, reader cos.ReadCloseSizer, hdr any) (_ bool /*stop*/, err error) {
	if c.tw == nil {
		debug.Assert(c.num == 0)
		c.tw = tar.NewWriter(c.w)
	}
	c.num++
	tarHdr, ok := hdr.(*tar.Header)
	debug.Assert(ok)
	if err = c.tw.WriteHeader(tarHdr); err == nil {
		_, err = io.Copy(c.tw, reader)
	}
	return false, err
}

func (c *rcbCtx) fini() {
	if c.tw != nil {
		debug.Assert(c.num > 0)
		c.tw.Close()
	}
}

//
// mem pools
//

var (
	goiPool, poiPool, sndPool sync.Pool

	goi0 getOI
	poi0 putOI
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
