// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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
		locked     bool          // true if the LOM is already locked by the caller
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
		chunked    bool       // chunked transfer (en)coding: https://tools.ietf.org/html/rfc7230#page-36
		unlocked   bool       // internal
		verchanged bool       // version changed
		retry      bool       // once
		cold       bool       // true if executed backend.Get
		latestVer  bool       // QparamLatestVer || 'versioning.*_warm_get'
		isIOErr    bool       // to count GET error as a "IO error"; see `Trunner._softErrs()`
		rget       bool       // when reading remote source via backend.GetObjReader, scenarios including: cold-GET, copy, transform, blob
	}
	_uplock struct {
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
		dm        *bundle.DM
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
		poi.workFQN = poi.lom.GenFQN(fs.WorkCT, fs.WorkfilePut)
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

func (poi *putOI) chunk(chunkSize int64) (ecode int, err error) {
	var (
		lom      = poi.lom
		uploadID string
	)
	debug.Assertf(!poi.coldGET || poi.locked, "expecting locked LOM for cold-GET")

	debug.Assertf(poi.size > 0, "poi.size is required in chunk, object name: %s", poi.lom.Cname())
	if uploadID, err = poi.t.ups.start(poi.oreq, lom, poi.coldGET); err != nil {
		poi.t.ups.abort(poi.oreq, lom, uploadID)
		return http.StatusInternalServerError, err
	}

	// Loop through poi.r and divide it into chunks
	var (
		total          int64
		partNum        = 1
		completedParts = make(apc.MptCompletedParts, 0, poi.size/chunkSize)
	)
	for total < poi.size {
		// Determine how many bytes to read for this part
		remainingBytes := poi.size - total
		thisChunkSize := min(chunkSize, remainingBytes)

		// Create a limited reader for this chunk
		limitedReader := &io.LimitedReader{
			R: poi.r,
			N: thisChunkSize,
		}
		chunkReader := io.NopCloser(limitedReader)

		args := partArgs{
			reader:   chunkReader,
			size:     thisChunkSize,
			lom:      lom,
			uploadID: uploadID,
			partNum:  partNum,
			coldGET:  poi.coldGET,
		}
		etag, ec, er := poi.t.ups.putPart(&args)
		if er != nil {
			poi.t.ups.abort(poi.oreq, lom, uploadID)
			return ec, er
		}

		// Calculate actual bytes read
		total += thisChunkSize

		// Track completed part
		completedParts = append(completedParts, apc.MptCompletedPart{
			ETag:       etag,
			PartNumber: partNum,
		})

		partNum++
	}

	_, ecode, err = poi.t.ups.complete(&completeArgs{
		r:        poi.oreq,
		lom:      lom,
		uploadID: uploadID,
		body:     nil,
		parts:    completedParts,
		isS3:     false,
		coldGET:  poi.coldGET,
		locked:   poi.locked,
	})
	return ecode, err
}

func (poi *putOI) putObject() (ecode int, err error) {
	maxMonoSize := int64(poi.lom.Bprops().Chunks.MaxMonolithicSize)
	// protect the bucket: if the object size exceeds the max monolithic size, MUST chunk
	// NOTE: if `poi.size` is not set, don't trigger chunking
	if maxMonoSize > 0 && poi.size > maxMonoSize {
		if cmn.Rom.V(5, cos.ModAIS) {
			nlog.Infoln("PUT", poi.lom.Cname(), "size", poi.size, "exceeds object size limit, PUT as chunks")
		}
		return poi.chunk(int64(poi.lom.Bprops().Chunks.ChunkSize))
	}
	poi.ltime = mono.NanoTime()

	// if checksums match PUT is a no-op
	if !poi.skipVC && !poi.coldGET {
		if poi.lom.EqCksum(poi.cksumToUse) {
			if cmn.Rom.V(4, cos.ModAIS) {
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
		// from ETL direct put
		if poi.owt == cmn.OwtTransform && poi.t2t {
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

	if cmn.Rom.V(5, cos.ModAIS) {
		nlog.Infoln(poi.loghdr())
	}
	return 0, nil
rerr:
	if poi.owt == cmn.OwtPut && poi.restful && !poi.t2t {
		vlabs := poi._vlabs(true /*detailed*/)
		poi.t.statsT.IncWith(stats.ErrPutCount, vlabs)

		if err != cmn.ErrSkip && !poi.remoteErr && err != io.ErrUnexpectedEOF && !cos.IsRetriableConnErr(err) && !cos.IsErrMv(err) {
			poi.t.statsT.IncWith(stats.IOErrPutCount, vlabs)
			if cmn.Rom.V(4, cos.ModAIS) {
				nlog.Warningln("io-error [", err, "]", poi.loghdr())
			}
		}
	}
	return ecode, err
}

// when detailed metrics are disabled, returns pre-allocated empty map
func (poi *putOI) _vlabs(detailed bool) map[string]string {
	if !detailed {
		return stats.EmptyBckXlabs
	}
	var xkind string
	if poi.xctn != nil {
		xkind = poi.xctn.Kind()
	}
	vlabs := map[string]string{stats.VlabBucket: poi.lom.Bck().Cname(""), stats.VlabXkind: xkind}
	return vlabs
}

func (poi *putOI) stats() {
	var (
		bck   = poi.lom.Bck()
		size  = poi.lom.Lsize()
		delta = mono.SinceNano(poi.ltime)
		fl    = cmn.Rom.Features()
		vlabs = poi._vlabs(fl.IsSet(feat.EnableDetailedPromMetrics))
	)
	poi.t.statsT.IncWith(stats.PutCount, vlabs)
	poi.t.statsT.AddWith(
		cos.NamedVal64{Name: stats.PutSize, Value: size, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.PutThroughput, Value: size, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.PutLatency, Value: delta, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.PutLatencyTotal, Value: delta, VarLabs: vlabs},
	)
	if poi.rltime > 0 {
		debug.Assert(bck.IsRemote())
		bp := poi.t.Backend(bck)
		poi.t.statsT.IncWith(bp.MetricName(stats.PutCount), vlabs)
		poi.t.statsT.AddWith(
			cos.NamedVal64{Name: bp.MetricName(stats.PutLatencyTotal), Value: poi.rltime, VarLabs: vlabs},
			cos.NamedVal64{Name: bp.MetricName(stats.PutE2ELatencyTotal), Value: delta, VarLabs: vlabs},
			cos.NamedVal64{Name: bp.MetricName(stats.PutSize), Value: size, VarLabs: vlabs},
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
		if err1 := cos.Stat(poi.workFQN); err1 == nil || !cos.IsNotExist(err1) {
			// cleanup: rm work-fqn
			if err1 == nil {
				err1 = err
			}
			if err2 := cos.RemoveFile(poi.workFQN); err2 != nil && !cos.IsNotExist(err2) {
				nlog.Errorf(fmtNested, poi.t, err1, "remove", poi.workFQN, err2)
			}
		}
		poi.lom.UncacheDel()
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

// poor man's retry when no rate-limit configured
// - only once
// - e.g. googleapi: "Error 503: We encountered an internal error. Please try again."
// - see docs/rate-limit
func (poi *putOI) _retry503() (ecode int, err error) {
	time.Sleep(time.Second)
	ecode, err = poi.putRemote()
	if err != nil {
		return ecode, err
	}
	nlog.Infoln("PUT [", poi.loghdr(), "] - retried 503 ok")
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
			if cmn.Rom.V(5, cos.ModAIS) {
				loghdr := poi.loghdr()
				nlog.Errorln("PUT [", loghdr, err, ecode, "]")
			}
			if !bck.Props.RateLimit.Backend.Enabled {
				if ecode == http.StatusServiceUnavailable || ecode == http.StatusTooManyRequests {
					ecode, err = poi._retry503()
				}
			}
			if err != nil {
				return ecode, err
			}
		}
	}

	// locking strategies: optimistic and otherwise
	// (see GetCold() implementation and cmn.OWT enum)
	switch poi.owt {
	case cmn.OwtGetTryLock, cmn.OwtGetLock, cmn.OwtGet, cmn.OwtChunks:
		// do nothing: lom is already wlocked
		debug.Assertf(lom.IsLocked() == apc.LockWrite, "lom %s is not write-locked", lom.Cname())
	case cmn.OwtGetPrefetchLock:
		if !lom.TryLock(true) {
			nlog.Warningln(poi.loghdr(), "is busy")
			return 0, cmn.ErrSkip // e.g. prefetch can skip it and keep on going // TODO: must be cmn.ErrBusy
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
		switch {
		case poi.owt >= cmn.OwtRebalance || poi.owt == cmn.OwtCopy:
			// rebalance, copy, get*: do nothing
		default:
			// best effort
			if remSrc, ok := lom.GetCustomKey(cmn.SourceObjMD); !ok || remSrc == "" {
				if err = lom.IncVersion(); err != nil {
					nlog.Errorln(err) // (unlikely)
				}
			}
		}
	}

	// done
	if err := lom.RenameFinalize(poi.workFQN); err != nil {
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
	return 0, lom.PersistMain(false /*isChunked*/)
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
		ecode int
		bp    = poi.t.Backend(lom.Bck())
	)
	ecode, err = bp.PutObj(context.Background(), lmfh, lom, poi.oreq)
	if err == nil {
		if !lom.Bck().IsRemoteAIS() {
			lom.SetCustomKey(cmn.SourceObjMD, bp.Provider())
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
		return nil, nil, nil, err
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
	case !cos.NoneC(poi.cksumToUse) && !poi.validateCksum(ckconf):
		// if the corresponding validation is not configured/enabled we just go ahead
		// and use the checksum that has arrived with the object
		poi.lom.SetCksum(poi.cksumToUse)
		// (ditto)
		written, err = cos.CopyBuffer(lmfh, poi.r, buf)
	default:
		writers := make([]io.Writer, 0, 3)
		cksums.store = cos.NewCksumHash(ckconf.Type) // always according to the bucket
		writers = append(writers, cksums.store.H)
		if !poi.skipVC && !cos.NoneC(poi.cksumToUse) && poi.validateCksum(ckconf) {
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
		return buf, slab, lmfh, err
	}

	// validate
	if cksums.compt != nil {
		cksums.finalized = cksums.compt == cksums.store
		cksums.compt.Finalize()
		if !cksums.compt.Equal(cksums.expct) {
			err = cos.NewErrDataCksum(cksums.expct, &cksums.compt.Cksum, poi.lom.Cname())
			poi.t.statsT.IncWith(stats.ErrPutCksumCount, poi._vlabs(true /*detailed*/))
			return buf, slab, lmfh, err
		}
	}

	// ok
	if poi.lom.IsFeatureSet(feat.FsyncPUT) {
		err = lmfh.Sync() // compare w/ cos.FlushClose
		debug.AssertNoErr(err)
	}

	cos.Close(lmfh)

	poi.lom.SetSize(written) // TODO: compare with non-zero lom.Lsize() that may have been set via oa.FromHeader()
	if cksums.store != nil {
		if !cksums.finalized {
			cksums.store.Finalize()
		}
		poi.lom.SetCksum(&cksums.store.Cksum)
	}
	return buf, slab, nil /*closed lmfh*/, err
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
	cos.DrainReader(poi.r)
	poi.r.Close()
	if lmfh != nil {
		if nerr := lmfh.Close(); nerr != nil {
			nlog.Errorf(fmtNested, poi.t, err, "close", poi.workFQN, nerr)
		}
	}
	if nerr := cos.RemoveFile(poi.workFQN); nerr != nil && !cos.IsNotExist(nerr) {
		nlog.Errorf(fmtNested, poi.t, err, "remove", poi.workFQN, nerr)
	}
}

func (poi *putOI) validateCksum(c *cmn.CksumConf) (v bool) {
	switch poi.owt {
	case cmn.OwtRebalance, cmn.OwtCopy:
		v = c.ValidateObjMove
	case cmn.OwtPut:
		v = true
	case cmn.OwtGetTryLock, cmn.OwtGetLock, cmn.OwtGet:
		v = c.ValidateColdGet
	case cmn.OwtGetPrefetchLock, cmn.OwtTransform:
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
do: // retry uplock or ec-recovery, the latter only once

	err = goi.lom.Load(true /*cache it*/, true /*locked*/)
	if err != nil {
		cold = cos.IsNotExist(err)
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
	if !cold && goi.lom.ValidateWarmGet() {
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
		bp := goi.t.Backend(goi.lom.Bck())
		if cs.IsNil() {
			cs = fs.Cap()
		}
		if cs.IsOOS() {
			return http.StatusInsufficientStorage, cs.Err()
		}

		// try upgrading rlock => wlock
		if !goi.lom.UpgradeLock() {
			if uplock == nil {
				uplock = goi.uplock(cmn.GCO.Get())
				nlog.Warningln(uplockWarn, goi.lom.String())
			}
			if err := uplock.do(goi.lom); err != nil {
				return http.StatusConflict, err
			}
			cold = false
			goto do // repeat
		}

		goi.lom.SetAtimeUnix(goi.atime)
		// zero-out prev. version custom metadata, if any
		goi.lom.SetCustomMD(nil)

		// get remote reader (compare w/ t.GetCold)
		goi.rget = true
		res := bp.GetObjReader(goi.ctx, goi.lom, 0, 0)
		if res.Err != nil {
			goi.lom.Unlock(true)
			goi.unlocked = true
			if !cos.IsNotExist(res.Err, res.ErrCode) {
				nlog.Infoln(ftcg, "(read)", goi.lom.Cname(), res.Err, res.ErrCode)
			}
			return res.ErrCode, res.Err
		}
		goi.cold = true

		if goi.isStreamingColdGet() {
			err = goi.coldStream(&res)
			goi.unlocked = true
			return 0, err
		}

		// regular path
		ecode, err = goi.coldPut(&res)
		if err != nil {
			goi.unlocked = true
			return ecode, err
		}
	}

	// read locally and stream back
fin:
	var fqn string
	fqn, ecode, err = goi.txfini()
	if err == nil {
		err = goi.expostfacto(fqn)
	} else if goi.retry {
		goi.retry = false
		if !retried {
			goi.lom.UncacheDel()
			nlog.Warningln("retrying", goi.lom.String(), err)
			retried = true
			cold = false
			goto do
		}
	}
	return ecode, err
}

func (goi *getOI) expostfacto(fqn string) error {
	lom := goi.lom

	// apc.QparamIsGFNRequest: update GFN filter to skip _rebalancing_ this one
	if goi.dpq.isGFN {
		bname := cos.UnsafeBptr(lom.UnamePtr())
		goi.t.reb.FilterAdd(*bname)
		return nil
	}
	if goi.cold || goi.rget { // GFN & cold-GET: must be already loaded w/ atime set
		return nil
	}
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		if !cmn.IsErrObjNought(err) {
			fs.CleanPathErr(err)
			goi.isIOErr = true
			goi.t.FSHC(err, lom.Mountpath(), fqn)
		}
		return cmn.ErrGetTxBenign
	}
	lom.SetAtimeUnix(goi.atime)
	lom.Recache()
	return nil
}

func (goi *getOI) isStreamingColdGet() bool {
	if !goi.lom.IsFeatureSet(feat.StreamingColdGET) {
		return false
	}

	// assorted limitations each of which (or all together) can be lifted if need be
	switch {
	case goi.dpq.arch.path != "" || goi.dpq.arch.regx != "":
		return false
	case goi.ranges.Range != "":
		return false
	case goi.lom.ValidateColdGet():
		return false
	case goi.lom.Bprops().Chunks.AutoEnabled():
		return false
	}
	return true
}

func (goi *getOI) coldPut(res *core.GetReaderResult) (int, error) {
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
		poi.workFQN = lom.GenFQN(fs.WorkCT, fs.WorkfileColdget)
		poi.atime = goi.atime
		poi.owt = cmn.OwtGet
		poi.cksumToUse = res.ExpCksum // expected checksum (to validate if the bucket's `validate_cold_get == true`)
		poi.coldGET = true
		poi.locked = true
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
func (goi *getOI) validateRecover() (coldGet bool, ecode int, err error) {
	var (
		lom     = goi.lom
		retried bool
	)
validate:
	err = lom.ValidateMetaChecksum()
	if err == nil {
		err = lom.ValidateContentChecksum(true /*locked*/)
	}
	if err == nil {
		return false, 0, nil
	}
	ecode = http.StatusInternalServerError
	if !cos.IsErrBadCksum(err) {
		return false, ecode, err
	}
	if !lom.Bck().IsAIS() && !goi.lom.IsFeatureSet(feat.DisableColdGET) {
		return true, ecode, err
	}

	nlog.Warningln(err)
	redundant := lom.HasCopies() || lom.ECEnabled()
	//
	// return err if there's no redundancy OR already recovered once (and failed)
	//
	if retried || !redundant {
		// TODO: mark `deleted` and postpone actual deletion
		if erl := lom.RemoveObj(true /*force through rlock*/); erl != nil {
			nlog.Warningf("%s: failed to remove corrupted %s, err: %v", goi.t, lom, erl)
		}
		return false, ecode, err
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
			goto validate
		}
	}
	if lom.ECEnabled() {
		retried = true
		goi.lom.Unlock(false)
		lom.RemoveMain()
		_, ecode, err = goi.restoreFromAny(true /*skipLomRestore*/)
		goi.lom.Lock(false)
		if err == nil {
			nlog.Warningf("%s: recovered corrupted %s from EC slices", goi.t, lom)
			goto validate
		}
	}

	// TODO: ditto
	if erl := lom.RemoveObj(true /*force through rlock*/); erl != nil {
		nlog.Warningf("%s: failed to remove corrupted %s, err: %v", goi.t, lom, erl)
	}
	return false, ecode, err
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
		return false, 0, err
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
				nlog.Infoln(goi.lom.String(), "restored to location")
				return false, 0, nil
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
			return false, 0, nil
		}
	}

	// restore from existing EC slices
	ecErr := ec.ECM.Recover(goi.lom)
	if ecErr == nil {
		ecErr = goi.lom.Load(true /*cache it*/, false /*locked*/) // TODO: optimize locking
		if ecErr == nil {
			nlog.Infoln(goi.t.String(), "EC-recovered", goi.lom.Cname())
			return false, 0, nil
		}
		err = cmn.NewErrFailedTo(goi.t, "load EC-recovered", goi.lom.Cname(), ecErr)
		nlog.Errorln(ecErr)
	} else if ecErr != ec.ErrorECDisabled {
		err = cmn.NewErrFailedTo(goi.t, "EC-recover", goi.lom.Cname(), ecErr)
		if cmn.IsErrCapExceeded(ecErr) {
			ecode = http.StatusInsufficientStorage
		}
		return doubleCheck, ecode, err
	}

	if err != nil {
		if _, ok := err.(*cmn.ErrFailedTo); !ok {
			err = cmn.NewErrFailedTo(goi.t, "goi-restore-any", goi.lom.Cname(), err)
		}
	} else {
		err = cos.NewErrNotFound(goi.t, goi.lom.Cname())
	}
	return doubleCheck, http.StatusNotFound, err
}

func (goi *getOI) getFromNeighbor(lom *core.LOM, tsi *meta.Snode) bool {
	config := cmn.GCO.Get()
	params := &core.GfnParams{
		Lom:    lom,
		Tsi:    tsi,
		Config: config,
		Size:   lom.Lsize(true),
	}
	resp, err := goi.t.GetFromNeighbor(params) //nolint:bodyclose // closed by poi.put()
	if err != nil {
		nlog.Errorf("%s: gfn failure, %s %q, err: %v", goi.t, tsi, lom, err)
		return false
	}

	cksumToUse := lom.ObjAttrs().FromHeader(resp.Header)
	workFQN := lom.GenFQN(fs.WorkCT, fs.WorkfileRemote)
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
	if poi.atime == 0 {
		poi.atime = time.Now().UnixNano()
	}
	ecode, erp := poi.putObject()
	freePOI(poi)
	if erp == nil {
		if cmn.Rom.V(5, cos.ModAIS) {
			nlog.Infoln(goi.t.String(), "gfn", goi.lom.String(), "<=", tsi.StringEx())
		}
		return true
	}
	nlog.Errorf("%s: gfn-GET failed to PUT locally: %v(%d)", goi.t, erp, ecode)
	return false
}

func (goi *getOI) txfini() (fqn string, ecode int, err error) {
	var (
		lmfh cos.LomReader
		hrng *htrange
		dpq  = goi.dpq
		lom  = goi.lom
	)
	// open
	if cmn.Rom.Features().IsSet(feat.LoadBalanceGET) && !goi.cold && !dpq.isGFN && !lom.IsChunked() {
		// [feat] best-effort GET load balancing across mirrored copies
		if lmfh, fqn = lom.OpenCopy(); lmfh == nil {
			fqn = lom.FQN
			lmfh, err = lom.Open()
		}
	} else {
		fqn = lom.FQN
		lmfh, err = lom.Open()
	}

	if err != nil {
		if cos.IsNotExist(err) {
			// NOTE: retry only once and only when ec-enabled - see goi.restoreFromAny()
			ecode = http.StatusNotFound
			goi.retry = lom.ECEnabled()
		} else {
			goi.t.FSHC(err, lom.Mountpath(), fqn)
			ecode = http.StatusInternalServerError
			err = cmn.NewErrFailedTo(goi.t, "goi-finalize", lom.Cname(), err, ecode)
		}
		return fqn, ecode, err
	}

	whdr := goi.w.Header()

	// transmit (range, arch, regular)
	switch {
	case goi.ranges.Range != "":
		debug.Assert(!dpq.isArch())
		rsize := lom.Lsize()
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
	return fqn, ecode, err
}

const (
	checksumRangeSizeThreshold = 4 * cos.MiB // see goi._txrng
)

func (goi *getOI) _ckrange(hrng *htrange) bool {
	// S3 GetObject does not support or define "range checksum"
	if goi.dpq.isS3 || goi.lom.CksumType() == cos.ChecksumNone {
		return false
	}
	return goi.lom.CksumConf().EnableReadRange && (hrng.Start > 0 || hrng.Length < goi.lom.Lsize())
}

func (goi *getOI) _txrng(fqn string, lmfh cos.LomReader, whdr http.Header, hrng *htrange) error {
	var (
		r      io.Reader
		sgl    *memsys.SGL
		cksum  = goi.lom.Checksum()
		size   = hrng.Length
		ra, ok = lmfh.(io.ReaderAt)
	)
	debug.Assertf(ok, "expecting ReaderAt, got (%T)", lmfh) // m.b. fh or UfestReader

	r = io.NewSectionReader(ra, hrng.Start, size)

	if goi._ckrange(hrng) {
		if !goi.lom.IsChunked() && size <= checksumRangeSizeThreshold {
			// non-chunked object & relatively small range -- pagecache
			_, cksumH, err := cos.CopyAndChecksum(io.Discard, r, nil, goi.lom.CksumType())
			if err != nil {
				goi.isIOErr = true
				return err
			}
			cksum = &cksumH.Cksum
			r = io.NewSectionReader(ra, hrng.Start, size)
		} else {
			// large (+ sgl)
			sgl = goi.t.gmm.NewSGL(size)
			_, cksumH, err := cos.CopyAndChecksum(sgl /*as ReaderFrom*/, r, nil, goi.lom.CksumType())
			if err != nil {
				sgl.Free()
				goi.isIOErr = true
				return err
			}
			cksum = &cksumH.Cksum
			r = sgl
		}
	}

	goi.setwhdr(whdr, cksum, size)

	// transmit
	buf, slab := goi.t.gmm.AllocSize(_txsize(size))
	err := goi.transmit(r, buf, fqn, size)
	slab.Free(buf)
	if sgl != nil {
		sgl.Free()
	}
	return err
}

// buffer sizing for range and arch reads (compare w/ txreg)
func _txsize(size int64) int64 {
	if size <= 0 {
		return memsys.PageSize
	}
	if size >= 256*cos.KiB {
		return memsys.MaxPageSlabSize
	}
	return min(size, memsys.DefaultBuf2Size)
}

func (goi *getOI) setwhdr(whdr http.Header, cksum *cos.Cksum, size int64) {
	whdr.Set(cos.HdrContentType, cos.ContentBinary)
	if goi.dpq.isS3 {
		whdr.Set(cos.HdrContentLength, strconv.FormatInt(size, 10))
		s3.SetS3Headers(whdr, goi.lom)
	} else {
		cmn.ToHeader(goi.lom.ObjAttrs(), whdr, size, cksum)
	}
}

// in particular, setup reader and writer and set headers
func (goi *getOI) _txreg(fqn string, lmfh cos.LomReader, whdr http.Header) (err error) {
	// set response header
	size := goi.lom.Lsize()
	goi.setwhdr(whdr, goi.lom.Checksum(), size)

	// Tx
	buf, slab := goi.t.gmm.AllocSize(min(size, memsys.MaxPageSlabSize))
	err = goi.transmit(lmfh, buf, fqn, size)
	slab.Free(buf)
	return err
}

// TODO: checksum
func (goi *getOI) _txarch(fqn string, lmfh cos.LomReader, whdr http.Header) error {
	var (
		dpq = goi.dpq
		lom = goi.lom
	)
	// read single
	if dpq.arch.path != "" {
		csl, err := lom.NewArchpathReader(lmfh, dpq.arch.path, dpq.arch.mime)
		if err != nil {
			return err
		}
		// found
		var (
			size = csl.Size()
		)
		debug.Assert(size >= 0, "negative archive entry size for", lom.Cname(), "/", dpq.arch.path)
		// (compare w/ goi.setwhdr)
		whdr.Set(cos.HdrContentType, cos.ContentBinary)
		whdr.Set(cos.HdrContentLength, strconv.FormatInt(size, 10))

		buf, slab := goi.t.gmm.AllocSize(_txsize(size))
		err = goi.transmit(csl, buf, fqn, size)
		slab.Free(buf)
		csl.Close()
		return err
	}

	// multi match; writing & streaming tar =>(directly)=> response writer
	debug.Assert(dpq.arch.mmode != "")
	mime, err := archive.MimeFile(lmfh, goi.t.smm, dpq.arch.mime, lom.ObjName)
	if err != nil {
		return err
	}

	var ar archive.Reader
	ar, err = archive.NewReader(mime, lmfh, lom.Lsize())
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", lom.Cname(), err)
	}

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

func (goi *getOI) transmit(r io.Reader, buf []byte, fqn string, size int64) error {
	var (
		errTx error
	)
	written, err := cos.CopyBuffer(goi.w, r, buf)
	if err != nil || written != size {
		errTx = goi._txerr(err, fqn /*lbget*/, written, size)
	}
	if errTx != nil {
		return errTx
	}

	//
	// stats
	//
	goi.stats(written)
	return nil
}

func (goi *getOI) _txerr(err error, fqn string, written, size int64) error {
	const act = "(transmit)"
	lom := goi.lom
	cname := lom.Cname()

	// enforce transmit size
	if err == nil && written != size {
		// [corruption?]
		goi.isIOErr = true
		lom.UncacheDel()
		errTx := &errGetTxSevere{
			msg: fmt.Sprintf("%s %s: invalid size %d != %d", act, cname, written, size),
		}
		nlog.WarningDepth(1, err)
		return errTx
	}

	// [failure to transmit] return cmn.ErrGetTxBenign
	switch {
	case cos.IsRetriableConnErr(err):
		if cmn.Rom.V(5, cos.ModAIS) {
			nlog.WarningDepth(1, act, cname, "err:", err)
		}
	case cmn.IsErrObjNought(err):
		lom.UncacheDel()
		if cmn.Rom.V(4, cos.ModAIS) {
			nlog.WarningDepth(1, act, cname, "err:", err)
		}
	default: // notwithstanding
		goi.t.FSHC(err, lom.Mountpath(), fqn)
		lom.UncacheDel()
		nlog.ErrorDepth(1, act, cname, "err:", err)
	}

	return cmn.ErrGetTxBenign
}

func (goi *getOI) stats(written int64) {
	var (
		bck   = goi.lom.Bck()
		delta = mono.SinceNano(goi.ltime)
		vlabs = bvlabs(bck)
	)
	goi.t.statsT.IncWith(stats.GetCount, vlabs)
	goi.t.statsT.AddWith(
		cos.NamedVal64{Name: stats.GetSize, Value: written, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.GetThroughput, Value: written, VarLabs: vlabs}, // vis-à-vis user (as written m.b. range)
		cos.NamedVal64{Name: stats.GetLatency, Value: delta, VarLabs: vlabs},      // see also: per-backend *LatencyTotal below
		cos.NamedVal64{Name: stats.GetLatencyTotal, Value: delta, VarLabs: vlabs}, // ditto
	)

	if !goi.rget {
		debug.Assert(!goi.verchanged)
		return
	}

	// always provide non-empty vlabs for backend stats
	cname := bck.Cname("")
	if vlabs[stats.VlabBucket] == "" { // i.e. stats.EmptyBckVlabs
		vlabs = map[string]string{stats.VlabBucket: cname}
	}
	backend := goi.t.Backend(bck)
	goi.t.rgetstats(backend, cname, "" /*xkind*/, written, delta)

	if !goi.verchanged {
		return
	}

	goi.t.statsT.IncWith(stats.VerChangeCount, vlabs)
	goi.t.statsT.AddWith(
		cos.NamedVal64{Name: stats.VerChangeSize, Value: goi.lom.Lsize(), VarLabs: vlabs},
	)

	goi.t.statsT.IncWith(backend.MetricName(stats.VerChangeCount), vlabs)
	goi.t.statsT.AddWith(
		cos.NamedVal64{Name: backend.MetricName(stats.VerChangeSize), Value: goi.lom.Lsize(), VarLabs: vlabs},
	)
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
// errGetTx* (benign and severe)
//

type (
	errGetTxSevere struct {
		msg string
	}
)

func (e *errGetTxSevere) Error() string { return e.msg }

func isErrGetTxSevere(err error) bool {
	_, ok := err.(*errGetTxSevere)
	return ok
}

func newErrGetTxSevere(err error, lom *core.LOM, tag string) error {
	return &errGetTxSevere{fmt.Sprintf("failed to finalize GET response: %s %s [%v]", tag, lom.Cname(), err)}
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
		cksumType = strings.ToLower(cksumType)
		a.cksum = cos.NewCksum(cksumType, cksumValue)
		if err := a.cksum.Validate(); err != nil {
			return "", 0, err
		}
	}

	switch a.op {
	case apc.AppendOp:
		buf, slab := a.t.gmm.Alloc()
		packedHdl, err = a.apnd(buf)
		slab.Free(buf)
	case apc.FlushOp:
		ecode, err = a.flush()
	default:
		err = fmt.Errorf("invalid operation %q (expecting either %q or %q) - check %q query",
			a.op, apc.AppendOp, apc.FlushOp, apc.QparamAppendType)
	}

	return packedHdl, ecode, err
}

func (a *apndOI) apnd(buf []byte) (packedHdl string, err error) {
	var (
		fh      cos.LomWriter
		workFQN = a.hdl.workFQN
	)
	if workFQN == "" {
		workFQN = a.lom.GenFQN(fs.WorkCT, fs.WorkfileAppend)
		a.lom.Lock(false)
		if a.lom.Load(false /*cache it*/, false /*locked*/) == nil {
			_, a.hdl.partialCksum, err = cos.CopyFile(a.lom.FQN, workFQN, buf, a.lom.CksumType())
			a.lom.Unlock(false)
			if err != nil {
				return "", err
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
		return "", err
	}

	w := cos.NewWriterMulti(fh, a.hdl.partialCksum.H)
	_, err = cos.CopyBuffer(w, a.r, buf)
	cos.Close(fh)
	if err != nil {
		return "", err
	}

	packedHdl = a.pack(workFQN)

	// stats (TODO: add `stats.FlushCount` for symmetry)
	lat := time.Now().UnixNano() - a.started
	vlabs := map[string]string{stats.VlabBucket: a.lom.Bck().Cname("")}
	a.t.statsT.IncWith(stats.AppendCount, vlabs)
	a.t.statsT.AddWith(
		cos.NamedVal64{Name: stats.AppendLatency, Value: lat, VarLabs: vlabs},
	)
	if cmn.Rom.V(4, cos.ModAIS) {
		nlog.Infoln("APPEND", a.lom.String(), time.Duration(lat))
	}
	return packedHdl, nil
}

func (a *apndOI) flush() (int, error) {
	if a.hdl.workFQN == "" {
		return 0, fmt.Errorf("failed to finalize append-file operation: empty source in the %+v handle", a.hdl)
	}

	// finalize checksum
	debug.Assert(a.hdl.partialCksum != nil)
	a.hdl.partialCksum.Finalize()
	partialCksum := a.hdl.partialCksum.Clone()
	if !cos.NoneC(a.cksum) && !partialCksum.Equal(a.cksum) {
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
func (coi *coi) do(t *target, dm *bundle.DM, lom *core.LOM) (res xs.CoiRes) {
	if coi.ETLArgs == nil {
		coi.ETLArgs = &core.ETLArgs{}
	}

	if coi.DryRun {
		return coi._dryRun(lom, coi.ObjnameTo, coi.ETLArgs)
	}

	// (no-op transform) and (remote source) => same flow as actual transform but with default reader
	if coi.GetROC == nil && lom.Bck().IsRemote() {
		coi.GetROC = core.GetDefaultROC
	}

	// 1: dst location
	smap := t.owner.smap.Get()
	uname := coi.BckTo.MakeUname(coi.ObjnameTo)
	tsi, err := smap.HrwName2T(uname)
	if err != nil {
		return xs.CoiRes{Err: err}
	}

	// use escaped URL to simplify parsing on the ETL side)
	daddr, err := url.Parse(cos.JoinPath(tsi.URL(cmn.NetIntraData), url.PathEscape(cos.UnsafeS(uname))))
	if err != nil {
		return xs.CoiRes{Err: err}
	}
	if coi.Xact != nil {
		// include xid and owt for statistics on direct put
		q := daddr.Query()
		q.Set(apc.QparamUUID, coi.Xact.ID())
		q.Set(apc.QparamOWT, coi.OWT.ToS())
		daddr.RawQuery = q.Encode()
	}

	if tsi.ID() != t.SID() {
		if coi.ETLArgs.Pipeline == nil {
			coi.ETLArgs.Pipeline = make(apc.ETLPipeline, 0, 1)
		}
		coi.ETLArgs.Pipeline.Join(daddr.String()) // attach direct put destination target to the pipeline
		var r cos.ReadOpenCloser
		if coi.PutWOC != nil {
			_, ecode, err := coi.PutWOC(lom, coi.LatestVer, coi.Sync, nil, coi.ETLArgs)
			return xs.CoiRes{Err: err, Ecode: ecode}
		} else if coi.GetROC != nil {
			resp := coi.GetROC(lom, coi.LatestVer, coi.Sync, coi.ETLArgs)
			// skip t2t send if encounter error during GetROC, (etl direct put will return ErrSkip in this case)
			if resp.Err != nil {
				return xs.CoiRes{Err: resp.Err, Ecode: resp.Ecode}
			}
			coi.OAH = resp.OAH
			r = resp.R
		}
		return coi.send(t, dm, lom, r, tsi) // lom is the source of reader if no reader specified
	}

	// dst is this target
	// 2, 3: with transformation and without
	dst := core.AllocLOM(coi.ObjnameTo)
	defer core.FreeLOM(dst)

	if err := dst.InitBck(coi.BckTo.Bucket()); err != nil {
		return xs.CoiRes{Err: err}
	}
	dstMaxMonoSize := dst.Bprops().Chunks.MaxMonolithicSize

	switch {
	// no-op
	case coi.isNOP(lom, dst, dm):
		if cmn.Rom.V(5, cos.ModAIS) {
			nlog.Infoln("copying", lom.String(), "=>", dst.String(), "is a no-op: destination exists and is identical")
		}
	case coi.PutWOC != nil: // take precedence over GetROC, if any
		res = coi._writer(t, lom, dst, coi.ETLArgs)
		if res.Ecode == http.StatusNotFound && !cos.IsNotExist(err) {
			res.Err = cos.NewErrNotFound(t, res.Err.Error())
		}
	case coi.GetROC != nil:
		res = coi._reader(t, dm, lom, dst, coi.ETLArgs)
		if res.Ecode == http.StatusNotFound && !cos.IsNotExist(err) {
			// to keep not-found
			res.Err = cos.NewErrNotFound(t, res.Err.Error())
		}
	case lom.FQN == dst.FQN:
		if cmn.Rom.V(5, cos.ModAIS) {
			nlog.Infoln("copying", lom.String(), "=>", dst.String(), "is a no-op (resilvering with a single mountpath?)")
		}
	case lom.Bprops().Chunks.MaxMonolithicSize != dstMaxMonoSize && lom.Lsize() > int64(dstMaxMonoSize):
		// source and destination buckets have different chunks config => rechunk if the source exceeds the destination's limit
		res = coi._chunk(t, lom, dst, int64(dst.Bprops().Chunks.ChunkSize))
	default:
		// fast path: destination is _this_ target
		// (note coi.send(=> another target) above)
		lcopy := lom.Uname() == dst.Uname() // n-way copy
		lom.Lock(lcopy)
		res = coi._regular(t, lom, dst, lcopy)
		lom.Unlock(lcopy)
	}

	return res
}

func (coi *coi) isNOP(lom, dst *core.LOM, dm *bundle.DM) bool {
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

func (coi *coi) _dryRun(lom *core.LOM, objnameTo string, args *core.ETLArgs) (res xs.CoiRes) {
	if coi.GetROC == nil {
		uname := coi.BckTo.MakeUname(objnameTo)
		if lom.Uname() != cos.UnsafeS(uname) {
			res.Lsize = lom.Lsize(true)
		}
		return res
	}

	resp := coi.GetROC(lom, false /*latestVer*/, false /*sync*/, args)
	if resp.Err != nil {
		return xs.CoiRes{Err: resp.Err}
	}
	// discard the reader and be done
	size, err := io.Copy(io.Discard, resp.R)
	resp.R.Close()
	return xs.CoiRes{Lsize: size, Err: err}
}

func (coi *coi) _writer(t *target, lom, dst *core.LOM, args *core.ETLArgs) (res xs.CoiRes) {
	workFQN := dst.GenFQN(fs.WorkCT, fs.WorkfileTransform)
	lomWriter, err := dst.CreateWork(workFQN) // closed in the `coi.PutWOC` call
	if err != nil {
		return xs.CoiRes{Err: err}
	}
	size, ecode, err := coi.PutWOC(lom, coi.LatestVer, coi.Sync, lomWriter, args)
	if err != nil {
		cos.RemoveFile(workFQN)
		return xs.CoiRes{Err: err, Ecode: ecode}
	}

	// finalize
	dst.SetSize(size)
	ecode, err = t.FinalizeObj(dst, workFQN, coi.Xact, coi.OWT)
	if err != nil {
		cos.RemoveFile(workFQN)
		return xs.CoiRes{Err: err, Ecode: ecode}
	}
	res.Lsize = size
	return res
}

// PUT lom => dst
// NOTE: no assumpions are being made on whether the source lom is present in cluster.
// (can be a "pure" metadata of a (non-existing) Cloud object; accordingly, GetROC must
// be able to handle cold get, warm get, etc.)
//
// If destination bucket is remote:
// - create a local replica of the object on one of the targets, and
// - putRemote (with one exception below)
//
// An option for _not_ storing the object _in_ the cluster would be a _feature_ that can be
// further debated.
func (coi *coi) _reader(t *target, dm *bundle.DM, lom, dst *core.LOM, args *core.ETLArgs) (res xs.CoiRes) {
	debug.Assertf(coi.GetROC != nil, "coi.GetROC is nil in _reader, object name: %s", coi.ObjnameTo)
	resp := coi.GetROC(lom, coi.LatestVer, coi.Sync, args)
	if resp.Err != nil {
		return xs.CoiRes{Ecode: resp.Ecode, Err: resp.Err}
	}
	poi := allocPOI()
	defer freePOI(poi)
	{
		poi.t = t
		poi.lom = dst
		poi.config = coi.Config
		poi.r = resp.R
		poi.xctn = coi.Xact // on behalf of
		poi.workFQN = dst.GenFQN(fs.WorkCT, "copy-dp")
		poi.atime = resp.OAH.AtimeUnix()
		poi.cksumToUse = resp.OAH.Checksum()

		poi.owt = coi.OWT
		if dm != nil {
			poi.owt = dm.OWT() // (precedence; cmn.OwtCopy, cmn.OwtTransform - what else?)
		}
	}
	if poi.owt == cmn.OwtCopy {
		// preserve src metadata when copying (vs. transforming)
		dst.CopyVersion(lom)
		dst.SetCustomMD(lom.GetCustomMD())
	}

	ecode, err := poi.putObject()
	if err != nil {
		return xs.CoiRes{Ecode: ecode, Err: err}
	}
	res.Lsize = poi.lom.Lsize()

	return res
}

func (coi *coi) _regular(t *target, lom, dst *core.LOM, lcopy bool) (res xs.CoiRes) {
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		if !cos.IsNotExist(err) {
			err = cmn.NewErrFailedTo(t, "coi-load", lom.Cname(), err)
		}
		return xs.CoiRes{Err: err}
	}

	// w-lock the destination unless already locked (above)
	if !lcopy {
		dst.Lock(true)
		defer dst.Unlock(true)
		if err := dst.Load(false /*cache it*/, true /*locked*/); err == nil {
			if lom.EqCksum(dst.Checksum()) {
				return xs.CoiRes{}
			}
		} else if cmn.IsErrBucketNought(err) {
			return xs.CoiRes{Err: err}
		}
	}

	// TODO: add a metric to count and size local copying
	dst2, err := lom.Copy2FQN(dst.FQN, coi.Buf)
	if res.Err = err; res.Err == nil {
		res.Lsize = lom.Lsize()
		if coi.Finalize {
			t.putMirror(dst2)
		}
	}
	if dst2 != nil {
		core.FreeLOM(dst2)
	}
	return res
}

func (coi *coi) _chunk(t *target, lom, dst *core.LOM, dstChunkSize int64) (res xs.CoiRes) {
	resp := lom.GetROC(coi.LatestVer, coi.Sync)
	if resp.Err != nil {
		return xs.CoiRes{Ecode: resp.Ecode, Err: resp.Err}
	}
	poi := allocPOI()
	defer freePOI(poi)
	{
		poi.t = t
		poi.lom = dst
		poi.r = resp.R
		poi.size = lom.Lsize()
		poi.xctn = coi.Xact // on behalf of
		poi.owt = coi.OWT
		poi.config = coi.Config
	}
	ecode, err := poi.chunk(dstChunkSize)
	if err != nil {
		return xs.CoiRes{Ecode: ecode, Err: err}
	}
	res.Lsize = poi.lom.Lsize()

	return res
}

// send object => designated target
// * source is a LOM or a reader (that may be reading from remote)
// * one of the two equivalent transmission mechanisms: PUT or transport Send
func (coi *coi) send(t *target, dm *bundle.DM, lom *core.LOM, reader cos.ReadOpenCloser, tsi *meta.Snode) (res xs.CoiRes) {
	debug.Assert(coi.OWT > 0)
	sargs := allocSnda()
	{
		sargs.objNameTo = coi.ObjnameTo
		sargs.bckTo = coi.BckTo
		sargs.reader = reader
		sargs.objAttrs = coi.OAH
		sargs.tsi = tsi
		sargs.dm = dm

		sargs.owt = coi.OWT
		if dm != nil {
			sargs.owt = dm.OWT() // (precedence; cmn.OwtCopy, cmn.OwtTransform - what else?)
		}
	}
	res = coi._send(t, lom, sargs)
	freeSnda(sargs)
	return res
}

func (coi *coi) _send(t *target, lom *core.LOM, sargs *sendArgs) (res xs.CoiRes) {
	if sargs.dm != nil {
		// clone the `lom` to use it in the async operation (free it via `_sendObjDM` callback)
		lom = lom.Clone()
	}

	if sargs.reader == nil {
		// migrate/replicate in-cluster lom
		lom.Lock(false)
		if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
			lom.Unlock(false)
			return xs.CoiRes{Err: err}
		}
		reader, err := lom.NewDeferROC(true /*loaded*/)
		if err != nil {
			return xs.CoiRes{Err: err}
		}
		res.Lsize = lom.Lsize()
		sargs.reader, sargs.objAttrs = reader, lom
	}

	// do
	if sargs.dm != nil {
		res.Err = coi._dm(lom /*for attrs*/, sargs)
	} else {
		res.Err = coi.put(t, sargs)
	}
	return res
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
		size  = sargs.objAttrs.Lsize(true)
	)
	cmn.ToHeader(sargs.objAttrs, hdr, size)
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
	req, _, cancel, errN := reqArgs.ReqWith(coi.Config.Timeout.SendFile.D())
	if errN != nil {
		cos.Close(sargs.reader)
		return fmt.Errorf("unexpected failure to create request, err: %w", errN)
	}

	resp, err := g.client.data.Do(req)
	if err != nil {
		err = cmn.NewErrFailedTo(t, "coi.put "+sargs.bckTo.Cname(sargs.objNameTo), sargs.tsi, err)
	} else {
		cos.DrainReader(resp.Body)
		resp.Body.Close()
	}

	cmn.HreqFree(req)
	cancel()
	return err
}

//
// PUT a new shard _or_ APPEND to an existing one (w/ read/write/list via cmn/archive)
//

// TODO -- FIXME: add support for chunks (lom.Open below)
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
			workFQN   = a.lom.GenFQN(fs.WorkCT, fs.WorkfileAppendToArch)
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
		err, erc error
		wfh      *os.File
		lmfh     cos.LomReader
		workFQN  string
		cksum    cos.CksumHashSize
		aw       archive.Writer
	)
	workFQN = a.lom.GenFQN(fs.WorkCT, fs.WorkfileAppendToArch)
	wfh, err = os.OpenFile(workFQN, os.O_CREATE|os.O_WRONLY, cos.PermRWR)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	// currently, arch writers only use size and time but it may change
	oah := cos.SimpleOAH{Size: a.size, Atime: a.started}
	if a.put {
		// when append becomes PUT (TODO: checksum type)
		cksum.Init(cos.ChecksumCesXxh)
		aw = archive.NewWriter(a.mime, wfh, &cksum, nil /*opts*/)
		err = aw.Write(a.filename, oah, a.r)
		erc = aw.Fini()
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
		erc = aw.Fini() // in that order
		cos.Close(lmfh)
	}

	// finalize
	cos.Close(wfh)
	if err == nil {
		err = erc
	}
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
	ecode := cos.Ternary(cmn.IsErrCapExceeded(err), http.StatusInsufficientStorage, http.StatusInternalServerError)
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
// put mirror (main)
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

func (goi *getOI) uplock(c *cmn.Config) (u *_uplock) {
	u = &_uplock{sleep: cmn.ColdGetConflictMin}

	// jitter
	j := (goi.ltime & 0x7) - 3
	jitter := time.Millisecond * time.Duration(j<<1)
	u.sleep += jitter

	u.timeout = cos.NonZero(c.Timeout.ColdGetConflict.D(), cmn.ColdGetConflictDflt)
	return u
}

func (u *_uplock) do(lom *core.LOM) error {
	if u.elapsed > u.timeout {
		err := cmn.NewErrBusy("node", core.T.String(), uplockWarn+" '"+lom.Cname()+"'")
		nlog.ErrorDepth(1, err)
		return err
	}

	lom.Unlock(false)
	time.Sleep(u.sleep)
	lom.Lock(false) // all over again: try load and check all respective conditions

	u.elapsed += u.sleep
	if u.sleep < u.timeout>>1 {
		u.sleep += u.sleep >> 1
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
	goiPool = sync.Pool{New: func() any { return new(getOI) }}
	poiPool = sync.Pool{New: func() any { return new(putOI) }}
	sndPool = sync.Pool{New: func() any { return new(sendArgs) }}

	goi0 getOI
	poi0 putOI
	snd0 sendArgs
)

func allocGOI() *getOI {
	return goiPool.Get().(*getOI)
}

func freeGOI(a *getOI) {
	*a = goi0
	goiPool.Put(a)
}

func allocPOI() *putOI {
	return poiPool.Get().(*putOI)
}

func freePOI(a *putOI) {
	*a = poi0
	poiPool.Put(a)
}

func allocSnda() *sendArgs {
	return sndPool.Get().(*sendArgs)
}

func freeSnda(a *sendArgs) {
	*a = snd0
	sndPool.Put(a)
}
