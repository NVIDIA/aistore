// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/stats"
	"github.com/OneOfOne/xxhash"
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
		// Custom version that should be set after object is successfully put.
		version string
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
		// tag from object name, from s3://bucket/object!tf
		tag string
		// Offset of the object from where the reading should start.
		offset int64
		// Length determines how many bytes should be read from the file,
		// starting from provided offset.
		length int64
		// Determines if it is GFN request
		isGFN bool
		// true: chunked transfer (en)coding as per https://tools.ietf.org/html/rfc7230#page-36
		chunked bool
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
		op       string
		filePath string
	}

	writerOnly struct {
		io.Writer
	}
)

////////////////
// PUT OBJECT //
////////////////

func (poi *putObjInfo) putObject() (err error, errCode int) {
	lom := poi.lom
	// optimize out if the checksums do match
	if poi.cksumToCheck != nil {
		if cmn.EqCksum(lom.Cksum(), poi.cksumToCheck) {
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
			poi.t.fshc(err, poi.workFQN)
			if err = cmn.RemoveFile(poi.workFQN); err != nil {
				glog.Errorf("Nested error: %s => (remove %s => err: %v)", err1, poi.workFQN, err)
			}
		}
		poi.lom.Uncache()
		return
	}
	if ecErr := ec.ECM.EncodeObject(poi.lom); ecErr != nil && ecErr != ec.ErrorECDisabled {
		err = ecErr
		return
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
		cmn.Assert(lom.Cksum() != nil)
		var version string
		if bck.IsCloud() {
			version, err, errCode = poi.putCloud()
		} else {
			version, err, errCode = poi.putRemoteAIS()
		}
		if err != nil {
			err = fmt.Errorf("%s: PUT failed, err: %v", lom, err)
			return
		}
		if lom.VerConf().Enabled {
			poi.version = version
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

	if poi.version != "" {
		// TODO: currently we set the version to opaque string. It can possibly
		//  break the default incrementation if the opaque string is not an
		//  number. See: #722. To fix this we should probably maintain different
		//  versions or the origin of the object.
		lom.SetVersion(poi.version)
	} else if bck.IsAIS() && lom.VerConf().Enabled && !poi.migrated {
		if err = lom.IncVersion(); err != nil {
			return
		}
	}

	if err := cmn.Rename(poi.workFQN, lom.FQN); err != nil {
		return fmt.Errorf("rename failed => %s: %w", lom, err), 0
	}

	if err = lom.DelAllCopies(); err != nil {
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
	ver, err, errCode = poi.t.Cloud(bck).PutObj(poi.ctx, file, lom)
	file.Close()
	return
}

func (poi *putObjInfo) putRemoteAIS() (ver string, err error, errCode int) {
	var (
		lom = poi.lom
		bck = lom.Bck()
	)
	cmn.Assert(bck.IsRemoteAIS())
	fh, errOpen := cmn.NewFileHandle(poi.workFQN)
	if errOpen != nil {
		err = fmt.Errorf("failed to open %s err: %w", poi.workFQN, errOpen)
		return
	}
	ver, err, errCode = poi.t.Cloud(bck).PutObj(poi.ctx, fh, lom)
	fh.Close()
	return
}

// NOTE: LOM is updated on the end of the call with proper size and checksum.
// NOTE: `roi.r` is closed on the end of the call.
func (poi *putObjInfo) writeToFile() (err error) {
	var (
		file   *os.File
		buf    []byte
		slab   *memsys.Slab
		reader = poi.r
	)
	if daemon.dryRun.disk {
		return
	}
	if file, err = poi.lom.CreateFile(poi.workFQN); err != nil {
		return
	}

	if poi.size == 0 {
		buf, slab = daemon.gmm.Alloc()
	} else {
		buf, slab = daemon.gmm.Alloc(poi.size)
	}
	defer func() { // free & cleanup on err
		slab.Free(buf)
		reader.Close()

		if err != nil {
			if nestedErr := file.Close(); nestedErr != nil {
				glog.Errorf("Nested (%v): failed to close received object %s, err: %v", err, poi.workFQN, nestedErr)
			}
			if nestedErr := cmn.RemoveFile(poi.workFQN); nestedErr != nil {
				glog.Errorf("Nested (%v): failed to remove %s, err: %v", err, poi.workFQN, nestedErr)
			}
		}
	}()

	// receive and checksum
	var (
		written int64

		checkCksumType      string
		expectedCksum       *cmn.Cksum
		saveHash, checkHash hash.Hash
		hashes              []hash.Hash
	)

	poiCkConf := poi.lom.CksumConf()
	if !poi.cold && poiCkConf.Type != cmn.ChecksumNone {
		checkCksumType = poiCkConf.Type
		cmn.AssertMsg(checkCksumType == cmn.ChecksumXXHash, checkCksumType)

		if !poi.migrated || poiCkConf.ValidateObjMove {
			saveHash = xxhash.New64()
			hashes = []hash.Hash{saveHash}

			// if sender provided checksum we need to ensure that it is correct
			if expectedCksum = poi.cksumToCheck; expectedCksum != nil {
				checkHash = saveHash
			}
		} else {
			// if migration validation is not configured we can just take
			// the checksum that has arrived with the object (and compute it if not present)
			poi.lom.SetCksum(poi.cksumToCheck)
			if poi.cksumToCheck == nil {
				saveHash = xxhash.New64()
				hashes = []hash.Hash{saveHash}
			}
		}
	} else if poi.cold {
		// compute xxhash (the default checksum) and save it as part of the object metadata
		saveHash = xxhash.New64()
		hashes = []hash.Hash{saveHash}

		// if validate-cold-get and the cksum is provided we should also check md5 hash (aws, gcp)
		if poiCkConf.ValidateColdGet && poi.cksumToCheck != nil {
			expectedCksum = poi.cksumToCheck
			checkCksumType, _ = expectedCksum.Get()
			cmn.AssertMsg(checkCksumType == cmn.ChecksumMD5 || checkCksumType == cmn.ChecksumCRC32C, checkCksumType)

			checkHash = md5.New()
			if checkCksumType == cmn.ChecksumCRC32C {
				checkHash = cmn.NewCRC32C()
			}

			hashes = append(hashes, checkHash)
		}
	}

	if written, err = cmn.ReceiveAndChecksum(file, reader, buf, hashes...); err != nil {
		return
	}

	if checkHash != nil {
		computedCksum := cmn.NewCksum(checkCksumType, cmn.HashToStr(checkHash))
		if !cmn.EqCksum(expectedCksum, computedCksum) {
			err = cmn.NewBadDataCksumError(expectedCksum, computedCksum, poi.lom.String())
			poi.t.statsT.AddMany(
				stats.NamedVal64{Name: stats.ErrCksumCount, Value: 1},
				stats.NamedVal64{Name: stats.ErrCksumSize, Value: written},
			)
			return
		}
	}
	poi.lom.SetSize(written)
	if saveHash != nil {
		poi.lom.SetCksum(cmn.NewCksum(cmn.ChecksumXXHash, cmn.HashToStr(saveHash)))
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
		doubleCheck, retry, retried, coldGet, capRead bool
		capInfo                                       cmn.CapacityInfo
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
		capRead = true // set flag to avoid reading capUsed extra time later
		capInfo = goi.t.AvgCapUsed(goi.lom.Config())
		if capInfo.OOS {
			// immediate return for no space left to restore object
			goi.lom.Unlock(false)
			return capInfo.Err, http.StatusInternalServerError
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
		err = goi.lom.ValidateMetaChecksum()
		if err == nil {
			err = goi.lom.ValidateContentChecksum()
		}
		if err != nil {
			glog.Error(err)
			if _, ok := err.(*cmn.BadCksumError); ok {
				if goi.lom.Bck().IsAIS() {
					// TODO: recover from copies or EC if available (scruber).
					// Removing object and copies at this point seems too harsh.
					if err := goi.lom.Remove(); err != nil {
						glog.Warningf("%s - failed to remove, err: %v", err, err)
					}
					goi.lom.Unlock(false)
					return err, http.StatusInternalServerError
				}
				coldGet = true
			} else {
				goi.lom.Unlock(false)
				return err, http.StatusInternalServerError
			}
		}
	}

	// 3. coldget
	if coldGet {
		goi.lom.Unlock(false) // `GetCold` will lock again and return with object locked
		if !capRead {
			capRead = true
			capInfo = goi.t.AvgCapUsed(goi.lom.Config())
		}
		if capInfo.OOS {
			// no space left to prefetch object
			return capInfo.Err, http.StatusBadRequest
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

// an attempt to restore an object that is missing in the ais bucket - from:
// 1) local FS
// 2) other FSes or targets when resilvering (rebalancing) is running (aka GFN)
// 3) other targets if the bucket erasure coded
// 4) Cloud
func (goi *getObjInfo) tryRestoreObject() (doubleCheck bool, err error, errCode int) {
	var (
		tsi, gfnNode     *cluster.Snode
		smap             = goi.t.owner.smap.get()
		tname            = goi.t.si.String()
		aborted, running = reb.IsRebalancing(cmn.ActResilver)
		gfnActive        = goi.t.gfn.local.active()
		ecEnabled        = goi.lom.Bprops().EC.Enabled
	)
	tsi, err = cluster.HrwTarget(goi.lom.Uname(), &smap.Smap)
	if err != nil {
		return
	}
	if aborted || running || gfnActive {
		if goi.lom.RestoreObjectFromAny() { // get-from-neighbor local (mountpaths) variety
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s restored", goi.lom)
			}
			return
		}
		doubleCheck = running
	}

	// FIXME: if there're not enough EC targets to restore an sliced object,
	// we might be able to restore it if it was replicated. In this case even
	// just one additional target might be sufficient. This won't succeed if
	// an object was sliced, neither will ecmanager.RestoreObject(lom)
	enoughECRestoreTargets := goi.lom.Bprops().EC.RequiredRestoreTargets() <= goi.t.owner.smap.Get().CountTargets()

	// cluster-wide lookup ("get from neighbor")
	aborted, running = reb.IsRebalancing(cmn.ActRebalance)
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
	if running || !enoughECRestoreTargets || ((aborted || gfnActive) && !ecEnabled) {
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
		atime      int64
		cksumValue = resp.Header.Get(cmn.HeaderObjCksumVal)
		cksumType  = resp.Header.Get(cmn.HeaderObjCksumType)
		cksum      = cmn.NewCksum(cksumType, cksumValue)
		version    = resp.Header.Get(cmn.HeaderObjVersion)
		workFQN    = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfileRemote)
	)
	if atime, err = cmn.S2UnixNano(resp.Header.Get(cmn.HeaderObjAtime)); err != nil {
		glog.Error(err)
		return
	}
	lom.SetAtimeUnix(atime)
	lom.SetCksum(cksum)
	poi := &putObjInfo{
		t:        goi.t,
		lom:      lom,
		r:        resp.Body,
		version:  version,
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
			file.Close()
		}
		if buf != nil {
			slab.Free(buf)
		}
		if sgl != nil {
			sgl.Free()
		}
	}()

	cksumConf := goi.lom.CksumConf()
	cksumRange := cksumConf.Type != cmn.ChecksumNone && goi.length > 0 && cksumConf.EnableReadRange

	if rw, ok := goi.w.(http.ResponseWriter); ok && goi.tag == "" {
		hdr = rw.Header()
		if goi.lom.Cksum() != nil && !cksumRange {
			cksumType, cksumValue := goi.lom.Cksum().Get()
			hdr.Set(cmn.HeaderObjCksumType, cksumType)
			hdr.Set(cmn.HeaderObjCksumVal, cksumValue)
		}
		if goi.lom.Version() != "" {
			hdr.Set(cmn.HeaderObjVersion, goi.lom.Version())
		}
		hdr.Set(cmn.HeaderObjSize, strconv.FormatInt(goi.lom.Size(), 10))
		hdr.Set(cmn.HeaderObjAtime, cmn.UnixNano2S(goi.lom.AtimeUnix()))
	}

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

	fqn := goi.lom.FQN
	if !coldGet && !goi.isGFN {
		// best-effort GET load balancing (see also mirror.findLeastUtilized())
		// (TODO: check whether the timestamp is not too old)
		fqn = goi.lom.LoadBalanceGET(goi.started)
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

	w := goi.w
	if goi.tag == "" {
		if goi.length == 0 {
			reader = file
			if goi.chunked {
				w = writerOnly{goi.w} // hide ReadFrom; CopyBuffer will use the buffer instead
				buf, slab = daemon.gmm.Alloc(goi.lom.Size())
			} else {
				hdr.Set("Content-Length", strconv.FormatInt(goi.lom.Size(), 10))
			}
		} else {
			buf, slab = daemon.gmm.Alloc(goi.length)
			reader = io.NewSectionReader(file, goi.offset, goi.length)
			if cksumRange {
				var cksumValue string
				sgl = slab.MMSA().NewSGL(goi.length, slab.Size())
				if _, cksumValue, err = cmn.WriteWithHash(sgl, reader, buf, cksumConf.Type); err != nil {
					return
				}
				hdr.Set(cmn.HeaderObjCksumVal, cksumValue)
				hdr.Set(cmn.HeaderObjCksumType, cksumConf.Type)
				reader = io.NewSectionReader(file, goi.offset, goi.length)
			}
		}
		written, err = io.CopyBuffer(w, reader, buf)
	} else {
		written, err = transformTarToTFRecord(goi)
	}

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

func (aoi *appendObjInfo) appendObject() (filePath string, err error, errCode int) {
	filePath = aoi.filePath
	switch aoi.op {
	case cmn.AppendOp:
		var f *os.File
		if filePath == "" {
			filePath = fs.CSM.GenContentParsedFQN(aoi.lom.ParsedFQN, fs.WorkfileType, fs.WorkfileAppend)
			f, err = aoi.lom.CreateFile(filePath)
			if err != nil {
				return "", err, http.StatusInternalServerError
			}
		} else {
			f, err = os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				return "", err, http.StatusInternalServerError
			}
		}

		// append
		var (
			buf  []byte
			slab *memsys.Slab
		)
		if aoi.size == 0 {
			buf, slab = daemon.gmm.Alloc()
		} else {
			buf, slab = daemon.gmm.Alloc(aoi.size)
		}
		_, err = io.CopyBuffer(f, aoi.r, buf)

		// cleanup
		slab.Free(buf)
		f.Close()
		if err != nil {
			return "", err, http.StatusInternalServerError
		}
	case cmn.FlushOp:
		if filePath == "" {
			err = errors.New("handle not provided")
			return "", err, http.StatusBadRequest
		}

		if err := aoi.t.PromoteFile(filePath, aoi.lom.Bck(), aoi.lom.ObjName, true /*overwrite*/, false /*safe*/, false /*verbose*/); err != nil {
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
