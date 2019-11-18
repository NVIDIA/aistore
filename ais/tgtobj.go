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
	"io/ioutil"
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
		// object size aka Content-Length
		size int64
		// Context used when putting the object which should be contained in
		// cloud bucket. It usually contains credentials to access the cloud.
		ctx context.Context
		// FQN which is used only temporarily for receiving file. After
		// successful receive is renamed to actual FQN.
		workFQN string
		// Determines if the object was already in cluster and was received
		// because some kind of migration.
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
			io.Copy(ioutil.Discard, poi.r) // drain the reader
			return nil, 0
		}
	}

	if !dryRun.disk {
		if err := poi.writeToFile(); err != nil {
			return err, http.StatusInternalServerError
		}

		if err, errCode := poi.finalize(); err != nil {
			return err, errCode
		}
	}

	delta := time.Since(poi.started)
	poi.t.statsif.AddMany(
		stats.NamedVal64{Name: stats.PutCount, Value: 1},
		stats.NamedVal64{Name: stats.PutLatency, Value: int64(delta)},
	)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("PUT %s: %d µs", lom, int64(delta/time.Microsecond))
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
	if ecErr := poi.t.ecmanager.EncodeObject(poi.lom); ecErr != nil && ecErr != ec.ErrorECDisabled {
		err = ecErr
		return
	}

	poi.t.putMirror(poi.lom)
	return
}

// poi.workFQN => LOM
func (poi *putObjInfo) tryFinalize() (err error, errCode int) {
	var (
		ver string
		lom = poi.lom
	)
	if !lom.IsAIS() && !poi.migrated {
		file, err1 := os.Open(poi.workFQN)
		if err1 != nil {
			err = fmt.Errorf("failed to open %s err: %v", poi.workFQN, err1)
			return
		}
		cmn.Assert(lom.Cksum() != nil)
		ver, err, errCode = poi.t.cloud.putObj(poi.ctx, file, lom)
		file.Close()
		if err != nil {
			err = fmt.Errorf("%s: PUT failed, err: %v", lom, err)
			return
		}
		lom.SetVersion(ver)
	}

	// check if bucket was destroyed while PUT was in the progress.
	// TODO: support cloud case
	if lom.IsAIS() && !poi.t.bmdowner.Get().IsAIS(lom.Bucket()) {
		err = fmt.Errorf("Bucket %s was destroyed while PUTting %s was in the progress",
			lom.Bucket(), lom.Objname)
		errCode = http.StatusBadRequest
		return
	}

	lom.Lock(true)
	defer lom.Unlock(true)

	if lom.IsAIS() && lom.VerConf().Enabled {
		if ver, err = lom.IncObjectVersion(); err != nil {
			return
		}
		lom.SetVersion(ver)
	}

	if err := cmn.Rename(poi.workFQN, lom.FQN); err != nil {
		return fmt.Errorf("rename failed => %s: %v", lom, err), 0
	}

	if err = lom.DelAllCopies(); err != nil {
		return
	}
	lom.ReCache()
	return
}

// NOTE: LOM is updated on the end of the call with proper size and checksum.
// NOTE: `roi.r` is closed on the end of the call.
func (poi *putObjInfo) writeToFile() (err error) {
	var (
		file   *os.File
		buf    []byte
		slab   *memsys.Slab2
		reader = poi.r
	)
	if dryRun.disk {
		return
	}
	if file, err = cmn.CreateFile(poi.workFQN); err != nil {
		poi.t.fshc(err, poi.workFQN)
		return fmt.Errorf("failed to create %s, err: %s", poi.workFQN, err)
	}

	if poi.size == 0 {
		buf, slab = nodeCtx.mm.AllocDefault()
	} else {
		buf, slab = nodeCtx.mm.AllocForSize(poi.size)
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
			err = cmn.NewBadDataCksumError(expectedCksum, computedCksum, poi.lom.StringEx())
			poi.t.statsif.AddMany(
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
		return fmt.Errorf("failed to close received file %s, err: %v", poi.workFQN, err)
	}
	return nil
}

////////////////
// GET OBJECT //
////////////////

func (goi *getObjInfo) getObject() (err error, errCode int) {
	var doubleCheck, retry, retried bool
	// under lock: lom init, restore from cluster
	goi.lom.Lock(false)
do:
	// all the next checks work with disks - skip all if dryRun.disk=true
	coldGet := false
	if dryRun.disk {
		goto get
	}

	err = goi.lom.Load()
	if err != nil {
		goi.lom.Unlock(false)
		return err, http.StatusInternalServerError
	}

	coldGet = !goi.lom.Exists()
	if coldGet && goi.lom.IsAIS() {
		// try lookup and restore
		goi.lom.Unlock(false)
		doubleCheck, err, errCode = goi.tryRestoreObject(goi.lom)
		if doubleCheck && err != nil {
			lom2 := &cluster.LOM{T: goi.t, Objname: goi.lom.Objname}
			er2 := lom2.Init(goi.lom.Bucket(), goi.lom.Bck().Provider)
			if er2 == nil {
				er2 = lom2.Load()
				if er2 == nil && lom2.Exists() {
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
	if !coldGet && !goi.lom.IsAIS() { // exists && cloud-bucket : check ver if requested
		if goi.lom.Version() != "" && goi.lom.VerConf().ValidateWarmGet {
			goi.lom.Unlock(false)
			if coldGet, err, errCode = goi.t.checkCloudVersion(goi.ctx, goi.lom); err != nil {
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
				if goi.lom.IsAIS() {
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
		if err := goi.lom.AllowColdGET(); err != nil {
			return err, http.StatusBadRequest
		}
		goi.lom.SetAtimeUnix(goi.started.UnixNano())
		if err, errCode := goi.t.GetCold(goi.ctx, goi.lom, false); err != nil {
			return err, errCode
		}
		lom := goi.lom.Clone(goi.lom.FQN)
		goi.t.putMirror(lom)
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
//     1) local FS, 2) this cluster, 3) other tiers in the DC 4) from other
//		targets using erasure coding (if enabled)
func (goi *getObjInfo) tryRestoreObject(lom *cluster.LOM) (doubleCheck bool, err error, errCode int) {
	var (
		tsi, gfnNode     *cluster.Snode
		smap             = goi.t.smapowner.get()
		tname            = goi.t.si.Name()
		aborted, running = goi.t.xactions.isRebalancing(cmn.ActLocalReb)
		gfnActive        = goi.t.gfn.local.active()
	)
	tsi, err = cluster.HrwTarget(lom.Bck(), lom.Objname, &smap.Smap)
	if err != nil {
		return
	}
	if aborted || running || gfnActive {
		if lom.RestoreObjectFromAny() { // get-from-neighbor local (mountpaths) variety
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s restored", lom)
			}
			return
		}
		doubleCheck = running
	}

	// FIXME: if there're not enough EC targets to restore an sliced object,
	// we might be able to restore it if it was replicated. In this case even
	// just one additional target might be sufficient. This won't succeed if
	// an object was sliced, neither will ecmanager.RestoreObject(lom)
	enoughECRestoreTargets := lom.Bprops().EC.RequiredRestoreTargets() <= goi.t.smapowner.Get().CountTargets()

	// cluster-wide lookup ("get from neighbor")
	aborted, running = goi.t.xactions.isRebalancing(cmn.ActGlobalReb)
	if running {
		doubleCheck = true
	}
	gfnActive = goi.t.gfn.global.active()
	if running && tsi.DaemonID != goi.t.si.DaemonID {
		if goi.t.lookupRemoteSingle(lom, tsi) {
			gfnNode = tsi
			goto gfn
		}
	}
	if running || aborted || gfnActive || !enoughECRestoreTargets {
		gfnNode = goi.t.lookupRemoteAll(lom, smap)
	}

gfn:
	if gfnNode != nil {
		if goi.getFromNeighbor(lom, gfnNode) {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s: GFN %s <= %s", tname, lom, gfnNode.Name())
			}
			return
		}
	}

	// restore from existing EC slices if possible
	if ecErr := goi.t.ecmanager.RestoreObject(lom); ecErr == nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: EC-recovered %s", tname, lom)
		}
		lom.Load()
		return
	} else if ecErr != ec.ErrorECDisabled {
		err = fmt.Errorf("%s: failed to EC-recover %s: %v", tname, lom, ecErr)
	}

	s := fmt.Sprintf("GET local: %s(%s) %s", lom, lom.FQN, cmn.DoesNotExist)
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
	query.Add(cmn.URLParamProvider, cmn.ProviderFromBool(lom.IsAIS()))
	query.Add(cmn.URLParamIsGFNRequest, "true")
	reqArgs := cmn.ReqArgs{
		Method: http.MethodGet,
		Base:   tsi.URL(cmn.NetworkIntraData),
		Path:   cmn.URLPath(cmn.Version, cmn.Objects, lom.Bucket(), lom.Objname),
		Query:  query,
	}
	req, _, cancel, err := reqArgs.ReqWithTimeout(lom.Config().Timeout.SendFile)
	if err != nil {
		glog.Errorf("failed to create request, err: %v", err)
		return
	}
	defer cancel()

	resp, err := goi.t.httpclientGetPut.Do(req)
	if err != nil {
		glog.Errorf("GFN failure, URL %q, err: %v", reqArgs.URL(), err)
		return
	}
	var (
		cksumValue = resp.Header.Get(cmn.HeaderObjCksumVal)
		cksumType  = resp.Header.Get(cmn.HeaderObjCksumType)
		cksum      = cmn.NewCksum(cksumType, cksumValue)
		version    = resp.Header.Get(cmn.HeaderObjVersion)
		workFQN    = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfileRemote)
		atimeStr   = resp.Header.Get(cmn.HeaderObjAtime)
	)
	// The string in the header is an int represented as a string, NOT a formatted date string.
	atime, err := cmn.S2TimeUnix(atimeStr)
	if err != nil {
		glog.Error(err)
		return
	}
	lom.SetCksum(cksum)
	lom.SetVersion(version)
	lom.SetAtimeUnix(atime)
	poi := &putObjInfo{
		t:        goi.t,
		lom:      lom,
		workFQN:  workFQN,
		r:        resp.Body,
		migrated: true,
	}
	if err = poi.writeToFile(); err != nil {
		glog.Error(err)
		return
	}
	// commit
	if err = cmn.Rename(workFQN, lom.FQN); err != nil {
		glog.Error(err)
		return
	}
	if err = lom.Persist(); err != nil {
		glog.Error(err)
		return
	}
	lom.ReCache()
	ok = true
	return
}

func (goi *getObjInfo) finalize(coldGet bool) (retry bool, err error, errCode int) {
	var (
		file    *os.File
		sgl     *memsys.SGL
		slab    *memsys.Slab2
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

	if rw, ok := goi.w.(http.ResponseWriter); ok {
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

		var timeInt int64
		if !goi.lom.Atime().IsZero() {
			timeInt = goi.lom.Atime().UnixNano()
		}
		hdr.Set(cmn.HeaderObjAtime, strconv.FormatInt(timeInt, 10))
	}

	// loopback if disk IO is disabled
	if dryRun.disk {
		rd := newDryReader(dryRun.size)
		if _, err = io.Copy(goi.w, rd); err != nil {
			err = fmt.Errorf("dry-run: failed to send random response, err: %v", err)
			errCode = http.StatusInternalServerError
			goi.t.statsif.Add(stats.ErrGetCount, 1)
			return
		}
		delta := time.Since(goi.started)
		goi.t.statsif.AddMany(
			stats.NamedVal64{Name: stats.GetCount, Value: 1},
			stats.NamedVal64{Name: stats.GetLatency, Value: int64(delta)},
		)
		return
	}

	if goi.lom.Size() == 0 {
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
			err = fmt.Errorf("%s: err: %v", goi.lom, err)
			errCode = http.StatusInternalServerError
		}
		return
	}

	w := goi.w
	if goi.length == 0 {
		reader = file
		if goi.chunked {
			w = writerOnly{goi.w} // hide ReadFrom; CopyBuffer will use the buffer instead
			buf, slab = nodeCtx.mm.AllocForSize(goi.lom.Size())
		} else {
			hdr.Set("Content-Length", strconv.FormatInt(goi.lom.Size(), 10))
		}
	} else {
		buf, slab = nodeCtx.mm.AllocForSize(goi.length)
		reader = io.NewSectionReader(file, goi.offset, goi.length)
		if cksumRange {
			var cksumValue string
			sgl = nodeCtx.mm.NewSGL(goi.length, slab.Size())
			if _, cksumValue, err = cmn.WriteWithHash(sgl, reader, buf); err != nil {
				return
			}
			hdr.Set(cmn.HeaderObjCksumVal, cksumValue)
			hdr.Set(cmn.HeaderObjCksumType, cksumConf.Type)
			reader = io.NewSectionReader(file, goi.offset, goi.length)
		}
	}

	written, err = io.CopyBuffer(w, reader, buf)
	if err != nil {
		if cmn.IsErrConnectionReset(err) {
			return
		}
		goi.t.fshc(err, fqn)
		err = fmt.Errorf("failed to GET %s, err: %v", fqn, err)
		errCode = http.StatusInternalServerError
		goi.t.statsif.Add(stats.ErrGetCount, 1)
		return
	}

	// GFN: atime must be already set
	if !coldGet && !goi.isGFN {
		goi.lom.SetAtimeUnix(goi.started.UnixNano())
		goi.lom.ReCache() // GFN and cold GETs already did this
	}

	// Update objects which were sent during GFN. Thanks to this we will not
	// have to resend them in global rebalance. In case of race between rebalance
	// and GFN, the former wins and it will result in double send.
	if goi.isGFN {
		goi.t.rebManager.filterGFN.Insert([]byte(goi.lom.Uname()))
	}

	delta := time.Since(goi.started)
	if glog.FastV(4, glog.SmoduleAIS) {
		s := fmt.Sprintf("GET: %s(%s), %d µs", goi.lom, cmn.B2S(written, 1), int64(delta/time.Microsecond))
		if coldGet {
			s += " (cold)"
		}
		glog.Infoln(s)
	}
	goi.t.statsif.AddMany(
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
			f, err = cmn.CreateFile(filePath)
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
			slab *memsys.Slab2
		)
		if aoi.size == 0 {
			buf, slab = nodeCtx.mm.AllocDefault()
		} else {
			buf, slab = nodeCtx.mm.AllocForSize(aoi.size)
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

		if err := aoi.t.PromoteFile(filePath, aoi.lom.Bck(), aoi.lom.Objname, true /*overwrite*/, false /*safe*/, false /*verbose*/); err != nil {
			return "", err, 0
		}
	default:
		cmn.AssertMsg(false, aoi.op)
	}

	delta := time.Since(aoi.started)
	aoi.t.statsif.AddMany(
		stats.NamedVal64{Name: stats.AppendCount, Value: 1},
		stats.NamedVal64{Name: stats.AppendLatency, Value: int64(delta)},
	)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("PUT %s: %d µs", aoi.lom, int64(delta/time.Microsecond))
	}
	return
}
