// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ec"
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
		cksumToCheck cmn.Cksummer
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
		gfn bool
		// true: chunked transfer (en)coding as per https://tools.ietf.org/html/rfc7230#page-36
		chunked bool
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
	poi.t.statsif.AddMany(stats.NamedVal64{stats.PutCount, 1}, stats.NamedVal64{stats.PutLatency, int64(delta)})
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("PUT %s: %d µs", lom, int64(delta/time.Microsecond))
	}
	return nil, 0
}

func (poi *putObjInfo) finalize() (err error, errCode int) {
	if err, errCode = poi.tryFinalize(); err != nil {
		if _, err1 := os.Stat(poi.workFQN); err1 == nil || !os.IsNotExist(err1) {
			if err1 == nil {
				err1 = err
			}
			poi.t.fshc(err, poi.workFQN)
			if err = os.Remove(poi.workFQN); err != nil {
				glog.Errorf("Nested error: %s => (remove %s => err: %v)", err1, poi.workFQN, err)
			}
		}
		poi.lom.Uncache()
		return
	}
	if err1 := poi.t.ecmanager.EncodeObject(poi.lom); err1 != nil && err1 != ec.ErrorECDisabled {
		err = err1
	}
	poi.t.putMirror(poi.lom)
	return
}

func (poi *putObjInfo) tryFinalize() (err error, errCode int) {
	var (
		ver string
		lom = poi.lom
	)
	if !lom.BckIsLocal && !poi.migrated {
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

	cluster.ObjectLocker.Lock(lom.Uname(), true)
	defer cluster.ObjectLocker.Unlock(lom.Uname(), true)

	if lom.BckIsLocal && lom.VerConf().Enabled {
		if ver, err = lom.IncObjectVersion(); err != nil {
			return
		}
		lom.SetVersion(ver)
	}
	// Don't persist meta, it will be persisted after move
	if err = lom.DelAllCopies(); err != nil {
		return
	}
	if err := cmn.Rename(poi.workFQN, lom.FQN); err != nil {
		return fmt.Errorf("Rename failed => %s: %v", lom, err), 0
	}
	if err1 := lom.Persist(); err1 != nil {
		err = err1
		glog.Errorf("failed to persist %s: %s", lom, err1)
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
			if nestedErr := os.Remove(poi.workFQN); nestedErr != nil {
				glog.Errorf("Nested (%v): failed to remove %s, err: %v", err, poi.workFQN, nestedErr)
			}
		}
	}()

	// receive and checksum
	var (
		written int64

		checkCksumType      string
		expectedCksum       cmn.Cksummer
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

		// if validate-cold-get and the cksum is provied we should also check md5 hash (aws, gcp)
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
			s := cmn.BadCksum(expectedCksum, computedCksum) + ", " + poi.lom.StringEx() + "[" + poi.workFQN + "]"
			err = fmt.Errorf(s)
			poi.t.statsif.AddMany(stats.NamedVal64{stats.ErrCksumCount, 1}, stats.NamedVal64{stats.ErrCksumSize, written})
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

// slight variation vs t.doPut() above
func (t *targetrunner) PutObject(workFQN string, reader io.ReadCloser, lom *cluster.LOM,
	recvType cluster.RecvType, cksum cmn.Cksummer, started time.Time) error {

	poi := &putObjInfo{
		started: started,
		t:       t,
		lom:     lom,
		r:       reader,
		workFQN: workFQN,
		ctx:     context.Background(),
	}
	if recvType == cluster.ColdGet {
		poi.cold = true
		poi.cksumToCheck = cksum
	}
	err, _ := poi.putObject()
	return err
}

////////////////
// GET OBJECT //
////////////////

func (goi *getObjInfo) getObject() (err error, errCode int) {
	var (
		fromCache bool
		retried   bool
	)

	// under lock: lom init, restore from cluster
	cluster.ObjectLocker.Lock(goi.lom.Uname(), false)
do:
	// all the next checks work with disks - skip all if dryRun.disk=true
	coldGet := false
	if dryRun.disk {
		goto get
	}

	fromCache, err = goi.lom.Load(true)
	if err != nil {
		cluster.ObjectLocker.Unlock(goi.lom.Uname(), false)
		return err, http.StatusInternalServerError
	}

	coldGet = !goi.lom.Exists()
	if coldGet && goi.lom.BckIsLocal {
		// does not exist in the local bucket: restore from neighbors
		if err, errCode = goi.t.restoreObjLBNeigh(goi.lom); err != nil {
			cluster.ObjectLocker.Unlock(goi.lom.Uname(), false)
			return
		}
		goto get
	}
	if !coldGet && !goi.lom.BckIsLocal { // exists && cloud-bucket : check ver if requested
		if goi.lom.Version() != "" && goi.lom.VerConf().ValidateWarmGet {
			if coldGet, err, errCode = goi.t.checkCloudVersion(goi.ctx, goi.lom); err != nil {
				goi.lom.Uncache()
				cluster.ObjectLocker.Unlock(goi.lom.Uname(), false)
				return
			}
		}
	}

	// checksum validation, if requested
	if !coldGet && goi.lom.CksumConf().ValidateWarmGet {
		if fromCache {
			err = goi.lom.ValidateChecksum(true)
		} else {
			err = goi.lom.ValidateDiskChecksum()
		}

		if err != nil {
			if goi.lom.BadCksum {
				glog.Error(err)
				if goi.lom.BckIsLocal {
					if err := os.Remove(goi.lom.FQN); err != nil {
						glog.Warningf("%s - failed to remove, err: %v", err, err)
					}
					cluster.ObjectLocker.Unlock(goi.lom.Uname(), false)
					return err, http.StatusInternalServerError
				}
				coldGet = true
			} else {
				cluster.ObjectLocker.Unlock(goi.lom.Uname(), false)
				return err, http.StatusInternalServerError
			}
		}
	}

	// 3. coldget
	if coldGet {
		cluster.ObjectLocker.Unlock(goi.lom.Uname(), false) // `GetCold` will lock again and return with object locked
		if err := goi.lom.AllowColdGET(); err != nil {
			return err, http.StatusBadRequest
		}
		goi.lom.SetAtimeUnix(goi.started.UnixNano())
		if err, errCode := goi.t.GetCold(goi.ctx, goi.lom, false); err != nil {
			return err, errCode
		}
		goi.t.putMirror(goi.lom)
	}

	// 4. get locally and stream back
get:
	_, retry, err, errCode := goi.finalize(coldGet)
	if retry && !retried {
		glog.Warningf("GET %s: uncaching and retrying...", goi.lom)
		retried = true
		goi.lom.Uncache()
		goto do
	}

	cluster.ObjectLocker.Unlock(goi.lom.Uname(), false)
	return
}

func (goi *getObjInfo) finalize(coldGet bool) (written int64, retry bool, err error, errCode int) {
	var (
		file   *os.File
		sgl    *memsys.SGL
		slab   *memsys.Slab2
		buf    []byte
		reader io.Reader
		hdr    http.Header // if it is http request we will write also header
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
			stats.NamedVal64{Name: stats.GetCount, Val: 1},
			stats.NamedVal64{Name: stats.GetLatency, Val: int64(delta)},
		)
		return
	}

	if goi.lom.Size() == 0 {
		return
	}

	fqn := goi.lom.FQN
	if !coldGet && !goi.gfn {
		// best-effort GET load balancing (see also mirror.findLeastUtilized())
		// (TODO: check whether the timestamp is not too old)
		fqn = goi.lom.LoadBalanceGET(goi.started)
	}
	file, err = os.Open(fqn)
	if err != nil {
		if os.IsNotExist(err) {
			errCode = http.StatusNotFound
			retry = true // (!lom.BckIsLocal || lom.ECEnabled() || GFN...)
		} else {
			goi.t.fshc(err, fqn)
			err = fmt.Errorf("%s: err: %v", goi.lom, err)
			errCode = http.StatusInternalServerError
		}
		return
	}

	// TODO: we should probably support offset != 0 even if goi.length == 0
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
		if cksumRange {
			var (
				cksumValue string
			)
			cksumValue, sgl, reader, err = goi.t.rangeCksum(file, fqn, goi.offset, goi.length, buf)
			if err != nil {
				return
			}
			hdr.Set(cmn.HeaderObjCksumVal, cksumValue)
			hdr.Set(cmn.HeaderObjCksumType, cksumConf.Type)
		} else {
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
	if !coldGet && !goi.gfn {
		goi.lom.SetAtimeUnix(goi.started.UnixNano())
		goi.lom.ReCache() // GFN and cold GETs already did this
	}

	// Update objects which were sent during GFN. Thanks to this we will not
	// have to resend them in global rebalance. In case of race between rebalance
	// and GFN, the former wins and it will result in double send.
	if goi.gfn {
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
		stats.NamedVal64{Name: stats.GetThroughput, Val: written},
		stats.NamedVal64{Name: stats.GetLatency, Val: int64(delta)},
		stats.NamedVal64{Name: stats.GetCount, Val: 1},
	)
	return
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
