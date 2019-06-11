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

		if errstr, errCode := poi.finalize(); errstr != "" {
			return errors.New(errstr), errCode
		}
	}

	delta := time.Since(poi.started)
	poi.t.statsif.AddMany(stats.NamedVal64{stats.PutCount, 1}, stats.NamedVal64{stats.PutLatency, int64(delta)})
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("PUT %s: %d µs", lom, int64(delta/time.Microsecond))
	}
	return nil, 0
}

func (poi *putObjInfo) finalize() (errstr string, errCode int) {
	if errstr, errCode = poi.tryFinalize(); errstr != "" {
		if _, err := os.Stat(poi.workFQN); err == nil || !os.IsNotExist(err) {
			if err == nil {
				err = errors.New(errstr)
			}
			poi.t.fshc(err, poi.workFQN)
			if err = os.Remove(poi.workFQN); err != nil {
				glog.Errorf("Nested error: %s => (remove %s => err: %v)", errstr, poi.workFQN, err)
			}
		}
		poi.lom.Uncache()
		return
	}
	if err := poi.t.ecmanager.EncodeObject(poi.lom); err != nil && err != ec.ErrorECDisabled {
		errstr = err.Error()
	}
	poi.t.putMirror(poi.lom)
	return
}

func (poi *putObjInfo) tryFinalize() (errstr string, errCode int) {
	var (
		ver string
		lom = poi.lom
	)
	if !lom.BckIsLocal && !poi.migrated {
		file, err := os.Open(poi.workFQN)
		if err != nil {
			errstr = fmt.Sprintf("failed to open %s err: %v", poi.workFQN, err)
			return
		}
		cmn.Assert(lom.Cksum() != nil)
		ver, err, errCode = getcloudif().putobj(poi.ctx, file, lom)
		file.Close()
		if err != nil {
			errstr = fmt.Sprintf("%s: PUT failed, err: %v", lom, err)
			return
		}
		lom.SetVersion(ver)
	}

	cluster.ObjectLocker.Lock(lom.Uname(), true)
	defer cluster.ObjectLocker.Unlock(lom.Uname(), true)

	if lom.BckIsLocal && lom.VerConf().Enabled {
		if ver, errstr = lom.IncObjectVersion(); errstr != "" {
			return
		}
		lom.SetVersion(ver)
	}
	// Don't persist meta, it will be persisted after move
	if errstr = lom.DelAllCopies(); errstr != "" {
		return
	}
	if err := cmn.MvFile(poi.workFQN, lom.FQN); err != nil {
		errstr = fmt.Sprintf("MvFile failed => %s: %v", lom, err)
		return
	}
	if err := lom.Persist(); err != nil {
		errstr = err.Error()
		glog.Errorf("failed to persist %s: %s", lom, errstr)
	}
	lom.ReCache()
	return
}

// NOTE: LOM is updated on the end of the call with proper size and checksum.
// NOTE: `roi.r` is closed on the end of the call.
func (poi *putObjInfo) writeToFile() (err error) {
	var (
		file   *os.File
		reader = poi.r
	)

	if dryRun.disk {
		return
	}

	if file, err = cmn.CreateFile(poi.workFQN); err != nil {
		poi.t.fshc(err, poi.workFQN)
		return fmt.Errorf("failed to create %s, err: %s", poi.workFQN, err)
	}

	buf, slab := gmem2.AllocFromSlab2(0)
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
		errstr    string
	)

	// under lock: lom init, restore from cluster
	cluster.ObjectLocker.Lock(goi.lom.Uname(), false)
	defer cluster.ObjectLocker.Unlock(goi.lom.Uname(), false)
do:
	// all the next checks work with disks - skip all if dryRun.disk=true
	coldGet := false
	if dryRun.disk {
		goto get
	}

	fromCache, errstr = goi.lom.Load(true)
	if errstr != "" {
		return errors.New(errstr), http.StatusInternalServerError
	}

	coldGet = !goi.lom.Exists()
	if coldGet && goi.lom.BckIsLocal {
		// does not exist in the local bucket: restore from neighbors
		if err, errCode = goi.t.restoreObjLBNeigh(goi.lom); err != nil {
			return
		}
		goto get
	}
	if !coldGet && !goi.lom.BckIsLocal { // exists && cloud-bucket : check ver if requested
		if goi.lom.Version() != "" && goi.lom.VerConf().ValidateWarmGet {
			if coldGet, err, errCode = goi.t.checkCloudVersion(goi.ctx, goi.lom); err != nil {
				goi.lom.Uncache()
				return
			}
		}
	}

	// checksum validation, if requested
	if !coldGet && goi.lom.CksumConf().ValidateWarmGet {
		if fromCache {
			errstr = goi.lom.ValidateChecksum(true)
		} else {
			errstr = goi.lom.ValidateDiskChecksum()
		}

		if errstr != "" {
			if goi.lom.BadCksum {
				glog.Errorln(errstr)
				if goi.lom.BckIsLocal {
					if err := os.Remove(goi.lom.FQN); err != nil {
						glog.Warningf("%s - failed to remove, err: %v", errstr, err)
					}
					return errors.New(errstr), http.StatusInternalServerError
				}
				coldGet = true
			} else {
				return errors.New(errstr), http.StatusInternalServerError
			}
		}
	}

	// 3. coldget
	if coldGet {
		if err := goi.lom.AllowColdGET(); err != nil {
			return err, http.StatusBadRequest
		}
		goi.lom.SetAtimeUnix(goi.started.UnixNano())
		if errstr, errCode := goi.t.GetCold(goi.ctx, goi.lom, false); errstr != "" {
			return errors.New(errstr), errCode
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

	if err != nil {
		return err, errCode
	}
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
		glog.Warningf("%s: size=0 (zero)", goi.lom) // TODO: optimize out much of the below
		return
	}

	fqn := goi.lom.LoadBalanceGET() // coldGet => len(CopyFQN) == 0
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
	if goi.length == 0 {
		reader = file
		buf, slab = gmem2.AllocFromSlab2(lom.Size())
		// hdr.Set("Content-Length", strconv.FormatInt(lom.Size(), 10)) // TODO: optimize
	} else {
		buf, slab = gmem2.AllocFromSlab2(goi.length)
		if cksumRange {
			var (
				cksum, errstr string
			)
			cksum, sgl, reader, errstr = goi.t.rangeCksum(file, fqn, goi.offset, goi.length, buf)
			if errstr != "" {
				return
			}
			hdr.Set(cmn.HeaderObjCksumType, cksumConf.Type)
			hdr.Set(cmn.HeaderObjCksumVal, cksum)
		} else {
			reader = io.NewSectionReader(file, goi.offset, goi.length)
		}
	}

	w.WriteHeader(http.StatusOK)
	written, err = io.CopyBuffer(goi.w, reader, buf)
	if err != nil {
		goi.t.fshc(err, fqn)
		err = fmt.Errorf("Failed to GET %s, err: %v", fqn, err)
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

// slight variation vs t.httpobjeget() above
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
