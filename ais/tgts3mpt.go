// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/ais/backend"
	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

func decodeXML[T any](body []byte) (result T, _ error) {
	if err := xml.Unmarshal(body, &result); err != nil {
		return result, err
	}
	return result, nil
}

func multiWriter(writers ...io.Writer) io.Writer {
	a := make([]io.Writer, 0, 3)
	for _, w := range writers {
		if w != nil {
			a = append(a, w)
		}
	}
	return io.MultiWriter(a...)
}

// Initialize multipart upload.
// - Generate UUID for the upload
// - Return the UUID to a caller
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
func (t *target) startMpt(w http.ResponseWriter, r *http.Request, items []string, bck *meta.Bck, q url.Values) {
	var (
		objName  = s3.ObjName(items)
		lom      = &core.LOM{ObjName: objName}
		metadata map[string]string
		uploadID string
		ecode    int
	)
	err := lom.InitBck(bck.Bucket())
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	switch {
	case bck.IsRemoteS3():
		metadata = cmn.BackendHelpers.Amazon.DecodeMetadata(r.Header)
		uploadID, ecode, err = backend.StartMptAWS(lom, r, q)
		if err != nil {
			s3.WriteErr(w, r, err, ecode)
			return
		}
	case bck.IsRemoteOCI():
		metadata = cmn.BackendHelpers.OCI.DecodeMetadata(r.Header)
		uploadID, ecode, err = backend.StartMptOCI(t.Backend(lom.Bck()), lom, r, q)
		if err != nil {
			s3.WriteErr(w, r, err, ecode)
			return
		}
	default:
		uploadID = cos.GenUUID()
	}

	s3.InitUpload(uploadID, bck.Name, objName, metadata)
	result := &s3.InitiateMptUploadResult{Bucket: bck.Name, Key: objName, UploadID: uploadID}

	sgl := t.gmm.NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	sgl.WriteTo2(w)
	sgl.Free()
}

// PUT a part of the multipart upload.
// Body is empty, everything in the query params and the header.
//
// "Content-MD5" in the part headers seems be to be deprecated:
// either not present (s3cmd) or cannot be trusted (aws s3api).
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
func (t *target) putMptPart(w http.ResponseWriter, r *http.Request, items []string, q url.Values, bck *meta.Bck) {
	var (
		remotePutLatency int64
		startTime        = mono.NanoTime()
	)
	// 1. parse/validate
	uploadID := q.Get(s3.QparamMptUploadID)
	if uploadID == "" {
		s3.WriteErr(w, r, errors.New("empty uploadId"), 0)
		return
	}
	part := q.Get(s3.QparamMptPartNo)
	if part == "" {
		s3.WriteErr(w, r, fmt.Errorf("upload %q: missing part number", uploadID), 0)
		return
	}
	partNum, err := s3.ParsePartNum(part)
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	if partNum < 1 || partNum > s3.MaxPartsPerUpload {
		err := fmt.Errorf("upload %q: invalid part number %d, must be between 1 and %d",
			uploadID, partNum, s3.MaxPartsPerUpload)
		s3.WriteErr(w, r, err, 0)
		return
	}

	// 2. init lom, create part file
	objName := s3.ObjName(items)
	lom := &core.LOM{ObjName: objName}
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	if !s3.UploadExists(uploadID) {
		s3.WriteMptErr(w, r, s3.NewErrNoSuchUpload(uploadID), http.StatusNotFound, lom, uploadID)
		return
	}
	// workfile name format: <upload-id>.<part-number>.<obj-name>
	prefix := uploadID + "." + strconv.FormatInt(int64(partNum), 10)
	wfqn := fs.CSM.Gen(lom, fs.WorkfileType, prefix)
	partFh, errC := lom.CreatePart(wfqn)
	if errC != nil {
		s3.WriteMptErr(w, r, errC, 0, lom, uploadID)
		return
	}

	var (
		etag         string
		size         int64
		ecode        int
		partSHA      = r.Header.Get(cos.S3HdrContentSHA256)
		checkPartSHA = partSHA != "" && partSHA != cos.S3UnsignedPayload
		cksumSHA     = &cos.CksumHash{}
		cksumMD5     = &cos.CksumHash{}
		remote       = bck.IsRemoteS3() || bck.IsRemoteOCI()
	)
	if checkPartSHA {
		cksumSHA = cos.NewCksumHash(cos.ChecksumSHA256)
	}
	if !remote {
		cksumMD5 = cos.NewCksumHash(cos.ChecksumMD5)
	}

	// 3. write
	mw := multiWriter(cksumMD5.H, cksumSHA.H, partFh)

	if !remote {
		// write locally
		buf, slab := t.gmm.Alloc()
		size, err = io.CopyBuffer(mw, r.Body, buf)
		slab.Free(buf)
	} else {
		// write locally and utilize TeeReader to simultaneously send data to S3
		tr := io.NopCloser(io.TeeReader(r.Body, mw))
		size = r.ContentLength
		debug.Assert(size > 0, "mpt upload: expecting positive content-length")
		remoteStart := mono.NanoTime()
		if bck.IsRemoteS3() {
			etag, ecode, err = backend.PutMptPartAWS(lom, tr, r, q, uploadID, size, partNum)
		} else {
			debug.Assert(bck.IsRemoteOCI())
			etag, ecode, err = backend.PutMptPartOCI(t.Backend(lom.Bck()), lom, tr, r, q, uploadID, size, partNum)
		}
		remotePutLatency = mono.SinceNano(remoteStart)
	}

	cos.Close(partFh)
	if err != nil {
		if nerr := cos.RemoveFile(wfqn); nerr != nil && !cos.IsNotExist(nerr) {
			nlog.Errorf(fmtNested, t, err, "remove", wfqn, nerr)
		}
		s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
		return
	}

	// 4. finalize the part (expecting the part's remote etag to be md5 checksum)
	md5 := etag
	if cksumMD5.H != nil {
		debug.Assert(etag == "")
		cksumMD5.Finalize()
		md5 = cksumMD5.Value()
	}
	if checkPartSHA {
		cksumSHA.Finalize()
		recvSHA := cos.NewCksum(cos.ChecksumSHA256, partSHA)
		if !cksumSHA.Equal(recvSHA) {
			detail := fmt.Sprintf("upload %q, %s, part %d", uploadID, lom, partNum)
			err = cos.NewErrDataCksum(&cksumSHA.Cksum, recvSHA, detail)
			s3.WriteMptErr(w, r, err, http.StatusInternalServerError, lom, uploadID)
			return
		}
	}
	npart := &s3.MptPart{
		MD5:  md5,
		FQN:  wfqn,
		Size: size,
		Num:  partNum,
	}
	if ecode, err := s3.AddPart(uploadID, npart); err != nil {
		s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
		return
	}
	w.Header().Set(cos.S3CksumHeader, md5) // s3cmd checks this one

	delta := mono.SinceNano(startTime)
	vlabs := map[string]string{stats.VlabBucket: bck.Cname(""), stats.VlabXkind: ""}
	t.statsT.AddWith(
		cos.NamedVal64{Name: stats.PutSize, Value: size, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.PutLatency, Value: delta, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.PutLatencyTotal, Value: delta, VarLabs: vlabs},
	)
	if remotePutLatency > 0 {
		backendBck := t.Backend(bck)
		t.statsT.AddWith(
			cos.NamedVal64{Name: backendBck.MetricName(stats.PutSize), Value: size, VarLabs: vlabs},
			cos.NamedVal64{Name: backendBck.MetricName(stats.PutLatencyTotal), Value: remotePutLatency, VarLabs: vlabs},
			cos.NamedVal64{Name: backendBck.MetricName(stats.PutE2ELatencyTotal), Value: delta, VarLabs: vlabs},
		)
	}
}

// Complete multipart upload.
// Body contains XML with the list of parts that must be on the storage already.
// 1. Check that all parts from request body present
// 2. Merge all parts into a single file and calculate its ETag
// 3. Return ETag to a caller
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
func (t *target) completeMpt(w http.ResponseWriter, r *http.Request, items []string, q url.Values, bck *meta.Bck) {
	// parse/validate
	uploadID := q.Get(s3.QparamMptUploadID)
	if uploadID == "" {
		s3.WriteErr(w, r, errors.New("empty uploadId"), 0)
		return
	}

	body, err := cos.ReadAllN(r.Body, r.ContentLength)
	if err != nil {
		s3.WriteErr(w, r, err, http.StatusBadRequest)
		return
	}
	partList, err := decodeXML[*s3.CompleteMptUpload](body)
	if err != nil {
		s3.WriteErr(w, r, err, http.StatusBadRequest)
		return
	}
	if len(partList.Parts) == 0 {
		s3.WriteErr(w, r, fmt.Errorf("upload %q: empty list of upload parts", uploadID), 0)
		return
	}
	objName := s3.ObjName(items)
	lom := &core.LOM{ObjName: objName} // TODO: use core.AllocLOM()
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	size, ecode, errN := s3.ObjSize(uploadID)
	if errN != nil {
		s3.WriteMptErr(w, r, errN, ecode, lom, uploadID)
		return
	}

	// call s3
	var (
		version string
		etag    string
		started = time.Now()
		remote  = bck.IsRemoteS3() || bck.IsRemoteOCI()
	)
	if remote {
		if bck.IsRemoteS3() {
			v, e, ecode, err := backend.CompleteMptAWS(lom, r, q, uploadID, body, partList)
			if err != nil {
				s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
				return
			}
			version = v
			etag = e
		} else {
			debug.Assert(bck.IsRemoteOCI())
			v, e, ecode, err := backend.CompleteMptOCI(t.Backend(lom.Bck()), lom, r, q, uploadID, body, partList)
			if err != nil {
				s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
				return
			}
			version = v
			etag = e
		}
	}

	// append parts and finalize locally
	var (
		mw          io.Writer
		concatMD5   string // => ETag
		actualCksum *cos.CksumHash
	)
	// .1 sort and check parts
	sort.Slice(partList.Parts, func(i, j int) bool {
		return *partList.Parts[i].PartNumber < *partList.Parts[j].PartNumber
	})
	nparts, ecode, err := s3.CheckParts(uploadID, partList.Parts)
	if err != nil {
		s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
		return
	}
	// 2. <upload-id>.complete.<obj-name>
	prefix := uploadID + ".complete"
	wfqn := fs.CSM.Gen(lom, fs.WorkfileType, prefix)
	wfh, errC := lom.CreateWork(wfqn)
	if errC != nil {
		s3.WriteMptErr(w, r, errC, 0, lom, uploadID)
		return
	}
	if remote && lom.CksumConf().Type != cos.ChecksumNone {
		actualCksum = cos.NewCksumHash(lom.CksumConf().Type)
	} else {
		actualCksum = cos.NewCksumHash(cos.ChecksumMD5)
	}
	mw = multiWriter(actualCksum.H, wfh)

	// .3 write
	buf, slab := t.gmm.Alloc()
	concatMD5, written, errA := _appendMpt(nparts, buf, mw)
	slab.Free(buf)

	if lom.IsFeatureSet(feat.FsyncPUT) {
		errS := wfh.Sync()
		debug.AssertNoErr(errS)
	}
	cos.Close(wfh)

	if errA == nil && written != size {
		errA = fmt.Errorf("upload %q %q: expected full size=%d, got %d", uploadID, lom.Cname(), size, written)
	}
	if errA != nil {
		if nerr := cos.RemoveFile(wfqn); nerr != nil && !cos.IsNotExist(nerr) {
			nlog.Errorf(fmtNested, t, errA, "remove", wfqn, nerr)
		}
		s3.WriteMptErr(w, r, errA, 0, lom, uploadID)
		return
	}

	// .4 (s3 client => ais://) compute resulting MD5 and, optionally, ETag
	if actualCksum.H != nil {
		actualCksum.Finalize()
		lom.SetCksum(actualCksum.Cksum.Clone())
	}
	if etag == "" {
		debug.Assert(!remote)
		debug.Assert(concatMD5 != "")

		resMD5Val := cos.ChecksumB2S(cos.UnsafeB(concatMD5), cos.ChecksumMD5)
		etag = `"` + resMD5Val + cmn.AwsMultipartDelim + strconv.Itoa(len(partList.Parts)) + `"`
	}

	// .5 finalize
	lom.SetSize(size)
	if remote {
		lom.SetCustomKey(cmn.SourceObjMD, apc.AWS)
		if version != "" {
			lom.SetCustomKey(cmn.VersionObjMD, version)
		}
		metadata, ecode, err := s3.GetUploadMetadata(uploadID)
		if err != nil {
			s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
			return
		}
		for k, v := range cmn.BackendHelpers.Amazon.EncodeMetadata(metadata) {
			lom.SetCustomKey(k, v)
		}
	}
	lom.SetCustomKey(cmn.ETag, etag)

	poi := allocPOI()
	{
		poi.t = t
		poi.atime = started.UnixNano()
		poi.lom = lom
		poi.workFQN = wfqn
		poi.owt = cmn.OwtNone
	}
	ecode, errF := poi.finalize()
	freePOI(poi)

	// .6 cleanup parts - unconditionally
	_, cleanupErr := s3.CleanupUpload(uploadID, lom, false /*aborted*/)
	debug.Assert(cleanupErr == nil)

	if errF != nil {
		// NOTE: not failing if remote op. succeeded
		if !remote {
			s3.WriteMptErr(w, r, errF, ecode, lom, uploadID)
			return
		}
		nlog.Errorf("upload %q: failed to complete %s locally: %v(%d)", uploadID, lom.Cname(), errF, ecode)
	}

	// .7 respond
	result := &s3.CompleteMptUploadResult{Bucket: bck.Name, Key: objName, ETag: etag}
	sgl := t.gmm.NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	s3.SetS3Headers(w.Header(), lom)
	sgl.WriteTo2(w)
	sgl.Free()

	// stats
	vlabs := map[string]string{stats.VlabBucket: bck.Cname(""), stats.VlabXkind: ""}
	t.statsT.IncWith(stats.PutCount, vlabs)
	if remote {
		t.statsT.IncWith(t.Backend(bck).MetricName(stats.PutCount), vlabs)
	}
}

func _appendMpt(nparts []*s3.MptPart, buf []byte, mw io.Writer) (concatMD5 string, written int64, err error) {
	for _, partInfo := range nparts {
		var (
			partFh   *os.File
			partSize int64
		)
		concatMD5 += partInfo.MD5
		if partFh, err = os.Open(partInfo.FQN); err != nil {
			return "", 0, err
		}
		partSize, err = io.CopyBuffer(mw, partFh, buf)
		cos.Close(partFh)
		if err != nil {
			return "", 0, err
		}
		written += partSize
	}
	return concatMD5, written, nil
}

// Abort an active multipart upload.
// Body is empty, only URL query contains uploadID
// 1. uploadID must exists
// 2. Remove all temporary files
// 3. Remove all info from in-memory structs
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
func (t *target) abortMpt(w http.ResponseWriter, r *http.Request, items []string, q url.Values) {
	bck, ecode, err := meta.InitByNameOnly(items[0], t.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, ecode)
		return
	}
	objName := s3.ObjName(items)
	lom := &core.LOM{ObjName: objName}
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	uploadID := q.Get(s3.QparamMptUploadID)

	if bck.IsRemoteS3() {
		ecode, err := backend.AbortMptAWS(lom, r, q, uploadID)
		if err != nil {
			s3.WriteErr(w, r, err, ecode)
			return
		}
	} else if bck.IsRemoteOCI() {
		ecode, err := backend.AbortMptOCI(t.Backend(lom.Bck()), lom, r, q, uploadID)
		if err != nil {
			s3.WriteErr(w, r, err, ecode)
			return
		}
	}

	if ecode, err := s3.CleanupUpload(uploadID, lom, true /*aborted*/); err != nil {
		s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
		return
	}

	// Respond with status 204(!see the docs) and empty body.
	w.WriteHeader(http.StatusNoContent)
}

// List already stored parts of the active multipart upload by bucket name and uploadID.
// (NOTE: `s3cmd` lists upload parts before checking if any parts can be skipped.)
// s3cmd is OK to receive an empty body in response with status=200. In this
// case s3cmd sends all parts.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
func (t *target) listMptParts(w http.ResponseWriter, r *http.Request, bck *meta.Bck, objName string, q url.Values) {
	uploadID := q.Get(s3.QparamMptUploadID)

	lom := &core.LOM{ObjName: objName}
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	parts, ecode, err := s3.ListParts(uploadID, lom)
	if err != nil {
		s3.WriteErr(w, r, err, ecode)
		return
	}
	result := &s3.ListPartsResult{Bucket: bck.Name, Key: objName, UploadID: uploadID, Parts: parts}
	sgl := t.gmm.NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	sgl.WriteTo2(w)
	sgl.Free()
}

// List all active multipart uploads for a bucket.
// See https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
// GET /?uploads&delimiter=Delimiter&encoding-type=EncodingType&key-marker=KeyMarker&
// max-uploads=MaxUploads&prefix=Prefix&upload-id-marker=UploadIdMarker
func (t *target) listMptUploads(w http.ResponseWriter, bck *meta.Bck, q url.Values) {
	var (
		maxUploads int
		idMarker   string
	)
	if s := q.Get(s3.QparamMptMaxUploads); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			maxUploads = v
		}
	}
	idMarker = q.Get(s3.QparamMptUploadIDMarker)
	result := s3.ListUploads(bck.Name, idMarker, maxUploads)
	sgl := t.gmm.NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	sgl.WriteTo2(w)
	sgl.Free()
}

// Acts on an already multipart-uploaded object, returns `partNumber` (URL query)
// part of the object.
// The object must have been multipart-uploaded beforehand.
// See:
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
func (t *target) getMptPart(w http.ResponseWriter, r *http.Request, bck *meta.Bck, lom *core.LOM, q url.Values) {
	startTime := mono.NanoTime()
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	partNum, err := s3.ParsePartNum(q.Get(s3.QparamMptPartNo))
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	lom.Lock(false)
	defer lom.Unlock(false)

	// load mpt xattr and find out the part num's offset & size
	off, size, status, err := s3.OffsetSorted(lom, partNum)
	if err != nil {
		s3.WriteErr(w, r, err, status)
		return
	}
	fh, err := lom.Open()
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	buf, slab := t.gmm.AllocSize(size)
	reader := io.NewSectionReader(fh, off, size)
	if _, err := io.CopyBuffer(w, reader, buf); err != nil {
		s3.WriteErr(w, r, err, 0)
	}
	cos.Close(fh)
	slab.Free(buf)

	vlabs := map[string]string{stats.VlabBucket: bck.Cname("")}
	t.statsT.IncWith(stats.GetCount, vlabs)
	t.statsT.AddWith(
		cos.NamedVal64{Name: stats.GetSize, Value: size, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.GetLatencyTotal, Value: mono.SinceNano(startTime), VarLabs: vlabs},
	)
}
