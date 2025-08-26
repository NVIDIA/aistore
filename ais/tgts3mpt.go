// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

// 1. in-memory state
//    - active uploads kept purely in-memory (ups map)
//    - no support for target restart recovery during active uploads
//      (if target restarts during upload, client must restart the entire upload)
//
// 2. t.completeMpt() locks/unlocks two times - consider CoW
//
// 3. TODO cleanup; orphan chunks, abandoned (partial) manifests
//
// 4. parts ordering
//    - Add(chunk) keeps chunks ("parts") sorted

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
)

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
	)
	err := lom.InitBck(bck.Bucket())
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	uploadID, metadata, err = t.ups.start(w, r, lom, q)
	if err != nil {
		return
	}
	if err := t.ups.init(uploadID, lom, metadata); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	result := &s3.InitiateMptUploadResult{Bucket: bck.Name, Key: objName, UploadID: uploadID}

	nlog.Infoln("start", uploadID)

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
	partNum, err := t.ups.parsePartNum(part)
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	// 2. init lom, load/create chunk manifest
	objName := s3.ObjName(items)
	lom := &core.LOM{ObjName: objName}
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	// TODO: compare with listMptUploads (that does fromFS)
	manifest := t.ups.get(uploadID)
	if manifest == nil {
		s3.WriteMptErr(w, r, s3.NewErrNoSuchUpload(uploadID, nil), http.StatusNotFound, lom, uploadID)
		return
	}

	// Generate chunk file path
	chunkPath, err := manifest.ChunkFQN(int(partNum))
	if err != nil {
		s3.WriteMptErr(w, r, err, 0, lom, uploadID)
		return
	}

	partFh, errC := lom.CreatePart(chunkPath)
	if errC != nil {
		s3.WriteMptErr(w, r, errC, 0, lom, uploadID)
		return
	}

	var (
		etag         string
		expectedSize int64
		partSHA      = r.Header.Get(cos.S3HdrContentSHA256)
		checkPartSHA = partSHA != "" && partSHA != cos.S3UnsignedPayload
		cksumSHA     *cos.CksumHash
		cksumMD5     = &cos.CksumHash{}
		remote       = bck.IsRemoteS3() || bck.IsRemoteOCI()
	)
	if checkPartSHA {
		cksumSHA = cos.NewCksumHash(cos.ChecksumSHA256)
	}
	if !remote {
		cksumMD5 = cos.NewCksumHash(cos.ChecksumMD5)
	}

	if r.ContentLength <= 0 {
		err := fmt.Errorf("put-part invalid size (%d)", r.ContentLength)
		s3.WriteMptErr(w, r, err, 0, lom, uploadID)
		return
	}

	// 3. write
	// for remote buckets, use SGL buffering when memory is available;
	// fall back to TeeReader to avoid high memory usage under pressure
	var (
		ecode int
		mw    = cos.IniWriterMulti(cksumMD5.H, cksumSHA.H, partFh)
	)
	switch {
	case !remote:
		buf, slab := t.gmm.AllocSize(r.ContentLength)
		expectedSize, err = io.CopyBuffer(mw, r.Body, buf)
		slab.Free(buf)
	case t.gmm.Pressure() < memsys.PressureHigh:
		// write 1) locally + sgl + checksums; 2) write sgl => backend
		sgl := t.gmm.NewSGL(r.ContentLength)
		mw.Append(sgl)
		expectedSize, err = io.Copy(mw, r.Body)
		if err == nil {
			remoteStart := mono.NanoTime()
			etag, ecode, err = t.ups.putPartRemote(lom, sgl, r, q, uploadID, expectedSize, partNum)
			remotePutLatency = mono.SinceNano(remoteStart)
		}
		sgl.Free()
	default:
		// utilize TeeReader to simultaneously write => backend
		expectedSize = r.ContentLength
		tr := io.NopCloser(io.TeeReader(r.Body, mw))
		remoteStart := mono.NanoTime()
		etag, ecode, err = t.ups.putPartRemote(lom, tr, r, q, uploadID, expectedSize, partNum)
		remotePutLatency = mono.SinceNano(remoteStart)
	}
	cos.Close(partFh)
	if err != nil {
		if nerr := cos.RemoveFile(chunkPath); nerr != nil && !cos.IsNotExist(nerr) {
			nlog.Errorf(fmtNested, t, err, "remove", chunkPath, nerr)
		}
		s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
		return
	}

	size := mw.Size()
	if size != expectedSize {
		err := fmt.Errorf("part %d size mismatch (%d vs %d)", partNum, size, expectedSize)
		s3.WriteMptErr(w, r, err, 0, lom, uploadID)
		return
	}

	// 4. finalize the part (expecting the part's remote etag to be md5 checksum)
	if checkPartSHA {
		cksumSHA.Finalize()
		recvSHA := cos.NewCksum(cos.ChecksumSHA256, partSHA)
		if !cksumSHA.Equal(recvSHA) {
			detail := fmt.Sprintf("part %d", partNum)
			err = cos.NewErrDataCksum(&cksumSHA.Cksum, recvSHA, detail)
			s3.WriteMptErr(w, r, err, http.StatusInternalServerError, lom, uploadID)
			return
		}
	}

	chunk := &core.Uchunk{
		ETag: etag,
		Path: chunkPath,
	}
	if cksumMD5.H != nil {
		chunk.MD5 = cksumMD5.H.Sum(nil)
		debug.Assert(len(chunk.MD5) == 16, len(chunk.MD5))
	}
	if checkPartSHA {
		chunk.SetCksum(&cksumSHA.Cksum)
	}

	// - see NOTE above in re "active uploads in memory"
	// - TODO: this is the place to call Ufest.Store(partial manifest)
	if err := manifest.Add(chunk, size, int64(partNum)); err != nil {
		s3.WriteMptErr(w, r, err, 0, lom, uploadID)
		return
	}

	// s3 compliance
	if etag != "" {
		w.Header().Set(cos.S3CksumHeader, etag)
	} else {
		debug.Assert(len(chunk.MD5) == 16)
		w.Header().Set(cos.S3CksumHeader, cmn.MD5ToETag(chunk.MD5))
	}

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
	nlog.Infoln("complete", uploadID)

	body, err := cos.ReadAllN(r.Body, r.ContentLength)
	if err != nil {
		s3.WriteErr(w, r, err, http.StatusBadRequest)
		return
	}
	partList, err := s3.DecodeXML[*s3.CompleteMptUpload](body)
	if err != nil {
		s3.WriteErr(w, r, err, http.StatusBadRequest)
		return
	}
	if partList == nil || len(partList.Parts) == 0 {
		s3.WriteErr(w, r, errors.New("no parts"), http.StatusBadRequest)
		return
	}

	objName := s3.ObjName(items)
	lom := &core.LOM{ObjName: objName} // TODO: use core.AllocLOM()
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	// (compare with listMptUploads)
	manifest, metadata := t.ups.getWithMeta(uploadID)
	if manifest == nil {
		s3.WriteMptErr(w, r, s3.NewErrNoSuchUpload(uploadID, nil), http.StatusNotFound, lom, uploadID)
		return
	}

	// validate/enforce parts, compute _whole" checksum and etag
	var etag string
	manifest.Lock()
	etag, err = validateChecksumEtag(w, r, lom, manifest, partList)
	manifest.Unlock()
	if err != nil {
		return
	}

	// call remote
	remote := bck.IsRemoteS3() || bck.IsRemoteOCI()
	if remote {
		var err error
		etag, err = t.ups.completeRemote(w, r, lom, q, uploadID, body, partList)
		if err != nil {
			return
		}
	}

	if remote {
		md := t.ups.encodeRemoteMetadata(lom, metadata) // TODO -- FIXME: vs ufest.mdmap
		for k, v := range md {
			lom.SetCustomKey(k, v)
		}
	}
	lom.SetCustomKey(cmn.ETag, etag)

	// atomically flip: persist manifest, mark chunked, persist main
	if err := lom.CompleteUfest(manifest); err != nil {
		if !remote {
			s3.WriteMptErr(w, r, err, 0, lom, uploadID)
			return
		}
		nlog.Errorf("upload %q: failed to complete %s locally: %v", uploadID, lom.Cname(), err)
		return
	}

	t.ups.del(uploadID)

	nlog.Infoln(uploadID, "completed")

	// respond
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

// (under manifest lock)
func validateChecksumEtag(w http.ResponseWriter, r *http.Request, lom *core.LOM, manifest *core.Ufest, partList *s3.CompleteMptUpload) (string, error) {
	uploadID := manifest.ID()
	if err := manifest.Check(); err != nil {
		s3.WriteMptErr(w, r, err, 0, lom, uploadID)
		return "", err
	}
	if ecode, err := s3.EnforceCompleteAllParts(partList, manifest.Count()); err != nil {
		s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
		return "", err
	}

	// compute "whole" checksum (TODO: sha256 may take precedence when implied by `partSHA`)
	var (
		wholeCksum *cos.CksumHash
		bck        = lom.Bck()
		remote     = bck.IsRemoteS3() || bck.IsRemoteOCI()
	)
	if remote && lom.CksumConf().Type != cos.ChecksumNone {
		wholeCksum = cos.NewCksumHash(lom.CksumConf().Type)
	} else {
		wholeCksum = cos.NewCksumHash(cos.ChecksumMD5)
	}
	if wholeCksum != nil {
		if err := manifest.ComputeWholeChecksum(wholeCksum); err != nil {
			s3.WriteMptErr(w, r, err, 0, lom, uploadID)
			return "", err
		}
		lom.SetCksum(&wholeCksum.Cksum)
	}

	// compute multipart-compliant ETag if need be
	var etag string
	if !remote {
		var err error
		if etag, err = manifest.ETagS3(); err != nil {
			s3.WriteMptErr(w, r, err, 0, lom, uploadID)
			return "", err
		}
	}

	return etag, nil
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
	if bck.IsRemote() {
		if err := t.ups.abortRemote(w, r, lom, q, uploadID); err != nil {
			return
		}
	}
	if ecode, err := t.ups.abort(uploadID, lom); err != nil {
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

	manifest := t.ups.get(uploadID)
	if manifest == nil {
		var err error
		if manifest, err = t.ups.loadPartial(uploadID, lom, true /*add*/); err != nil {
			s3.WriteMptErr(w, r, s3.NewErrNoSuchUpload(uploadID, err), http.StatusNotFound, lom, uploadID)
			return
		}
	}

	parts, ecode, err := s3.ListParts(manifest)
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
	all := t.ups.toSlice()
	result := s3.ListUploads(all, bck.Name, idMarker, maxUploads)
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
	partNum, err := t.ups.parsePartNum(q.Get(s3.QparamMptPartNo))
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	manifest, err := core.NewUfest("", lom, true /*must-exist*/)
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	lom.Lock(false)
	defer lom.Unlock(false)

	// load chunk manifest and find out the part num's offset & size
	err = manifest.LoadCompleted(lom)
	if err != nil {
		s3.WriteErr(w, r, err, http.StatusNotFound)
		return
	}

	// get specific chunk
	manifest.Lock()
	chunk := manifest.GetChunk(int(partNum), true)
	manifest.Unlock()
	if chunk == nil {
		err := fmt.Errorf("part %d not found", partNum)
		s3.WriteErr(w, r, err, http.StatusNotFound)
		return
	}

	// read chunk file
	fh, err := os.Open(chunk.Path)
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	defer cos.Close(fh)

	buf, slab := t.gmm.AllocSize(chunk.Size())
	defer slab.Free(buf)

	if _, err := io.CopyBuffer(w, fh, buf); err != nil {
		s3.WriteErr(w, r, err, 0)
	}

	vlabs := map[string]string{stats.VlabBucket: bck.Cname("")}
	t.statsT.IncWith(stats.GetCount, vlabs)
	t.statsT.AddWith(
		cos.NamedVal64{Name: stats.GetSize, Value: chunk.Size(), VarLabs: vlabs},
		cos.NamedVal64{Name: stats.GetLatencyTotal, Value: mono.SinceNano(startTime), VarLabs: vlabs},
	)
}
