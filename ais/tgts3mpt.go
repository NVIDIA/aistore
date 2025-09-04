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
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
)

// Initialize multipart upload.
// - Generate UUID for the upload
// - Return the UUID to a caller
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
func (t *target) startMpt(w http.ResponseWriter, r *http.Request, items []string, bck *meta.Bck) {
	var (
		objName = s3.ObjName(items)
		lom     = &core.LOM{ObjName: objName}
	)
	err := lom.InitBck(bck.Bucket())
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	uploadID, err := t.createMptUpload(r, lom)
	if err != nil {
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
func (t *target) putMptPartS3(w http.ResponseWriter, r *http.Request, items []string, q url.Values, bck *meta.Bck) {
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

	etag, ecode, err := t.putMptPart(r, lom, uploadID, int(partNum))
	// convert generic error to s3 error
	if cos.IsErrNotFound(err) {
		s3.WriteMptErr(w, r, s3.NewErrNoSuchUpload(uploadID, nil), ecode, lom, uploadID)
		return
	}
	if err != nil {
		s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
		return
	}

	// s3 compliance
	if etag != "" {
		w.Header().Set(cos.S3CksumHeader, etag)
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
	s3PartList, err := s3.DecodeXML[*s3.CompleteMptUpload](body)
	if err != nil {
		s3.WriteErr(w, r, err, http.StatusBadRequest)
		return
	}
	if s3PartList == nil || len(s3PartList.Parts) == 0 {
		s3.WriteErr(w, r, errors.New("no parts"), http.StatusBadRequest)
		return
	}

	objName := s3.ObjName(items)
	lom := &core.LOM{ObjName: objName} // TODO: use core.AllocLOM()
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	// convert s3PartList to aistore native type
	partList := make(apc.MptCompletedParts, 0, len(s3PartList.Parts))
	for _, part := range s3PartList.Parts {
		if part.PartNumber == nil {
			continue
		}
		mptPart := apc.MptCompletedPart{
			PartNumber: int(*part.PartNumber),
		}
		if part.ETag != nil {
			mptPart.ETag = *part.ETag
		}
		partList = append(partList, mptPart)
	}

	etag, ecode, err := t.completeMptUpload(r, lom, uploadID, body, partList)
	// convert generic error to s3 error
	if cos.IsErrNotFound(err) {
		s3.WriteMptErr(w, r, s3.NewErrNoSuchUpload(uploadID, nil), ecode, lom, uploadID)
		return
	}
	if err != nil {
		s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
		return
	}

	// respond
	result := &s3.CompleteMptUploadResult{Bucket: bck.Name, Key: objName, ETag: etag}
	sgl := t.gmm.NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	s3.SetS3Headers(w.Header(), lom)
	sgl.WriteTo2(w)
	sgl.Free()
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
	ecode, err = t.abortMptUpload(r, lom, uploadID)
	if err != nil {
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

	manifest, _ := t.ups.get(uploadID, lom)
	if manifest == nil {
		s3.WriteMptErr(w, r, s3.NewErrNoSuchUpload(uploadID, nil), http.StatusNotFound, lom, uploadID)
		return
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
	fh, err := os.Open(chunk.Path())
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
