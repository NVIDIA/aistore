// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/stats"
)

// objHeadV2 handles HEAD requests with selective property retrieval via the `props` query parameter.
func (t *target) objHeadV2(r *http.Request, whdr http.Header, dpq *dpq, bck *meta.Bck, lom *core.LOM) (int, error) {
	var (
		started     = mono.NanoTime()
		fltPresence int
		exists      = true
	)
	if tmp := dpq.get(apc.QparamFltPresence); tmp != "" {
		var erp error
		fltPresence, erp = strconv.Atoi(tmp)
		debug.AssertNoErr(erp)
	}
	if err := lom.InitBck(bck); err != nil {
		if cmn.IsErrBucketNought(err) {
			return http.StatusNotFound, err
		}
		return 0, err
	}
	if err := lom.Load(true /*cache it*/, false /*locked*/); err == nil {
		if apc.IsFltNoProps(fltPresence) {
			return 0, nil
		}
		if fltPresence == apc.FltExistsOutside {
			return 0, fmt.Errorf(fmtOutside, lom.Cname(), fltPresence)
		}
	} else {
		if !cmn.IsErrObjNought(err) {
			return 0, err
		}
		exists = false
		if fltPresence == apc.FltPresentCluster {
			exists = lom.RestoreToLocation()
		}
	}

	if !exists {
		if bck.IsAIS() || apc.IsFltPresent(fltPresence) {
			return http.StatusNotFound, cos.NewErrNotFound(t, lom.Cname())
		}
	}

	// Cold HEAD: check remote backend if object not found locally or if latest version requested
	var attrs *cmn.ObjAttrs
	latest := dpq.latestVer
	if !exists || latest {
		oa, ecode, err := t.HeadCold(lom, r)
		if err != nil {
			switch {
			case ecode == http.StatusTooManyRequests || ecode == http.StatusServiceUnavailable:
				debug.Assertf(cmn.IsErrTooManyRequests(err), "expecting err-remote-retriable, got %T", err)
			case ecode != http.StatusNotFound:
				err = cmn.NewErrFailedTo(t, "HEAD", lom.Cname(), err)
			case latest:
				ecode = http.StatusGone
			}
			return ecode, err
		}
		if apc.IsFltNoProps(fltPresence) {
			return 0, nil
		}

		if exists && latest {
			if e := lom.ObjAttrs().CheckEq(oa); e != nil {
				return http.StatusConflict, cmn.NewErrRemoteMetadataMismatch(e)
			}
			attrs = lom.ObjAttrs()
		} else {
			oa.Atime = 0
			attrs = oa
		}
	} else {
		attrs = lom.ObjAttrs()
	}

	// Build and serialize to response headers (V2: selective properties)
	requestedProps := dpq.get(apc.QparamProps)
	writeObjPropsV2ToHeader(lom, exists, attrs, requestedProps, whdr)

	delta := mono.SinceNano(started)
	vlabs := bvlabs(bck)
	t.statsT.IncWith(stats.HeadCount, vlabs)
	t.statsT.AddWith(
		cos.NamedVal64{Name: stats.HeadLatencyTotal, Value: delta, VarLabs: vlabs},
	)
	return 0, nil
}

// writeObjPropsV2ToHeader serializes selective properties to HTTP headers
func writeObjPropsV2ToHeader(lom *core.LOM, exists bool, attrs *cmn.ObjAttrs, requestedProps string, hdr http.Header) {
	if attrs.Cksum == nil {
		attrs.Cksum = cos.NoneCksum
	}
	// Serialize base ObjAttrs to headers
	cmn.ToHeader(attrs, hdr, attrs.Size)

	// Set present header
	hdr.Set(apc.PropToHeader("present"), strconv.FormatBool(exists))

	if !exists {
		return
	}

	hdr.Set(cos.HdrLastModified, lom.LastModifiedStr())
	if etag := lom.ETag(true /*allowGen*/); etag != "" {
		hdr.Set(cos.HdrETag, etag)
	}

	// Populate optional fields based on requested properties
	for prop := range strings.SplitSeq(requestedProps, apc.LsPropsSepa) {
		prop = strings.TrimSpace(prop)
		switch prop {
		case apc.GetPropsLocation:
			hdr.Set(cmn.PropToHeader("location"), lom.Location())
		case apc.GetPropsCopies:
			mirror := cmn.Mirror{Copies: lom.NumCopies(), Paths: lom.MirrorPaths()}
			mirror.ToHeader(hdr)
		case apc.GetPropsEC:
			if lom.ECEnabled() {
				if md, err := ec.ObjectMetadata(lom.Bck(), lom.ObjName); err == nil {
					ecmd := &cmn.EC{
						Generation:   md.Generation,
						DataSlices:   md.Data,
						ParitySlices: md.Parity,
						IsECCopy:     md.IsCopy,
					}
					ecmd.ToHeader(hdr)
				}
			}
		case apc.GetPropsChunked:
			if lom.IsChunked() {
				ufest, err := core.NewUfest("", lom, true /*mustExist*/)
				if err == nil {
					lom.Lock(false)
					if err = ufest.LoadCompleted(lom); err == nil {
						var maxChunkSize int64
						count := ufest.Count()
						for i := 1; i <= count; i++ {
							if chunk, errChunk := ufest.GetChunk(i); errChunk == nil && chunk != nil {
								if chunkSize := chunk.Size(); chunkSize > maxChunkSize {
									maxChunkSize = chunkSize
								}
							}
						}
						chunks := &cmn.Chunks{ChunkCount: count, MaxChunkSize: maxChunkSize}
						chunks.ToHeader(hdr)
					}
					lom.Unlock(false)
				}
			}
		}
	}
}
