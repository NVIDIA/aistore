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
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
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

	whdr.Set(apc.PropToHeader("present"), strconv.FormatBool(exists))

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
	if err := _objHeadV2(lom, exists, attrs, requestedProps, whdr); err != nil {
		return http.StatusBadRequest, err
	}

	delta := mono.SinceNano(started)
	vlabs := bvlabs(bck)
	t.statsT.IncWith(stats.HeadCount, vlabs)
	t.statsT.AddWith(
		cos.NamedVal64{Name: stats.HeadLatencyTotal, Value: delta, VarLabs: vlabs},
	)
	return 0, nil
}

// serialize assorted properties to response header
func _objHeadV2(lom *core.LOM, exists bool, attrs *cmn.ObjAttrs, requestedProps string, hdr http.Header) error {
	var (
		withChecksum     bool
		withAtime        bool
		withVersion      bool
		withCustom       bool
		withLastModified bool
		withETag         bool
	)
	if requestedProps != "" {
		for prop := range strings.SplitSeq(requestedProps, apc.LsPropsSepa) {
			prop = strings.TrimSpace(prop)
			if prop == "" {
				continue
			}
			switch prop {
			// name and size are always included (no-op, but valid props)
			case apc.GetPropsName, apc.GetPropsSize:
			// base attrs (via cmn.ToHeaderV2)
			case apc.GetPropsChecksum:
				withChecksum = true
			case apc.GetPropsAtime:
				withAtime = true
			case apc.GetPropsVersion:
				withVersion = true
			case apc.GetPropsCustom:
				withCustom = true
			case apc.GetPropsLastModified:
				withLastModified = true
			case apc.GetPropsETag:
				withETag = true

			// extended attrs
			case apc.GetPropsLocation:
				if exists {
					hdr.Set(cmn.PropToHeader("location"), lom.Location())
				}
			case apc.GetPropsCopies:
				if exists {
					mirror := cmn.Mirror{Copies: lom.NumCopies(), Paths: lom.MirrorPaths()}
					mirror.ToHeader(hdr)
				}
			case apc.GetPropsEC:
				if exists && lom.ECEnabled() {
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
				if exists && lom.IsChunked() {
					lom.Lock(false)
					_chunksHeadV2(lom, hdr)
					lom.Unlock(false)
				}
			default:
				return fmt.Errorf("invalid property %q in props list", prop)
			}
		}
	}

	// serialize requested base attrs: atime, checksum, etc.
	cmn.ToHeaderV2(attrs, hdr, withChecksum, withAtime, withVersion, withCustom)

	// finally, ETag and LastModified (NOTE: may execute syscall)
	var (
		mtimeStr string
		mtime    time.Time
	)
	if withLastModified {
		if mtimeStr, mtime = lom.LastModifiedStr(); mtimeStr != "" {
			hdr.Set(cos.HdrLastModified, mtimeStr)
		}
	}
	if withETag {
		if etag := lom.ETag(mtime, true /*allow syscall*/); etag != "" { // empty w/ no checksum
			hdr.Set(cos.HdrETag, cmn.QuoteETag(etag))
		}
	}
	return nil
}

// TODO: revisit to consider average chunk size, and more
func _chunksHeadV2(lom *core.LOM, hdr http.Header) {
	ufest, err := core.NewUfest("", lom, true /*mustExist*/)
	if err != nil {
		nlog.Errorln(err, "[", lom.Cname(), "]") // (unlikely)
		return
	}
	if err = ufest.LoadCompleted(lom); err != nil {
		if cmn.Rom.V(4, cos.ModAIS) {
			nlog.Warningln(err, "[", lom.Cname(), "]")
		}
		return
	}

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
