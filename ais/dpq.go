// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// Data Path Query parameters (DPQ)

// DPQ is a performance-motivated alternative to the conventional url.Query.
// The parsing follows 7 distinct categories:
//
// 1. Parameters that don't need unescaping (MptPartNo, FltPresence, BinfoWithOrWithoutRemote) -
//    stored directly in the map as-is to avoid unnecessary processing.
//
// 2. Archive fields (path, mime, regx, mode) - retain dedicated struct members and
//    specialized parsing logic including validation.
//
// 3. Bucket (provider, namespace) - receive special handling due to their
//    fundamental role. Provider stays as-is, namespace gets URL-unescaped.
//
// 4. Special system fields (ptime, pid, uuid, owt, origURL, objto) - system
//    parameters that are used throughout the codebase. Some require URL unescaping.
//
// 5. CSK/HMAC signature verification fields.
//
// 6. Boolean fields (skipVC, isGFN, dontAddRemote, silent, et al.) -
//    remain as struct members for zero memory cost, direct access
//    performance, and type safety. No unescaping.
//
// 7. All other parameters - stored in the map with URL unescaping applied by default.
//    This includes ETL parameters, multipart upload parameters, append parameters,
//    and any future additions. Exceptions are made for S3 headers (X-Amz-* prefix)
//    and explicitly listed exceptions in the _except_ map.
//
// Note: it's always a good idea to explicitly add each and every new query into one of the
// non-default categories, potentially 3, 4, or 5.

const dpqTag = "dpq"

type (
	cskgrp struct {
		nonce   uint64 // QparamNonce
		smapVer int64  // QparamSmapVer
		hmacSig string // QparamHMAC
	}
	dpq struct {
		m    cos.StrKVs // see groups 1 and 7 above
		arch struct {
			path, mime, regx, mmode string // QparamArchpath et al. (plus archmode below) - keep as-is
		}
		bck struct {
			provider, namespace string // bucket
		}
		sys struct {
			pid     string // QparamPID
			ptime   string // req timestamp at calling/redirecting proxy (QparamUnixTime)
			uuid    string // xaction
			origURL string // ht://url->
			owt     string // object write transaction { OwtPut, ... }
			objto   string // uname of the destination object
		}
		csk cskgrp // csk envelope (group CSK/HMAC)

		// boolean fields
		skipVC        bool // QparamSkipVC (skip loading existing object's metadata)
		isGFN         bool // QparamIsGFNRequest
		dontAddRemote bool // QparamDontAddRemote
		silent        bool // QparamSilent
		latestVer     bool // QparamLatestVer
		sync          bool // QparamSync

		// Special use (internal context)
		isS3 bool // frontend S3 API
	}
)

var _except = map[string]bool{
	apc.QparamDontHeadRemote: false,

	// flows that utilize the following query parameters perform conventional r.URL.Query()
	// TODO -- FIXME: multipart must definitely transition to dpq
	s3.QparamMptUploadID: false,
	s3.QparamMptUploads:  false,
	s3.QparamMptPartNo:   false,

	s3.QparamAccessKeyID: false,
	s3.QparamExpires:     false,
	s3.QparamSignature:   false,
	s3.QparamXID:         false,

	// plus, all headers that have s3.HeaderPrefix "X-Amz-"
}

var (
	dpqPool = sync.Pool{
		New: func() any { return &dpq{m: make(cos.StrKVs, iniDpqCap)} },
	}
	dpq0 dpq
)

const (
	iniDpqCap = 8
	maxDpqCap = 100
)

func dpqAlloc() (d *dpq) {
	d = dpqPool.Get().(*dpq)
	return
}

func dpqFree(d *dpq) {
	c := len(d.m)
	m := d.m

	clear(m)
	*d = dpq0
	if c >= max(maxDpqCap-16, iniDpqCap*4) {
		d.m = make(cos.StrKVs, iniDpqCap) // gc & new
	} else {
		d.m = m // reuse
	}
	dpqPool.Put(d)
}

func (dpq *dpq) get(qparam string) string { return dpq.m[qparam] }

func (dpq *dpq) parse(rawQuery string) error {
	var (
		query = rawQuery // r.URL.RawQuery
		iters int
	)
	for query != "" && iters < maxDpqCap {
		var (
			err   error
			value string
			key   = query
		)
		if i := strings.IndexByte(key, '&'); i >= 0 {
			key, query = key[:i], key[i+1:]
		} else {
			query = "" // last iter
		}
		if k, v, ok := _dpqKeqV(key); ok {
			key, value = k, v
		}

		switch key {
		// Bucket fields - special handling
		case apc.QparamProvider:
			dpq.bck.provider = value
		case apc.QparamNamespace:
			dpq.bck.namespace, err = _unescape(value)

		// Archive fields - special parsing likewise
		case apc.QparamArchpath, apc.QparamArchmime, apc.QparamArchregx, apc.QparamArchmode:
			err = dpq._arch(key, value)

		// All boolean fields are struct members
		case apc.QparamSkipVC:
			dpq.skipVC = cos.IsParseBool(value)
		case apc.QparamIsGFNRequest:
			dpq.isGFN = cos.IsParseBool(value)
		case apc.QparamDontAddRemote:
			dpq.dontAddRemote = cos.IsParseBool(value)
		case apc.QparamSilent:
			dpq.silent = cos.IsParseBool(value)
		case apc.QparamLatestVer:
			dpq.latestVer = cos.IsParseBool(value)
		case apc.QparamSync:
			dpq.sync = cos.IsParseBool(value)

		// System fields
		case apc.QparamUnixTime:
			dpq.sys.ptime = value
		case apc.QparamPID:
			dpq.sys.pid = value
		case apc.QparamUUID:
			dpq.sys.uuid = value
		case apc.QparamOWT:
			dpq.sys.owt = value
		case apc.QparamOrigURL:
			dpq.sys.origURL, err = _unescape(value)
		case apc.QparamObjTo:
			dpq.sys.objto, err = _unescape(value)

		// CSK/HMAC fields
		case apc.QparamNonce:
			dpq.csk.nonce, err = strconv.ParseUint(value, cskBase, 64)
		case apc.QparamSmapVer:
			dpq.csk.smapVer, err = strconv.ParseInt(value, cskBase, 64)
		case apc.QparamHMAC:
			dpq.csk.hmacSig = value // (base64.RawURLEncoding)

		// Parameters that don't need unescaping
		case apc.QparamMptUploadID, apc.QparamMptPartNo, apc.QparamFltPresence, apc.QparamBinfoWithOrWithoutRemote,
			apc.QparamAppendType, apc.QparamETLName, apc.QparamETLTransformArgs:
			dpq.m[key] = value

		// Finally, assorted named exceptions that we simply skip, and b) all the rest parameters
		default:
			// Check for exceptions first
			if strings.HasPrefix(key, s3.HeaderPrefix) {
				continue
			}
			if _, ok := _except[key]; ok {
				continue
			}

			// Default: unescape and store in map
			dpq.m[key], err = _unescape(value)
		}

		// common error return
		if err != nil {
			if errors.Is(err, strconv.ErrSyntax) {
				err = strconv.ErrSyntax
			}
			return fmt.Errorf("%s: '%s=%s', err: %v", dpqTag, key, value, err)
		}
		iters++
	}
	if iters >= maxDpqCap {
		return fmt.Errorf("%s: exceeded maximum number of iterations (%d)", dpqTag, iters)
	}

	return nil
}

func _dpqKeqV(s string) (string, string, bool) {
	if i := strings.IndexByte(s, '='); i > 0 {
		return s[:i], s[i+1:], true
	}
	return s, "", false
}

func _unescape(s string) (string, error) {
	if !strings.ContainsAny(s, "%+") {
		return s, nil
	}
	return url.QueryUnescape(s)
}

//
// archive query
//

// parse & validate - keep existing logic
func (dpq *dpq) _arch(key, val string) (err error) {
	switch key {
	case apc.QparamArchpath:
		dpq.arch.path, err = _unescape(val)
		if err == nil && dpq.arch.path != "" {
			if dpq.arch.path[0] == '/' {
				dpq.arch.path = dpq.arch.path[1:]
			}
			err = cos.ValidateArchpath(dpq.arch.path)
		}
	case apc.QparamArchmime:
		dpq.arch.mime, err = _unescape(val)
	case apc.QparamArchregx:
		dpq.arch.regx, err = _unescape(val)
	case apc.QparamArchmode:
		dpq.arch.mmode, err = archive.ValidateMatchMode(val)
	}
	if err != nil {
		return err
	}
	// either/or
	if dpq.arch.path != "" && dpq.arch.mmode != "" { // (empty arch.regx is fine - is EmptyMatchAny)
		err = fmt.Errorf("%s: archpath=%q (match one) and archregx=%q (match many) are mutually exclusive", dpqTag,
			apc.QparamArchpath, apc.QparamArchregx)
	}
	return err
}

func (dpq *dpq) isArch() bool { return dpq.arch.path != "" || dpq.arch.mmode != "" }

func (dpq *dpq) isRedirect() (ptime string) {
	if dpq.sys.pid == "" || dpq.sys.ptime == "" {
		return
	}
	return dpq.sys.ptime
}

// err & log
func (dpq *dpq) _archstr() string {
	if dpq.arch.path != "" {
		return fmt.Sprintf("(%s=%s)", apc.QparamArchpath, dpq.arch.path)
	}
	return fmt.Sprintf("(%s=%s, %s=%s)", apc.QparamArchregx, dpq.arch.regx, apc.QparamArchmode, dpq.arch.mmode)
}

//
// url.Values => cskgrp
//

func cskFromQ(q url.Values) (*cskgrp, error) {
	const tag = "from-q"
	sig := q.Get(apc.QparamHMAC)
	if sig == "" {
		return nil, nil
	}
	if len(sig) != cskSigLen {
		return nil, fmt.Errorf("%s: invalid signature length: %d", tag, len(sig))
	}

	s := q.Get(apc.QparamNonce)
	nonce, err := strconv.ParseUint(s, cskBase, 64)
	if err != nil {
		return nil, fmt.Errorf("%s: invalid nonce '%s=%s': %v", tag, apc.QparamNonce, s, err)
	}

	s = q.Get(apc.QparamSmapVer)
	smapVer, err := strconv.ParseInt(s, cskBase, 64)
	if err != nil {
		return nil, fmt.Errorf("%s: invalid Smap version %q: %v", tag, s, err)
	}
	return &cskgrp{nonce: nonce, smapVer: smapVer, hmacSig: sig}, nil
}
