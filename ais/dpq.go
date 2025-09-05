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

// DPQ is, essentially, performance-motivated alternative to the conventional url.Query.
// The parsing follows 6 distinct categories:
//
// 1. Bucket (provider, namespace) - receive special handling due to their
//    fundamental role in bucket identification. Provider stays as-is, namespace
//    gets URL-unescaped.
//
// 2. Archive fields (path, mime, regx, mode) - retain dedicated struct members and
//    specialized parsing logic including validation.
//
// 3. Boolean fields (skipVC, isGFN, dontAddRemote, silent, et al.) -
//    remain as struct members for zero memory cost, direct access
//    performance, and type safety. No unescaping.
//
// 4. Special system fields (ptime, pid, uuid, owt, origURL, objto) - core system
//    parameters that need direct struct access for performance and are used throughout
//    the codebase. Some require URL unescaping.
//
// 5. Parameters that don't need unescaping (MptPartNo, FltPresence, BinfoWithOrWithoutRemote) -
//    stored directly in the map as-is to avoid unnecessary processing.
//
// 6. All other parameters - stored in the map with URL unescaping applied by default.
//    This includes ETL parameters, multipart upload parameters, append parameters,
//    and any future additions. Exceptions are made for S3 headers (X-Amz-* prefix)
//    and explicitly listed exceptions in the _except_ map.
//
// BEWARE:
//
// When adding a new query parameter, PLEASE DO NOT rely on the default (category 6).
// Instead, explicitly add each new query parameter to the most appropriate category
// 1 through 5 (above).

type dpq struct {
	bck struct {
		provider, namespace string // bucket
	}
	arch struct {
		path, mime, regx, mmode string // QparamArchpath et al. (plus archmode below) - keep as-is
	}

	m     cos.StrKVs
	count int

	ptime   string // req timestamp at calling/redirecting proxy (QparamUnixTime)
	pid     string // QparamPID
	uuid    string // xaction
	origURL string // ht://url->
	owt     string // object write transaction { OwtPut, ... }
	objto   string // uname of the destination object

	// Boolean fields - keep as struct members
	skipVC        bool // QparamSkipVC (skip loading existing object's metadata)
	isGFN         bool // QparamIsGFNRequest
	dontAddRemote bool // QparamDontAddRemote
	silent        bool // QparamSilent
	latestVer     bool // QparamLatestVer
	sync          bool // QparamSync
	isS3          bool // special use: frontend S3 API
}

var _except = map[string]bool{
	apc.QparamDontHeadRemote: false,

	// flows that utilize the following query parameters perform conventional r.URL.Query()
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
	dpqPool sync.Pool
	dpq0    dpq
)

const (
	dpqIniCap = 8
	dpqMaxCap = 100
)

func dpqAlloc() *dpq {
	v := dpqPool.Get()
	if v == nil {
		return &dpq{m: make(cos.StrKVs, dpqIniCap)}
	}
	dpq := v.(*dpq)
	if dpq.m == nil { // (unlikely)
		dpq.m = make(cos.StrKVs, dpqIniCap)
	}
	return dpq
}

func dpqFree(dpq *dpq) {
	c := dpq.count
	m := dpq.m
	clear(m)
	*dpq = dpq0
	if c >= max(dpqMaxCap-16, dpqIniCap*4) {
		dpq.m = make(cos.StrKVs, dpqIniCap)
	} else {
		dpq.m = m
	}
	dpqPool.Put(dpq)
}

func (dpq *dpq) get(qparam string) string { return dpq.m[qparam] }

func (dpq *dpq) parse(rawQuery string) error {
	var (
		query = rawQuery // r.URL.RawQuery
		iters int
	)
	dpq.count = 0
	for query != "" && iters < dpqMaxCap {
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
		// 1. Bucket fields - special handling
		case apc.QparamProvider:
			dpq.bck.provider = value
		case apc.QparamNamespace:
			dpq.bck.namespace, err = _unescape(value)

		// 2. Archive fields - special parsing likewise
		case apc.QparamArchpath, apc.QparamArchmime, apc.QparamArchregx, apc.QparamArchmode:
			err = dpq._arch(key, value)

		// 3. All boolean fields are struct members
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

		// 4. Special system fields that need to be member variables
		case apc.QparamUnixTime:
			dpq.ptime = value
		case apc.QparamPID:
			dpq.pid = value
		case apc.QparamUUID:
			dpq.uuid = value
		case apc.QparamOWT:
			dpq.owt = value
		case apc.QparamOrigURL:
			dpq.origURL, err = _unescape(value)
		case apc.QparamObjTo:
			dpq.objto, err = _unescape(value)

			// 5. Parameters that don't need unescaping
		case apc.QparamMptUploadID, apc.QparamMptPartNo, apc.QparamFltPresence, apc.QparamBinfoWithOrWithoutRemote,
			apc.QparamAppendType, apc.QparamETLName, apc.QparamETLTransformArgs:
			dpq.m[key] = value
			dpq.count++

		// 6. Finally a) assorted named exceptions that we simply skip, and b) all the rest parameters
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
			dpq.count++
		}

		// common error return
		if err != nil {
			return err
		}
		iters++
	}
	if iters >= dpqMaxCap {
		return errors.New("exceeded max number of dpq iterations: " + strconv.Itoa(iters))
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
		err = fmt.Errorf("query parameters archpath=%q (match one) and archregx=%q (match many) are mutually exclusive",
			apc.QparamArchpath, apc.QparamArchregx)
	}
	return err
}

func (dpq *dpq) isArch() bool { return dpq.arch.path != "" || dpq.arch.mmode != "" }

func (dpq *dpq) isRedirect() (ptime string) {
	if dpq.pid == "" || dpq.ptime == "" {
		return
	}
	return dpq.ptime
}

// err & log
func (dpq *dpq) _archstr() string {
	if dpq.arch.path != "" {
		return fmt.Sprintf("(%s=%s)", apc.QparamArchpath, dpq.arch.path)
	}
	return fmt.Sprintf("(%s=%s, %s=%s)", apc.QparamArchregx, dpq.arch.regx, apc.QparamArchmode, dpq.arch.mmode)
}
