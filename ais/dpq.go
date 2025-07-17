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
	"github.com/NVIDIA/aistore/cmn/nlog"
)

// RESTful API: datapath query parameters
type dpq struct {
	bck struct {
		provider, namespace string // bucket
	}
	apnd struct {
		ty, hdl string // QparamAppendType, QparamAppendHandle
	}
	arch struct {
		path, mime, regx, mmode string // QparamArchpath et al. (plus archmode below)
	}
	etl struct {
		name, targs string // QparamETLName, QparamETLTransformArgs
	}

	ptime       string // req timestamp at calling/redirecting proxy (QparamUnixTime)
	uuid        string // xaction
	origURL     string // ht://url->
	owt         string // object write transaction { OwtPut, ... }
	fltPresence string // QparamFltPresence
	binfo       string // bucket info, with or without requirement to summarize remote obj-s
	objto       string // uname of the destination object

	skipVC        bool // QparamSkipVC (skip loading existing object's metadata)
	isGFN         bool // QparamIsGFNRequest
	dontAddRemote bool // QparamDontAddRemote
	silent        bool // QparamSilent
	latestVer     bool // QparamLatestVer
	isS3          bool // special use: frontend S3 API
}

var _except = map[string]bool{
	apc.QparamPID:            false,
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

func dpqAlloc() *dpq {
	if v := dpqPool.Get(); v != nil {
		return v.(*dpq)
	}
	return &dpq{}
}

func dpqFree(dpq *dpq) {
	*dpq = dpq0
	dpqPool.Put(dpq)
}

// Data Path Query structure (dpq):
// Parse URL query for a selected few parameters used in the datapath.
// (This is a faster alternative to the conventional and RFC-compliant URL.Query()
// to be used narrowly to handle those few (keys) and nothing else.)

const maxNumQparams = 100

func (dpq *dpq) parse(rawQuery string) error {
	var (
		iters int
		query = rawQuery // r.URL.RawQuery
	)

	for query != "" && iters < maxNumQparams {
		var (
			value string
			key   = query
		)
		if i := strings.IndexByte(key, '&'); i >= 0 {
			key, query = key[:i], key[i+1:]
			iters++
		} else {
			query = "" // last iter
		}
		if k, v, ok := _dpqKeqV(key); ok {
			key, value = k, v
		}

		// supported URL query parameters explicitly named below; attempt to parse anything
		// outside this list will fail
		switch key {
		case apc.QparamProvider:
			dpq.bck.provider = value
		case apc.QparamNamespace:
			var err error
			if dpq.bck.namespace, err = url.QueryUnescape(value); err != nil {
				return err
			}
		case apc.QparamObjTo:
			var err error
			if dpq.objto, err = url.QueryUnescape(value); err != nil {
				return err
			}
		case apc.QparamSkipVC:
			dpq.skipVC = cos.IsParseBool(value)
		case apc.QparamUnixTime:
			dpq.ptime = value
		case apc.QparamUUID:
			dpq.uuid = value
		case apc.QparamArchpath, apc.QparamArchmime, apc.QparamArchregx, apc.QparamArchmode:
			if err := dpq._arch(key, value); err != nil {
				return err
			}
		case apc.QparamIsGFNRequest:
			dpq.isGFN = cos.IsParseBool(value)
		case apc.QparamOrigURL:
			var err error
			if dpq.origURL, err = url.QueryUnescape(value); err != nil {
				return err
			}
		case apc.QparamAppendType:
			dpq.apnd.ty = value
		case apc.QparamAppendHandle:
			var err error
			if dpq.apnd.hdl, err = url.QueryUnescape(value); err != nil {
				return err
			}
		case apc.QparamOWT:
			dpq.owt = value

		case apc.QparamFltPresence:
			dpq.fltPresence = value
		case apc.QparamDontAddRemote:
			dpq.dontAddRemote = cos.IsParseBool(value)
		case apc.QparamBinfoWithOrWithoutRemote:
			dpq.binfo = value

		case apc.QparamETLName:
			dpq.etl.name = value
		case apc.QparamETLTransformArgs:
			dpq.etl.targs = value
		case apc.QparamSilent:
			dpq.silent = cos.IsParseBool(value)
		case apc.QparamLatestVer:
			dpq.latestVer = cos.IsParseBool(value)

		default: // the key must be known or `_except`-ed
			if strings.HasPrefix(key, s3.HeaderPrefix) {
				continue
			}
			if _, ok := _except[key]; !ok {
				err := fmt.Errorf("invalid query parameter: %q (raw query: %q)", key, rawQuery)
				nlog.Errorln(err)
				return err
			}
		}
	}
	if iters >= maxNumQparams {
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

//
// archive query
//

// parse & validate
func (dpq *dpq) _arch(key, val string) (err error) {
	switch key {
	case apc.QparamArchpath:
		dpq.arch.path, err = url.QueryUnescape(val)
		if err == nil && dpq.arch.path != "" {
			err = cos.ValidateArchpath(dpq.arch.path)
		}
	case apc.QparamArchmime:
		dpq.arch.mime, err = url.QueryUnescape(val)
	case apc.QparamArchregx:
		dpq.arch.regx, err = url.QueryUnescape(val)
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

// err & log
func (dpq *dpq) _archstr() string {
	if dpq.arch.path != "" {
		return fmt.Sprintf("(%s=%s)", apc.QparamArchpath, dpq.arch.path)
	}
	return fmt.Sprintf("(%s=%s, %s=%s)", apc.QparamArchregx, dpq.arch.regx, apc.QparamArchmode, dpq.arch.mmode)
}
