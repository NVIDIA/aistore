// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"net/url"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
)

// RESTful API parse context
type dpq struct {
	provider, namespace string // bucket
	pid, ptime          string // proxy ID, timestamp
	uuid                string // xaction
	skipVC              string // (skip loading existing object's metadata)
	archpath, archmime  string // archive
	isGFN               string // ditto
	origURL             string // ht://url->
	appendTy, appendHdl string // APPEND { apc.AppendOp, ... }
	owt                 string // object write transaction { OwtPut, ... }
	fltPresence         string // QparamFltPresence
	dontHeadRemote      string // QparamDontHeadRemote
	dontAddRemote       string // QparamDontAddRemote
	bsummRemote         string // QparamBsummRemote
	etlName             string // QparamETLName
	silent              string // QparamSilent
	latestVer           string // QparamLatestVer
	// special use: s3 only
	isS3 string
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
func (dpq *dpq) parse(rawQuery string) (err error) {
	query := rawQuery
	for query != "" {
		key, value := query, ""
		if i := strings.IndexByte(key, '&'); i >= 0 {
			key, query = key[:i], key[i+1:]
		} else {
			query = ""
		}
		if k, v, ok := keyEQval(key); ok {
			key, value = k, v
		}
		// supported URL query parameters explicitly named below; attempt to parse anything
		// outside this list will fail
		switch key {
		case apc.QparamProvider:
			dpq.provider = value
		case apc.QparamNamespace:
			if dpq.namespace, err = url.QueryUnescape(value); err != nil {
				return
			}
		case apc.QparamSkipVC:
			dpq.skipVC = value
		case apc.QparamProxyID:
			dpq.pid = value
		case apc.QparamUnixTime:
			dpq.ptime = value
		case apc.QparamUUID:
			dpq.uuid = value
		case apc.QparamArchpath:
			if dpq.archpath, err = url.QueryUnescape(value); err != nil {
				return
			}
		case apc.QparamArchmime:
			if dpq.archmime, err = url.QueryUnescape(value); err != nil {
				return
			}
		case apc.QparamIsGFNRequest:
			dpq.isGFN = value
		case apc.QparamOrigURL:
			if dpq.origURL, err = url.QueryUnescape(value); err != nil {
				return
			}
		case apc.QparamAppendType:
			dpq.appendTy = value
		case apc.QparamAppendHandle:
			if dpq.appendHdl, err = url.QueryUnescape(value); err != nil {
				return
			}
		case apc.QparamOWT:
			dpq.owt = value

		case apc.QparamFltPresence:
			dpq.fltPresence = value
		case apc.QparamDontHeadRemote:
			dpq.dontHeadRemote = value
		case apc.QparamDontAddRemote:
			dpq.dontAddRemote = value
		case apc.QparamBsummRemote:
			dpq.bsummRemote = value

		case apc.QparamETLName:
			dpq.etlName = value
		case apc.QparamSilent:
			dpq.silent = value
		case apc.QparamLatestVer:
			dpq.latestVer = value

		case s3.QparamMptUploadID, s3.QparamMptUploads, s3.QparamMptPartNo:
			// TODO: ignore for now
		default:
			err = errors.New("failed to fast-parse [" + rawQuery + "]")
			return
		}
	}
	return
}

func keyEQval(s string) (string, string, bool) {
	if i := strings.IndexByte(s, '='); i > 0 {
		return s[:i], s[i+1:], true
	}
	return s, "", false
}
