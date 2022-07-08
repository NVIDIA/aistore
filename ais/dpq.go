// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"net/url"
	"strings"
	"sync"

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
func (dpq *dpq) fromRawQ(rawQuery string) (err error) {
	query := rawQuery
	for query != "" {
		key, value := query, ""
		if i := strings.IndexAny(key, "&"); i >= 0 {
			key, query = key[:i], key[i+1:]
		} else {
			query = ""
		}
		if i := strings.Index(key, "="); i >= 0 {
			key, value = key[:i], key[i+1:]
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
		default:
			err = errors.New("failed to fast-parse [" + rawQuery + "]")
			return
		}
	}
	return
}
