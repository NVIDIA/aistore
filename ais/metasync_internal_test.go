// Package ais: internal unit tests
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools/tassert"

	jsoniter "github.com/json-iterator/go"
)

func TestMetasyncProxyRejectsMissingIntraHeaders(t *testing.T) {
	p := &proxy{}
	p.si = newSnode("p1", apc.Proxy)
	p.owner.smap = newSmapOwner(cmn.GCO.Get(), false /*isTarget*/)
	p.owner.smap.put(newSmap())

	payload := make(msPayload).marshal(memsys.PageMM())
	defer payload.Free()
	req := httptest.NewRequest(http.MethodPut, apc.URLPathMetasync.S, bytes.NewReader(payload.Bytes()))
	req = req.WithContext(context.WithValue(req.Context(), keyReqNet, reqNetCtrl))
	w := httptest.NewRecorder()
	p.metasyncHandler(w, req)

	herr := &cmn.ErrHTTP{}
	tassert.CheckFatal(t, jsoniter.Unmarshal(w.Body.Bytes(), herr))
	tassert.Fatalf(t, w.Code == http.StatusBadRequest && herr.Message == errNotIntraControl.Error(),
		"expected missing intra-control headers to fail with %q, got status %d and %q",
		errNotIntraControl, w.Code, herr.Message)
}

func TestExtractConfigHydratesSparse(t *testing.T) {
	current := cmn.GCO.Get()

	src := &globalConfig{}
	src.Version = current.Version + 1
	src.UUID = current.UUID
	tassert.CheckFatal(t, src.ClusterConfig.HydrateOmittables())

	sgl := src._encode(0)
	defer sgl.Free()

	// Verify that _encode actually produced the representation that caused the
	// regression: default-omittable sections are absent on the wire.
	var sparse globalConfig
	_, err := jsp.Decode(bytes.NewReader(sgl.Bytes()), &sparse, sparse.JspOpts(), "sparse config test")
	tassert.CheckFatal(t, err)

	tassert.Fatalf(t,
		sparse.Log == nil &&
			sparse.Client == nil &&
			sparse.Space == nil &&
			sparse.Transport == nil &&
			sparse.GetBatch == nil &&
			sparse.LRU == nil &&
			sparse.FSHC == nil &&
			sparse.Keepalive == nil &&
			sparse.Rebalance == nil,
		"expected sparse config, got log=%v client=%v space=%v transport=%v get-batch=%v lru=%v fshc=%v keepalive=%v rebalance=%v",
		sparse.Log, sparse.Client, sparse.Space, sparse.Transport, sparse.GetBatch,
		sparse.LRU, sparse.FSHC, sparse.Keepalive, sparse.Rebalance)

	// Exercise the real metasync receive boundary, not HydrateOmittables directly.
	payload := msPayload{revsConfTag: sgl.Bytes()}
	got, _, err := (&htrun{}).extractConfig(payload, "test")
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, got != nil, "extractConfig returned nil config")

	tassert.Fatalf(t,
		got.Log != nil &&
			got.Client != nil &&
			got.Space != nil &&
			got.Transport != nil &&
			got.GetBatch != nil &&
			got.LRU != nil &&
			got.FSHC != nil &&
			got.Keepalive != nil &&
			got.Rebalance != nil,
		"decoded config not hydrated: log=%v client=%v space=%v transport=%v get-batch=%v lru=%v fshc=%v keepalive=%v rebalance=%v",
		got.Log, got.Client, got.Space, got.Transport, got.GetBatch,
		got.LRU, got.FSHC, got.Keepalive, got.Rebalance)

	tassert.Fatalf(t, reflect.DeepEqual(got.Log, src.Log),
		"log mismatch: got %+v, expected %+v", got.Log, src.Log)
	tassert.Fatalf(t, reflect.DeepEqual(got.Client, src.Client),
		"client mismatch: got %+v, expected %+v", got.Client, src.Client)
	tassert.Fatalf(t, reflect.DeepEqual(got.Space, src.Space),
		"space mismatch: got %+v, expected %+v", got.Space, src.Space)
	tassert.Fatalf(t, reflect.DeepEqual(got.Transport, src.Transport),
		"transport mismatch: got %+v, expected %+v", got.Transport, src.Transport)
	tassert.Fatalf(t, reflect.DeepEqual(got.GetBatch, src.GetBatch),
		"get-batch mismatch: got %+v, expected %+v", got.GetBatch, src.GetBatch)
	tassert.Fatalf(t, reflect.DeepEqual(got.LRU, src.LRU),
		"lru mismatch: got %+v, expected %+v", got.LRU, src.LRU)
	tassert.Fatalf(t, reflect.DeepEqual(got.FSHC, src.FSHC),
		"fshc mismatch: got %+v, expected %+v", got.FSHC, src.FSHC)
	tassert.Fatalf(t, reflect.DeepEqual(got.Keepalive, src.Keepalive),
		"keepalive mismatch: got %+v, expected %+v", got.Keepalive, src.Keepalive)
	tassert.Fatalf(t, reflect.DeepEqual(got.Rebalance, src.Rebalance),
		"rebalance mismatch: got %+v, expected %+v", got.Rebalance, src.Rebalance)
}
