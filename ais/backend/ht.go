//go:build ht

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
)

type (
	htbp struct {
		t      core.TargetPut
		cliH   *http.Client
		cliTLS *http.Client
		base
	}
)

// interface guard
var _ core.Backend = (*htbp)(nil)

func NewHT(t core.TargetPut, config *cmn.Config, tstats stats.Tracker) (core.Backend, error) {
	bp := &htbp{
		t:    t,
		base: base{provider: apc.HT},
	}
	bp.cliH, bp.cliTLS = cmn.NewDefaultClients(config.Client.TimeoutLong.D())
	bp.init(t.Snode(), tstats)
	return bp, nil
}

func (htbp *htbp) client(u string) *http.Client {
	if cos.IsHTTPS(u) {
		return htbp.cliTLS
	}
	return htbp.cliH
}

func (htbp *htbp) HeadBucket(ctx context.Context, bck *meta.Bck) (bckProps cos.StrKVs, ecode int, err error) {
	// TODO: we should use `bck.RemoteBck()`.

	origURL, err := getOriginalURL(ctx, bck, "")
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	if cmn.Rom.FastV(4, cos.SmoduleBackend) {
		nlog.Infof("[head_bucket] original_url: %q", origURL)
	}

	// Contact the original URL - as long as we can make connection we assume it's good.
	resp, err := htbp.client(origURL).Head(origURL)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("HEAD(%s) failed, status %d", origURL, resp.StatusCode)
		return nil, resp.StatusCode, err
	}

	if resp.Header.Get(cos.HdrETag) == "" {
		// TODO: improve validation
		nlog.Errorf("Warning: missing header %s (response header: %+v)", cos.HdrETag, resp.Header)
	}

	bckProps = make(cos.StrKVs)
	bckProps[apc.HdrBackendProvider] = apc.HT
	return
}

func (*htbp) ListObjects(*meta.Bck, *apc.LsoMsg, *cmn.LsoRes) (ecode int, err error) {
	debug.Assert(false)
	return
}

func (*htbp) ListBuckets(cmn.QueryBcks) (bcks cmn.Bcks, ecode int, err error) {
	debug.Assert(false)
	return
}

func getOriginalURL(ctx context.Context, bck *meta.Bck, objName string) (string, error) {
	origURL, ok := ctx.Value(cos.CtxOriginalURL).(string)
	if !ok || origURL == "" {
		if bck.Props == nil {
			return "", fmt.Errorf("failed to HEAD (%s): original_url is empty", bck)
		}
		origURL = bck.Props.Extra.HTTP.OrigURLBck
		debug.Assert(origURL != "")
		if objName != "" {
			origURL = cos.JoinPath(origURL, objName) // see `cmn.URL2BckObj`
		}
	}
	return origURL, nil
}

func (htbp *htbp) HeadObj(ctx context.Context, lom *core.LOM, _ *http.Request) (oa *cmn.ObjAttrs, ecode int, err error) {
	var (
		h   = cmn.BackendHelpers.HTTP
		bck = lom.Bck() // TODO: This should be `cloudBck = lom.Bck().RemoteBck()`
	)
	origURL, err := getOriginalURL(ctx, bck, lom.ObjName)
	debug.AssertNoErr(err)

	if cmn.Rom.FastV(4, cos.SmoduleBackend) {
		nlog.Infof("[head_object] original_url: %q", origURL)
	}
	resp, err := htbp.client(origURL).Head(origURL)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, fmt.Errorf("error occurred: %v", resp.StatusCode)
	}
	oa = &cmn.ObjAttrs{}
	oa.SetCustomKey(cmn.SourceObjMD, apc.HT)
	if resp.ContentLength >= 0 {
		oa.Size = resp.ContentLength
	}
	if v, ok := h.EncodeVersion(resp.Header.Get(cos.HdrETag)); ok {
		oa.SetCustomKey(cmn.ETag, v)
	}
	if cmn.Rom.FastV(4, cos.SmoduleBackend) {
		nlog.Infof("[head_object] %s", lom)
	}
	return
}

func (htbp *htbp) GetObj(ctx context.Context, lom *core.LOM, owt cmn.OWT, _ *http.Request) (int, error) {
	res := htbp.GetObjReader(ctx, lom, 0, 0)
	if res.Err != nil {
		return res.ErrCode, res.Err
	}
	params := allocPutParams(res, owt)
	res.Err = htbp.t.PutObject(lom, params)
	core.FreePutParams(params)
	if res.Err != nil {
		return 0, res.Err
	}
	if cmn.Rom.FastV(4, cos.SmoduleBackend) {
		nlog.Infof("[get_object] %s", lom)
	}
	return 0, nil
}

func (htbp *htbp) GetObjReader(ctx context.Context, lom *core.LOM, offset, length int64) (res core.GetReaderResult) {
	var (
		req  *http.Request
		resp *http.Response
		h    = cmn.BackendHelpers.HTTP
		bck  = lom.Bck() // TODO: This should be `cloudBck = lom.Bck().RemoteBck()`
	)

	origURL, err := getOriginalURL(ctx, bck, lom.ObjName)
	debug.AssertNoErr(err)

	if cmn.Rom.FastV(4, cos.SmoduleBackend) {
		nlog.Infof("[HTTP CLOUD][GET] original_url: %q", origURL)
	}

	req, res.Err = http.NewRequest(http.MethodGet, origURL, http.NoBody)
	if err != nil {
		res.ErrCode = http.StatusInternalServerError
		return res
	}
	if length > 0 {
		rng := cmn.MakeRangeHdr(offset, length)
		req.Header = http.Header{cos.HdrRange: []string{rng}}
	}
	resp, res.Err = htbp.client(origURL).Do(req) //nolint:bodyclose // is closed by the caller
	if res.Err != nil {
		return res
	}
	if resp.StatusCode != http.StatusOK {
		res.ErrCode = resp.StatusCode
		res.Err = fmt.Errorf("error occurred: %v", resp.StatusCode)
		return res
	}

	if cmn.Rom.FastV(4, cos.SmoduleBackend) {
		nlog.Infof("[HTTP CLOUD][GET] success, size: %d", resp.ContentLength)
	}

	lom.SetCustomKey(cmn.SourceObjMD, apc.HT)
	lom.SetCustomKey(cmn.OrigURLObjMD, origURL)
	if v, ok := h.EncodeVersion(resp.Header.Get(cos.HdrETag)); ok {
		lom.SetCustomKey(cmn.ETag, v)
	}
	res.Size = resp.ContentLength
	res.R = resp.Body
	return res
}

func (*htbp) PutObj(io.ReadCloser, *core.LOM, *http.Request) (int, error) {
	return http.StatusBadRequest, cmn.NewErrUnsupp("PUT", " objects => HTTP backend")
}

func (*htbp) DeleteObj(*core.LOM) (int, error) {
	return http.StatusBadRequest, cmn.NewErrUnsupp("DELETE", " objects from HTTP backend")
}
