// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

type (
	httpProvider struct {
		t         cluster.TargetPut
		clientH   *http.Client
		clientTLS *http.Client
	}
)

// interface guard
var _ cluster.BackendProvider = (*httpProvider)(nil)

func NewHTTP(t cluster.TargetPut, config *cmn.Config) cluster.BackendProvider {
	var (
		hp    = &httpProvider{t: t}
		cargs = cmn.TransportArgs{
			Timeout:         config.Client.TimeoutLong.D(),
			WriteBufferSize: config.Net.HTTP.WriteBufferSize,
			ReadBufferSize:  config.Net.HTTP.ReadBufferSize,
		}
		sargs = cmn.TLSArgs{
			SkipVerify: true, // TODO: may need more tls config to access remote URLs
		}
	)
	hp.clientH = cmn.NewClient(cargs)
	hp.clientTLS = cmn.NewClientTLS(cargs, sargs)
	return hp
}

func (hp *httpProvider) client(u string) *http.Client {
	if cos.IsHTTPS(u) {
		return hp.clientTLS
	}
	return hp.clientH
}

func (*httpProvider) Provider() string  { return apc.HTTP }
func (*httpProvider) MaxPageSize() uint { return 10000 }

// TODO: can be done
func (hp *httpProvider) CreateBucket(*meta.Bck) (int, error) {
	return http.StatusNotImplemented, cmn.NewErrNotImpl("create", hp.Provider()+" bucket")
}

func (hp *httpProvider) HeadBucket(ctx context.Context, bck *meta.Bck) (bckProps cos.StrKVs, errCode int, err error) {
	// TODO: we should use `bck.RemoteBck()`.

	origURL, err := getOriginalURL(ctx, bck, "")
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	if verbose {
		nlog.Infof("[head_bucket] original_url: %q", origURL)
	}

	// Contact the original URL - as long as we can make connection we assume it's good.
	resp, err := hp.client(origURL).Head(origURL)
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
	bckProps[apc.HdrBackendProvider] = apc.HTTP
	return
}

func (*httpProvider) ListObjects(*meta.Bck, *apc.LsoMsg, *cmn.LsoResult) (errCode int, err error) {
	debug.Assert(false)
	return
}

func (*httpProvider) ListBuckets(cmn.QueryBcks) (bcks cmn.Bcks, errCode int, err error) {
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

func (hp *httpProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (oa *cmn.ObjAttrs, errCode int, err error) {
	var (
		h   = cmn.BackendHelpers.HTTP
		bck = lom.Bck() // TODO: This should be `cloudBck = lom.Bck().RemoteBck()`
	)
	origURL, err := getOriginalURL(ctx, bck, lom.ObjName)
	debug.AssertNoErr(err)

	if verbose {
		nlog.Infof("[head_object] original_url: %q", origURL)
	}
	resp, err := hp.client(origURL).Head(origURL)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, fmt.Errorf("error occurred: %v", resp.StatusCode)
	}
	oa = &cmn.ObjAttrs{}
	oa.SetCustomKey(cmn.SourceObjMD, apc.HTTP)
	if resp.ContentLength >= 0 {
		oa.Size = resp.ContentLength
	}
	if v, ok := h.EncodeVersion(resp.Header.Get(cos.HdrETag)); ok {
		oa.SetCustomKey(cmn.ETag, v)
	}
	if verbose {
		nlog.Infof("[head_object] %s", lom)
	}
	return
}

func (hp *httpProvider) GetObj(ctx context.Context, lom *cluster.LOM, owt cmn.OWT) (int, error) {
	res := hp.GetObjReader(ctx, lom)
	if res.Err != nil {
		return res.ErrCode, res.Err
	}
	params := allocPutObjParams(res, owt)
	res.Err = hp.t.PutObject(lom, params)
	cluster.FreePutObjParams(params)
	if res.Err != nil {
		return 0, res.Err
	}
	if verbose {
		nlog.Infof("[get_object] %s", lom)
	}
	return 0, nil
}

func (hp *httpProvider) GetObjReader(ctx context.Context, lom *cluster.LOM) (res cluster.GetReaderResult) {
	var (
		resp *http.Response
		h    = cmn.BackendHelpers.HTTP
		bck  = lom.Bck() // TODO: This should be `cloudBck = lom.Bck().RemoteBck()`
	)

	origURL, err := getOriginalURL(ctx, bck, lom.ObjName)
	debug.AssertNoErr(err)

	if verbose {
		nlog.Infof("[HTTP CLOUD][GET] original_url: %q", origURL)
	}

	resp, res.Err = hp.client(origURL).Get(origURL) //nolint:bodyclose // is closed by the caller
	if res.Err != nil {
		res.ErrCode = http.StatusInternalServerError
		return
	}
	if resp.StatusCode != http.StatusOK {
		res.ErrCode = resp.StatusCode
		res.Err = fmt.Errorf("error occurred: %v", resp.StatusCode)
		return
	}

	if verbose {
		nlog.Infof("[HTTP CLOUD][GET] success, size: %d", resp.ContentLength)
	}

	lom.SetCustomKey(cmn.SourceObjMD, apc.HTTP)
	lom.SetCustomKey(cmn.OrigURLObjMD, origURL)
	if v, ok := h.EncodeVersion(resp.Header.Get(cos.HdrETag)); ok {
		lom.SetCustomKey(cmn.ETag, v)
	}
	res.Size = resp.ContentLength
	res.R = resp.Body
	return
}

func (*httpProvider) PutObj(io.ReadCloser, *cluster.LOM) (int, error) {
	return http.StatusBadRequest, cmn.NewErrUnsupp("PUT", " objects => HTTP backend")
}

func (*httpProvider) DeleteObj(*cluster.LOM) (int, error) {
	return http.StatusBadRequest, cmn.NewErrUnsupp("DELETE", " objects from HTTP backend")
}
