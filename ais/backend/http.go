// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
)

type (
	httpProvider struct {
		t           cluster.Target
		httpClient  *http.Client
		httpsClient *http.Client
	}
)

// interface guard
var _ cluster.BackendProvider = (*httpProvider)(nil)

func NewHTTP(t cluster.Target, config *cmn.Config) (cluster.BackendProvider, error) {
	hp := &httpProvider{t: t}
	hp.httpClient = cmn.NewClient(cmn.TransportArgs{
		Timeout:         config.Client.TimeoutLong.D(),
		WriteBufferSize: config.Net.HTTP.WriteBufferSize,
		ReadBufferSize:  config.Net.HTTP.ReadBufferSize,
		UseHTTPS:        false,
		SkipVerify:      config.Net.HTTP.SkipVerify,
	})
	hp.httpsClient = cmn.NewClient(cmn.TransportArgs{
		Timeout:         config.Client.TimeoutLong.D(),
		WriteBufferSize: config.Net.HTTP.WriteBufferSize,
		ReadBufferSize:  config.Net.HTTP.ReadBufferSize,
		UseHTTPS:        true,
		SkipVerify:      config.Net.HTTP.SkipVerify,
	})
	return hp, nil
}

func (hp *httpProvider) client(u string) *http.Client {
	if strings.HasPrefix(u, "https") {
		return hp.httpsClient
	}
	return hp.httpClient
}

func (*httpProvider) Provider() string  { return apc.ProviderHTTP }
func (*httpProvider) MaxPageSize() uint { return 10000 }

func (hp *httpProvider) CreateBucket(*cluster.Bck) (int, error) {
	// TODO: We could support it.
	return creatingBucketNotSupportedErr(hp.Provider())
}

func (hp *httpProvider) HeadBucket(ctx context.Context, bck *cluster.Bck) (bckProps cos.SimpleKVs, errCode int, err error) {
	// TODO: we should use `bck.RemoteBck()`.

	origURL, err := getOriginalURL(ctx, bck, "")
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	if verbose {
		glog.Infof("[head_bucket] original_url: %q", origURL)
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

	if resp.Header.Get(cmn.HdrETag) == "" {
		// TODO: improve validation
		glog.Errorf("Warning: missing header %s (response header: %+v)", cmn.HdrETag, resp.Header)
	}

	bckProps = make(cos.SimpleKVs)
	bckProps[apc.HdrBackendProvider] = apc.ProviderHTTP
	return
}

func (*httpProvider) ListObjects(*cluster.Bck, *apc.ListObjsMsg) (bckList *cmn.BucketList, errCode int, err error) {
	debug.Assert(false)
	return
}

func (*httpProvider) ListBuckets(cmn.QueryBcks) (bcks cmn.Bcks, errCode int, err error) {
	debug.Assert(false)
	return
}

func getOriginalURL(ctx context.Context, bck *cluster.Bck, objName string) (string, error) {
	origURL, ok := ctx.Value(cos.CtxOriginalURL).(string)
	if !ok || origURL == "" {
		if bck.Props == nil {
			return "", fmt.Errorf("failed to HEAD (%s): original_url is empty", bck.Bck)
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
		glog.Infof("[head_object] original_url: %q", origURL)
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
	oa.SetCustomKey(cmn.SourceObjMD, apc.ProviderHTTP)
	if resp.ContentLength >= 0 {
		oa.Size = resp.ContentLength
	}
	if v, ok := h.EncodeVersion(resp.Header.Get(cmn.HdrETag)); ok {
		oa.SetCustomKey(cmn.ETag, v)
	}
	if verbose {
		glog.Infof("[head_object] %s", lom)
	}
	return
}

func (hp *httpProvider) GetObj(ctx context.Context, lom *cluster.LOM, owt cmn.OWT) (errCode int, err error) {
	reader, _, errCode, err := hp.GetObjReader(ctx, lom)
	if err != nil {
		return errCode, err
	}
	params := cluster.AllocPutObjParams()
	{
		params.WorkTag = fs.WorkfileColdget
		params.Reader = reader
		params.OWT = owt
		params.Atime = time.Now()
	}
	err = hp.t.PutObject(lom, params)
	cluster.FreePutObjParams(params)
	if err != nil {
		return
	}
	if verbose {
		glog.Infof("[get_object] %s", lom)
	}
	return
}

func (hp *httpProvider) GetObjReader(ctx context.Context, lom *cluster.LOM) (r io.ReadCloser, expectedCksm *cos.Cksum,
	errCode int, err error) {
	var (
		h   = cmn.BackendHelpers.HTTP
		bck = lom.Bck() // TODO: This should be `cloudBck = lom.Bck().RemoteBck()`
	)

	origURL, err := getOriginalURL(ctx, bck, lom.ObjName)
	debug.AssertNoErr(err)

	if verbose {
		glog.Infof("[HTTP CLOUD][GET] original_url: %q", origURL)
	}

	resp, err := hp.client(origURL).Get(origURL) // nolint:bodyclose // is closed by the caller
	if err != nil {
		return nil, nil, http.StatusInternalServerError, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil, resp.StatusCode, fmt.Errorf("error occurred: %v", resp.StatusCode)
	}

	if verbose {
		glog.Infof("[HTTP CLOUD][GET] success, size: %d", resp.ContentLength)
	}

	lom.SetCustomKey(cmn.SourceObjMD, apc.ProviderHTTP)
	lom.SetCustomKey(cmn.OrigURLObjMD, origURL)
	if v, ok := h.EncodeVersion(resp.Header.Get(cmn.HdrETag)); ok {
		lom.SetCustomKey(cmn.ETag, v)
	}
	setSize(ctx, resp.ContentLength)
	return wrapReader(ctx, resp.Body), nil, 0, nil
}

func (hp *httpProvider) PutObj(io.ReadCloser, *cluster.LOM) (int, error) {
	return http.StatusBadRequest, fmt.Errorf(cmn.FmtErrUnsupported, hp.Provider(), "creating new objects")
}

func (hp *httpProvider) DeleteObj(*cluster.LOM) (int, error) {
	return http.StatusBadRequest, fmt.Errorf(cmn.FmtErrUnsupported, hp.Provider(), "deleting object")
}
