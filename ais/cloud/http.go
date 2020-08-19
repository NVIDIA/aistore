// Package cloud contains implementation of various cloud providers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cloud

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

type (
	httpProvider struct {
		t cluster.Target
	}
)

var (
	_ cluster.CloudProvider = &httpProvider{}
)

func NewHTTP(t cluster.Target) (cluster.CloudProvider, error) {
	return &httpProvider{t: t}, nil
}

func (hp *httpProvider) Provider() string  { return cmn.ProviderHTTP }
func (hp *httpProvider) MaxPageSize() uint { return 10000 }

func (hp *httpProvider) ListBuckets(ctx context.Context, _ cmn.QueryBcks) (buckets cmn.BucketNames, err error, errCode int) {
	debug.Assert(false)
	return nil, fmt.Errorf("%q provider doesn't support listing buckets from the cloud", hp.Provider()), http.StatusBadRequest
}

func (hp *httpProvider) DeleteObj(ctx context.Context, lom *cluster.LOM) (error, int) {
	return fmt.Errorf("%q provider doesn't support deleting object", hp.Provider()), http.StatusBadRequest
}

func (hp *httpProvider) HeadBucket(ctx context.Context, bck *cluster.Bck) (bckProps cmn.SimpleKVs, err error, errCode int) {
	originalURL := ctx.Value(cmn.CtxOriginalURL).(string)
	if originalURL == "" {
		return nil, fmt.Errorf("%q provider doesn't support checking bucket", hp.Provider()), http.StatusBadRequest
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[HTTP CLOUD][HEAD] original_url: %q", originalURL)
	}

	// HEAD bucket is basically equivalent to checking if we are able to HEAD a given object.
	// TODO: maybe we should strip the object name and check if we can contact the URL?
	resp, err := hp.t.Client().Head(originalURL)
	if err != nil {
		return nil, err, http.StatusInternalServerError
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("error occurred: %v", resp.StatusCode), resp.StatusCode
	}

	bckProps = make(cmn.SimpleKVs)
	bckProps[cmn.HeaderCloudProvider] = cmn.ProviderHTTP
	return
}

func (hp *httpProvider) ListObjects(ctx context.Context, bck *cluster.Bck, msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int) {
	debug.Assert(false)
	return nil, fmt.Errorf("%q provider doesn't support listing objects from the cloud (use 'cached' option)", hp.Provider()), http.StatusBadRequest
}

func (hp *httpProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, err error, errCode int) {
	var (
		h           = cmn.CloudHelpers.HTTP
		originalURL = ctx.Value(cmn.CtxOriginalURL).(string)
	)
	debug.Assert(originalURL != "")

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[HTTP CLOUD][HEAD] original_url: %q", originalURL)
	}

	resp, err := hp.t.Client().Head(originalURL)
	if err != nil {
		return nil, err, http.StatusInternalServerError
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("error occurred: %v", resp.StatusCode), resp.StatusCode
	}
	objMeta = make(cmn.SimpleKVs, 2)
	objMeta[cmn.HeaderCloudProvider] = cmn.ProviderHTTP
	objMeta[cmn.HeaderObjSize] = strconv.FormatInt(resp.ContentLength, 10) // TODO: what if server does not return `ContentLength`?
	if v, ok := h.EncodeVersion(resp.Header.Get(cmn.HeaderETag)); ok {
		objMeta[cluster.VersionObjMD] = v
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[head_object] %s", lom)
	}
	return
}

func (hp *httpProvider) GetObj(ctx context.Context, workFQN string, lom *cluster.LOM) (err error, errCode int) {
	var (
		h           = cmn.CloudHelpers.HTTP
		originalURL = ctx.Value(cmn.CtxOriginalURL).(string)
	)
	if originalURL == "" {
		// This can happen when bucket exist but cached object was removed and now
		// we are trying to just retrieve the object with standard API.
		//
		// TODO: If we could recreate the URL from the bucket name then
		//  the original URL would be: reverse(lom.Bck.Name) + lom.ObjName
		return fmt.Errorf("%q provider doesn't support getting object without provided original URL", hp.Provider()), http.StatusBadRequest
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[HTTP CLOUD][GET] original_url: %q", originalURL)
	}

	resp, err := hp.t.Client().Get(originalURL)
	if err != nil {
		return err, http.StatusInternalServerError
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error occurred: %v", resp.StatusCode), resp.StatusCode
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[HTTP CLOUD][GET] success, size: %d", resp.ContentLength)
	}

	customMD := cmn.SimpleKVs{
		cluster.SourceObjMD:      cluster.SourceHTTPObjMD,
		cluster.OriginalURLObjMD: originalURL,
	}
	if v, ok := h.EncodeVersion(resp.Header.Get(cmn.HeaderETag)); ok {
		customMD[cluster.VersionObjMD] = v
	}

	lom.SetCustomMD(customMD)
	setSize(ctx, resp.ContentLength) // TODO: what if server does not return `ContentLength`?
	err = hp.t.PutObject(cluster.PutObjectParams{
		LOM:          lom,
		Reader:       wrapReader(ctx, resp.Body),
		WorkFQN:      workFQN,
		RecvType:     cluster.ColdGet,
		WithFinalize: false,
	})
	if err != nil {
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[get_object] %s", lom)
	}
	return
}

func (hp *httpProvider) PutObj(ctx context.Context, r io.Reader, lom *cluster.LOM) (version string, err error, errCode int) {
	return "", fmt.Errorf("%q provider doesn't support creating new objects", hp.Provider()), http.StatusBadRequest
}
