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
	"path/filepath"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

type (
	httpProvider struct {
		t           cluster.Target
		sClient     *http.Client
		plainClient *http.Client
	}
)

var (
	_ cluster.CloudProvider = &httpProvider{}
)

func NewHTTP(t cluster.Target, config *cmn.Config) (cluster.CloudProvider, error) {
	hp := &httpProvider{t: t}
	hp.plainClient = cmn.NewClient(cmn.TransportArgs{
		Timeout:         config.Client.TimeoutLong,
		WriteBufferSize: config.Net.HTTP.WriteBufferSize,
		ReadBufferSize:  config.Net.HTTP.ReadBufferSize,
		UseHTTPS:        false,
		SkipVerify:      config.Net.HTTP.SkipVerify,
	})
	hp.sClient = cmn.NewClient(cmn.TransportArgs{
		Timeout:         config.Client.TimeoutLong,
		WriteBufferSize: config.Net.HTTP.WriteBufferSize,
		ReadBufferSize:  config.Net.HTTP.ReadBufferSize,
		UseHTTPS:        true,
		SkipVerify:      config.Net.HTTP.SkipVerify,
	})
	return hp, nil
}

func (hp *httpProvider) Provider() string  { return cmn.ProviderHTTP }
func (hp *httpProvider) MaxPageSize() uint { return 10000 }

func (hp *httpProvider) client(url string) *http.Client {
	u := strings.ToLower(url)
	if strings.HasPrefix(u, "https") {
		return hp.sClient
	}
	return hp.plainClient
}

func (hp *httpProvider) ListBuckets(ctx context.Context,
	_ cmn.QueryBcks) (buckets cmn.BucketNames, err error, errCode int) {
	debug.Assert(false)
	return nil,
		fmt.Errorf("%q provider doesn't support listing buckets from the cloud", hp.Provider()), http.StatusBadRequest
}

func (hp *httpProvider) DeleteObj(ctx context.Context, lom *cluster.LOM) (error, int) {
	return fmt.Errorf("%q provider doesn't support deleting object", hp.Provider()), http.StatusBadRequest
}

func (hp *httpProvider) HeadBucket(ctx context.Context, bck *cluster.Bck) (bckProps cmn.SimpleKVs, err error, errCode int) {
	origURL := ctx.Value(cmn.CtxOriginalURL).(string)
	if origURL == "" {
		// TODO: NOTE that bck.Props.OrigURLBck does not contain URL.Scheme
		origURL = bck.Props.OrigURLBck
		if origURL == "" {
			return nil, fmt.Errorf("failed to HEAD(%s): original_url is empty", bck), http.StatusBadRequest
		}
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[HTTP CLOUD][HEAD] original_url: %q", origURL)
	}

	// HEAD bucket is basically equivalent to checking if we are able to HEAD a given object.
	// TODO: maybe we should strip the object name and check if we can contact the URL?
	resp, err := hp.client(origURL).Head(origURL)
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

func (hp *httpProvider) ListObjects(ctx context.Context, bck *cluster.Bck,
	msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int) {
	debug.Assert(false)
	return nil,
		fmt.Errorf("%q provider doesn't support listing objects from the cloud (use 'cached' option)",
			hp.Provider()), http.StatusBadRequest
}

func (hp *httpProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, err error, errCode int) {
	var (
		h       = cmn.CloudHelpers.HTTP
		origURL = ctx.Value(cmn.CtxOriginalURL).(string)
	)
	if origURL == "" {
		// try to recreate the original URL but NOTE: does not contain URL.Scheme
		bck := lom.Bck()
		if bck.Props.OrigURLBck == "" {
			return nil, fmt.Errorf("failed to HEAD(%s): original_url is empty", lom), http.StatusBadRequest
		}
		origURL = filepath.Join(bck.Props.OrigURLBck, lom.ObjName) // see cmn.URL2BckObj
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[HTTP CLOUD][HEAD] original_url: %q", origURL)
	}

	resp, err := hp.client(origURL).Head(origURL)
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
		h       = cmn.CloudHelpers.HTTP
		origURL = ctx.Value(cmn.CtxOriginalURL).(string)
	)
	if origURL == "" {
		// try to recreate the original URL - see NOTE above
		bck := lom.Bck()
		if bck.Props.OrigURLBck == "" {
			return fmt.Errorf("failed to GET(%s): original_url is empty", lom), http.StatusBadRequest
		}
		origURL = filepath.Join(bck.Props.OrigURLBck, lom.ObjName) // see cmn.URL2BckObj
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[HTTP CLOUD][GET] original_url: %q", origURL)
	}

	resp, err := hp.client(origURL).Get(origURL)
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
		cluster.SourceObjMD:  cluster.SourceHTTPObjMD,
		cluster.OrigURLObjMD: origURL,
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
