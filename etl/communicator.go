// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
	corev1 "k8s.io/api/core/v1"
)

type (
	// Communicator is responsible for managing communications with local ETL container.
	// Do() gets executed as part of (each) GET bucket/object by the user.
	// Communicator listens to cluster membership changes and terminates ETL container,
	// if need be.
	Communicator interface {
		cluster.Slistener

		Name() string
		PodName() string
		SvcName() string

		// Do() uses one of the two ETL container endpoints:
		// - Method "PUT", Path "/"
		// - Method "GET", Path "/bucket/object"
		Do(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error

		// Get() interface implementations realize offline ETL.
		// Get() is driven by `OfflineDataProvider` - not to confuse with
		// GET requests from users (such as training models and apps)
		// to perform on-the-fly transformation.
		Get(bck *cluster.Bck, objName string, ts ...time.Duration) (cos.ReadCloseSizer, error)
	}

	commArgs struct {
		listener       cluster.Slistener
		t              cluster.Target
		pod            *corev1.Pod
		name           string
		commType       string
		transformerURL string
	}

	baseComm struct {
		cluster.Slistener
		t cluster.Target

		name    string
		podName string

		transformerURL string
	}

	pushComm struct {
		baseComm
		mem *memsys.MMSA
	}
	redirectComm struct {
		baseComm
	}
	revProxyComm struct {
		baseComm
		rp *httputil.ReverseProxy
	}
)

// interface guard
var (
	_ Communicator = (*pushComm)(nil)
	_ Communicator = (*redirectComm)(nil)
	_ Communicator = (*revProxyComm)(nil)
)

//////////////
// baseComm //
//////////////

func makeCommunicator(args commArgs) Communicator {
	baseComm := baseComm{
		Slistener:      args.listener,
		t:              args.t,
		name:           args.name,
		podName:        args.pod.GetName(),
		transformerURL: args.transformerURL,
	}

	switch args.commType {
	case PushCommType:
		return &pushComm{baseComm: baseComm, mem: args.t.MMSA()}
	case RedirectCommType:
		return &redirectComm{baseComm: baseComm}
	case RevProxyCommType:
		transURL, err := url.Parse(baseComm.transformerURL)
		cos.AssertNoErr(err)
		rp := &httputil.ReverseProxy{
			Director: func(req *http.Request) {
				// Replacing the `req.URL` host with ETL container host
				req.URL.Scheme = transURL.Scheme
				req.URL.Host = transURL.Host
				req.URL.RawQuery = pruneQuery(req.URL.RawQuery)
				if _, ok := req.Header["User-Agent"]; !ok {
					// Explicitly disable `User-Agent` so it's not set to default value.
					req.Header.Set("User-Agent", "")
				}
			},
		}
		return &revProxyComm{baseComm: baseComm, rp: rp}
	default:
		cos.AssertMsg(false, args.commType)
	}
	return nil
}

func (c baseComm) Name() string    { return c.name }
func (c baseComm) PodName() string { return c.podName }
func (c baseComm) SvcName() string { return c.podName /*pod name is same as service name*/ }

//////////////
// pushComm //
//////////////

func (pc *pushComm) doRequest(bck *cluster.Bck, objName string, ts ...time.Duration) (r cos.ReadCloseSizer, err error) {
	lom := cluster.AllocLOM(objName)

	defer cluster.FreeLOM(lom)
	if err := lom.Init(bck.Bck); err != nil {
		return nil, err
	}

	r, err = pc.tryDoRequest(lom, ts...)
	if err != nil && cmn.IsObjNotExist(err) && bck.IsRemote() {
		_, err = pc.t.GetCold(context.Background(), lom, cluster.PrefetchWait)
		if err != nil {
			return nil, err
		}
		r, err = pc.tryDoRequest(lom, ts...)
	}
	return
}

func (pc *pushComm) tryDoRequest(lom *cluster.LOM, ts ...time.Duration) (cos.ReadCloseSizer, error) {
	lom.Lock(false)
	defer lom.Unlock(false)

	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		return nil, err
	}

	// `fh` is closed by Do(req).
	fh, err := cos.NewFileHandle(lom.FQN)
	if err != nil {
		return nil, err
	}

	var (
		req     *http.Request
		resp    *http.Response
		cleanup func()
	)
	if len(ts) > 0 && ts[0] != 0 {
		var ctx context.Context
		ctx, cleanup = context.WithTimeout(context.Background(), ts[0])
		req, err = http.NewRequestWithContext(ctx, http.MethodPut, pc.transformerURL, fh)
	} else {
		req, err = http.NewRequest(http.MethodPut, pc.transformerURL, fh)
	}
	if err != nil {
		goto finish
	}

	req.ContentLength = lom.Size()
	req.Header.Set(cmn.HdrContentType, cmn.ContentBinary)
	resp, err = pc.t.DataClient().Do(req) // nolint:bodyclose // Closed by the caller.
finish:
	if err != nil {
		if cleanup != nil {
			cleanup()
		}
		return nil, err
	}
	return cos.NewDeferRCS(cos.NewSizedRC(resp.Body, resp.ContentLength), cleanup), nil
}

func (pc *pushComm) Do(w http.ResponseWriter, _ *http.Request, bck *cluster.Bck, objName string) error {
	var (
		size   int64
		r, err = pc.doRequest(bck, objName)
	)
	if err != nil {
		return err
	}
	defer r.Close()
	if size = r.Size(); size < 0 {
		size = memsys.DefaultBufSize // TODO -- FIXME: track the average
	}
	buf, slab := pc.mem.Alloc(size)
	_, err = io.CopyBuffer(w, r, buf)
	slab.Free(buf)
	return err
}

func (pc *pushComm) Get(bck *cluster.Bck, objName string, ts ...time.Duration) (cos.ReadCloseSizer, error) {
	return pc.doRequest(bck, objName, ts...)
}

//////////////////
// redirectComm //
//////////////////

func (rc *redirectComm) Do(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error {
	redirectURL := cos.JoinPath(rc.transformerURL, transformerPath(bck, objName))
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
	return nil
}

func (rc *redirectComm) Get(bck *cluster.Bck, objName string, ts ...time.Duration) (cos.ReadCloseSizer, error) {
	etlURL := cos.JoinPath(rc.transformerURL, transformerPath(bck, objName))
	return getWithTimeout(rc.t.DataClient(), etlURL, ts...)
}

//////////////////
// revProxyComm //
//////////////////

func (pc *revProxyComm) Do(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error {
	path := transformerPath(bck, objName)
	r.URL.Path, _ = url.PathUnescape(path) // `Path` must be unescaped otherwise it will be escaped again.
	r.URL.RawPath = path                   // `RawPath` should be escaped version of `Path`.
	pc.rp.ServeHTTP(w, r)
	return nil
}

func (pc *revProxyComm) Get(bck *cluster.Bck, objName string, ts ...time.Duration) (cos.ReadCloseSizer, error) {
	etlURL := cos.JoinPath(pc.transformerURL, transformerPath(bck, objName))
	return getWithTimeout(pc.t.DataClient(), etlURL, ts...)
}

// prune query (received from AIS proxy) prior to reverse-proxying the request to/from container -
// not removing cmn.URLParamUUID, for instance, would cause infinite loop.
func pruneQuery(rawQuery string) string {
	vals, err := url.ParseQuery(rawQuery)
	if err != nil {
		glog.Errorf("failed to parse raw query %q, err: %v", rawQuery, err)
		return ""
	}
	for _, filtered := range []string{cmn.URLParamUUID, cmn.URLParamProxyID, cmn.URLParamUnixTime} {
		vals.Del(filtered)
	}
	return vals.Encode()
}

// TODO: Consider encoding bucket and object name without the necessity to escape.
func transformerPath(bck *cluster.Bck, objName string) string {
	return "/" + url.PathEscape(bck.MakeUname(objName))
}

func getWithTimeout(client *http.Client, url string, ts ...time.Duration) (r cos.ReadCloseSizer, err error) {
	var (
		req     *http.Request
		resp    *http.Response
		cleanup func()
	)
	if len(ts) > 0 && ts[0] != 0 {
		var ctx context.Context
		ctx, cleanup = context.WithTimeout(context.Background(), ts[0])
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	} else {
		req, err = http.NewRequest(http.MethodGet, url, nil)
	}
	if err != nil {
		goto finish
	}
	resp, err = client.Do(req) // nolint:bodyclose // Closed by the caller.
finish:
	if err != nil {
		if cleanup != nil {
			cleanup()
		}
		return nil, err
	}
	r = cos.NewDeferRCS(cos.NewSizedRC(resp.Body, resp.ContentLength), cleanup)
	return r, nil
}
