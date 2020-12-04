// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
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
		Get(bck *cluster.Bck, objName string) (io.ReadCloser, int64, error)
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
		cmn.AssertNoErr(err)
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
		cmn.AssertMsg(false, args.commType)
	}
	return nil
}

func (c baseComm) Name() string    { return c.name }
func (c baseComm) PodName() string { return c.podName }
func (c baseComm) SvcName() string { return c.podName /*pod name is same as service name*/ }

//////////////
// pushComm //
//////////////

func (pc *pushComm) doRequest(bck *cluster.Bck, objName string) (resp *http.Response, err error) {
	lom := &cluster.LOM{T: pc.t, ObjName: objName}
	if err := lom.Init(bck.Bck); err != nil {
		return nil, err
	}

	resp, err = pc.tryDoRequest(lom)
	if err != nil && cmn.IsObjNotExist(err) && bck.IsRemote() {
		_, err = pc.t.GetCold(context.Background(), lom, cluster.PrefetchWait)
		if err != nil {
			return nil, err
		}
		resp, err = pc.tryDoRequest(lom)
	}
	return
}

// TODO: make it work with cloud, including not cached objects.
func (pc *pushComm) tryDoRequest(lom *cluster.LOM) (*http.Response, error) {
	lom.Lock(false)
	defer lom.Unlock(false)
	if err := lom.Load(); err != nil {
		return nil, err
	}

	// `fh` is closed by Do(req).
	fh, err := cmn.NewFileHandle(lom.GetFQN())
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPut, pc.transformerURL, fh)
	if err != nil {
		return nil, err
	}

	req.ContentLength = lom.Size()
	req.Header.Set(cmn.HeaderContentType, cmn.ContentBinary)
	return pc.t.DataClient().Do(req)
}

func (pc *pushComm) Do(w http.ResponseWriter, _ *http.Request, bck *cluster.Bck, objName string) error {
	var (
		size      int64
		resp, err = pc.doRequest(bck, objName)
	)
	if err != nil {
		return err
	}
	if contentLength := resp.Header.Get(cmn.HeaderContentLength); contentLength != "" {
		size, err = strconv.ParseInt(contentLength, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid Content-Length %q", contentLength)
		}
		w.Header().Set(cmn.HeaderContentLength, contentLength)
	} else {
		size = memsys.DefaultBufSize // TODO -- FIXME: track the average
	}
	buf, slab := pc.mem.Alloc(size)
	_, err = io.CopyBuffer(w, resp.Body, buf)
	slab.Free(buf)
	erc := resp.Body.Close()
	debug.AssertNoErr(erc)
	if err != nil {
		return err
	}
	return nil
}

func (pc *pushComm) Get(bck *cluster.Bck, objName string) (io.ReadCloser, int64, error) {
	resp, err := pc.doRequest(bck, objName)
	return handleResp(resp, err)
}

//////////////////
// redirectComm //
//////////////////

// TODO: make sure that it works with cloud, even when cold get by ETL transformer is required.
func (rc *redirectComm) Do(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error {
	redirectURL := cmn.JoinPath(rc.transformerURL, transformerPath(bck, objName))
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
	return nil
}

func (rc *redirectComm) Get(bck *cluster.Bck, objName string) (io.ReadCloser, int64, error) {
	etlURL := cmn.JoinPath(rc.transformerURL, transformerPath(bck, objName))
	resp, err := rc.t.DataClient().Get(etlURL)
	return handleResp(resp, err)
}

//////////////////
// revProxyComm //
//////////////////

func (pc *revProxyComm) Do(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error {
	r.URL.Path = transformerPath(bck, objName) // NOTE: using /bucket/object endpoint
	pc.rp.ServeHTTP(w, r)
	return nil
}

func (pc *revProxyComm) Get(bck *cluster.Bck, objName string) (io.ReadCloser, int64, error) {
	etlURL := cmn.JoinPath(pc.transformerURL, transformerPath(bck, objName))
	resp, err := pc.t.DataClient().Get(etlURL)
	return handleResp(resp, err)
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

func transformerPath(bck *cluster.Bck, objName string) string {
	return cmn.JoinWords(bck.Name, objName)
}

func handleResp(resp *http.Response, err error) (io.ReadCloser, int64, error) {
	if err != nil {
		return nil, 0, err
	}
	return resp.Body, resp.ContentLength, nil
}
