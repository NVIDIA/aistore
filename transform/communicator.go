// Package transform provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

import (
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	corev1 "k8s.io/api/core/v1"
)

var (
	// These parameters are not needed by the transformer
	// WARNING: Sending UUID might cause infinite loop where we GET objects and
	// then requests are redirected back to the transformer (since we didn't remove UUID from query parameters).
	toBeFiltered = []string{cmn.URLParamUUID, cmn.URLParamProxyID, cmn.URLParamUnixTime}
)

type Communicator interface {
	Name() string
	PodName() string
	SvcName() string
	// DoTransform can use one of 2 transformer endpoints:
	// Method "POST", Path "/"
	// Method "GET", Path "/bucket/object"
	DoTransform(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error
}

func filterQueryParams(rawQuery string) string {
	vals, err := url.ParseQuery(rawQuery)
	if err != nil {
		glog.Errorf("error parsing raw query: %q", rawQuery)
		return ""
	}
	for _, filtered := range toBeFiltered {
		vals.Del(filtered)
	}
	return vals.Encode()
}
func makeCommunicator(t cluster.Target, pod *corev1.Pod, commType, transformerURL, name string) Communicator {
	baseComm := baseComm{
		transformerAddress: transformerURL,
		name:               name,
		podName:            pod.GetName(),
	}
	switch commType {
	case PushCommType:
		return &pushComm{baseComm: baseComm, t: t}
	case RedirectCommType:
		return &redirComm{baseComm: baseComm}
	case RevProxyCommType:
		transURL, err := url.Parse(transformerURL)
		cmn.AssertNoErr(err)
		rp := &httputil.ReverseProxy{
			Director: func(req *http.Request) {
				// Replacing the `req.URL` host with transformer host
				req.URL.Scheme = transURL.Scheme
				req.URL.Host = transURL.Host
				req.URL.RawQuery = filterQueryParams(req.URL.RawQuery)
				if _, ok := req.Header["User-Agent"]; !ok {
					// Explicitly disable `User-Agent` so it's not set to default value.
					req.Header.Set("User-Agent", "")
				}
			},
		}
		return &pushPullComm{baseComm: baseComm, rp: rp}
	}
	cmn.AssertMsg(false, commType)
	return nil
}

type baseComm struct {
	transformerAddress string
	name               string
	podName            string
}

func (c baseComm) Name() string    { return c.name }
func (c baseComm) PodName() string { return c.podName }
func (c baseComm) SvcName() string { return c.podName /*pod name is same as service name*/ }

type pushPullComm struct {
	baseComm
	rp *httputil.ReverseProxy
}

func (ppc *pushPullComm) DoTransform(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error {
	r.URL.Path = transformerPath(bck, objName) // Reverse proxy should always use /bucket/object endpoint.
	ppc.rp.ServeHTTP(w, r)
	return nil
}

func (ppc *pushPullComm) Name() string {
	return ppc.name
}

type redirComm struct {
	baseComm
}

func (repc *redirComm) DoTransform(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error {
	redirectURL := repc.transformerAddress + transformerPath(bck, objName)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
	return nil
}

type pushComm struct {
	baseComm
	t cluster.Target
}

func (pushc *pushComm) DoTransform(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error {
	lom := &cluster.LOM{T: pushc.t, ObjName: objName}
	if err := lom.Init(bck.Bck); err != nil {
		return err
	}
	lom.Lock(false)
	defer lom.Unlock(false)
	if err := lom.Load(); err != nil {
		return err
	}

	// `fh` is closed by Do(req)
	fh, err := cmn.NewFileHandle(lom.GetFQN())
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, pushc.transformerAddress, fh)
	if err != nil {
		return err
	}

	req.ContentLength = lom.Size()
	req.Header.Set("Content-Type", "octet-stream")
	resp, err := pushc.t.Client().Do(req)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, resp.Body)
	debug.AssertNoErr(err)
	err = resp.Body.Close()
	debug.AssertNoErr(err)
	return nil
}

func transformerPath(bck *cluster.Bck, objName string) string {
	return cmn.URLPath(bck.Name, objName)
}
