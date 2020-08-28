// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

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
		RemoteAddrIP() string
		// Do() uses one of the two ETL container endpoints:
		// - Method "PUT", Path "/"
		// - Method "GET", Path "/bucket/object"
		Do(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error
	}
	baseComm struct {
		cluster.Slistener
		transformerAddress string
		name               string
		podName            string
		remoteAddr         string
	}
	pushComm struct {
		baseComm
		t cluster.Target
	}
	redirComm struct {
		baseComm
	}
	revProxyComm struct {
		baseComm
		rp *httputil.ReverseProxy
	}
)

// interface guard
var (
	_ Communicator = &pushComm{}
	_ Communicator = &redirComm{}
	_ Communicator = &revProxyComm{}
)

//////////////
// baseComm //
//////////////

func makeCommunicator(t cluster.Target, pod *corev1.Pod, commType, podIP, transformerURL, name string,
	listener cluster.Slistener) Communicator {
	baseComm := baseComm{
		Slistener:          listener,
		transformerAddress: transformerURL,
		name:               name,
		podName:            pod.GetName(),
		remoteAddr:         podIP,
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
	}
	cmn.AssertMsg(false, commType)
	return nil
}

func (c baseComm) Name() string         { return c.name }
func (c baseComm) PodName() string      { return c.podName }
func (c baseComm) RemoteAddrIP() string { return c.remoteAddr }
func (c baseComm) SvcName() string      { return c.podName /*pod name is same as service name*/ }

//////////////
// pushComm //
//////////////

func (pushc *pushComm) Do(w http.ResponseWriter, _ *http.Request, bck *cluster.Bck, objName string) error {
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
	req, err := http.NewRequest(http.MethodPut, pushc.transformerAddress, fh)
	if err != nil {
		return err
	}

	req.ContentLength = lom.Size()
	req.Header.Set(cmn.HeaderContentType, cmn.ContentBinary)
	resp, err := pushc.t.Client().Do(req)
	if err != nil {
		return err
	}
	if contentLength := resp.Header.Get(cmn.HeaderContentLength); contentLength != "" {
		w.Header().Add(cmn.HeaderContentLength, contentLength)
	}
	_, err = io.Copy(w, resp.Body)
	debug.AssertNoErr(err)
	err = resp.Body.Close()
	debug.AssertNoErr(err)
	return nil
}

///////////////
// redirComm //
///////////////

func (repc *redirComm) Do(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error {
	redirectURL := cmn.JoinPath(repc.transformerAddress, transformerPath(bck, objName))
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
	return nil
}

//////////////////
// revProxyComm //
//////////////////

func (ppc *revProxyComm) Do(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error {
	r.URL.Path = transformerPath(bck, objName) // Reverse proxy should always use /bucket/object endpoint.
	ppc.rp.ServeHTTP(w, r)
	return nil
}

// prune query (received from AIS proxy) prior to reverse-proxying the request to/from container -
// not removing cmn.URLParamUUID, forinstance, would cause infinite loop
func pruneQuery(rawQuery string) string {
	vals, err := url.ParseQuery(rawQuery)
	if err != nil {
		glog.Errorf("error parsing raw query: %q", rawQuery)
		return ""
	}
	for _, filtered := range []string{cmn.URLParamUUID, cmn.URLParamProxyID, cmn.URLParamUnixTime} {
		vals.Del(filtered)
	}
	return vals.Encode()
}

func transformerPath(bck *cluster.Bck, objName string) string { return cmn.URLPath(bck.Name, objName) }
