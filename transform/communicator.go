// Package transform provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
)

type Communicator interface {
	Name() string
	PodName() string
	DoTransform(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error
}

func makeCommunicator(t cluster.Target, pod *corev1.Pod, commType, transformerURL, name string) Communicator {
	baseComm := baseComm{
		url:     transformerURL,
		name:    name,
		podName: pod.GetName(),
	}
	switch commType {
	case putCommType:
		return &putComm{baseComm: baseComm, t: t}
	case redirectCommType:
		return &redirComm{baseComm: baseComm}
	case revProxyCommType:
		urlp, _ := url.Parse(transformerURL)
		rp := &httputil.ReverseProxy{
			Director: func(req *http.Request) {
				req.URL = urlp
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
	url     string
	name    string
	podName string
}

func (c baseComm) Name() string    { return c.name }
func (c baseComm) PodName() string { return c.podName }

type pushPullComm struct {
	baseComm
	rp *httputil.ReverseProxy
}

func (ppc *pushPullComm) DoTransform(w http.ResponseWriter, r *http.Request, _ *cluster.Bck, _ string) error {
	ppc.rp.ServeHTTP(w, r)
	return nil
}

func (ppc *pushPullComm) Name() string {
	return ppc.name
}

type redirComm struct {
	baseComm
}

func (repc *redirComm) DoTransform(w http.ResponseWriter, r *http.Request, _ *cluster.Bck, _ string) error {
	redirectURL := fmt.Sprintf("%s%s", repc.url, r.URL.Path)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
	return nil
}

type putComm struct {
	baseComm
	t cluster.Target
}

func (putc *putComm) DoTransform(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error {
	lom := &cluster.LOM{T: putc.t, ObjName: objName}
	if err := lom.Init(bck.Bck); err != nil {
		return err
	}
	if err := lom.Load(); err != nil {
		return err
	}

	var (
		group  = &errgroup.Group{}
		rp, wp = io.Pipe()
	)

	group.Go(func() error {
		req, err := http.NewRequest(http.MethodPost, putc.url, rp)
		if err != nil {
			rp.CloseWithError(err)
			return err
		}

		req.ContentLength = lom.Size()
		req.Header.Set("Content-Type", "octet-stream")
		resp, err := putc.t.Client().Do(req)
		if err != nil {
			return err
		}
		_, err = io.Copy(w, resp.Body)
		debug.AssertNoErr(err)
		err = resp.Body.Close()
		debug.AssertNoErr(err)
		return nil
	})

	err := putc.t.GetObject(wp, lom, time.Now())
	wp.CloseWithError(err)
	if err != nil {
		return err
	}
	return group.Wait()
}
