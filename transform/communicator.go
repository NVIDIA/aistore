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
)

type Communicator interface {
	DoTransform(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) error
}

func makeCommunicator(commType, transformerURL string, t cluster.Target) Communicator {
	baseComm := baseComm{url: transformerURL}
	switch commType {
	case pushPullCommType:
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
		return &pushPullComm{rp: rp}
	case putCommType:
		return &putComm{baseComm: baseComm, t: t}
	case redirectCommType:
		return &redirComm{baseComm: baseComm}
	}
	cmn.AssertMsg(false, commType)
	return nil
}

type baseComm struct {
	url string
}

type pushPullComm struct {
	rp *httputil.ReverseProxy
}

func (ppc *pushPullComm) DoTransform(w http.ResponseWriter, r *http.Request, _ *cluster.Bck, _ string) error {
	ppc.rp.ServeHTTP(w, r)
	return nil
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
