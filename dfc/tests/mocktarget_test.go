/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/tutils"
	jsoniter "github.com/json-iterator/go"
)

const (
	mockTargetPort = "8079"
)

type targetMocker interface {
	filehdlr(w http.ResponseWriter, r *http.Request)
	daemonhdlr(w http.ResponseWriter, r *http.Request)
	votehdlr(w http.ResponseWriter, r *http.Request)
}

func runMockTarget(t *testing.T, proxyURL string, mocktgt targetMocker, stopch chan struct{}, smap *cluster.Smap) {
	mux := http.NewServeMux()

	mux.HandleFunc(cmn.URLPath(cmn.Version, cmn.Buckets), mocktgt.filehdlr)
	mux.HandleFunc(cmn.URLPath(cmn.Version, cmn.Objects), mocktgt.filehdlr)
	mux.HandleFunc(cmn.URLPath(cmn.Version, cmn.Daemon), mocktgt.daemonhdlr)
	mux.HandleFunc(cmn.URLPath(cmn.Version, cmn.Vote), mocktgt.votehdlr)
	mux.HandleFunc(cmn.URLPath(cmn.Version, cmn.Health), func(w http.ResponseWriter, r *http.Request) {})

	ip := ""
	for _, v := range smap.Tmap {
		ip = v.PublicNet.NodeIPAddr
		break
	}

	s := &http.Server{Addr: ip + ":" + mockTargetPort, Handler: mux}
	go s.ListenAndServe()

	err := registerMockTarget(proxyURL, mocktgt, smap)
	if err != nil {
		t.Fatalf("failed to start http server for mock target: %v", err)
	}

	<-stopch
	err = tutils.UnregisterTarget(proxyURL, mockDaemonID)
	tutils.CheckFatal(err, t)
	s.Shutdown(context.Background())
}

func registerMockTarget(proxyURL string, mocktgt targetMocker, smap *cluster.Smap) error {
	var (
		jsonDaemonInfo []byte
		err            error
	)

	// borrow a random target's ip but using a different port to register the mock target
	for _, v := range smap.Tmap {
		v.DaemonID = mockDaemonID
		v.PublicNet = cluster.NetInfo{
			NodeIPAddr: v.PublicNet.NodeIPAddr,
			DaemonPort: mockTargetPort,
			DirectURL:  "http://" + v.PublicNet.NodeIPAddr + ":" + mockTargetPort,
		}
		v.IntraControlNet = v.PublicNet
		v.IntraDataNet = v.PublicNet
		jsonDaemonInfo, err = jsoniter.Marshal(v)
		if err != nil {
			return err
		}
		break
	}
	baseParams := tutils.BaseAPIParams(proxyURL)
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Cluster)
	_, err = api.DoHTTPRequest(baseParams, path, jsonDaemonInfo)
	return err
}

type voteRetryMockTarget struct {
	voteInProgress bool
	errCh          chan error
}

func (*voteRetryMockTarget) filehdlr(w http.ResponseWriter, r *http.Request) {
	// Ignore all file requests
}

func (p *voteRetryMockTarget) daemonhdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		msg := dfc.NewVoteMsg(p.voteInProgress) // treat all Get requests as requests for a VoteMsg
		jsbytes, err := jsoniter.Marshal(msg)
		if err == nil {
			_, err = w.Write(jsbytes)
		}

		if err != nil {
			p.errCh <- fmt.Errorf("Error writing message: %v\n", err)
		}

	default:
	}
}

func (p *voteRetryMockTarget) votehdlr(w http.ResponseWriter, r *http.Request) {
	// Always vote yes.
	w.Write([]byte(dfc.VoteYes))
}
