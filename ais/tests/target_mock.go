// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/ais"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
	jsoniter "github.com/json-iterator/go"
)

const (
	mockTargetPort = "8079"
)

type targetMocker interface {
	filehdlr(w http.ResponseWriter, r *http.Request)
	daemonhdlr(w http.ResponseWriter, r *http.Request)
	votehdlr(w http.ResponseWriter, r *http.Request)
	healthdlr(w http.ResponseWriter, r *http.Request)
}

type MockRegRequest struct {
	SI *cluster.Snode `json:"si"`
}

func runMockTarget(t *testing.T, proxyURL string, mocktgt targetMocker, stopch chan struct{}, smap *cluster.Smap) {
	mux := http.NewServeMux()

	mux.HandleFunc(cmn.URLPath(cmn.Version, cmn.Buckets), mocktgt.filehdlr)
	mux.HandleFunc(cmn.URLPath(cmn.Version, cmn.Objects), mocktgt.filehdlr)
	mux.HandleFunc(cmn.URLPath(cmn.Version, cmn.Daemon), mocktgt.daemonhdlr)
	mux.HandleFunc(cmn.URLPath(cmn.Version, cmn.Vote), mocktgt.votehdlr)
	mux.HandleFunc(cmn.URLPath(cmn.Version, cmn.Health), mocktgt.healthdlr)

	ip := ""
	for _, v := range smap.Tmap {
		ip = v.PublicNet.NodeIPAddr
		break
	}

	s := &http.Server{Addr: ip + ":" + mockTargetPort, Handler: mux}
	go s.ListenAndServe()

	err := registerMockTarget(proxyURL, smap)
	if err != nil {
		t.Errorf("failed to start http server for mock target: %v", err)
		return
	}

	<-stopch
	err = tutils.UnregisterNode(proxyURL, tutils.MockDaemonID)
	tassert.CheckFatal(t, err)
	s.Shutdown(context.Background())
}

func registerMockTarget(proxyURL string, smap *cluster.Smap) error {
	var (
		jsonDaemonInfo []byte
		err            error
	)

	// borrow a random target's ip but using a different port to register the mock target
	for _, v := range smap.Tmap {
		v.DaemonID = tutils.MockDaemonID
		v.PublicNet = cluster.NetInfo{
			NodeIPAddr: v.PublicNet.NodeIPAddr,
			DaemonPort: mockTargetPort,
			DirectURL:  "http://" + v.PublicNet.NodeIPAddr + ":" + mockTargetPort,
		}
		v.IntraControlNet = v.PublicNet
		v.IntraDataNet = v.PublicNet
		regReq := MockRegRequest{SI: v}
		jsonDaemonInfo, err = jsoniter.Marshal(regReq)
		if err != nil {
			return err
		}
		break
	}
	baseParams := tutils.BaseAPIParams(proxyURL)
	baseParams.Method = http.MethodPost
	err = api.DoHTTPRequest(api.ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Cluster, cmn.AutoRegister),
		Body:       jsonDaemonInfo,
	})
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
		msg := ais.NewVoteMsg(p.voteInProgress) // treat all Get requests as requests for a VoteMsg
		jsbytes, err := jsoniter.Marshal(msg)
		if err == nil {
			_, err = w.Write(jsbytes)
		}
		if err != nil {
			p.errCh <- fmt.Errorf("error writing vote message: %v", err)
		}
	default:
	}
}

func (p *voteRetryMockTarget) votehdlr(w http.ResponseWriter, r *http.Request) {
	// Always vote yes.
	w.Write([]byte(ais.VoteYes))
}

func (p *voteRetryMockTarget) healthdlr(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	getRebStatus := cmn.IsParseBool(query.Get(cmn.URLParamRebStatus))
	if getRebStatus {
		status := &reb.Status{}
		status.RebID = math.MaxInt64 // to abort t[MOCK] join triggered rebalance
		body := cmn.MustMarshal(status)
		_, err := w.Write(body)
		if err != nil {
			p.errCh <- fmt.Errorf("error writing reb-status: %v", err)
		}
	}
}
