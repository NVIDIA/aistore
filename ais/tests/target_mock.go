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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/reb"
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

	mux.HandleFunc(cmn.URLPathBuckets.S, mocktgt.filehdlr)
	mux.HandleFunc(cmn.URLPathObjects.S, mocktgt.filehdlr)
	mux.HandleFunc(cmn.URLPathDaemon.S, mocktgt.daemonhdlr)
	mux.HandleFunc(cmn.URLPathVote.S, mocktgt.votehdlr)
	mux.HandleFunc(cmn.URLPathHealth.S, mocktgt.healthdlr)

	target, _ := smap.GetRandTarget()
	ip := target.PublicNet.NodeHostname

	s := &http.Server{Addr: ip + ":" + mockTargetPort, Handler: mux}
	go s.ListenAndServe()

	err := registerMockTarget(proxyURL, smap)
	if err != nil {
		t.Errorf("failed to start http server for mock target: %v", err)
		return
	}

	<-stopch
	err = tutils.RemoveNodeFromSmap(proxyURL, tutils.MockDaemonID)
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
			NodeHostname: v.PublicNet.NodeHostname,
			DaemonPort:   mockTargetPort,
			DirectURL:    "http://" + v.PublicNet.NodeHostname + ":" + mockTargetPort,
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
	err = api.DoHTTPRequest(api.ReqParams{BaseParams: baseParams, Path: cmn.URLPathClusterAutoReg.S, Body: jsonDaemonInfo})
	return err
}

type voteRetryMockTarget struct {
	voteInProgress bool
	errCh          chan error
}

type cluMetaRedux struct {
	Smap           *cluster.Smap
	VoteInProgress bool `json:"voting"`
}

func newVoteMsg(inp bool) cluMetaRedux {
	return cluMetaRedux{VoteInProgress: inp, Smap: &cluster.Smap{Version: 1}}
}

func (*voteRetryMockTarget) filehdlr(w http.ResponseWriter, r *http.Request) {
	// Ignore all file requests
}

func (p *voteRetryMockTarget) daemonhdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		msg := newVoteMsg(p.voteInProgress) // treat all Get requests as requests for a VoteMsg
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

func (*voteRetryMockTarget) votehdlr(w http.ResponseWriter, r *http.Request) {
	// Always vote yes.
	w.Write([]byte(ais.VoteYes))
}

func (p *voteRetryMockTarget) healthdlr(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	getRebStatus := cos.IsParseBool(query.Get(cmn.URLParamRebStatus))
	if getRebStatus {
		status := &reb.Status{}
		status.RebID = math.MaxInt64 // to abort t[MOCK] join triggered rebalance
		body := cos.MustMarshal(status)
		_, err := w.Write(body)
		if err != nil {
			p.errCh <- fmt.Errorf("error writing reb-status: %v", err)
		}
	}
}
