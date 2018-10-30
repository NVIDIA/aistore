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
	"github.com/NVIDIA/dfcpub/common"
	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
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

	mux.HandleFunc(common.URLPath(api.Version, api.Buckets), mocktgt.filehdlr)
	mux.HandleFunc(common.URLPath(api.Version, api.Objects), mocktgt.filehdlr)
	mux.HandleFunc(common.URLPath(api.Version, api.Daemon), mocktgt.daemonhdlr)
	mux.HandleFunc(common.URLPath(api.Version, api.Vote), mocktgt.votehdlr)
	mux.HandleFunc(common.URLPath(api.Version, api.Health), func(w http.ResponseWriter, r *http.Request) {})

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
	unregisterMockTarget(proxyURL, mocktgt)
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
		v.InternalNet = v.PublicNet
		jsonDaemonInfo, err = jsoniter.Marshal(v)
		if err != nil {
			return err
		}

		break
	}

	url := proxyURL + common.URLPath(api.Version, api.Cluster)
	return client.HTTPRequest(http.MethodPost, url, client.NewBytesReader(jsonDaemonInfo))
}

func unregisterMockTarget(proxyURL string, mocktgt targetMocker) error {
	url := proxyURL + common.URLPath(api.Version, api.Cluster, api.Daemon, "MOCK")
	return client.HTTPRequest(http.MethodDelete, url, nil)
}

type voteRetryMockTarget struct {
	voteInProgress bool
	errCh          chan error
}

func (*voteRetryMockTarget) filehdlr(w http.ResponseWriter, r *http.Request) {
	// Ignore all file requests
	return
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
