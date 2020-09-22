// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/stats"
)

const (
	waitNodeStarted = 10 * time.Second
)

// GetClusterMap API
//
// GetClusterMap retrieves AIStore cluster map
func GetClusterMap(baseParams BaseParams) (smap *cluster.Smap, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Daemon),
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSmap}},
	}, &smap)
	return
}

// GetNodeClusterMap API
//
// GetNodeClusterMap retrieves AIStore cluster map from specific node
func GetNodeClusterMap(baseParams BaseParams, nodeID string) (smap *cluster.Smap, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Reverse, cmn.Daemon),
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSmap}},
		Header:     http.Header{cmn.HeaderNodeID: []string{nodeID}},
	}, &smap)
	return
}

// GetClusterSysInfo API
//
// GetClusterSysInfo retrieves AIStore system info
func GetClusterSysInfo(baseParams BaseParams) (sysInfo cmn.ClusterSysInfo, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Cluster),
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSysInfo}},
	}, &sysInfo)
	return
}

// GetClusterStats API
//
// GetClusterStats retrieves AIStore cluster stats (all targets and current proxy)
func GetClusterStats(baseParams BaseParams) (clusterStats stats.ClusterStats, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Cluster),
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatStats}},
	}, &clusterStats)
	return
}

func GetTargetDiskStats(baseParams BaseParams, targetID string) (diskStats map[string]*ios.SelectedDiskStats, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Reverse, cmn.Daemon),
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatDiskStats}},
		Header:     http.Header{cmn.HeaderNodeID: []string{targetID}},
	}, &diskStats)
	return
}

func GetRemoteAIS(baseParams BaseParams) (aisInfo cmn.CloudInfoAIS, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Cluster),
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatRemoteAIS}},
	}, &aisInfo)
	return
}

// RegisterNode API
//
// Registers an existing node to the clustermap.
func RegisterNode(baseParams BaseParams, nodeInfo *cluster.Snode) error {
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Cluster, cmn.UserRegister),
		Body:       cmn.MustMarshal(nodeInfo),
	})
}

// UnregisterNode API
//
// Unregisters an existing node from the clustermap.
func UnregisterNode(baseParams BaseParams, unregisterSID string) error {
	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Cluster, cmn.Daemon, unregisterSID),
	})
}

// SetPrimaryProxy API
//
// Given a daemonID, it sets that corresponding proxy as the primary proxy of the cluster
func SetPrimaryProxy(baseParams BaseParams, newPrimaryID string) error {
	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Cluster, cmn.Proxy, newPrimaryID),
	})
}

// SendOwnershipTbl API
//
// Given a daemonID of a IC member, send the ownership table
func SendOwnershipTbl(baseParams BaseParams, daemonID string) error {
	baseParams.Method = http.MethodPut
	msg := cmn.ActionMsg{Action: cmn.ActSendOwnershipTbl, Value: daemonID}
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Cluster),
		Body:       cmn.MustMarshal(msg),
	})
}

// SetClusterConfig API
//
// Given key-value pairs of cluster configuration parameters,
// this operation sets the cluster-wide configuration accordingly.
// Setting cluster-wide configuration requires sending the request to a proxy
func SetClusterConfig(baseParams BaseParams, nvs cmn.SimpleKVs) error {
	q := url.Values{}
	for key, val := range nvs {
		q.Add(key, val)
	}
	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Cluster, cmn.ActSetConfig),
		Query:      q,
	})
}

// AttachRemoteAIS API
//
// TODO: add APIs to attach or enable (detach or disable) mountpath - use cmn.GetWhatMountpaths
//
func AttachRemoteAIS(baseParams BaseParams, alias, u string) error {
	q := make(url.Values)
	q.Set(cmn.URLParamWhat, cmn.GetWhatRemoteAIS)
	q.Set(alias, u)
	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Cluster, cmn.ActAttach),
		Query:      q,
	})
}

// DetachRemoteAIS API
//
func DetachRemoteAIS(baseParams BaseParams, alias string) error {
	q := make(url.Values)
	q.Set(cmn.URLParamWhat, cmn.GetWhatRemoteAIS)
	q.Set(alias, "")
	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Cluster, cmn.ActDetach),
		Query:      q,
	})
}

// Maintenance API
//
func Maintenance(baseParams BaseParams, sid, action string) error {
	msg := cmn.ActionMsg{
		Action: action,
		Value:  sid,
	}
	baseParams.Method = http.MethodPut
	err := DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Cluster),
		Body:       cmn.MustMarshal(msg),
	})
	return err
}

func Health(baseParams BaseParams) error {
	baseParams.Method = http.MethodGet
	return DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.JoinWords(cmn.Version, cmn.Health)})
}

func WaitNodeAdded(baseParams BaseParams, nodeID string) (*cluster.Smap, error) {
	var (
		i, max = 0, 2
	)

retry:
	smap, err := GetClusterMap(baseParams)
	if err != nil {
		return nil, err
	}
	node := smap.GetNode(nodeID)
	if node != nil {
		baseParams.URL = node.URL(cmn.NetworkPublic)
		return smap, waitStarted(baseParams)
	}
	time.Sleep(waitNodeStarted)
	i++
	if i > max {
		return nil, fmt.Errorf("max retry (%d) exceeded - node not in smap", max)
	}

	goto retry
}

func waitStarted(baseParams BaseParams) (err error) {
	var (
		i, max = 0, 2
	)
while503:
	err = Health(baseParams)
	if err == nil {
		return nil
	}
	if HTTPStatus(err) != http.StatusServiceUnavailable {
		return
	}
	time.Sleep(waitNodeStarted)
	i++
	if i > max {
		return fmt.Errorf("node start failed - max retries (%d) exceeded", max)
	}
	goto while503
}
