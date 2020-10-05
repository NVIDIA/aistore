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

// GetClusterMap retrieves AIStore cluster map.
func GetClusterMap(baseParams BaseParams) (smap *cluster.Smap, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Daemon),
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSmap}},
	}, &smap)
	return
}

// GetNodeClusterMap retrieves AIStore cluster map from specific node.
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

// GetClusterSysInfo retrieves AIStore system info.
func GetClusterSysInfo(baseParams BaseParams) (sysInfo cmn.ClusterSysInfo, err error) {
	baseParams.Method = http.MethodGet
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Cluster),
		Query:      url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSysInfo}},
	}, &sysInfo)
	return
}

// GetClusterStats retrieves AIStore cluster stats (all targets and current proxy).
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

// RegisterNode registers an existing node to the cluster map.
func RegisterNode(baseParams BaseParams, nodeInfo *cluster.Snode) error {
	baseParams.Method = http.MethodPost
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Cluster, cmn.UserRegister),
		Body:       cmn.MustMarshal(nodeInfo),
	})
}

// UnregisterNode unregisters an existing node from the cluster map.
func UnregisterNode(baseParams BaseParams, unregisterSID string) error {
	baseParams.Method = http.MethodDelete
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Cluster, cmn.Daemon, unregisterSID),
	})
}

// SetPrimaryProxy given a daemonID sets that corresponding proxy as the
// primary proxy of the cluster.
func SetPrimaryProxy(baseParams BaseParams, newPrimaryID string) error {
	baseParams.Method = http.MethodPut
	return DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Cluster, cmn.Proxy, newPrimaryID),
	})
}

// SetClusterConfig given key-value pairs of cluster configuration parameters,
// sets the cluster-wide configuration accordingly. Setting cluster-wide
// configuration requires sending the request to a proxy.
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
func Maintenance(baseParams BaseParams, action string, actValue *cmn.ActValDecommision) (id string, err error) {
	msg := cmn.ActionMsg{
		Action: action,
		Value:  actValue,
	}
	baseParams.Method = http.MethodPut
	err = DoHTTPRequest(ReqParams{
		BaseParams: baseParams,
		Path:       cmn.JoinWords(cmn.Version, cmn.Cluster),
		Body:       cmn.MustMarshal(msg),
	}, &id)
	return id, err
}

func Health(baseParams BaseParams) error {
	baseParams.Method = http.MethodGet
	return DoHTTPRequest(ReqParams{BaseParams: baseParams, Path: cmn.JoinWords(cmn.Version, cmn.Health)})
}

func WaitNodeAdded(baseParams BaseParams, nodeID string) (*cluster.Smap, error) {
	i, max := 0, 2

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
	i, max := 0, 2
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
