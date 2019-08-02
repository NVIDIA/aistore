// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/ios"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
	jsoniter "github.com/json-iterator/go"
)

// GetClusterMap API
//
// GetClusterMap retrieves AIStore cluster map
func GetClusterMap(baseParams *BaseParams) (smap cluster.Smap, err error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Daemon)
	params := OptionalParams{Query: url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSmap}}}

	resp, err := doHTTPRequestGetResp(baseParams, path, nil, params)
	if err != nil {
		return cluster.Smap{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return cluster.Smap{}, fmt.Errorf("failed to get Smap, HTTP status %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return cluster.Smap{}, err
	}
	err = json.Unmarshal(body, &smap)
	if err != nil {
		return cluster.Smap{}, fmt.Errorf("failed to unmarshal Smap, err: %v", err)
	}
	return smap, nil
}

// GetNodeClusterMap API
//
// GetNodeClusterMap retrieves AIStore cluster map from specific node
func GetNodeClusterMap(baseParams *BaseParams, nodeID string) (smap cluster.Smap, err error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Reverse, cmn.Daemon)
	params := OptionalParams{
		Query:  url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSmap}},
		Header: http.Header{cmn.HeaderNodeID: []string{nodeID}},
	}

	resp, err := doHTTPRequestGetResp(baseParams, path, nil, params)
	if err != nil {
		return cluster.Smap{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return cluster.Smap{}, fmt.Errorf("failed to get Smap, HTTP status %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return cluster.Smap{}, err
	}
	err = json.Unmarshal(body, &smap)
	if err != nil {
		return cluster.Smap{}, fmt.Errorf("failed to unmarshal Smap, err: %v", err)
	}
	return smap, nil
}

// GetClusterSysInfo API
//
// GetClusterSysInfo retrieves AIStore system info
func GetClusterSysInfo(baseParams *BaseParams) (sysinfo cmn.ClusterSysInfo, err error) {
	baseParams.Method = http.MethodGet
	query := url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSysInfo}}
	path := cmn.URLPath(cmn.Version, cmn.Cluster)
	params := OptionalParams{Query: query}

	resp, err := doHTTPRequestGetResp(baseParams, path, nil, params)
	if err != nil {
		return cmn.ClusterSysInfo{}, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return cmn.ClusterSysInfo{}, err
	}
	err = json.Unmarshal(body, &sysinfo)
	if err != nil {
		return cmn.ClusterSysInfo{}, fmt.Errorf("failed to unmarshal Smap, err: %v", err)
	}

	return sysinfo, nil
}

// GetClusterStats API
//
// GetClusterStats retrieves AIStore cluster stats (all targets and current proxy)
func GetClusterStats(baseParams *BaseParams) (clusterStats stats.ClusterStats, err error) {
	baseParams.Method = http.MethodGet
	query := url.Values{cmn.URLParamWhat: []string{cmn.GetWhatStats}}
	path := cmn.URLPath(cmn.Version, cmn.Cluster)
	params := OptionalParams{Query: query}

	resp, err := doHTTPRequestGetResp(baseParams, path, nil, params)
	if err != nil {
		return stats.ClusterStats{}, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return stats.ClusterStats{}, err
	}
	err = json.Unmarshal(body, &clusterStats)
	if err != nil {
		return stats.ClusterStats{}, fmt.Errorf("failed to unmarshal cluster stats, err: %v", err)
	}
	return clusterStats, nil
}

func GetTargetDiskStats(baseParams *BaseParams, targetID string) (map[string]*ios.SelectedDiskStats, error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Reverse, cmn.Daemon)
	params := OptionalParams{
		Query:  url.Values{cmn.URLParamWhat: []string{cmn.GetWhatDiskStats}},
		Header: http.Header{cmn.HeaderNodeID: []string{targetID}},
	}

	resp, err := doHTTPRequestGetResp(baseParams, path, nil, params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var diskStats map[string]*ios.SelectedDiskStats
	err = json.Unmarshal(body, &diskStats)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal target disk stats, err: %v", err)
	}
	return diskStats, nil
}

// RegisterNode API
//
// Registers an existing node to the clustermap.
func RegisterNode(baseParams *BaseParams, nodeInfo *cluster.Snode) error {
	nodeJSON, err := jsoniter.Marshal(nodeInfo)
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Register)
	_, err = DoHTTPRequest(baseParams, path, nodeJSON)
	return err
}

// UnregisterNode API
//
// Unregisters an existing node from the clustermap.
func UnregisterNode(baseParams *BaseParams, unregisterSID string) error {
	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Daemon, unregisterSID)
	_, err := DoHTTPRequest(baseParams, path, nil)
	return err
}

// SetPrimaryProxy API
//
// Given a daemonID, it sets that corresponding proxy as the primary proxy of the cluster
func SetPrimaryProxy(baseParams *BaseParams, newPrimaryID string) error {
	baseParams.Method = http.MethodPut
	path := cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Proxy, newPrimaryID)
	_, err := DoHTTPRequest(baseParams, path, nil)
	return err
}

// SetClusterConfig API
//
// Given key-value pairs of cluster configuration parameters,
// this operation sets the cluster-wide configuration accordingly.
// Setting cluster-wide configuration requires sending the request to a proxy
func SetClusterConfig(baseParams *BaseParams, nvs cmn.SimpleKVs) error {
	optParams := OptionalParams{}
	q := url.Values{}

	for key, val := range nvs {
		q.Add(key, val)
	}
	baseParams.Method = http.MethodPut
	path := cmn.URLPath(cmn.Version, cmn.Cluster, cmn.ActSetConfig)
	optParams.Query = q
	_, err := DoHTTPRequest(baseParams, path, nil, optParams)
	return err
}
