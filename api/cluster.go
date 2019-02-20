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

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

// GetClusterMap API
//
// GetClusterMap retrieves AIStore cluster map
func GetClusterMap(baseParams *BaseParams) (cluster.Smap, error) {
	var (
		q    = url.Values{}
		body []byte
		smap cluster.Smap
	)
	q.Add(cmn.URLParamWhat, cmn.GetWhatSmap)
	query := q.Encode()
	requestURL := fmt.Sprintf("%s?%s", baseParams.URL+cmn.URLPath(cmn.Version, cmn.Daemon), query)

	resp, err := baseParams.Client.Get(requestURL)
	if err != nil {
		return cluster.Smap{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return cluster.Smap{}, fmt.Errorf("get Smap, HTTP status %d", resp.StatusCode)
	}

	body, err = ioutil.ReadAll(resp.Body)
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
func GetClusterSysInfo(baseParams *BaseParams) (cmn.ClusterSysInfo, error) {
	var (
		q       = url.Values{}
		body    []byte
		sysinfo cmn.ClusterSysInfo
	)
	q.Add(cmn.URLParamWhat, cmn.GetWhatSysInfo)
	query := q.Encode()
	requestURL := fmt.Sprintf("%s?%s", baseParams.URL+cmn.URLPath(cmn.Version, cmn.Cluster), query)

	resp, err := baseParams.Client.Get(requestURL)
	if err != nil {
		return cmn.ClusterSysInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return cmn.ClusterSysInfo{}, fmt.Errorf("get SysInfo, HTTP status %d", resp.StatusCode)
	}

	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return cmn.ClusterSysInfo{}, err
	}
	err = json.Unmarshal(body, &sysinfo)
	if err != nil {
		return cmn.ClusterSysInfo{}, fmt.Errorf("failed to unmarshal Smap, err: %v", err)
	}

	return sysinfo, nil
}

// RegisterTarget API
//
// Registers an existing target to the clustermap.
func RegisterTarget(baseParams *BaseParams, targetInfo *cluster.Snode) error {
	targetJSON, err := jsoniter.Marshal(*targetInfo)
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Register)
	_, err = DoHTTPRequest(baseParams, path, targetJSON)
	return err
}

// UnregisterTarget API
//
// Unregisters an existing target to the clustermap.
func UnregisterTarget(baseParams *BaseParams, unregisterSID string) error {
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
// Given a key and a value for a specific configuration parameter
// this operation sets the cluster-wide configuration accordingly.
// Setting cluster-wide configuration requires sending the request to a proxy
func SetClusterConfig(baseParams *BaseParams, key string, value interface{}) error {
	valstr, err := convertToString(value)
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPut
	path := cmn.URLPath(cmn.Version, cmn.Cluster)
	configMsg := cmn.ActionMsg{
		Action: cmn.ActSetConfig,
		Name:   key,
		Value:  valstr,
	}
	msg, err := jsoniter.Marshal(configMsg)
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(baseParams, path, msg)
	return err
}
