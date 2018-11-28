// Package api provides RESTful API to DFC object storage
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

	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/json-iterator/go"
)

// GetClusterMap API operation for DFC
//
// GetClusterMap retrives a DFC's server map
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
		return cluster.Smap{}, fmt.Errorf("Failed to unmarshal Smap, err: %v", err)
	}
	return smap, nil
}

// RegisterTarget API operation for DFC
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

// UnregisterTarget API operation for DFC
//
// Unregisters an existing target to the clustermap.
func UnregisterTarget(baseParams *BaseParams, unregisterSID string) error {
	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Daemon, unregisterSID)
	_, err := DoHTTPRequest(baseParams, path, nil)
	return err
}

// SetPrimaryProxy API operation for DFC
//
// Given a daemonID, it sets that corresponding proxy as the primary proxy of the cluster
func SetPrimaryProxy(baseParams *BaseParams, newPrimaryID string) error {
	baseParams.Method = http.MethodPut
	path := cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Proxy, newPrimaryID)
	_, err := DoHTTPRequest(baseParams, path, nil)
	return err
}

// SetClusterConfig API operation for DFC
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
