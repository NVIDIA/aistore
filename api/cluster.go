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
func GetClusterMap(httpClient *http.Client, proxyURL string) (cluster.Smap, error) {
	var (
		q    = url.Values{}
		body []byte
		smap cluster.Smap
	)
	q.Add(cmn.URLParamWhat, cmn.GetWhatSmap)
	query := q.Encode()
	requestURL := fmt.Sprintf("%s?%s", proxyURL+cmn.URLPath(cmn.Version, cmn.Daemon), query)

	resp, err := httpClient.Get(requestURL)
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
func RegisterTarget(httpClient *http.Client, proxyURL string, targetInfo *cluster.Snode) error {
	targetJSON, err := jsoniter.Marshal(*targetInfo)
	if err != nil {
		return err
	}
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Register)
	_, err = DoHTTPRequest(httpClient, http.MethodPost, url, targetJSON)
	return err
}

// UnregisterTarget API operation for DFC
//
// Unregisters an existing target to the clustermap.
func UnregisterTarget(httpClient *http.Client, proxyURL, unregisterSID string) error {
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Daemon, unregisterSID)
	_, err := DoHTTPRequest(httpClient, http.MethodDelete, url, nil)
	return err
}

// SetPrimaryProxy API operation for DFC
//
// Given a daemonID, it sets that corresponding proxy as the primary proxy of the cluster
func SetPrimaryProxy(httpClient *http.Client, proxyURL, newPrimaryID string) error {
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Proxy, newPrimaryID)
	_, err := DoHTTPRequest(httpClient, http.MethodPut, url, nil)
	return err
}
