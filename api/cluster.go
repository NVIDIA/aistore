/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
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
)

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
