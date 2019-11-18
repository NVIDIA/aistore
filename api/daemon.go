// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
	jsoniter "github.com/json-iterator/go"
)

// GetMountpaths API
//
// Given the direct public URL of the target, GetMountPaths returns its mountpaths and error, if any exists
func GetMountpaths(baseParams BaseParams, node *cluster.Snode) (*cmn.MountpathList, error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Reverse, cmn.Daemon)
	params := OptionalParams{
		Query: url.Values{cmn.URLParamWhat: []string{cmn.GetWhatMountpaths}},
		Header: http.Header{
			cmn.HeaderNodeID:  []string{node.ID()},
			cmn.HeaderNodeURL: []string{node.URL(cmn.NetworkPublic)},
		},
	}

	b, err := DoHTTPRequest(baseParams, path, nil, params)
	if err != nil {
		return nil, err
	}
	mpl := &cmn.MountpathList{}
	err = json.Unmarshal(b, mpl)
	return mpl, err
}

// AddMountpath API
func AddMountpath(baseParams BaseParams, node *cluster.Snode, mountpath string) error {
	baseParams.Method = http.MethodPut
	path := cmn.URLPath(cmn.Version, cmn.Reverse, cmn.Daemon, cmn.Mountpaths)
	params := OptionalParams{
		Header: http.Header{
			cmn.HeaderNodeID:  []string{node.ID()},
			cmn.HeaderNodeURL: []string{node.URL(cmn.NetworkPublic)},
		},
	}
	msg, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathAdd, Value: mountpath})
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(baseParams, path, msg, params)
	return err
}

// RemoveMountpath API
func RemoveMountpath(baseParams BaseParams, nodeID string, mountpath string) error {
	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Reverse, cmn.Daemon, cmn.Mountpaths)
	params := OptionalParams{
		Header: http.Header{cmn.HeaderNodeID: []string{nodeID}},
	}
	msg, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathRemove, Value: mountpath})
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(baseParams, path, msg, params)
	return err
}

// EnableMountpath API
func EnableMountpath(baseParams BaseParams, node *cluster.Snode, mountpath string) error {
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Reverse, cmn.Daemon, cmn.Mountpaths)
	params := OptionalParams{
		Header: http.Header{
			cmn.HeaderNodeID:  []string{node.ID()},
			cmn.HeaderNodeURL: []string{node.URL(cmn.NetworkPublic)},
		},
	}
	msg, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathEnable, Value: mountpath})
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(baseParams, path, msg, params)
	return err
}

// DisableMountpath API
func DisableMountpath(baseParams BaseParams, nodeID string, mountpath string) error {
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Reverse, cmn.Daemon, cmn.Mountpaths)
	params := OptionalParams{
		Header: http.Header{cmn.HeaderNodeID: []string{nodeID}},
	}
	msg, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathDisable, Value: mountpath})
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(baseParams, path, msg, params)
	return err
}

// GetConfig API
//
// Returns the configuration of a specific daemon in a cluster
func GetDaemonConfig(baseParams BaseParams, nodeID string) (config *cmn.Config, err error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Reverse, cmn.Daemon)
	params := OptionalParams{
		Query:  url.Values{cmn.URLParamWhat: []string{cmn.GetWhatConfig}},
		Header: http.Header{cmn.HeaderNodeID: []string{nodeID}},
	}

	resp, err := doHTTPRequestGetResp(baseParams, path, nil, params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = jsoniter.Unmarshal(b, &config)
	if err != nil {
		return nil, err
	}
	return
}

// GetDaemonSysInfo API
//
// Returns the system info of a specific daemon in the cluster
// Note that FSCapacity will be zero if proxy
func GetDaemonSysInfo(baseParams BaseParams, nodeID string) (sysInfo *cmn.TSysInfo, err error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Reverse, cmn.Daemon)
	params := OptionalParams{
		Query:  url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSysInfo}},
		Header: http.Header{cmn.HeaderNodeID: []string{nodeID}},
	}

	resp, err := doHTTPRequestGetResp(baseParams, path, nil, params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = jsoniter.Unmarshal(b, &sysInfo)
	if err != nil {
		return nil, err
	}
	return
}

// GetDaemonInfo API
//
// Returns the info of a specific Daemon in the cluster
func GetDaemonStatus(baseParams BaseParams, nodeID string) (daeInfo *stats.DaemonStatus, err error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Reverse, cmn.Daemon)
	params := OptionalParams{
		Query:  url.Values{cmn.URLParamWhat: []string{cmn.GetWhatDaemonStatus}},
		Header: http.Header{cmn.HeaderNodeID: []string{nodeID}},
	}

	resp, err := doHTTPRequestGetResp(baseParams, path, nil, params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = jsoniter.Unmarshal(b, &daeInfo)
	if err != nil {
		return nil, err
	}
	return
}

// SetDaemonConfig API
//
// Given key value pairs, this operation sets the configuration accordingly for a specific daemon
func SetDaemonConfig(baseParams BaseParams, nodeID string, nvs cmn.SimpleKVs) error {
	baseParams.Method = http.MethodPut
	path := cmn.URLPath(cmn.Version, cmn.Reverse, cmn.Daemon, cmn.ActSetConfig)
	optParams := OptionalParams{
		Query:  url.Values{},
		Header: http.Header{cmn.HeaderNodeID: []string{nodeID}},
	}
	for key, val := range nvs {
		optParams.Query.Add(key, val)
	}
	_, err := DoHTTPRequest(baseParams, path, nil, optParams)
	return err
}
