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

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
	jsoniter "github.com/json-iterator/go"
)

// GetMountpaths API
//
// Given the direct public URL of the target, GetMountPaths returns its mountpaths and error, if any exists
func GetMountpaths(baseParams *BaseParams) (*cmn.MountpathList, error) {
	q := url.Values{cmn.URLParamWhat: []string{cmn.GetWhatMountpaths}}
	params := OptionalParams{Query: q}
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Daemon)

	b, err := DoHTTPRequest(baseParams, path, nil, params)
	if err != nil {
		return nil, err
	}
	mpl := &cmn.MountpathList{}
	err = json.Unmarshal(b, mpl)
	return mpl, err
}

// AddMountpath API
func AddMountpath(baseParams *BaseParams, mountPath string) error {
	baseParams.Method = http.MethodPut
	path := cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathAdd, Value: mountPath})
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(baseParams, path, msg)
	return err
}

// RemoveMountpath API
func RemoveMountpath(baseParams *BaseParams, mountPath string) error {
	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathRemove, Value: mountPath})
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(baseParams, path, msg)
	return err
}

// EnableMountpath API
func EnableMountpath(baseParams *BaseParams, mountPath string) error {
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathEnable, Value: mountPath})
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(baseParams, path, msg)
	return err
}

// DisableMountpath API
func DisableMountpath(baseParams *BaseParams, mountPath string) error {
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathDisable, Value: mountPath})
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(baseParams, path, msg)
	return err
}

// GetConfig API
//
// Returns the configuration of a specific daemon in a cluster
func GetDaemonConfig(baseParams *BaseParams) (config *cmn.Config, err error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Daemon)
	query := url.Values{cmn.URLParamWhat: []string{cmn.GetWhatConfig}}
	params := OptionalParams{Query: query}

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
func GetDaemonSysInfo(baseParams *BaseParams) (sysInfo *cmn.TSysInfo, err error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Daemon)
	query := url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSysInfo}}
	params := OptionalParams{Query: query}

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
func GetDaemonStatus(baseParams *BaseParams) (daeInfo *stats.DaemonStatus, err error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Daemon)
	query := url.Values{cmn.URLParamWhat: []string{cmn.GetWhatDaemonStatus}}
	params := OptionalParams{Query: query}

	resp, err := doHTTPRequestGetResp(baseParams, path, nil, params)
	if err != nil {
		return &stats.DaemonStatus{}, err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return &stats.DaemonStatus{}, err
	}

	err = jsoniter.Unmarshal(b, &daeInfo)
	if err != nil {
		return &stats.DaemonStatus{}, err
	}
	return
}

// SetDaemonConfig API
//
// Given key value pairs, this operation sets the configuration accordingly for a specific daemon
func SetDaemonConfig(baseParams *BaseParams, nvs cmn.SimpleKVs) error {
	optParams := OptionalParams{}
	q := url.Values{}

	for key, val := range nvs {
		q.Add(key, val)
	}
	baseParams.Method = http.MethodPut
	path := cmn.URLPath(cmn.Version, cmn.Daemon, cmn.ActSetConfig)
	optParams.Query = q
	_, err := DoHTTPRequest(baseParams, path, nil, optParams)
	return err
}
