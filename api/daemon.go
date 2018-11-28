// Package api provides RESTful API to DFC object storage
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/NVIDIA/dfcpub/cmn"
	jsoniter "github.com/json-iterator/go"
)

// GetMountpaths API operation for DFC
//
// Given the direct public URL of the target, GetMountPaths returns its mountpaths and error, if any exists
func GetMountpaths(baseParams *BaseParams) (*cmn.MountpathList, error) {
	q := url.Values{cmn.URLParamWhat: []string{cmn.GetWhatMountpaths}}
	optParams := OptionalParams{Query: q}
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Daemon)
	b, err := DoHTTPRequest(baseParams, path, nil, optParams)
	if err != nil {
		return nil, err
	}
	mpl := &cmn.MountpathList{}
	err = json.Unmarshal(b, mpl)
	return mpl, err
}

// AddMountpath API operation for DFC
func AddMountpath(baseParams *BaseParams, mountPath string) error {
	baseParams.Method = http.MethodPut
	path := cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathAdd, Value: mountPath})
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(baseParams, path, msg)
	return err
}

// RemoveMountpath API operation for DFC
func RemoveMountpath(baseParams *BaseParams, mountPath string) error {
	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathRemove, Value: mountPath})
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(baseParams, path, msg)
	return err
}

// EnableMountpath API operation for DFC
func EnableMountpath(baseParams *BaseParams, mountPath string) error {
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathEnable, Value: mountPath})
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(baseParams, path, msg)
	return err
}

// DisableMountpath API operation for DFC
func DisableMountpath(baseParams *BaseParams, mountPath string) error {
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathDisable, Value: mountPath})
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(baseParams, path, msg)
	return err
}

// GetConfig API operation for DFC
//
// Returns the configuration of a specific daemon in a cluster
func GetDaemonConfig(baseParams *BaseParams) (config *cmn.Config, err error) {
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Daemon)
	query := url.Values{cmn.URLParamWhat: []string{cmn.GetWhatConfig}}
	optParams := OptionalParams{Query: query}
	resp, err := doHTTPRequestGetResp(baseParams, path, nil, optParams)
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

// SetDaemonConfig API operation for DFC
//
// Given a key and a value for a specific configuration parameter
// this operation sets the configuration accordingly for a specific daemon
func SetDaemonConfig(baseParams *BaseParams, key string, value interface{}) error {
	valstr, err := convertToString(value)
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPut
	path := cmn.URLPath(cmn.Version, cmn.Daemon)
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
