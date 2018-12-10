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
func GetMountpaths(httpClient *http.Client, targetURL string) (*cmn.MountpathList, error) {
	q := url.Values{cmn.URLParamWhat: []string{cmn.GetWhatMountpaths}}
	optParams := ParamsOptional{Query: q}
	url := targetURL + cmn.URLPath(cmn.Version, cmn.Daemon)
	b, err := DoHTTPRequest(httpClient, http.MethodGet, url, nil, optParams)
	if err != nil {
		return nil, err
	}
	mpl := &cmn.MountpathList{}
	err = json.Unmarshal(b, mpl)
	return mpl, err
}

// AddMountpath API operation for DFC
func AddMountpath(httpClient *http.Client, targetURL, mountPath string) error {
	url := targetURL + cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathAdd, Value: mountPath})
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(httpClient, http.MethodPut, url, msg)
	return err
}

// RemoveMountpath API operation for DFC
func RemoveMountpath(httpClient *http.Client, targetURL, mountPath string) error {
	url := targetURL + cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathRemove, Value: mountPath})
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(httpClient, http.MethodDelete, url, msg)
	return err
}

// EnableMountpath API operation for DFC
func EnableMountpath(httpClient *http.Client, targetURL, mountPath string) error {
	url := targetURL + cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathEnable, Value: mountPath})
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(httpClient, http.MethodPost, url, msg)
	return err
}

// DisableMountpath API operation for DFC
func DisableMountpath(httpClient *http.Client, targetURL, mountPath string) error {
	url := targetURL + cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathDisable, Value: mountPath})
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(httpClient, http.MethodPost, url, msg)
	return err
}

// GetConfig API operation for DFC
//
// Returns the configuration of a specific daemon in a cluster
func GetDaemonConfig(httpClient *http.Client, daemonURL string) (dfcfg map[string]interface{}, err error) {
	reqURL := daemonURL + cmn.URLPath(cmn.Version, cmn.Daemon)
	query := url.Values{cmn.URLParamWhat: []string{cmn.GetWhatConfig}}
	optParams := ParamsOptional{Query: query}
	resp, err := doHTTPRequestGetResp(httpClient, http.MethodGet, reqURL, nil, optParams)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = jsoniter.Unmarshal(b, &dfcfg)
	if err != nil {
		return nil, err
	}
	return
}

// SetDaemonConfig API operation for DFC
//
// Given a key and a value for a specific configuration parameter
// this operation sets the configuration accordingly for a specific daemon
func SetDaemonConfig(httpClient *http.Client, daemonURL, key string, value interface{}) error {
	valstr, err := convertToString(value)
	if err != nil {
		return err
	}
	url := daemonURL + cmn.URLPath(cmn.Version, cmn.Daemon)
	configMsg := cmn.ActionMsg{
		Action: cmn.ActSetConfig,
		Name:   key,
		Value:  valstr,
	}
	msg, err := jsoniter.Marshal(configMsg)
	if err != nil {
		return err
	}
	_, err = DoHTTPRequest(httpClient, http.MethodPut, url, msg)
	return err
}
