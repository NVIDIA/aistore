// Package api provides RESTful API to DFC object storage
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/NVIDIA/dfcpub/cmn"
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
