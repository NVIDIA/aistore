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

func GetMountpaths(httpClient *http.Client, targetURL string) (*cmn.MountpathList, error) {
	q := url.Values{cmn.URLParamWhat: []string{cmn.GetWhatMountpaths}}
	url := targetURL + cmn.URLPath(cmn.Version, cmn.Daemon)
	b, err := doHTTPRequest(httpClient, http.MethodGet, url, nil, q)
	if err != nil {
		return nil, err
	}
	mpl := &cmn.MountpathList{}
	err = json.Unmarshal(b, mpl)
	return mpl, err
}

func AddMountpath(httpClient *http.Client, targetURL, mountPath string) error {
	url := targetURL + cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathAdd, Value: mountPath})
	if err != nil {
		return err
	}
	_, err = doHTTPRequest(httpClient, http.MethodPut, url, msg)
	return err
}

func RemoveMountpath(httpClient *http.Client, targetURL, mountPath string) error {
	url := targetURL + cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathRemove, Value: mountPath})
	if err != nil {
		return err
	}
	_, err = doHTTPRequest(httpClient, http.MethodDelete, url, msg)
	return err
}

func EnableMountpath(httpClient *http.Client, targetURL, mountPath string) error {
	url := targetURL + cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathEnable, Value: mountPath})
	if err != nil {
		return err
	}
	_, err = doHTTPRequest(httpClient, http.MethodPost, url, msg)
	return err
}

func DisableMountpath(httpClient *http.Client, targetURL, mountPath string) error {
	url := targetURL + cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Mountpaths)
	msg, err := json.Marshal(cmn.ActionMsg{Action: cmn.ActMountpathDisable, Value: mountPath})
	if err != nil {
		return err
	}
	_, err = doHTTPRequest(httpClient, http.MethodPost, url, msg)
	return err
}
