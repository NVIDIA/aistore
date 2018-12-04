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

	"github.com/NVIDIA/dfcpub/cmn"
)

func GetMountpaths(httpClient *http.Client, targetURL string) (*cmn.MountpathList, error) {
	q := url.Values{cmn.URLParamWhat: []string{cmn.GetWhatMountpaths}}
	url := targetURL + cmn.URLPath(cmn.Version, cmn.Daemon)
	resp, err := doHTTPRequestGetResp(httpClient, http.MethodGet, url, nil, q)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to read response, err: %v", err)
	}
	mp := &cmn.MountpathList{}
	err = json.Unmarshal(b, mp)
	return mp, err
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
