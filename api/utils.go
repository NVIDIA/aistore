/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package api

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/NVIDIA/dfcpub/memsys"
)

var (
	Mem2 *memsys.Mem2
)

func init() {
	Mem2 = memsys.Init()
}

func doHTTPRequest(httpClient *http.Client, method, url string, b []byte) ([]byte, error) {
	resp, err := doHTTPRequestGetResp(httpClient, method, url, b)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

func doHTTPRequestGetResp(httpClient *http.Client, method, url string, b []byte, query ...url.Values) (*http.Response, error) {
	var reqBody io.Reader
	if b != nil {
		reqBody = bytes.NewBuffer(b)
	}
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("Failed to create request, err: %v", err)
	}
	if len(query) > 0 && len(query[0]) > 0 {
		req.URL.RawQuery = query[0].Encode()
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Failed to %s, err: %v", method, err)
	}
	if resp.StatusCode >= http.StatusBadRequest {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("Failed to read response, err: %v", err)
		}
		return nil, fmt.Errorf("HTTP error = %d, message = %s", resp.StatusCode, string(b))
	}
	return resp, nil
}

func getObjectOptParams(options GetObjectInput) (w io.Writer, q map[string][]string) {
	if options.Writer != nil {
		w = options.Writer
	}
	if len(options.Query) > 0 {
		q = options.Query
	}
	return
}
