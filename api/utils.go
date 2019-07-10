// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/memsys"
)

var (
	Mem2 *memsys.Mem2
)

type BaseParams struct {
	Client *http.Client
	URL    string
	Method string
}

func (bp *BaseParams) Copy() *BaseParams {
	return &BaseParams{
		Client: bp.Client,
		URL:    bp.URL,
		Method: bp.Method,
	}
}

// OptionalParams is used in constructing client-side API requests to the AIStore.
// Stores Query and Headers for providing arguments that are not used commonly in API requests
type OptionalParams struct {
	Query  url.Values
	Header http.Header
}

func init() {
	Mem2 = memsys.GMM()
}

// DoHTTPRequest sends one HTTP request and returns only the body of the response
func DoHTTPRequest(baseParams *BaseParams, path string, b []byte, optParams ...OptionalParams) ([]byte, error) {
	resp, err := doHTTPRequestGetResp(baseParams, path, b, optParams...)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

// doHTTPRequestGetResp sends one HTTP request and returns the whole response
func doHTTPRequestGetResp(baseParams *BaseParams, path string, b []byte, optParams ...OptionalParams) (*http.Response, error) {
	var (
		reqBody io.Reader
	)
	if b != nil {
		reqBody = bytes.NewBuffer(b)
	}
	url := baseParams.URL + path
	req, err := http.NewRequest(baseParams.Method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request, err: %v", err)
	}
	if len(optParams) > 0 {
		setRequestOptParams(req, optParams[0])
	}

	resp, err := baseParams.Client.Do(req)
	if err != nil {
		sleep := httpRetrySleep
		if cmn.IsErrBrokenPipe(err) || cmn.IsErrConnectionRefused(err) {
			for i := 0; i < httpMaxRetries && err != nil; i++ {
				time.Sleep(sleep)
				resp, err = baseParams.Client.Do(req)
				sleep += sleep / 2
			}
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to %s, err: %v", baseParams.Method, err)
	}
	return checkBadStatus(req, resp)
}

func checkBadStatus(req *http.Request, resp *http.Response) (*http.Response, error) {
	if resp.StatusCode >= http.StatusBadRequest {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read response, err: %v", err)
		}

		err, _ = cmn.NewHTTPError(req, string(b), resp.StatusCode)
		return nil, err
	}
	return resp, nil
}

// Given an existing HTTP Request and optional API parameters, setRequestOptParams
// sets the optional fields of the request if provided
func setRequestOptParams(req *http.Request, optParams OptionalParams) {
	if len(optParams.Query) != 0 {
		req.URL.RawQuery = optParams.Query.Encode()
	}
	req.Header = optParams.Header
}

func getObjectOptParams(options GetObjectInput) (w io.Writer, q url.Values) {
	w = ioutil.Discard
	if options.Writer != nil {
		w = options.Writer
	}
	if len(options.Query) != 0 {
		q = options.Query
	}
	return
}
