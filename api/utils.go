// Package api provides RESTful API to DFC object storage
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
	"strconv"

	"github.com/NVIDIA/dfcpub/memsys"
)

var (
	Mem2 *memsys.Mem2
)

type BaseParams struct {
	Client *http.Client
	URL    string
	Method string
}

// OptionalParams is used in constructing client-side API requests to the DFC backend.
// Stores Query and Headers for providing arguments that are not used commonly in API requests
type OptionalParams struct {
	Query  url.Values
	Header http.Header
}

func init() {
	Mem2 = memsys.Init()
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
		return nil, fmt.Errorf("Failed to create request, err: %v", err)
	}
	if len(optParams) > 0 {
		setRequestOptParams(req, optParams[0])
	}

	resp, err := baseParams.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Failed to %s, err: %v", baseParams.Method, err)
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

func convertToString(value interface{}) (valstr string, err error) {
	if v, ok := value.(string); ok {
		valstr = v
	} else if v, ok := value.(bool); ok {
		valstr = strconv.FormatBool(v)
	} else if v, ok := value.(int); ok {
		valstr = strconv.Itoa(v)
	} else if v, ok := value.(int64); ok {
		valstr = strconv.FormatInt(v, 10)
	} else if v, ok := value.(float64); ok {
		valstr = strconv.FormatFloat(v, 'f', -1, 64)
	} else {
		err = fmt.Errorf("Failed to assert type on config param: %v (type %T)", value, value)
	}
	return
}
