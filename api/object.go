/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package api

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/NVIDIA/dfcpub/cmn"
)

// HeadObject API operation for DFC
//
// Returns the size and version of the object specified by bucket/object
func HeadObject(httpClient *http.Client, proxyURL, bucket, object string) (*cmn.ObjectProps, error) {
	r, err := httpClient.Head(proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, bucket, object))
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()

	if r != nil && r.StatusCode >= http.StatusBadRequest {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, fmt.Errorf("Failed to read response, err: %v", err)
		}
		return nil, fmt.Errorf("HEAD bucket/object: %s/%s failed, HTTP status: %d, HTTP response: %s",
			bucket, object, r.StatusCode, string(b))
	}

	size, err := strconv.Atoi(r.Header.Get(cmn.HeaderSize))
	if err != nil {
		return nil, err
	}

	return &cmn.ObjectProps{
		Size:    size,
		Version: r.Header.Get(cmn.HeaderVersion),
	}, nil
}

// DeleteObject API operation for DFC
//
// Deletes an object specified by bucket/object
func DeleteObject(httpClient *http.Client, proxyURL, bucket, object string) (err error) {
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)
	_, err = doHTTPRequest(httpClient, http.MethodDelete, url, nil)
	return err
}
