/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package api

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
)

func doHTTPRequest(httpClient *http.Client, method, url string, b []byte) ([]byte, error) {
	req, err := http.NewRequest(method, url, bytes.NewBuffer(b))

	if err != nil {
		return nil, fmt.Errorf("Failed to create request, err: %v", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Failed to %s, err: %v", method, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("Failed to read response, err: %v", err)
		}

		return nil, fmt.Errorf("HTTP error = %d, message = %s", resp.StatusCode, string(b))
	}
	return ioutil.ReadAll(resp.Body)
}
