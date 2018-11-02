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

func doHTTPRequest(httpClient *http.Client, action, method, url string, b []byte) error {
	req, err := http.NewRequest(method, url, bytes.NewBuffer(b))

	if err != nil {
		return fmt.Errorf("Failed to create request, err: %v", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("Failed to %s on %s, err: %v", action, method, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("Failed to read response, err: %v", err)
		}

		return fmt.Errorf("HTTP error = %d, message = %s", resp.StatusCode, string(b))
	}
	_, err = ioutil.ReadAll(resp.Body)
	return err
}
