// Package main - authorization server for AIStore. See README.md for more info.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"bytes"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais"
	"github.com/NVIDIA/aistore/cmn"
)

// update list of revoked token on all clusters
func (m *userManager) broadcastRevoked(token string) {
	tokenList := ais.TokenList{Tokens: []string{token}}
	body := cmn.MustMarshal(tokenList)
	m.broadcast(http.MethodDelete, cmn.Tokens, body)
}

// broadcast the request to all clusters. If a cluster has a few URLS,
// it sends to the first working one. Clusters are processed in parallel.
func (m *userManager) broadcast(method, path string, body []byte) {
	cluList, err := m.clusterList()
	if err != nil {
		glog.Errorf("Failed to read cluster list: %v", err)
		return
	}
	wg := &sync.WaitGroup{}
	for _, clu := range cluList {
		wg.Add(1)
		go func(clu *cmn.AuthCluster) {
			defer wg.Done()
			var err error
			for _, u := range clu.URLs {
				if err = m.proxyRequest(method, u, path, body); err == nil {
					break
				}
			}
			if err != nil {
				glog.Errorf("Failed to sync revoked tokens with %q: %v", clu.ID, err)
			}
		}(clu)
	}
	wg.Wait()
}

// Generic function to send everything to a proxy
func (m *userManager) proxyRequest(method, proxyURL, path string, injson []byte) error {
	startRequest := time.Now()
	for {
		url := proxyURL + cmn.URLPath(cmn.Version, path)
		request, err := http.NewRequest(method, url, bytes.NewBuffer(injson))
		if err != nil {
			return err
		}

		client := m.clientHTTP
		if cmn.IsHTTPS(proxyURL) {
			client = m.clientHTTPS
		}
		request.Header.Set(cmn.HeaderContentType, cmn.ContentJSON)
		response, err := client.Do(request)
		var respCode int
		if response != nil {
			respCode = response.StatusCode
			if response.Body != nil {
				response.Body.Close()
			}
		}
		if err == nil && respCode < http.StatusBadRequest {
			return nil
		}

		if !cmn.IsErrConnectionRefused(err) {
			return err
		}
		if time.Since(startRequest) > proxyTimeout {
			return fmt.Errorf("sending data to primary proxy timed out")
		}

		glog.Errorf("failed to http-call %s %s: error %v", method, url, err)
		time.Sleep(proxyRetryTime)
	}
}
