// Package main - authorization server for AIStore. See README.md for more info.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
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
	conf.Cluster.mtx.RLock()
	if len(conf.Cluster.Conf) == 0 {
		conf.Cluster.mtx.RUnlock()
		glog.Warning("primary proxy is not defined")
		return
	}
	conf.Cluster.mtx.RUnlock()

	tokenList := ais.TokenList{Tokens: []string{token}}
	body := cmn.MustMarshal(tokenList)

	m.broadcast(http.MethodDelete, cmn.Tokens, body)
}

// broadcast the request to all clusters. If a cluster has a few URLS,
// it sends to the first working one. Clusters are processed in parallel.
func (m *userManager) broadcast(method, path string, body []byte) {
	conf.Cluster.mtx.RLock()
	defer conf.Cluster.mtx.RUnlock()
	wg := &sync.WaitGroup{}
	for cid, urls := range conf.Cluster.Conf {
		wg.Add(1)
		go func(cid string, urls []string) {
			defer wg.Done()
			var err error
			for _, u := range urls {
				if err = m.proxyRequest(method, u, path, body); err == nil {
					break
				}
			}
			if err != nil {
				glog.Errorf("Failed to sync revoked tokens with %q: %v", cid, err)
			}
		}(cid, urls)
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
		request.Header.Set("Content-Type", "application/json")
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
