// Package authnsrv provides AuthN server for AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package authnsrv

import (
	"bytes"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// update list of revoked token on all clusters
func (m *UserManager) broadcastRevoked(token string) {
	tokenList := authn.TokenList{Tokens: []string{token}}
	body := cos.MustMarshal(tokenList)
	m.broadcast(http.MethodDelete, apc.Tokens, body)
}

// broadcast the request to all clusters. If a cluster has a few URLS,
// it sends to the first working one. Clusters are processed in parallel.
func (m *UserManager) broadcast(method, path string, body []byte) {
	cluList, err := m.clusterList()
	if err != nil {
		glog.Errorf("Failed to read cluster list: %v", err)
		return
	}
	wg := &sync.WaitGroup{}
	for _, clu := range cluList {
		wg.Add(1)
		go func(clu *authn.CluACL) {
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

// Send valid and non-expired revoked token list to a cluster.
func (m *UserManager) syncTokenList(cluster *authn.CluACL) {
	tokenList, err := m.generateRevokedTokenList()
	if err != nil {
		glog.Errorf("failed to sync token list with %q: %v", cluster.ID, err)
		return
	}
	if len(tokenList) == 0 {
		return
	}
	body := cos.MustMarshal(authn.TokenList{Tokens: tokenList})
	for _, u := range cluster.URLs {
		if err = m.proxyRequest(http.MethodDelete, u, apc.Tokens, body); err == nil {
			break
		}
		err = fmt.Errorf("failed to sync revoked tokens with %q: %v", cluster.ID, err)
	}
	if err != nil {
		glog.Error(err)
	}
}

// Generic function to send everything to a proxy
func (m *UserManager) proxyRequest(method, proxyURL, path string, injson []byte) error {
	startRequest := time.Now()
	for {
		url := proxyURL + cos.JoinWords(apc.Version, path)
		request, err := http.NewRequest(method, url, bytes.NewBuffer(injson))
		if err != nil {
			return err
		}

		client := m.clientHTTP
		if cos.IsHTTPS(proxyURL) {
			client = m.clientHTTPS
		}
		request.Header.Set(cmn.HdrContentType, cmn.ContentJSON)
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

		if !cos.IsRetriableConnErr(err) {
			return err
		}
		if time.Since(startRequest) > proxyTimeout {
			return fmt.Errorf("timed out sending data to primary at %s", proxyURL)
		}

		glog.Warningf("failed to execute \"%s %s\": %v", method, url, err)
		time.Sleep(proxyRetryTime)
	}
}
