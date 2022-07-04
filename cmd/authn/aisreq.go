// Package authn provides AuthN server for AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
)

const (
	retries   = 4
	retryTime = 3 * time.Second
)

func (m *mgr) validateSecret(clu *authn.CluACL) (err error) {
	var (
		secret = Conf.Secret()
		cksum  = cos.NewCksumHash(cos.ChecksumSHA256)
	)
	cksum.H.Write([]byte(secret))
	cksum.Finalize()

	body := cos.MustMarshal(&authn.ServerConf{Secret: cksum.Val()})
	for _, u := range clu.URLs {
		if err = m.call(http.MethodPost, u, apc.Tokens, body); err == nil {
			return
		}
		err = fmt.Errorf("failed to validate secret with %q(%q): %v", clu.ID, clu.Alias, err)
	}
	return
}

// update list of revoked token on all clusters
func (m *mgr) broadcastRevoked(token string) {
	tokenList := authn.TokenList{Tokens: []string{token}}
	body := cos.MustMarshal(tokenList)
	m.broadcast(http.MethodDelete, apc.Tokens, body)
}

// broadcast the request to all clusters. If a cluster has a few URLS,
// it sends to the first working one. Clusters are processed in parallel.
func (m *mgr) broadcast(method, path string, body []byte) {
	clus, err := m.clus()
	if err != nil {
		glog.Errorf("Failed to read cluster list: %v", err)
		return
	}
	wg := &sync.WaitGroup{}
	for _, clu := range clus {
		wg.Add(1)
		go func(clu *authn.CluACL) {
			defer wg.Done()
			var err error
			for _, u := range clu.URLs {
				if err = m.call(method, u, path, body); err == nil {
					break
				}
			}
			if err != nil {
				glog.Errorf("failed to sync revoked tokens with %q(%q): %v", clu.ID, clu.Alias, err)
			}
		}(clu)
	}
	wg.Wait()
}

// Send valid and non-expired revoked token list to a cluster.
func (m *mgr) syncTokenList(clu *authn.CluACL) {
	tokenList, err := m.generateRevokedTokenList()
	if err != nil {
		glog.Errorf("failed to sync token list with %q(%q): %v", clu.ID, clu.Alias, err)
		return
	}
	if len(tokenList) == 0 {
		return
	}
	body := cos.MustMarshal(authn.TokenList{Tokens: tokenList})
	for _, u := range clu.URLs {
		if err = m.call(http.MethodDelete, u, apc.Tokens, body); err == nil {
			break
		}
		err = fmt.Errorf("failed to sync revoked tokens with %q(%q): %v", clu.ID, clu.Alias, err)
	}
	if err != nil {
		glog.Error(err)
	}
}

func (m *mgr) call(method, proxyURL, path string, injson []byte) error {
	var (
		rerr     error
		client   = m.clientHTTP
		url      = proxyURL + cos.JoinWords(apc.Version, path)
		req, err = http.NewRequest(method, url, bytes.NewBuffer(injson))
	)
	if err != nil {
		return err
	}
	if cos.IsHTTPS(proxyURL) {
		client = m.clientHTTPS
	}
	req.Header.Set(cos.HdrContentType, cos.ContentJSON)

	// while cos.IsRetriableConnErr()
	for i := 1; i <= retries; i++ {
		var msg []byte
		resp, err := client.Do(req)
		if resp != nil {
			if resp.Body != nil {
				msg, _ = io.ReadAll(resp.Body)
				resp.Body.Close()
			}
		}
		if err == nil && resp.StatusCode < http.StatusBadRequest {
			return nil
		}
		if err != nil && cos.IsRetriableConnErr(err) {
			continue
		}
		if resp.StatusCode != http.StatusServiceUnavailable {
			var httpErr *cmn.ErrHTTP
			if jsonErr := jsoniter.Unmarshal(msg, &httpErr); jsonErr == nil {
				return httpErr
			}
		}
		rerr = fmt.Errorf("failed to execute \"%s %s\": %v", method, url, err)
		if i < retries {
			glog.Warningf("%v - retrying...", rerr)
			time.Sleep(retryTime)
		}
	}
	return rerr
}
