// Package main contains the independent authentication server for AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"

	jsoniter "github.com/json-iterator/go"
)

const (
	retryCount = 4
	retrySleep = 3 * time.Second
	retry503   = time.Minute
)

// Send request to the defined cluster to validate that the cluster will allow tokens issued by this AuthN service
func (m *mgr) validateCluster(clu *authn.CluACL) (err error) {
	var (
		tag  string
		body []byte
	)
	switch {
	case m.cm.HasHMACSecret():
		tag = "validate-secret"
		body = cos.MustMarshal(&authn.ServerConf{Secret: m.cm.GetSecretChecksum()})
	case m.rsaConfigured():
		tag = "validate-key"
		body = cos.MustMarshal(&authn.ServerConf{PubKey: apc.Ptr(m.rm.GetPublicKeyPEM())})
	default:
		return errors.New("invalid cluster configuration, no signing key configured")
	}

	for _, u := range clu.URLs {
		if err = m.call(http.MethodPost, u, apc.Tokens, body, tag); err == nil {
			return
		}
		err = fmt.Errorf("failed to %s with %s: %v", tag, clu, err)
	}
	return err
}

// update list of revoked token on all clusters
func (m *mgr) broadcastRevoked(token string) {
	tokenList := authn.TokenList{Tokens: []string{token}}
	body := cos.MustMarshal(tokenList)
	m.broadcast(http.MethodDelete, apc.Tokens, body, "broadcast-revoked")
}

// broadcast the request to all clusters. If a cluster has a few URLS,
// it sends to the first working one. Clusters are processed in parallel.
func (m *mgr) broadcast(method, path string, body []byte, tag string) {
	clus, code, err := m.clus()
	if err != nil {
		nlog.Errorf("Failed to read cluster list: %v (%d)", err, code)
		return
	}
	wg := &sync.WaitGroup{}
	for _, clu := range clus {
		wg.Add(1)
		go func(clu *authn.CluACL) {
			var err error
			for _, u := range clu.URLs {
				if err = m.call(method, u, path, body, tag); err == nil {
					break
				}
			}
			if err != nil {
				nlog.Errorf("failed to %s with %s: %v", tag, clu, err)
			}
			wg.Done()
		}(clu)
	}
	wg.Wait()
}

// Send valid and non-expired revoked token list to a cluster.
func (m *mgr) syncTokenList(ctx context.Context, clu *authn.CluACL) {
	const tag = "sync-tokens"
	tokenList, code, err := m.generateRevokedTokenList(ctx)
	if err != nil {
		nlog.Errorf("failed to sync token list with %q(%q): %v (%d)", clu.ID, clu.Alias, err, code)
		return
	}
	if len(tokenList) == 0 {
		return
	}
	body := cos.MustMarshal(authn.TokenList{Tokens: tokenList})
	for _, u := range clu.URLs {
		if err = m.call(http.MethodDelete, u, apc.Tokens, body, tag); err == nil {
			break
		}
		err = fmt.Errorf("failed to %s with %s: %v", tag, clu, err)
	}
	if err != nil {
		nlog.Errorln(err)
	}
}

// TODO: reuse api/client.go reqParams.do()
func (m *mgr) call(method, proxyURL, path string, injson []byte, tag string) error {
	var (
		rerr    error
		msg     []byte
		retries = retryCount
		sleep   = retrySleep
		url     = proxyURL + cos.JoinW0(apc.Version, path)
		client  = m.clientH
	)
	if cos.IsHTTPS(proxyURL) {
		client = m.clientTLS
	}

	// while cos.IsErrRetriableConn()
	for i := 1; i <= retries; i++ {
		req, nerr := http.NewRequestWithContext(context.Background(), method, url, bytes.NewBuffer(injson))
		if nerr != nil {
			return nerr
		}
		req.Header.Set(cos.HdrContentType, cos.ContentJSON)
		resp, err := client.Do(req)
		if resp != nil && resp.Body != nil {
			var e error
			msg, e = cos.ReadAllN(resp.Body, resp.ContentLength)
			resp.Body.Close()
			if err == nil {
				err = e
			}
		}
		if err == nil {
			if resp.StatusCode < http.StatusBadRequest {
				return nil
			}
		} else {
			if cos.IsErrRetriableConn(err) {
				continue
			}
			if resp == nil {
				return err
			}
		}
		if resp.StatusCode == http.StatusServiceUnavailable {
			if retries == retryCount {
				retries = int(retry503 / retrySleep)
			}
		} else {
			var herr *cmn.ErrHTTP
			if jsonErr := jsoniter.Unmarshal(msg, &herr); jsonErr == nil {
				return herr
			}
		}
		if i < retries {
			nlog.Warningf("failed to %q %s: %v - retrying...", tag, url, err)
			time.Sleep(sleep)
			if i > retries/2+1 && sleep == retrySleep {
				sleep *= 2
			}
		}
		rerr = err
	}
	return rerr
}
