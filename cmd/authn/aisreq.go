// Package main contains the independent authentication server for AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

const (
	retrySleep     = 3 * time.Second
	softErrTimeout = time.Minute
)

// Send request to the defined cluster to validate that the cluster will allow tokens issued by this AuthN service
func (m *mgr) validateCluster(ctx context.Context, clu *authn.CluACL) (code int, err error) {
	conf := m.getSigner().ValidationConf()
	body := cos.MustMarshal(conf)
	tag := "validate-signer"

	for _, u := range clu.URLs {
		if code, err = m.call(ctx, http.MethodPost, u, apc.Tokens, body, tag); err == nil {
			return
		}
		err = fmt.Errorf("failed to %s with %s: %v", tag, clu, err)
	}
	return
}

// update list of revoked tokens on all clusters
func (m *mgr) broadcastRevoked(ctx context.Context, token string) {
	tokenList := authn.TokenList{Tokens: []string{token}}
	body := cos.MustMarshal(tokenList)
	m.broadcast(ctx, http.MethodDelete, apc.Tokens, body, "broadcast-revoked")
}

// broadcast the request to all clusters. If a cluster has a few URLS,
// it sends to the first working one. Clusters are processed in parallel.
func (m *mgr) broadcast(ctx context.Context, method, path string, body []byte, tag string) {
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
				if _, err = m.call(ctx, method, u, path, body, tag); err == nil {
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
func (m *mgr) syncRevokedTokens(ctx context.Context, clu *authn.CluACL) (int, error) {
	const tag = "sync-tokens"
	tokenList, code, err := m.generateRevokedTokenList(ctx)
	if err != nil {
		return code, fmt.Errorf("failed to sync token list with %q(%q): %v (%d)", clu.ID, clu.Alias, err, code)
	}
	if len(tokenList) == 0 {
		return code, nil
	}
	body := cos.MustMarshal(authn.TokenList{Tokens: tokenList})
	for _, u := range clu.URLs {
		code, err = m.call(ctx, http.MethodDelete, u, apc.Tokens, body, tag)
		if err == nil {
			return code, nil
		}
		err = fmt.Errorf("failed to %s with %s: %v", tag, clu, err)
		nlog.Errorln(err)
	}
	return code, err
}

func (m *mgr) call(ctx context.Context, method, proxyURL, path string, injson []byte, tag string) (int, error) {
	client := m.clientH
	if cos.IsHTTPS(proxyURL) {
		client = m.clientTLS
	}
	versionedPath := cos.JoinW0(apc.Version, path)
	urlPath := proxyURL + versionedPath

	var resp *http.Response // (used in closure)

	cleanupResp := func() {
		if resp != nil && resp.Body != nil {
			cos.DrainReader(resp.Body)
			resp.Body.Close()
		}
		resp = nil
	}
	args := cmn.RetryArgs{
		Call: func() (int, error) {
			req, err := http.NewRequestWithContext(ctx, method, urlPath, bytes.NewReader(injson))
			if err != nil {
				return 0, err
			}
			cleanupResp() // (cleanup prev. response)

			req.Header.Set(cos.HdrContentType, cos.ContentJSON)
			resp, err = client.Do(req) //nolint:bodyclose // closed after args.Do() returns

			if resp == nil {
				return 0, err
			}
			if err == nil && resp.StatusCode == http.StatusServiceUnavailable {
				return resp.StatusCode, cos.NewRetriableSoftFromStatus(resp.StatusCode)
			}
			return resp.StatusCode, err
		},
		SoftErr:   int(softErrTimeout / retrySleep),
		Sleep:     retrySleep,
		BackOff:   true,
		IsClient:  true,
		Verbosity: cmn.RetryLogVerbose,
		Action:    tag,
	}
	code, err := args.Do()
	if err == nil && resp != nil {
		err = cmn.CheckResp(resp, method, versionedPath)
	}
	cleanupResp()

	return code, err
}
