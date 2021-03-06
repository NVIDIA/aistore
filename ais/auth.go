// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package ais provides core functionality for the AIStore object storage.
// Authentication flow:
// 1. If AuthN server is disabled or directory with user credentials is not set:
//    Token in HTTP request header is ignored.
//    All user credentials are read from default files and environment variables.
//    AWS: file ~/.aws/credentials
//    GCP: file ~/gcp_creds.json and GOOGLE_CLOUD_PROJECT variable
//         a user should have logged at least once to GCP before running any AIStore operation
// 2. AuthN server is enabled and everything is set up
//    - AIS reads userID from HTTP request header: 'Authorization: Bearer <token>'.
package ais

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

type (
	// TokenList is a list of tokens pushed by authn
	TokenList struct {
		Tokens  []string `json:"tokens"`
		Version int64    `json:"version,string"`
	}

	authList map[string]*cmn.AuthToken

	authManager struct {
		sync.Mutex
		// cache of decrypted tokens
		tokens authList
		// list of invalid tokens(revoked or of deleted users)
		// Authn sends these tokens to primary for broadcasting
		revokedTokens map[string]bool
		version       int64
	}
)

// interface guard
var _ revs = (*TokenList)(nil)

// Decrypts JWT token and returns all encrypted information.
// Used by proxy and by AuthN.
func decryptToken(tokenStr string) (*cmn.AuthToken, error) {
	secret := cmn.GCO.Get().Auth.Secret
	return cmn.DecryptToken(tokenStr, secret)
}

// Add tokens to list of invalid ones. After that it cleans up the list
// from expired tokens
func (a *authManager) updateRevokedList(tokens *TokenList) {
	if tokens == nil {
		return
	}

	a.Lock()
	if tokens.Version == 0 {
		// a user manually revoked a token
		a.version++
	} else if tokens.Version > a.version {
		a.version = tokens.Version
	} else {
		glog.Errorf("Current token list v%d is greater than received v%d",
			a.version, tokens.Version)
		a.Unlock()
		return
	}

	for _, token := range tokens.Tokens {
		a.revokedTokens[token] = true
		delete(a.tokens, token)
	}
	// clean up the list from obsolete data
	for token := range a.revokedTokens {
		rec, err := a.extractTokenData(token)
		if err == nil && rec.Expires.Before(time.Now()) {
			delete(a.revokedTokens, token)
		}
	}
	a.Unlock()
}

// Checks if a token is valid:
//   - must not be revoked one
//   - must not be expired
//   - must have all mandatory fields: userID, creds, issued, expires
// Returns decrypted token information if it is valid
func (a *authManager) validateToken(token string) (ar *cmn.AuthToken, err error) {
	a.Lock()

	if _, ok := a.revokedTokens[token]; ok {
		ar, err = nil, cmn.ErrInvalidToken
		a.Unlock()
		return
	}

	ar, err = a.extractTokenData(token)
	a.Unlock()
	return
}

// Decrypts token and returns information about a user for whom the token
// was issued. Return error is the token expired or does not include all
// mandatory fields
// It is internal service function, so it does not lock anything
func (a *authManager) extractTokenData(token string) (*cmn.AuthToken, error) {
	var err error

	auth, ok := a.tokens[token]
	if !ok || auth == nil {
		if auth, err = decryptToken(token); err != nil {
			glog.Errorf("Invalid token was received: %s", token)
			return nil, cmn.ErrInvalidToken
		}
		a.tokens[token] = auth
	}

	if auth == nil {
		return nil, cmn.ErrInvalidToken
	}

	if auth.Expires.Before(time.Now()) {
		glog.Errorf("Expired token was used: %s", token)
		delete(a.tokens, token)
		return nil, fmt.Errorf("token expired")
	}

	return auth, nil
}

func (a *authManager) revokedTokenList() *TokenList {
	a.Lock()
	tlist := &TokenList{
		Tokens:  make([]string, len(a.revokedTokens)),
		Version: a.version,
	}

	idx := 0
	for token := range a.revokedTokens {
		tlist.Tokens[idx] = token
		idx++
	}

	a.Unlock()
	return tlist
}

//
// Implementation of revs interface
//
// Token list doesn't need versioning: receivers keep adding received tokens to their internal lists
//
func (t *TokenList) tag() string    { return revsTokenTag }
func (t *TokenList) version() int64 { return t.Version }
func (t *TokenList) marshal() []byte {
	return cos.MustMarshal(t)
}
