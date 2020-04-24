// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
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
//    - A user credentials is loaded for the userID
//      AWS: credentials are loaded from INI-file in memory. File must include the following lines:
//       region = AWSREGION
//       aws_access_key_id = USERACCESSKEY
//       aws_secret_access_key = USERSECRETKEY
//      GCP: credentials from memory saved to file <config.Auth.CredDir>/<ProvideGoogle>/<UserID>.json.
//	    Then GCP session is intialized with the file content (GCP API does
//          not have a way to load credentials from memory)
// 3. If anything goes wrong: no user credentials found, invalid credentials
//    format etc then default session is created (as if AuthN is disabled)
package ais

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	jwt "github.com/dgrijalva/jwt-go"
)

type (
	// TokenList is a list of tokens pushed by authn
	TokenList struct {
		Tokens  []string `json:"tokens"`
		Version int64    `json:"version,string"`
	}

	authRec struct {
		userID  string
		issued  time.Time
		expires time.Time
		creds   cmn.SimpleKVs
		isGuest bool
	}

	authList map[string]*authRec

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

var (
	// interface guard
	_ revs = &TokenList{}
)

var guestAcc = &authRec{
	userID:  "guest",
	isGuest: true,
}

// Decrypts JWT token and returns all encrypted information.
// Used by proxy - to check a user access and token validity(e.g, expiration),
// and by target - only to get a user name for AWS/GCP access
func decryptToken(tokenStr string) (*authRec, error) {
	var (
		issueStr, expireStr string
		invalTokenErr       = fmt.Errorf("invalid token")
	)
	rec := &authRec{}
	token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}

		return []byte(cmn.GCO.Get().Auth.Secret), nil
	})
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return nil, invalTokenErr
	}
	if rec.userID, ok = claims["username"].(string); !ok {
		return nil, invalTokenErr
	}
	if issueStr, ok = claims["issued"].(string); !ok {
		return nil, invalTokenErr
	}
	if rec.issued, err = time.Parse(time.RFC822, issueStr); err != nil {
		return nil, invalTokenErr
	}
	if expireStr, ok = claims["expires"].(string); !ok {
		return nil, invalTokenErr
	}
	if rec.expires, err = time.Parse(time.RFC822, expireStr); err != nil {
		return nil, invalTokenErr
	}
	rec.creds = make(cmn.SimpleKVs, 10)
	if cc, ok := claims["creds"].(map[string]interface{}); ok {
		for key, value := range cc {
			if asStr, ok := value.(string); ok {
				rec.creds[key] = asStr
			} else {
				glog.Warningf("Value is not string: %v [%T]", value, value)
			}
		}
	} else if glog.V(4) {
		glog.Infof("Token for %s does not contain credentials", rec.userID)
	}

	return rec, nil
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
		if err == nil && rec.expires.Before(time.Now()) {
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
func (a *authManager) validateToken(token string) (ar *authRec, err error) {
	a.Lock()

	if _, ok := a.revokedTokens[token]; ok {
		ar, err = nil, fmt.Errorf("invalid token")
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
func (a *authManager) extractTokenData(token string) (*authRec, error) {
	var err error

	auth, ok := a.tokens[token]
	if !ok || auth == nil {
		if auth, err = decryptToken(token); err != nil {
			glog.Errorf("Invalid token was received: %s", token)
			return nil, fmt.Errorf("invalid token")
		}
		a.tokens[token] = auth
	}

	if auth == nil {
		return nil, fmt.Errorf("invalid token")
	}

	if auth.expires.Before(time.Now()) {
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
	b, err := jsonCompat.Marshal(t)
	cmn.AssertNoErr(err)
	return b
}
