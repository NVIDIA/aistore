// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
)

type (
	tokenList authn.TokenList           // token strings
	tokenMap  map[string]*tok.AISClaims // map token strings to claim structs

	authManager struct {
		// used for parsing and validating claims from token strings
		tokenParser *tok.TokenParser
		// cache of decrypted token claims
		tokenMap tokenMap
		// list of invalid tokens(revoked or of deleted users)
		// Authn sends these tokens to primary for broadcasting
		revokedTokens map[string]bool
		// latest revoked version
		version int64
		// lock
		sync.Mutex
	}
)

/////////////////
// authManager //
/////////////////

func newAuthManager(config *cmn.Config) *authManager {
	rsaPubKey, err := tok.ParsePubKey(cos.Right(config.Auth.PubKey, os.Getenv(env.AisAuthPublicKey)))
	if err != nil {
		cos.ExitLogf("Failed to parse RSA public key: %v", err)
	}
	secret := cos.Right(config.Auth.Secret, os.Getenv(env.AisAuthSecretKey))
	requiredClaims := &tok.RequiredClaims{
		Aud: config.Auth.Aud,
	}
	return &authManager{
		tokenMap:      make(tokenMap),
		revokedTokens: make(map[string]bool), // TODO: preallocate
		version:       1,
		tokenParser:   tok.NewTokenParser(secret, rsaPubKey, requiredClaims),
	}
}

// Add tokens to the list of invalid ones and clean up the list from expired tokens.
func (a *authManager) updateRevokedList(newRevoked *tokenList) (allRevoked *tokenList) {
	a.Lock()
	defer a.Unlock()

	switch {
	case newRevoked.Version == 0: // Manually revoked tokens
		a.version++
	case newRevoked.Version > a.version:
		a.version = newRevoked.Version
	default:
		nlog.Errorf("Current token list v%d is greater than received v%d", a.version, newRevoked.Version)
		return nil
	}

	// Add new revoked tokens and remove them from the valid token list.
	for _, token := range newRevoked.Tokens {
		a.revokedTokens[token] = true
		delete(a.tokenMap, token)
	}

	allRevoked = &tokenList{
		Tokens:  make([]string, 0, len(a.revokedTokens)),
		Version: a.version,
	}

	// Clean up expired tokens from the revoked list.
	for token := range a.revokedTokens {
		_, err := a.tokenParser.ValidateToken(token)
		switch {
		case errors.Is(err, tok.ErrTokenExpired):
			delete(a.revokedTokens, token)
		case err == nil:
			allRevoked.Tokens = append(allRevoked.Tokens, token)
		default:
			nlog.Errorf("Unexpected token validation error: %v (token: %s)", err, token)
		}
	}
	if len(allRevoked.Tokens) == 0 {
		allRevoked = nil
	}
	return allRevoked
}

func (a *authManager) revokedTokenList() (allRevoked *tokenList) {
	a.Lock()
	l := len(a.revokedTokens)
	if l == 0 {
		a.Unlock()
		return
	}
	allRevoked = &tokenList{Tokens: make([]string, 0, l), Version: a.version}
	for token := range a.revokedTokens {
		allRevoked.Tokens = append(allRevoked.Tokens, token)
	}
	a.Unlock()
	return
}

// Checks if a token is valid:
//   - must not be revoked one
//   - must not be expired
//   - must have valid JWT claims or equivalent: sub, iss, aud, exp
//
// Caches and returns decoded token claims if it is valid
func (a *authManager) validateToken(token string) (*tok.AISClaims, error) {
	a.Lock()
	defer a.Unlock()
	if _, ok := a.revokedTokens[token]; ok {
		return nil, fmt.Errorf("%w [err: %w]: %s", tok.ErrInvalidToken, tok.ErrTokenRevoked, token)
	}
	claims, ok := a.tokenMap[token]
	debug.Assert(!ok || claims != nil)
	// If token string exists in cache, only need to check claim expiry
	if ok && claims != nil {
		if claims.IsExpired() {
			delete(a.tokenMap, token)
			return nil, fmt.Errorf("%w [err: %w]: %s", tok.ErrInvalidToken, tok.ErrTokenExpired, token)
		}
		return claims, nil
	}
	return a.cacheNewToken(token)
}

// Take a token string, validate the signature and claims, and add to the authManager cache
func (a *authManager) cacheNewToken(token string) (claims *tok.AISClaims, err error) {
	// Validate token signature and extract
	claims, err = a.tokenParser.ValidateToken(token)
	if err != nil {
		nlog.Infof("Received invalid token, error: %v", err)
		return nil, err
	}
	a.tokenMap[token] = claims
	return claims, nil
}

///////////////
// tokenList //
///////////////

// interface guard
var _ revs = (*tokenList)(nil)

func (*tokenList) tag() string         { return revsTokenTag }
func (t *tokenList) version() int64    { return t.Version } // no versioning: receivers keep adding tokens to their lists
func (*tokenList) uuid() string        { return "" }        // TODO: add
func (t *tokenList) marshal() []byte   { return cos.MustMarshal(t) }
func (t *tokenList) jit(_ *proxy) revs { return t }
func (*tokenList) sgl() *memsys.SGL    { return nil }
func (t *tokenList) String() string    { return fmt.Sprintf("TokenList v%d", t.Version) }

//
// proxy cont-ed
//

// [METHOD] /v1/tokens
func (p *proxy) tokenHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		p.validateKey(w, r)
	case http.MethodDelete:
		p.delToken(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodPost, http.MethodDelete)
	}
}

// Given a secret key or a public key, validate if that key is valid for requests to this cluster
func (p *proxy) validateKey(w http.ResponseWriter, r *http.Request) {
	if _, err := p.parseURL(w, r, apc.URLPathTokens.L, 0, false); err != nil {
		return
	}

	reqConf := &authn.ServerConf{}
	if err := cmn.ReadJSON(w, r, reqConf); err != nil {
		return
	}

	if reqConf.Secret == "" && reqConf.PubKey == nil {
		p.writeErrf(w, r, "no secret or public key provided to validate")
		return
	}
	if reqConf.Secret != "" {
		if !p.authn.tokenParser.IsSecretCksumValid(reqConf.Secret) {
			p.writeErrf(w, r, "%s: invalid secret sha256(%q)", p, cos.SHead(reqConf.Secret))
		}
	}
	if reqConf.PubKey != nil {
		valid, err := p.authn.tokenParser.IsPublicKeyValid(*reqConf.PubKey)
		if err != nil {
			p.writeErrf(w, r, "%s: invalid public key (%q)", p, cos.SHead(*reqConf.PubKey))
			return
		}
		if !valid {
			p.writeErrf(w, r, "%s: provided public key (%q) does not match cluster's public key", p, cos.SHead(*reqConf.PubKey))
			return
		}
	}
}

func (p *proxy) delToken(w http.ResponseWriter, r *http.Request) {
	if _, err := p.parseURL(w, r, apc.URLPathTokens.L, 0, false); err != nil {
		return
	}
	if p.forwardCP(w, r, nil, "revoke token") {
		return
	}
	tokenList := &tokenList{}
	if err := cmn.ReadJSON(w, r, tokenList); err != nil {
		return
	}
	allRevoked := p.authn.updateRevokedList(tokenList)
	if allRevoked != nil && p.owner.smap.get().isPrimary(p.si) {
		msg := p.newAmsgStr(apc.ActNewPrimary, nil)
		_ = p.metasyncer.sync(revsPair{allRevoked, msg})
	}
}

// Validates a token from the request header
func (p *proxy) validateToken(hdr http.Header) (*tok.AISClaims, error) {
	token, err := tok.ExtractToken(hdr)
	if err != nil {
		return nil, err
	}
	claims, err := p.authn.validateToken(token)
	if err != nil {
		nlog.Errorf("invalid token: %v", err)
		return nil, err
	}
	return claims, nil
}

// When AuthN is on, accessing a bucket requires two permissions:
//   - access to the bucket is granted to a user
//   - bucket ACL allows the required operation
//     Exception: a superuser can always PATCH the bucket/Set ACL
//
// If AuthN is off, only bucket permissions are checked.
//
//	Exceptions:
//	- read-only access to a bucket is always granted
//	- PATCH cannot be forbidden
func (p *proxy) checkAccess(w http.ResponseWriter, r *http.Request, bck *meta.Bck, ace apc.AccessAttrs) (err error) {
	if err = p.access(r.Header, bck, ace); err != nil {
		p.writeErr(w, r, err, aceErrToCode(err))
	}
	return
}

func aceErrToCode(err error) (status int) {
	switch {
	case err == nil:
	case errors.Is(err, tok.ErrNoToken) || errors.Is(err, tok.ErrInvalidToken):
		status = http.StatusUnauthorized
	default:
		status = http.StatusForbidden
	}
	return status
}

func (p *proxy) access(hdr http.Header, bck *meta.Bck, ace apc.AccessAttrs) (err error) {
	var (
		claims *tok.AISClaims
		bucket *cmn.Bck
	)
	if p.checkIntraCall(hdr, false /*from primary*/) == nil {
		return nil
	}
	if cmn.Rom.AuthEnabled() { // config.Auth.Enabled
		claims, err = p.validateToken(hdr)
		if err != nil {
			// NOTE: making exception to allow 3rd party clients read remote ht://bucket
			if errors.Is(err, tok.ErrNoToken) && bck != nil && bck.IsHT() {
				err = nil
			}
			return err
		}
		uid := p.owner.smap.Get().UUID
		if bck != nil {
			bucket = bck.Bucket()
		}
		if err = claims.CheckPermissions(uid, bucket, ace); err != nil {
			return err
		}
	}
	if bck == nil {
		// cluster ACL: create/list buckets, node management, etc.
		return nil
	}

	// bucket access conventions:
	// - without AuthN: read-only access, PATCH, and ACL
	// - with AuthN:    superuser can PATCH and change ACL
	if !cmn.Rom.AuthEnabled() {
		ace &^= (apc.AcePATCH | apc.AceBckSetACL | apc.AccessRO)
	} else if claims != nil && claims.IsAdmin {
		ace &^= (apc.AcePATCH | apc.AceBckSetACL)
	}
	if ace == 0 {
		return nil
	}
	return bck.Allow(ace)
}
